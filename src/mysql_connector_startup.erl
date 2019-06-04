%%%-------------------------------------------------------------------
%%% @doc
%%% mysql connection startup
%%% @end
%%%-------------------------------------------------------------------
-module(mysql_connector_startup).
%% API functions
-export([connect/1]).
%% Includes
-include("mysql_connector.hrl").
%%%===================================================================
%%% API functions
%%%===================================================================
%% @doc connect
-spec connect(Args :: term()) -> {ok, State :: #state{}, Timeout :: timeout()}.
connect(Args) ->
    %% args
    Host     = proplists:get_value(host,     Args, "localhost"),
    Port     = proplists:get_value(port,     Args, 3306),
    User     = proplists:get_value(user,     Args, ""),
    Password = proplists:get_value(password, Args, ""),
    Database = proplists:get_value(database, Args, ""),
    Encoding = proplists:get_value(encoding, Args, ""),
    %% connect
    case gen_tcp:connect(Host, Port, [{mode, binary}, {packet, 0}, {buffer, 65536}, {active, false}]) of
        {ok, Socket} ->
            %% login
            State = #state{socket_type = gen_tcp, socket = Socket},
            NewState = decode_handshake(State, User, Password, Database, <<0:8>>, <<>>),
            %% change database
            ChangeDatabaseResult = mysql_connector_executor:change_database(State, Database),
            not is_record(ChangeDatabaseResult, ok) andalso erlang:error(ChangeDatabaseResult),
            %% set encoding
            SetEncodingResult = mysql_connector_executor:set_encoding(State, Encoding),
            not is_record(SetEncodingResult, ok) andalso erlang:error(SetEncodingResult),
            %% succeeded
            {ok, NewState, 60 * 1000};
        {error, Reason} ->
            erlang:error(Reason)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
%% get verify greeting data
decode_handshake(State, User, Password, Database, Number = <<Seq:8>>, <<Length:24/little, Number:1/binary-unit:8, Packet:Length/binary-unit:8, Rest/binary>>) ->
    case Packet of
        <<10:8, PacketRest/binary>> ->
            %% Protocol version 10.
            VersionLength = decode_c_string(PacketRest, 0),
            <<Version:VersionLength/binary-unit:8, 0, Rest1/binary>> = PacketRest,
            <<ConnectionId:32/little, Salt1:8/binary-unit:8, 0:8, CapabilitiesLower:16/little, CharSet:8, Status:16/little, CapabilitiesUpper:16/little, _SaltLength:8, _Reserved:10/binary-unit:8, Rest2/binary>> = Rest1,
            Capabilities = CapabilitiesLower + 16#10000 * CapabilitiesUpper,
            %% lower half part salt
            Salt2Length = decode_c_string(Rest2, 0),
            <<Salt2:Salt2Length/binary-unit:8, 0, Rest3/binary>> = Rest2,
            %% plugin name
            %% MySQL server 5.5.8 has a bug where end byte is not send
            PluginLength = decode_c_string(Rest3, 0),
            <<Plugin:PluginLength/binary-unit:8, 0, _/binary>> = Rest3,
            Handshake = #handshake{version = Version, id = ConnectionId, capabilities = Capabilities, charset = CharSet, status = Status, salt = <<Salt1/binary, Salt2/binary>>, plugin = Plugin},
            switch_to_ssl(State, Handshake, User, Password, Database, <<(Seq + 1):8>>, Rest);
        <<?ERROR:8, Code:16, _/binary>> ->
            erlang:error({handshake_error, Code});
        <<Protocol:8, _/binary>> ->
            erlang:error({unsupported_handshake_protocol, Protocol})
    end;
decode_handshake(_, _, _, _, <<Number:8>>, <<Length:24/little, SequenceNumber:8, _:Length/binary-unit:8, _/binary>>) ->
    %% chaos sequence number
    erlang:error(<<"Got packets out of order (self): ", (integer_to_binary(Number))/binary, " (server): ", (integer_to_binary(SequenceNumber))/binary>>);
decode_handshake(State, User, Password, Database, Number, Data) ->
    InData = receive_data(State),
    decode_handshake(State, User, Password, Database, Number, <<Data/binary, InData/binary>>).

%% switch to ssl
switch_to_ssl(State = #state{socket = Socket}, Handshake = #handshake{capabilities = Capabilities, charset = Charset}, User, Password, Database, Number = <<Seq:8>>, <<Rest/binary>>) ->
    BaseFlag = ?CLIENT_LONG_PASSWORD bor ?CLIENT_LONG_FLAG bor ?CLIENT_CONNECT_WITH_DB bor ?CLIENT_PROTOCOL_41 bor ?CLIENT_TRANSACTIONS bor ?CLIENT_RESERVED_2 bor ?CLIENT_MULTI_STATEMENTS bor ?CLIENT_MULTI_RESULTS,
    Flag = plugin_support(Capabilities, ssl_support(Capabilities, BaseFlag)),
    case Capabilities band ?CLIENT_SSL =/= 0 of
        true ->
            %% switch to ssl handshake
            Binary = <<Flag:32/little, ?CLIENT_SSL_VERIFY_SERVER_CERT:32/little, Charset:8, 0:23/unit:8>>,
            send_packet(State, Binary, Number),
            %% start ssl application
            ssl:start(),
            %% force wrap gen_tcp socket success
            {ok, SSLSocket} = ssl:connect(Socket, [{verify, verify_none}, {versions, proplists:get_value(available, ssl:versions(), [])}]),
            NewState = State#state{socket_type = ssl, socket = SSLSocket},
            HandshakePacket = encode_handshake(Handshake, User, Password, Database, Flag),
            send_packet(NewState, HandshakePacket, <<(Seq + 1):8>>),
            verify(NewState, Handshake, Password, <<(Seq + 2):8>>, Rest);
        false ->
            HandshakePacket = encode_handshake(Handshake, User, Password, Database, Flag),
            send_packet(State, HandshakePacket, Number),
            verify(State, Handshake, Password, <<(Seq + 1):8>>, Rest)
    end.

%% new authentication method mysql_native_password support mysql 5.x or later
encode_handshake(#handshake{charset = Charset, salt = Salt, plugin = Plugin}, User, Password, Database, Flag) ->
    %% user name
    UserBinary = <<(unicode:characters_to_binary(User))/binary, 0:8>>,
    %% database
    DatabaseBinary = <<(unicode:characters_to_binary(Database))/binary, 0:8>>,
    %% authentication plugin
    PluginBinary = <<(unicode:characters_to_binary(Plugin))/binary, 0:8>>,
    %% password encrypt
    PasswordBinary = encrypt_password(Password, Salt, Plugin),
    <<Flag:32/little, ?CLIENT_SSL_VERIFY_SERVER_CERT:32/little, Charset:8, 0:23/unit:8, UserBinary/binary, PasswordBinary/binary, DatabaseBinary/binary, PluginBinary/binary>>.

%% password authentication plugin
%% construct password hash digest
%% https://dev.mysql.com/doc/internals/en/secure-password-authentication.html
encrypt_password([], _, <<"mysql_native_password">>) ->
    encode_string(<<>>);
encrypt_password([], _, <<"caching_sha2_password">>) ->
    encode_string(<<>>);
encrypt_password(Password, Salt, <<"mysql_native_password">>) ->
    %% MySQL 4.1 - 5.x default plugin
    Hash = <<HashBinary:160>> = crypto:hash(sha, unicode:characters_to_binary(Password)),
    DoubleHash = crypto:hash(sha, Hash),
    <<FinalBinary:160>> = crypto:hash_final(crypto:hash_update(crypto:hash_update(crypto:hash_init(sha), Salt), DoubleHash)),
    %% hash length 8 bit
    encode_string(<<(HashBinary bxor FinalBinary):160>>);
encrypt_password(Password, Salt, <<"caching_sha2_password">>) ->
    %% MySQL 8.x or later default plugin
    Hash = <<HashBinary:256>> = crypto:hash(sha256, unicode:characters_to_binary(Password)),
    DoubleHash = crypto:hash(sha256, Hash),
    <<FinalBinary:256>> = crypto:hash_final(crypto:hash_update(crypto:hash_init(sha256), <<DoubleHash/binary, Salt/binary>>)),
    %% hash length 8 bit
    encode_string(<<(HashBinary bxor FinalBinary):256>>);
encrypt_password(_, _, PluginName) ->
    %% unsupported plugin
    erlang:error({unsupported_plugin, PluginName}).

%% capabilities flag support ssl
ssl_support(Capabilities, Basic) ->
    flag_support(Capabilities, Basic, ?CLIENT_SSL).

%% capabilities flag support plugin auth
plugin_support(Capabilities, Basic) ->
    flag_support(Capabilities, Basic, ?CLIENT_PLUGIN_AUTH).

flag_support(Capabilities, Basic, Flag) ->
    case Capabilities band Flag =/= 0 of
        true ->
            Basic bor Flag;
        false ->
            Basic
    end.

%% login verify
verify(State, Handshake, Password, Number = <<Seq:8>>, <<Length:24/little, Number:1/binary-unit:8, Packet:Length/binary-unit:8, Rest/binary>>) ->
    case Packet of
        <<?OK:8, _Rest/binary>> ->
            %% New auth success
            %% {AffectedRows, Rest1} = decode_packet(Rest),
            %% {InsertId, Rest2} = decode_packet(Rest1),
            %% <<StatusFlags:16/little, WarningsNumber:16/little, Msg/binary>> = Rest2,
            %% check status, ignoring bit 16#4000, SERVER_SESSION_STATE_CHANGED
            %% and bit 16#0002, SERVER_STATUS_AUTOCOMMIT.
            State;
        <<?EOF:8>> ->
            %% Old Authentication Method Switch Request Packet consisting of a
            %% single 0xfe byte. It is sent by server to request client to
            %% switch to Old Password Authentication if CLIENT_PLUGIN_AUTH
            %% capability is not supported (by either the client or the server)
            %% MySQL 4.0 or earlier old auth already unsupported
            erlang:error(unsupported_authentication_method);
        <<?EOF:8, SwitchData/binary>> ->
            %% Authentication Method Switch Request Packet. If both server and
            %% client support CLIENT_PLUGIN_AUTH capability, server can send
            %% this packet to ask client to use another authentication method.
            PluginLength = decode_c_string(SwitchData, 0),
            <<Plugin:PluginLength/binary-unit:8, 0, Salt/binary>> = SwitchData,
            Binary = encrypt_password(Password, Salt, Plugin),
            send_packet(State, Binary, <<(Seq + 1):8>>),
            verify(State, Handshake#handshake{plugin = Plugin}, Password, <<(Seq + 2):8>>, Rest);
        <<1:8, 3:8, _/binary>> ->
            %% Authentication password confirm do not need
            verify(State, Handshake, Password, <<(Seq + 1):8>>, Rest);
        <<1:8, 4:8, _/binary>> ->
            %% Authentication password confirm full
            Binary = <<(unicode:characters_to_binary(Password))/binary, 0:8>>,
            send_packet(State, Binary, <<(Seq + 1):8>>),
            verify(State, Handshake, Password, <<(Seq + 2):8>>, Rest);
        <<?ERROR:8, PacketRest/binary>> ->
            %% verify failed, user or password invalid
            erlang:error(decode_error_packet(PacketRest));
        _ ->
            %% unknown packet
            erlang:error({unknown_packet, Packet})
    end;
verify(_, _, _, <<Number:8>>, <<Length:24/little, SequenceNumber:8, _:Length/binary-unit:8, _/binary>>) ->
    %% chaos sequence number
    erlang:error(<<"Got packets out of order (self): ", (integer_to_binary(Number))/binary, " (server): ", (integer_to_binary(SequenceNumber))/binary>>);
verify(State, Handshake, Password, Number, Data) ->
    InData = receive_data(State),
    verify(State, Handshake, Password, Number, <<Data/binary, InData/binary>>).

%%%===================================================================
%%% decode packet part
%%%===================================================================
%% decode error packet
decode_error_packet(<<Code:16/little, Status:6/binary-unit:8, Message/binary>>) ->
    #error{code = Code, status = Status, message = Message}.

%% length-encode-integer
encode_integer(Value) when Value < 16#FB ->
    <<Value>>;
encode_integer(Value) when Value =< 16#FFFF ->
    <<16#FC:8, Value:16/little>>;
encode_integer(Value) when Value =< 16#FFFFFF ->
    <<16#FD:8, Value:24/little>>;
encode_integer(Value) when Value =< 16#FFFFFFFFFFFFFFFF ->
    <<16#FE:8, Value:64/little>>.

%% length-encode-string
encode_string(Value) ->
    <<(encode_integer(byte_size(Value)))/binary, Value/binary>>.

%% c lang string ends with \0
decode_c_string(<<0, _/binary>>, Length) ->
    Length;
decode_c_string(<<_, Rest/binary>>, Length) ->
    decode_c_string(Rest, Length + 1).

%%%===================================================================
%%% io part
%%%===================================================================

%% send packet with sequence number
send_packet(#state{socket_type = SocketType, socket = Socket}, Packet, Number) ->
    SocketType:send(Socket, <<(byte_size(Packet)):24/little, Number/binary, Packet/binary>>).

receive_data(#state{socket_type = SocketType, socket = Socket}) ->
    %% not completed packet, receive continue
    case SocketType:recv(Socket, 0, infinity) of
        {ok, InData} ->
            InData;
        {error, Reason} ->
            erlang:error(Reason)
    end.
