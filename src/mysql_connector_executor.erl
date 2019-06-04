%%%-------------------------------------------------------------------
%%% @doc
%%% mysql sql executor
%%% @end
%%%-------------------------------------------------------------------
-module(mysql_connector_executor).
%% API functions
-export([change_database/2]).
-export([set_encoding/2]).
-export([ping/1]).
-export([quit/1]).
-export([execute/2]).
%% Includes
-include("mysql_connector.hrl").
%%%===================================================================
%%% database about part
%%%===================================================================
%% @doc change database
-spec change_database(State :: #state{}, Sql :: sql()) -> #ok{} | #data{} | #error{}.
change_database(_State, []) ->
    #ok{};
change_database(_State, <<>>) ->
    #ok{};
change_database(State, Database) ->
    Sql = <<"USE `", (unicode:characters_to_binary(Database))/binary, "`">>,
    execute(State, Sql).

%% @doc set encoding
-spec set_encoding(State :: #state{}, Sql :: sql()) -> #ok{} | #data{} | #error{}.
set_encoding(_State, []) ->
    #ok{};
set_encoding(_State, <<>>) ->
    #ok{};
set_encoding(State, Encoding) ->
    Sql = <<"SET NAMES '", (unicode:characters_to_binary(Encoding))/binary, "'">>,
    execute(State, Sql).

%%%===================================================================
%%% ping
%%%===================================================================
%% @doc ping
-spec ping(State :: #state{}) -> ok | {error, term()}.
ping(State) ->
    Packet = <<?COM_PING>>,
    %% query packet sequence number start with 0
    send_packet(State, Packet, <<0:8>>),
    %% get result now
    PingResult = handle(State, <<1:8>>, <<>>),
    not is_record(PingResult, ok) andalso erlang:error(PingResult),
    ok.

%%%===================================================================
%%% quit
%%%===================================================================
%% @doc quit
-spec quit(State :: #state{}) -> ok.
quit(State) ->
    Packet = <<?COM_QUIT>>,
    %% Server closes the connection or returns ERR_Packet.
    send_packet(State, Packet, <<0:8>>),
    ok.

%%%===================================================================
%%% execute sql request part
%%%===================================================================
%% @doc execute sql
-spec execute(State :: #state{}, Sql :: sql()) -> #ok{} | #data{} | #error{}.
execute(State, Sql) ->
    Packet = <<?COM_QUERY, (iolist_to_binary(Sql))/binary>>,
    %% packet sequence number start with 0
    send_packet(State, Packet, <<0:8>>),
    %% get response now
    handle(State, <<1:8>>, <<>>).

%% handle execute result
handle(State, Number = <<Seq:8>>, <<Length:24/little, Number:1/binary-unit:8, Packet:Length/binary-unit:8, Rest/binary>>) ->
    case Packet of
        <<?OK:8, PacketRest/binary>> ->
            decode_ok_packet(PacketRest);
        <<?ERROR:8, PacketRest/binary>> ->
            decode_error_packet(PacketRest);
        _ ->
            %% tabular data decode
            %% {FieldsNumber, <<>>} = decode_integer(Packet),
            decode_fields_info(State, <<(Seq + 1):8>>, Rest, [])
    end;
handle(_, <<Number:8>>, <<Length:24/little, SequenceNumber:8, _:Length/binary-unit:8, _/binary>>) ->
    %% chaos sequence number
    erlang:error(<<"Got packets out of order (self): ", (integer_to_binary(Number))/binary, " (server): ", (integer_to_binary(SequenceNumber))/binary>>);
handle(State, Number, Data) ->
    InData = receive_data(State),
    handle(State, Number, <<Data/binary, InData/binary>>).

%% decode fields info, read n field packet, read an eof packet
decode_fields_info(State, Number = <<Seq:8>>, <<Length:24/little, Number:1/binary-unit:8, Packet:Length/binary-unit:8, Rest/binary>>, List) ->
    case Packet of
        <<?EOF:8, PacketRest:4/binary>> ->
            %% eof packet
            %% if (not capabilities & CLIENT_DEPRECATE_EOF)
            %% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
            FieldsInfo = lists:reverse(List),
            FieldsEof = decode_eof_packet(PacketRest),
            {RowsData, RowsEof} = decode_rows(State, <<(Seq + 1)>>, Rest, FieldsInfo, []),
            #data{fields_info = FieldsInfo, fields_eof = FieldsEof, rows_data = RowsData, rows_eof = RowsEof};
        _ ->
            %% column definition
            %% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html
            %% catalog
            {_Catalog, CatalogLength} = decode_string(Packet),
            <<_:CatalogLength/binary, CatalogRest/binary>> = Packet,
            %% schema
            {_Schema, SchemaLength} = decode_string(CatalogRest),
            <<_:SchemaLength/binary, SchemaRest/binary>> = CatalogRest,
            %% table
            {_Table, TableLength} = decode_string(SchemaRest),
            <<_:TableLength/binary, TableRest/binary>> = SchemaRest,
            %% origin table
            {_OriginTable, OriginTableLength} = decode_string(TableRest),
            <<_:OriginTableLength/binary, OriginTableRest/binary>> = TableRest,
            %% name
            {Name, NameLength} = decode_string(OriginTableRest),
            <<_:NameLength/binary, NameRest/binary>> = OriginTableRest,
            %% origin name
            {_OriginName, OriginNameLength} = decode_string(NameRest),
            <<_:OriginNameLength/binary, OriginNameRest/binary>> = NameRest,
            %% extract packet
            %% character set
            %% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_character_set.html
            %% flags
            %% https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html
            <<_Metadata:8/little, _CharacterSet:16/little, _Length:32/little, Type:8/little, Flags:16/little, Decimals:8/little, _Rest7/binary>> = OriginNameRest,
            %% collect one
            This = {Name, Type, Flags, Decimals},
            decode_fields_info(State, <<(Seq + 1):8>>, Rest, [This | List])
    end;
decode_fields_info(_, <<Number:8>>, <<Length:24/little, SequenceNumber:8, _:Length/binary-unit:8, _/binary>>, _) ->
    %% chaos sequence number
    erlang:error(<<"Got packets out of order (self): ", (integer_to_binary(Number))/binary, " (server): ", (integer_to_binary(SequenceNumber))/binary>>);
decode_fields_info(State, Number, Data, List) ->
    InData = receive_data(State),
    decode_fields_info(State, Number, <<Data/binary, InData/binary>>, List).

%% decode RowsData, read n field packet, read an eof packet
decode_rows(State, Number = <<Seq:8>>, <<Length:24/little, Number:1/binary-unit:8, Packet:Length/binary-unit:8, Rest/binary>>, FieldsInfo, List) ->
    case Packet of
        <<?EOF:8, PacketRest:4/binary>> ->
            %% if capabilities & CLIENT_DEPRECATE_EOF
            %% ok packet
            %% else eof packet
            %% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
            {lists:reverse(List), decode_eof_packet(PacketRest)};
        _ ->
            This = decode_fields(FieldsInfo, Packet, []),
            decode_rows(State, <<(Seq + 1):8>>, Rest, FieldsInfo, [This | List])
    end;
decode_rows(_, <<Number:8>>, <<Length:24/little, SequenceNumber:8, _:Length/binary-unit:8, _/binary>>, _, _) ->
    %% chaos sequence number
    erlang:error(<<"Got packets out of order (self): ", (integer_to_binary(Number))/binary, " (server): ", (integer_to_binary(SequenceNumber))/binary>>);
decode_rows(State, Number, Data, FieldsInfo, List) ->
    InData = receive_data(State),
    decode_rows(State, Number, <<Data/binary, InData/binary>>, FieldsInfo, List).

%% decode field
decode_fields([], <<>>, List) ->
    List;
decode_fields([{_, Type, _, _} | Fields], <<Packet/binary>>, List) ->
    {Column, Length} = decode_string(Packet),
    This = convert(Type, Column),
    <<_:Length/binary, Rest/binary>> = Packet,
    [This | decode_fields(Fields, Rest, List)].

%% integer format
convert(_, undefined) ->
    undefined;
convert(?MYSQL_TYPE_DECIMAL, Value) ->
    try
        binary_to_float(Value)
    catch _:_ ->
        binary_to_integer(Value)
    end;
convert(?MYSQL_TYPE_TINY, Value) ->
    binary_to_integer(Value);
convert(?MYSQL_TYPE_SHORT, Value) ->
    binary_to_integer(Value);
convert(?MYSQL_TYPE_LONG, Value) ->
    binary_to_integer(Value);
convert(?MYSQL_TYPE_FLOAT, Value) ->
    try
        binary_to_float(Value)
    catch _:_ ->
        binary_to_integer(Value)
    end;
convert(?MYSQL_TYPE_DOUBLE, Value) ->
    try
        binary_to_float(Value)
    catch _:_ ->
        binary_to_integer(Value)
    end;
convert(?MYSQL_TYPE_NULL, Value) ->
    Value;
convert(?MYSQL_TYPE_TIMESTAMP, <<Y:4/binary, "-", Mo:2/binary, "-", D:2/binary, " ", H:2/binary, ":", Mi:2/binary, ":", S:2/binary>>) ->
    {{binary_to_integer(Y), binary_to_integer(Mo), binary_to_integer(D)}, {binary_to_integer(H), binary_to_integer(Mi), binary_to_integer(S)}};
convert(?MYSQL_TYPE_LONGLONG, Value) ->
    binary_to_integer(Value);
convert(?MYSQL_TYPE_INT24, Value) ->
    binary_to_integer(Value);
convert(?MYSQL_TYPE_DATE, <<Y:4/binary, "-", M:2/binary, "-", D:2/binary>>) ->
    {binary_to_integer(Y), binary_to_integer(M), binary_to_integer(D)};
convert(?MYSQL_TYPE_TIME, <<H:2/binary, ":", M:2/binary, ":", S:2/binary>>) ->
    {binary_to_integer(H), binary_to_integer(M), binary_to_integer(S)};
convert(?MYSQL_TYPE_DATETIME, <<Y:4/binary, "-", Mo:2/binary, "-", D:2/binary, " ", H:2/binary, ":", Mi:2/binary, ":", S:2/binary>>) ->
    {{binary_to_integer(Y), binary_to_integer(Mo), binary_to_integer(D)}, {binary_to_integer(H), binary_to_integer(Mi), binary_to_integer(S)}};
convert(?MYSQL_TYPE_YEAR, Value) ->
    binary_to_integer(Value);
convert(?MYSQL_TYPE_NEW_DATE, Value) ->
    Value;
convert(?MYSQL_TYPE_VARCHAR, Value) ->
    Value;
convert(?MYSQL_TYPE_BIT, Value) ->
    Value;
convert(?MYSQL_TYPE_JSON, Value) ->
    Value;
convert(?MYSQL_TYPE_NEW_DECIMAL, Value) ->
    try
        binary_to_float(Value)
    catch _:_ ->
        binary_to_integer(Value)
    end;
convert(?MYSQL_TYPE_ENUM, Value) ->
    Value;
convert(?MYSQL_TYPE_SET, Value) ->
    Value;
convert(?MYSQL_TYPE_TINY_BLOB, Value) ->
    Value;
convert(?MYSQL_TYPE_MEDIUM_BLOB, Value) ->
    Value;
convert(?MYSQL_TYPE_LONG_BLOB, Value) ->
    Value;
convert(?MYSQL_TYPE_BLOB, Value) ->
    Value;
convert(?MYSQL_TYPE_VAR_STRING, Value) ->
    Value;
convert(?MYSQL_TYPE_STRING, Value) ->
    Value;
convert(?MYSQL_TYPE_GEOMETRY, Value) ->
    Value;
convert(Type, _) ->
    erlang:error({unknown_field_type, Type}).

%%%===================================================================
%%% decode packet part
%%%===================================================================
%% decode ok packet
decode_ok_packet(<<Packet/binary>>) ->
    {AffectedRows, AffectedRowsLength} = decode_integer(Packet),
    <<_:AffectedRowsLength/binary, AffectedRowsRest/binary>> = Packet,
    {InsertId, InsertIdLength} = decode_integer(AffectedRowsRest),
    <<_:InsertIdLength/binary, InsertIdRest/binary>> = AffectedRowsRest,
    <<Status:16/little, WarningsNumber:16/little, Message/binary>> = InsertIdRest,
    #ok{affected_rows = AffectedRows, insert_id = InsertId, status = Status, warnings_number = WarningsNumber, message = Message}.

%% decode eof result
decode_eof_packet(<<WarningsNumber:16/little, Status:16/little>>) ->
    #eof{warnings_number = WarningsNumber, status = Status}.

%% decode error packet
decode_error_packet(<<Code:16/little, Status:6/binary-unit:8, Message/binary>>) ->
    #error{code = Code, status = Status, message = Message}.

%% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_dt_integers.html
%% length-decoded-integer
decode_integer(<<Value:8, _/binary>>) when Value < 16#FB ->
    {Value, 1};
decode_integer(<<16#FB:8, _/binary>>) ->
    {undefined, 1};
decode_integer(<<16#FC:8, Value:16/little, _/binary>>) ->
    {Value, 1 + 2};
decode_integer(<<16#FD:8, Value:24/little, _/binary>>) ->
    {Value, 1 + 3};
decode_integer(<<16#FE:8, Value:64/little, _/binary>>) ->
    {Value, 1 + 8}.

%% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_dt_strings.html
%% length-decoded-string
decode_string(<<Length:8, Value:Length/binary, _/binary>>) when Length < 16#FB ->
    {Value, 1 + Length};
decode_string(<<16#FB:8, _/binary>>) ->
    {undefined, 1};
decode_string(<<16#FC:8, Length:16/little, Value:Length/binary, _/binary>>) ->
    {Value, 1 + 2 + Length};
decode_string(<<16#FD:8, Length:24/little, Value:Length/binary, _/binary>>) ->
    {Value, 1 + 3 + Length};
decode_string(<<16#FE:8, Length:64/little, Value:Length/binary, _/binary>>) ->
    {Value, 1 + 8 + Length}.

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
