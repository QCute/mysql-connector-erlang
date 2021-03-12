%%%-------------------------------------------------------------------
%%% @doc
%%% mysql-connector-erlang
%%% * erlang mysql connector in single file
%%% * connect/query/handle by single process
%%% * support version 4.1 or later (8.x caching_sha2_password plugin supported)
%%% * arguments pass by tuple list(config file compatibility) instead maps(otp 17 early supported)
%%% * nice pool compatibility
%%% * quick and easy to integrate in your project
%%% @end
%%%-------------------------------------------------------------------
-module(mysql_connector).
-behavior(application).
-behavior(gen_server).
%% API functions
-export([start_link/1]).
%% normal query interface
-export([execute/1, execute/2, execute/3, query/1, query/2, query/3, select/1, select/2, select/3, insert/1, insert/2, insert/3, update/1, update/2, update/3, delete/1, delete/2, delete/3]).
%% get execute result info interface
-export([get_insert_id/1, get_affected_rows/1, get_fields_info/1, get_rows/1, get_error_code/1, get_error_status/1, get_error_message/1]).
%% application callbacks
-export([start/2, stop/1]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
%%%-------------------------------------------------------------------
%%% Macros
%%%-------------------------------------------------------------------
%% https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__capabilities__flags.html
%% --- capability flags ---
-define(CLIENT_LONG_PASSWORD,                         16#00000001).
-define(CLIENT_FOUND_ROWS,                            16#00000002).
-define(CLIENT_LONG_FLAG,                             16#00000004).
-define(CLIENT_CONNECT_WITH_DB,                       16#00000008).
-define(CLIENT_NO_SCHEMA,                             16#00000010).
-define(CLIENT_COMPRESS,                              16#00000020).
-define(CLIENT_ODBC,                                  16#00000040).
-define(CLIENT_LOCAL_FILES,                           16#00000080).
-define(CLIENT_IGNORE_SPACE,                          16#00000100).
-define(CLIENT_PROTOCOL_41,                           16#00000200).
-define(CLIENT_INTERACTIVE,                           16#00000400).
-define(CLIENT_SSL,                                   16#00000800).
-define(CLIENT_IGNORE_SIGPIPE,                        16#00001000).
-define(CLIENT_TRANSACTIONS,                          16#00002000).
-define(CLIENT_RESERVED,                              16#00004000).
-define(CLIENT_RESERVED_2,                            16#00008000).
-define(CLIENT_MULTI_STATEMENTS,                      16#00010000).
-define(CLIENT_MULTI_RESULTS,                         16#00020000).
-define(CLIENT_PS_MULTI_RESULTS,                      16#00040000).
-define(CLIENT_PLUGIN_AUTH,                           16#00080000).
-define(CLIENT_CONNECT_ATTRS,                         16#00100000).
-define(CLIENT_PLUGIN_AUTH_LEN_ENC_CLIENT_DATA,       16#00200000).
-define(CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS,          16#00400000).
-define(CLIENT_SESSION_TRACK,                         16#00800000).
-define(CLIENT_DEPRECATE_EOF,                         16#01000000).
-define(CLIENT_OPTIONAL_RESULT_SET_METADATA,          16#02000000).
-define(CLIENT_Z_STD_COMPRESSION_ALGORITHM,           16#04000000).
-define(CLIENT_SSL_VERIFY_SERVER_CERT,                16#40000000).
-define(CLIENT_REMEMBER_OPTIONS,                      16#80000000).

%% https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#a1d854e841086925be1883e4d7b4e8cad
%% status enum flags include/mysql_com.h
%% -- server status flags --
-define(SERVER_STATUS_IN_TRANS,                       16#00000001).
-define(SERVER_STATUS_AUTOCOMMIT,                     16#00000002).
-define(SERVER_MORE_RESULTS_EXISTS,                   16#00000008).
-define(SERVER_STATUS_NO_GOOD_INDEX_USED,             16#00000010).
-define(SERVER_STATUS_NO_INDEX_USED,                  16#00000020).
-define(SERVER_STATUS_CURSOR_EXISTS,                  16#00000040).
-define(SERVER_STATUS_LAST_ROW_SENT,                  16#00000080).
-define(SERVER_STATUS_DB_DROPPED,                     16#00000100).
-define(SERVER_STATUS_NO_BACKSLASH_ESCAPES,           16#00000200).
-define(SERVER_STATUS_METADATA_CHANGED,               16#00000400).
-define(SERVER_QUERY_WAS_SLOW,                        16#00000800).
-define(SERVER_PS_OUT_PARAMS,                         16#00001000).
-define(SERVER_STATUS_IN_TRANS_READONLY,              16#00002000).
-define(SERVER_SESSION_STATE_CHANGED,                 16#00004000).

%% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase.html
%% -- command --
-define(COM_SLEEP,                                    16#00).
-define(COM_QUIT,                                     16#01).
-define(COM_INIT_DB,                                  16#02).
-define(COM_QUERY,                                    16#03).
-define(COM_FIELD_LIST,                               16#04).
-define(COM_CREATE_DB,                                16#05).
-define(COM_DROP_DB,                                  16#06).
-define(COM_REFRESH,                                  16#07).
-define(COM_SHUTDOWN,                                 16#08).
-define(COM_STATISTICS,                               16#09).
-define(COM_PROCESS_INFO,                             16#0A).
-define(COM_CONNECT,                                  16#0B).
-define(COM_PROCESS_KILL,                             16#0C).
-define(COM_DEBUG,                                    16#0D).
-define(COM_PING,                                     16#0E).
-define(COM_TIME,                                     16#0F).
-define(COM_DELAYED_INSERT,                           16#10).
-define(COM_CHANGE_USER,                              16#11).
-define(COM_BINLOG_DUMP,                              16#12).
-define(COM_TABLE_DUMP,                               16#13).
-define(COM_CONNECT_OUT,                              16#14).
-define(COM_REGISTER_SLAVE,                           16#15).
-define(COM_STMT_PREPARE,                             16#16).
-define(COM_STMT_EXECUTE,                             16#17).
-define(COM_STMT_SEND_LONG_DATA,                      16#18).
-define(COM_STMT_CLOSE,                               16#19).
-define(COM_STMT_RESET,                               16#1A).
-define(COM_SET_OPTION,                               16#1B).
-define(COM_STMT_FETCH,                               16#1C).

%% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_character_set.html
%% SELECT `id`, `collation_name` FROM information_schema.`collations` ORDER BY `id`;
%% --- basic character set ---
-define(UTF8MB4_GENERAL_CI,                           16#002D).
-define(UTF8MB4_UNICODE_CI,                           16#00E0).

%% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_response_packets.html
%% generic response packets
-define(OK,                                           16#00).
-define(EOF,                                          16#FE).
-define(ERROR,                                        16#FF).

%%%-------------------------------------------------------------------
%%% Records
%%%-------------------------------------------------------------------
%% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html
-record(ok, {
    affected_rows = 0 :: non_neg_integer(),
    insert_id = 0 :: non_neg_integer(),
    status = 0 :: non_neg_integer(),
    warnings_number = 0 :: non_neg_integer(),
    message = <<>> :: binary()
}).

%% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_eof_packet.html
-record(eof, {
    warnings_number = 0 :: non_neg_integer(),
    status = 0 :: non_neg_integer()
}).

%% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html
-record(error, {
    code = 0 :: non_neg_integer(),
    status = <<>> :: binary(),
    message = <<>> :: binary()
}).

%% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase.html
%% handshake
-record(handshake, {
    version = <<>> :: binary(),
    id = 0 :: integer(),
    capabilities = 0 :: integer(),
    charset = 0 :: integer(),
    status = 0 :: integer(),
    salt = <<>> :: binary(),
    plugin = <<>> :: binary()
}).

%% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html
-record(data, {
    fields_info = [] :: list(),
    rows = [] :: list()
}).

%% mysql connection state
-record(state, {
    socket_type :: gen_tcp | ssl,
    socket :: gen_tcp:socket() | ssl:sslsocket(),
    data = <<>> :: binary(),
    number = <<>> :: binary()
}).

%%%===================================================================
%%% API functions
%%%===================================================================

%% mysql connector arguments supported
%% +---------------------+---------------------+---------------------+
%% |    key              |      value          |  default            |
%% +---------------------+---------------------+---------------------+
%% |    host             |      Host           |  "localhost"        |
%% |    port             |      Port           |  3306               |
%% |    user             |      User           |  ""                 |
%% |    password         |      Password       |  ""                 |
%% |    database         |      Database       |  ""                 |
%% |    encoding         |      Encoding       |  ""                 |
%% +---------------------+---------------------+---------------------+

%% @doc start link
-spec start_link(Args :: [{Key :: atom(), Value :: list()}]) -> {'ok', pid()} | 'ignore' | {'error', term()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%% execute result
%% +-----------------------------++----------------------------------+
%% | type                        ||   value                          |
%% +-----------------------------++----------------------------------+
%% | tiny/small/medium/int/big   ||   integer                        |
%% | float/double/decimal        ||   integer/float                  |
%% | decimal/new decimal         ||   integer/float                  |
%% | year                        ||   integer                        |
%% | date                        ||   {Y, M, D}                      |
%% | time                        ||   {H, M, S}                      |
%% | datetime/timestamp          ||   {{Y, M, D}, {H, M, S}}         |
%% | char/var/text/blob          ||   binary                         |
%% | json/enum/set/bit/geometry  ||   binary                         |
%% | NULL                        ||   undefined                      |
%% +-----------------------------++----------------------------------+

%% @doc execute
-spec execute(Sql :: list() | binary()) -> #ok{} | #data{} | #error{}.
execute(Sql) ->
    execute(Sql, ?MODULE).

%% @doc execute
-spec execute(Sql :: list() | binary(), Connector :: pid() | atom()) -> #ok{} | #data{} | #error{}.
execute(Sql, Connector) ->
    execute(Sql, Connector, infinity).

%% @doc execute
-spec execute(Sql :: list() | binary(), Connector :: pid() | atom(), Timeout :: non_neg_integer() | infinity) -> #ok{} | #data{} | #error{}.
execute(Sql, Connector, Timeout) ->
    gen_server:call(Connector, {execute, Sql}, Timeout).

%% @doc query
-spec query(Sql :: list() | binary()) -> non_neg_integer() | list().
query(Sql) ->
    query(Sql, ?MODULE).

%% @doc query
-spec query(Sql :: list() | binary(), Connector :: pid() | atom()) -> non_neg_integer() | list().
query(Sql, Connector) ->
    query(Sql, Connector, infinity).

%% @doc query
-spec query(Sql :: list() | binary(), Connector :: pid() | atom(), Timeout :: non_neg_integer() | infinity) -> non_neg_integer() | list().
query(Sql, Connector, Timeout) ->
    case execute(Sql, Connector, Timeout) of
        #ok{insert_id = 0, affected_rows = AffectedRows} ->
            AffectedRows;
        #ok{insert_id = InsertId} ->
            InsertId;
        #data{rows = Rows} ->
            Rows;
        #error{code = Code, message = Message} ->
            erlang:exit({mysql_error, {Sql, Code, Message}})
    end.

%% @doc select
-spec select(Sql :: list() | binary()) -> {ok, Rows :: list()} | {error, Code :: non_neg_integer(), Message :: binary()}.
select(Sql) ->
    select(Sql, ?MODULE).

%% @doc select
-spec select(Sql :: list() | binary(), Connector :: pid() | atom()) -> {ok, Rows :: list()} | {error, Code :: non_neg_integer(), Message :: binary()}.
select(Sql, Connector) ->
    select(Sql, Connector, infinity).

%% @doc select
-spec select(Sql :: list() | binary(), Connector :: pid() | atom(), Timeout :: non_neg_integer() | infinity) -> {ok, Rows :: list()} | {error, Code :: non_neg_integer(), Message :: binary()}.
select(Sql, Connector, Timeout) ->
    case execute(Sql, Connector, Timeout) of
        #data{rows = Rows} ->
            {ok, Rows};
        #error{code = Code, message = Message} ->
            {error, Code, Message}
    end.

%% @doc insert
-spec insert(Sql :: list() | binary()) -> {ok, InsertId :: non_neg_integer()} | {error, Code :: non_neg_integer(), Message :: binary()}.
insert(Sql) ->
    insert(Sql, ?MODULE).

%% @doc insert
-spec insert(Sql :: list() | binary(), Connector :: pid() | atom()) -> {ok, InsertId :: non_neg_integer()} | {error, Code :: non_neg_integer(), Message :: binary()}.
insert(Sql, Connector) ->
    insert(Sql, Connector, infinity).

%% @doc insert
-spec insert(Sql :: list() | binary(), Connector :: pid() | atom(), Timeout :: non_neg_integer() | infinity) -> {ok, InsertId :: non_neg_integer()} | {error, Code :: non_neg_integer(), Message :: binary()}.
insert(Sql, Connector, Timeout) ->
    case execute(Sql, Connector, Timeout) of
        #ok{insert_id = InsertId} ->
            {ok, InsertId};
        #error{code = Code, message = Message} ->
            {error, Code, Message}
    end.

%% @doc update
-spec update(Sql :: list() | binary()) -> {ok, AffectedRows :: non_neg_integer()} | {error, Code :: non_neg_integer(), Message :: binary()}.
update(Sql) ->
    update(Sql, ?MODULE).

%% @doc update
-spec update(Sql :: list() | binary(), Connector :: pid() | atom()) -> {ok, AffectedRows :: non_neg_integer()} | {error, Code :: non_neg_integer(), Message :: binary()}.
update(Sql, Connector) ->
    update(Sql, Connector, infinity).

%% @doc update
-spec update(Sql :: list() | binary(), Connector :: pid() | atom(), non_neg_integer() | infinity) -> {ok, AffectedRows :: non_neg_integer()} | {error, Code :: non_neg_integer(), Message :: binary()}.
update(Sql, Connector, Timeout) ->
    case execute(Sql, Connector, Timeout) of
        #ok{affected_rows = AffectedRows} ->
            {ok, AffectedRows};
        #error{code = Code, message = Message} ->
            {error, Code, Message}
    end.

%% @doc delete
-spec delete(Sql :: list() | binary()) -> {ok, AffectedRows :: non_neg_integer()} | {error, Code :: non_neg_integer(), Message :: binary()}.
delete(Sql) ->
    delete(Sql, ?MODULE).

%% @doc delete
-spec delete(Sql :: list() | binary(), Connector :: pid() | atom()) -> {ok, AffectedRows :: non_neg_integer()} | {error, Code :: non_neg_integer(), Message :: binary()}.
delete(Sql, Connector) ->
    delete(Sql, Connector, infinity).

%% @doc delete
-spec delete(Sql :: list() | binary(), Connector :: pid() | atom(), Timeout :: non_neg_integer() | infinity) -> {ok, AffectedRows :: non_neg_integer()} | {error, Code :: non_neg_integer(), Message :: binary()}.
delete(Sql, Connector, Timeout) ->
    case execute(Sql, Connector, Timeout) of
        #ok{affected_rows = AffectedRows} ->
            {ok, AffectedRows};
        #error{code = Code, message = Message} ->
            {error, Code, Message}
    end.

%% @doc extract the insert id from MySQL result on insert
-spec get_insert_id(Result :: #ok{}) -> InsertId :: non_neg_integer().
get_insert_id(#ok{insert_id = InsertId}) ->
    InsertId.

%% @doc extract the affected rows from MySQL result on update
-spec get_affected_rows(Result :: #ok{}) -> AffectedRows :: non_neg_integer().
get_affected_rows(#ok{affected_rows = AffectedRows}) ->
    AffectedRows.

%% @doc extract the fields info from MySQL result on data received
-spec get_fields_info(Result :: #data{}) -> [{Field :: list(), Type :: non_neg_integer()}].
get_fields_info(#data{fields_info = FieldsInfo}) ->
    FieldsInfo.

%% @doc extract the data rows from MySQL result on data received
-spec get_rows(Result :: #data{}) -> Rows :: list().
get_rows(#data{rows = Rows}) ->
    Rows.

%% @doc extract the error code from MySQL result on error
-spec get_error_code(Result :: #error{}) -> Code :: non_neg_integer().
get_error_code(#error{code = Code}) ->
    Code.

%% @doc extract the error sql status from MySQL result on error
-spec get_error_status(Result :: #error{}) -> Status :: binary().
get_error_status(#error{status = Status}) ->
    Status.

%% @doc extract the error message from MySQL result on error
-spec get_error_message(Result :: #error{}) -> Message :: binary().
get_error_message(#error{message = Message}) ->
    Message.

%%%===================================================================
%%% application callbacks
%%%===================================================================
%% @doc start
-spec start(StartType :: term(), StartArgs :: list()) -> {ok, pid()} | {ok, pid(), term()} | {error, term()}.
start(_, _) ->
    {ok, Args} = application:get_env(mysql_connector),
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%% @doc stop
-spec stop(State :: term()) -> ok.
stop(_) ->
    ok.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
%% @doc init
-spec init(Args :: term()) -> {ok, State :: #state{}, timeout()}.
init(Args) ->
    process_flag(trap_exit, true),
    %% connect and login
    State = connect(Args),
    {ok, State, 60 * 1000}.

%% @doc handle_call
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: #state{}) -> {reply, Reply :: term(), State :: #state{}, timeout()}.
handle_call({execute, Sql}, _From, State) ->
    {reply, execute_sql(State, Sql), State, 60 * 1000};

handle_call(_Info, _From, State) ->
    {reply, ok, State, 60 * 1000}.

%% @doc handle_cast
-spec handle_cast(Request :: term(), State :: #state{}) -> {noreply, NewState :: #state{}, timeout()}.
handle_cast(_Info, State) ->
    {noreply, State, 60 * 1000}.

%% @doc handle_info
-spec handle_info(Info :: timeout | term(), State :: term()) -> {noreply, NewState :: #state{}, timeout()}.
handle_info(timeout, State) ->
    ping(State),
    {noreply, State, 60 * 1000};

handle_info(_Info, State) ->
    {noreply, State, 60 * 1000}.

%% @doc terminate
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()), State :: #state{}) -> {ok, NewState :: #state{}}.
terminate(_Reason, State) ->
    quit(State),
    {ok, State}.

%% @doc code_change
-spec code_change(OldVsn :: (term() | {down, term()}), State :: #state{}, Extra :: term()) -> {ok, NewState :: #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
%% tcp socket connect
connect(Args) ->
    Host     = proplists:get_value(host,     Args, "localhost"),
    Port     = proplists:get_value(port,     Args, 3306),
    User     = proplists:get_value(user,     Args, ""),
    Password = proplists:get_value(password, Args, ""),
    Database = proplists:get_value(database, Args, ""),
    Encoding = proplists:get_value(encoding, Args, ""),
    case gen_tcp:connect(Host, Port, [{mode, binary}, {packet, 0}, {active, false}]) of
        {ok, Socket} ->
            %% login
            NewState = login(#state{socket_type = gen_tcp, socket = Socket}, User, Password, Database),
            %% set database and encoding
            set_base(NewState, Database, Encoding),
            %% succeeded
            NewState;
        {error, Reason} ->
            erlang:exit(Reason)
    end.

%%%===================================================================
%%% login verify part
%%%===================================================================
%% login
login(State, User, Password, Database) ->
    {Packet, NewState} = receive_packet(State, 0),
    %% the handshake packet
    Handshake = decode_handshake(Packet),
    %% switch to ssl if server need
    NewestState = switch_to_ssl(NewState, Handshake, User, Password, Database),
    %% switch to ssl handshake
    HandshakePacket = encode_handshake(NewestState, Handshake, User, Password, Database),
    FinalState = send_packet(NewestState, HandshakePacket),
    %% enter verify step
    verify(FinalState, Handshake, Password).

%% switch to ssl
switch_to_ssl(State = #state{socket = Socket}, Handshake = #handshake{capabilities = Capabilities}, User, Password, Database) ->
    case Capabilities band ?CLIENT_SSL =/= 0 of
        true ->
            %% switch to ssl handshake
            Binary = encode_switch_handshake(State, Handshake, User, Password, Database),
            NewState = send_packet(State, Binary),
            %% start ssl application
            ssl:start(),
            %% force wrap gen_tcp socket success
            {ok, SSLSocket} = ssl:connect(Socket, [{verify, verify_none}, {versions, [tlsv1]}]),
            %% force handshake success
            %% ssl_connection:handshake(SSLSocket, infinity),
            NewState#state{socket_type = ssl, socket = SSLSocket};
        false ->
            State
    end.

%% login verify
verify(State, Handshake, Password) ->
    case receive_packet(State) of
        {<<?OK:8, _Rest/binary>>, NewState} ->
            %% New auth success
            %% {AffectedRows, Rest1} = decode_packet(Rest),
            %% {InsertId, Rest2} = decode_packet(Rest1),
            %% <<StatusFlags:16/little, WarningsNumber:16/little, Msg/binary>> = Rest2,
            %% check status, ignoring bit 16#4000, SERVER_SESSION_STATE_CHANGED
            %% and bit 16#0002, SERVER_STATUS_AUTOCOMMIT.
            NewState;
        {<<?EOF:8>>, _NewState} ->
            %% Old Authentication Method Switch Request Packet consisting of a
            %% single 0xfe byte. It is sent by server to request client to
            %% switch to Old Password Authentication if CLIENT_PLUGIN_AUTH
            %% capability is not supported (by either the client or the server)
            %% MySQL 4.0 or earlier old auth already unsupported
            erlang:exit(unsupported_authentication_method);
        {<<?EOF:8, SwitchData/binary>>, NewState} ->
            %% Authentication Method Switch Request Packet. If both server and
            %% client support CLIENT_PLUGIN_AUTH capability, server can send
            %% this packet to ask client to use another authentication method.
            [Plugin, Salt] = binary:split(SwitchData, <<0>>),
            Binary = encrypt_password(Password, Salt, Plugin),
            FinalState = send_packet(NewState, Binary),
            verify(FinalState, Handshake#handshake{plugin = Plugin}, Password);
        {<<1:8, 3:8, _/binary>>, NewState} ->
            %% Authentication password confirm do not need
            verify(NewState, Handshake, Password);
        {<<1:8, 4:8, _/binary>>, NewState} ->
            %% Authentication password confirm full
            Binary = <<(unicode:characters_to_binary(Password))/binary, 0:8>>,
            FinalState = send_packet(NewState, Binary),
            verify(FinalState, Handshake, Password);
        {<<?ERROR:8, Rest/binary>>, _NewState} ->
            %% verify failed, user or password invalid
            erlang:exit(decode_error_packet(Rest));
        {Packet, _NewState} ->
            %% unknown packet
            erlang:exit({unknown_packet, Packet})
    end.

%%%===================================================================
%%% login password auth part
%%%===================================================================
%% get verify greeting data
decode_handshake(<<10:8, Rest/binary>>) ->
    %% Protocol version 10.
    [Version, Rest1] = binary:split(Rest, <<0>>),
    <<ConnectionId:32/little, Salt1:8/binary-unit:8, 0:8, CapabilitiesLower:16/little, CharSet:8, Status:16/little, CapabilitiesUpper:16/little, _SaltLength:8, _Reserved:10/binary-unit:8, Rest2/binary>> = Rest1,
    Capabilities = CapabilitiesLower + 16#10000 * CapabilitiesUpper,
    %% lower half part salt
    [Salt2, Rest3] = binary:split(Rest2, <<0>>),
    %% plugin name
    %% MySQL server 5.5.8 has a bug where end byte is not send
    [Plugin | _] = binary:split(Rest3, <<0>>),
    #handshake{version = Version, id = ConnectionId, capabilities = Capabilities, charset = CharSet, status = Status, salt = <<Salt1/binary, Salt2/binary>>, plugin = Plugin};
decode_handshake(<<?ERROR:8, Code:16, Rest/binary>>) ->
    erlang:exit({handshake_error, Code, Rest});
decode_handshake(<<Protocol:8, _/binary>>) ->
    erlang:exit({unsupported_handshake_protocol, Protocol}).

%% authentication plugin switch response
encode_switch_handshake(#state{}, #handshake{capabilities = Capabilities, charset = Charset}, _, _, _) ->
    Flag = plugin_support(Capabilities, ssl_support(Capabilities, basic_flag())),
    <<Flag:32/little, ?CLIENT_SSL_VERIFY_SERVER_CERT:32/little, Charset:8, 0:23/unit:8>>.

%% new authentication method mysql_native_password support mysql 5.x or later
encode_handshake(#state{}, #handshake{capabilities = Capabilities, charset = Charset, salt = Salt, plugin = Plugin}, User, Password, Database) ->
    %% add authentication plugin support and ssl support if server need
    Flag = plugin_support(Capabilities, ssl_support(Capabilities, basic_flag())),
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
    %% unsupported plugin exit directly
    erlang:exit({unsupported_plugin, PluginName}).

%% capabilities flag
basic_flag() ->
    ?CLIENT_LONG_PASSWORD bor ?CLIENT_LONG_FLAG bor ?CLIENT_CONNECT_WITH_DB bor ?CLIENT_PROTOCOL_41 bor ?CLIENT_TRANSACTIONS bor ?CLIENT_RESERVED_2 bor ?CLIENT_MULTI_STATEMENTS bor ?CLIENT_MULTI_RESULTS.

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

%%%===================================================================
%%% database about part
%%%===================================================================
%% set base
set_base(State, Database, Encoding) ->
    %% change database
    ChangeDatabaseResult = change_database(State, Database),
    not is_record(ChangeDatabaseResult, ok) andalso erlang:exit(ChangeDatabaseResult),
    %% set encoding
    SetEncodingResult = set_encoding(State, Encoding),
    not is_record(ChangeDatabaseResult, ok) andalso erlang:exit(SetEncodingResult).

%% change database
change_database(_State, []) ->
    #ok{};
change_database(_State, <<>>) ->
    #ok{};
change_database(State, Database) ->
    Sql = <<"USE `", (unicode:characters_to_binary(Database))/binary, "`">>,
    execute_sql(State, Sql).

%% set encoding
set_encoding(_State, []) ->
    #ok{};
set_encoding(_State, <<>>) ->
    #ok{};
set_encoding(State, Encoding) ->
    Sql = <<"SET NAMES '", (unicode:characters_to_binary(Encoding))/binary, "'">>,
    execute_sql(State, Sql).

%%%===================================================================
%%% io part
%%%===================================================================
%% send packet
send_packet(State = #state{socket_type = SocketType, socket = Socket, number = <<Number:8>>}, Packet) ->
    SocketType:send(Socket, <<(byte_size(Packet)):24/little, (Number + 1):8, Packet/binary>>),
    State#state{number = <<(Number + 1):8>>}.

%% send packet with sequence number
send_packet(State = #state{socket_type = SocketType, socket = Socket}, Packet, Number) ->
    SocketType:send(Socket, <<(byte_size(Packet)):24/little, Number:8, Packet/binary>>),
    State#state{number = <<Number:8>>}.

%% receive packet with default timeout
receive_packet(State = #state{number = <<Number:8>>}) ->
    receive_packet_data(State#state{number = <<(Number + 1):8>>}, infinity).

%% receive packet with default timeout and sequence number
receive_packet(State, Number) ->
    receive_packet_data(State#state{number = <<Number:8>>}, infinity).

%% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_packets.html
%% mysql packets
receive_packet_data(State = #state{data = <<Length:24/little, Number:1/binary-unit:8, Packet:Length/binary-unit:8, Rest/binary>>, number = Number}, _) ->
    %% completed packet
    {Packet, State#state{data = Rest}};
receive_packet_data(#state{data = <<Length:24/little, SequenceNumber:1/binary-unit:8, _:Length/binary-unit:8, _/binary>>, number = Number}, _) ->
    %% chaos sequence number
    erlang:exit(<<"Got packets out of order (self): ", (integer_to_binary(Number))/binary, " (server): ", (integer_to_binary(SequenceNumber))/binary>>);
receive_packet_data(State = #state{socket_type = SocketType, socket = Socket, data = Data}, Timeout) ->
    %% not completed packet, receive continue
    case SocketType:recv(Socket, 0, Timeout) of
        {ok, InData} ->
            receive_packet_data(State#state{data = <<Data/binary, InData/binary>>}, Timeout);
        {error, Reason} ->
            erlang:exit(Reason)
    end.

%%%===================================================================
%%% ping
%%%===================================================================
%% ping
ping(State) ->
    Packet = <<?COM_PING>>,
    %% query packet sequence number start with 0
    NewState = send_packet(State, Packet, 0),
    %% get result now
    PingResult = handle_execute_result(NewState),
    not is_record(PingResult, ok) andalso erlang:exit(PingResult).

%%%===================================================================
%%% quit
%%%===================================================================
%% quit
quit(State) ->
    Packet = <<?COM_QUIT>>,
    %% Server closes the connection or returns ERR_Packet.
    send_packet(State, Packet, 0).

%%%===================================================================
%%% execute sql request part
%%%===================================================================
%% execute sql
execute_sql(State, Sql) ->
    Packet = <<?COM_QUERY, (iolist_to_binary(Sql))/binary>>,
    %% packet sequence number start with 0
    NewState = send_packet(State, Packet, 0),
    %% get response now
    handle_execute_result(NewState).

%% handle execute result
handle_execute_result(State) ->
    case receive_packet(State) of
        {<<?OK:8, Rest/binary>>, _} ->
            decode_ok_packet(Rest);
        {<<?ERROR:8, Rest/binary>>, _} ->
            decode_error_packet(Rest);
        {_, NewState} ->
            %% tabular data decode
            %% {FieldsNumber, <<>>} = decode_integer(Packet),
            {FieldsInfo, _FieldsInfoEof, NewestState} = decode_fields_info(NewState, []),
            {Rows, _RowsEof, _FinalState} = decode_rows(NewestState, FieldsInfo, []),
            #data{fields_info = FieldsInfo, rows = Rows}
    end.

%% decode fields info, read n field packet, read an eof packet
decode_fields_info(State, List) ->
    case receive_packet(State) of
        {<<?EOF:8, Rest:4/binary>>, NewState} ->
            %% eof packet
            %% if (not capabilities & CLIENT_DEPRECATE_EOF)
            %% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
            {lists:reverse(List), decode_eof_packet(Rest), NewState};
        {Packet, NewState} ->
            %% column definition
            %% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html
            %% catalog
            {_Catalog, CatalogRest} = decode_string(Packet),
            %% schema
            {_Schema, SchemaRest} = decode_string(CatalogRest),
            %% table
            {_Table, TableRest} = decode_string(SchemaRest),
            %% origin table
            {_OriginTable, OriginTableRest} = decode_string(TableRest),
            %% name
            {Name, NameRest} = decode_string(OriginTableRest),
            %% origin name
            {_OriginName, OriginNameRest} = decode_string(NameRest),
            %% extract packet
            %% character set
            %% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_character_set.html
            %% flags
            %% https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html
            <<_Metadata:8/little, _CharacterSet:16/little, _Length:32/little, Type:8/little, Flags:16/little, Decimals:8/little, _Rest7/binary>> = OriginNameRest,
            %% collect one
            This = {Name, Type, Flags, Decimals},
            decode_fields_info(NewState, [This | List])
    end.

%% decode rows, read n field packet, read an eof packet
decode_rows(State, FieldsInfo, List) ->
    case receive_packet(State) of
        {<<?EOF:8, Rest:4/binary>>, NewState} ->
            %% if capabilities & CLIENT_DEPRECATE_EOF
            %% ok packet
            %% else eof packet
            %% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
            {lists:reverse(List), decode_eof_packet(Rest), NewState};
        {Packet, NewState} ->
            This = decode_fields(FieldsInfo, Packet, []),
            decode_rows(NewState, FieldsInfo, [This | List])
    end.

%% decode field
decode_fields([], _, List) ->
    lists:reverse(List);
decode_fields([{_, Type, _, _} | Fields], Packet, List) ->
    {Column, Rest} = decode_string(Packet),
    This = convert_type(Type, Column),
    decode_fields(Fields, Rest, [This | List]).

%%%===================================================================
%%% decode packet part
%%%===================================================================

%% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_dt_integers.html
%% length-decoded-integer
decode_integer(<<Value:8, Rest/binary>>) when Value < 16#FB ->
    {Value, Rest};
decode_integer(<<16#FB:8, Rest/binary>>) ->
    {undefined, Rest};
decode_integer(<<16#FC:8, Value:16/little, Rest/binary>>) ->
    {Value, Rest};
decode_integer(<<16#FD:8, Value:24/little, Rest/binary>>) ->
    {Value, Rest};
decode_integer(<<16#FE:8, Value:64/little, Rest/binary>>) ->
    {Value, Rest}.

%% length-encode-integer
encode_integer(Value) when Value < 16#FB ->
    <<Value>>;
encode_integer(Value) when Value =< 16#FFFF ->
    <<16#FC:8, Value:16/little>>;
encode_integer(Value) when Value =< 16#FFFFFF ->
    <<16#FD:8, Value:24/little>>;
encode_integer(Value) when Value =< 16#FFFFFFFFFFFFFFFF ->
    <<16#FE:8, Value:64/little>>.

%% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_dt_strings.html
%% length-decoded-string
decode_string(<<Length:8, Value:Length/binary, Rest/binary>>) when Length < 16#FB ->
    {Value, Rest};
decode_string(<<16#FB:8, Rest/binary>>) ->
    {undefined, Rest};
decode_string(<<16#FC:8, Length:16/little, Value:Length/binary, Rest/binary>>) ->
    {Value, Rest};
decode_string(<<16#FD:8, Length:24/little, Value:Length/binary, Rest/binary>>) ->
    {Value, Rest};
decode_string(<<16#FE:8, Length:64/little, Value:Length/binary, Rest/binary>>) ->
    {Value, Rest}.

%% length-encode-string
encode_string(Value) ->
    <<(encode_integer(byte_size(Value)))/binary, Value/binary>>.

%% decode ok packet
decode_ok_packet(Packet) ->
    {AffectedRows, Rest2} = decode_integer(Packet),
    {InsertId, Rest3} = decode_integer(Rest2),
    <<Status:16/little, WarningsNumber:16/little, Message/binary>> = Rest3,
    #ok{affected_rows = AffectedRows, insert_id = InsertId, status = Status, warnings_number = WarningsNumber, message = Message}.

%% decode eof result
decode_eof_packet(<<WarningsNumber:16/little, Status:16/little>>) ->
    #eof{warnings_number = WarningsNumber, status = Status}.

%% decode error packet
decode_error_packet(<<Code:16/little, Status:6/binary-unit:8, Message/binary>>) ->
    #error{code = Code, status = Status, message = Message}.

%%%===================================================================
%%% data tool part
%%%===================================================================
%% bin log type define in mysql release include/binary_log_types.h
-define(MYSQL_TYPE_DECIMAL,                           0).
-define(MYSQL_TYPE_TINY,                              1).
-define(MYSQL_TYPE_SHORT,                             2).
-define(MYSQL_TYPE_LONG,                              3).
-define(MYSQL_TYPE_FLOAT,                             4).
-define(MYSQL_TYPE_DOUBLE,                            5).
-define(MYSQL_TYPE_NULL,                              6).
-define(MYSQL_TYPE_TIMESTAMP,                         7).
-define(MYSQL_TYPE_LONGLONG,                          8).
-define(MYSQL_TYPE_INT24,                             9).
-define(MYSQL_TYPE_DATE,                              10).
-define(MYSQL_TYPE_TIME,                              11).
-define(MYSQL_TYPE_DATETIME,                          12).
-define(MYSQL_TYPE_YEAR,                              13).
-define(MYSQL_TYPE_NEW_DATE,                          14).
-define(MYSQL_TYPE_VARCHAR,                           15).
-define(MYSQL_TYPE_BIT,                               16).
-define(MYSQL_TYPE_TIMESTAMP2,                        17).
-define(MYSQL_TYPE_DATETIME2,                         18).
-define(MYSQL_TYPE_TIME2,                             19).
-define(MYSQL_TYPE_JSON,                              245).
-define(MYSQL_TYPE_NEW_DECIMAL,                       246).
-define(MYSQL_TYPE_ENUM,                              247).
-define(MYSQL_TYPE_SET,                               248).
-define(MYSQL_TYPE_TINY_BLOB,                         249).
-define(MYSQL_TYPE_MEDIUM_BLOB,                       250).
-define(MYSQL_TYPE_LONG_BLOB,                         251).
-define(MYSQL_TYPE_BLOB,                              252).
-define(MYSQL_TYPE_VAR_STRING,                        253).
-define(MYSQL_TYPE_STRING,                            254).
-define(MYSQL_TYPE_GEOMETRY,                          255).

%% integer format
convert_type(_,                                       undefined) -> undefined;
convert_type(?MYSQL_TYPE_DECIMAL,                     Value)     -> try binary_to_float(Value) catch _:_ -> binary_to_integer(Value) end;
convert_type(?MYSQL_TYPE_TINY,                        Value)     -> binary_to_integer(Value);
convert_type(?MYSQL_TYPE_SHORT,                       Value)     -> binary_to_integer(Value);
convert_type(?MYSQL_TYPE_LONG,                        Value)     -> binary_to_integer(Value);
convert_type(?MYSQL_TYPE_FLOAT,                       Value)     -> try binary_to_float(Value) catch _:_ -> binary_to_integer(Value) end;
convert_type(?MYSQL_TYPE_DOUBLE,                      Value)     -> try binary_to_float(Value) catch _:_ -> binary_to_integer(Value) end;
convert_type(?MYSQL_TYPE_NULL,                        Value)     -> Value;
convert_type(?MYSQL_TYPE_TIMESTAMP,                   <<Y:4/binary, "-", Mo:2/binary, "-", D:2/binary, " ", H:2/binary, ":", Mi:2/binary, ":", S:2/binary>>) -> {{binary_to_integer(Y), binary_to_integer(Mo), binary_to_integer(D)}, {binary_to_integer(H), binary_to_integer(Mi), binary_to_integer(S)}};
convert_type(?MYSQL_TYPE_LONGLONG,                    Value)     -> binary_to_integer(Value);
convert_type(?MYSQL_TYPE_INT24,                       Value)     -> binary_to_integer(Value);
convert_type(?MYSQL_TYPE_DATE,                        <<Y:4/binary, "-", M:2/binary, "-", D:2/binary>>) -> {binary_to_integer(Y), binary_to_integer(M), binary_to_integer(D)};
convert_type(?MYSQL_TYPE_TIME,                        <<H:2/binary, ":", M:2/binary, ":", S:2/binary>>) -> {binary_to_integer(H), binary_to_integer(M), binary_to_integer(S)};
convert_type(?MYSQL_TYPE_DATETIME,                    <<Y:4/binary, "-", Mo:2/binary, "-", D:2/binary, " ", H:2/binary, ":", Mi:2/binary, ":", S:2/binary>>) -> {{binary_to_integer(Y), binary_to_integer(Mo), binary_to_integer(D)}, {binary_to_integer(H), binary_to_integer(Mi), binary_to_integer(S)}};
convert_type(?MYSQL_TYPE_YEAR,                        Value)     -> binary_to_integer(Value);
convert_type(?MYSQL_TYPE_NEW_DATE,                    Value)     -> Value;
convert_type(?MYSQL_TYPE_VARCHAR,                     Value)     -> Value;
convert_type(?MYSQL_TYPE_BIT,                         Value)     -> Value;
convert_type(?MYSQL_TYPE_JSON,                        Value)     -> Value;
convert_type(?MYSQL_TYPE_NEW_DECIMAL,                 Value)     -> try binary_to_float(Value) catch _:_ -> binary_to_integer(Value) end;
convert_type(?MYSQL_TYPE_ENUM,                        Value)     -> Value;
convert_type(?MYSQL_TYPE_SET,                         Value)     -> Value;
convert_type(?MYSQL_TYPE_TINY_BLOB,                   Value)     -> Value;
convert_type(?MYSQL_TYPE_MEDIUM_BLOB,                 Value)     -> Value;
convert_type(?MYSQL_TYPE_LONG_BLOB,                   Value)     -> Value;
convert_type(?MYSQL_TYPE_BLOB,                        Value)     -> Value;
convert_type(?MYSQL_TYPE_VAR_STRING,                  Value)     -> Value;
convert_type(?MYSQL_TYPE_STRING,                      Value)     -> Value;
convert_type(?MYSQL_TYPE_GEOMETRY,                    Value)     -> Value;
convert_type(Type,                                        _)     -> erlang:exit({unknown_field_type, Type}).
