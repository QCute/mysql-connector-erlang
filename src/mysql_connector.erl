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
-export([execute/1, execute/2, execute/3]).
-export([query/1, query/2, query/3]).
-export([select/1, select/2, select/3]).
-export([insert/1, insert/2, insert/3]).
-export([update/1, update/2, update/3]).
-export([delete/1, delete/2, delete/3]).
%% prepare interface
-export([prepare/2, prepare/3, prepare/4]).
-export([execute_prepare/2, execute_prepare/3, execute_prepare/4]).
-export([drop_prepare/1, drop_prepare/2, drop_prepare/3]).
%% transaction
-export([transaction/1, transaction/2, transaction/3]).
%% get execute result info interface
-export([get_affected_rows/1, get_insert_id/1]).
-export([get_fields_info/1, get_rows_data/1]).
-export([get_error_code/1, get_error_status/1, get_error_message/1]).
%% application callbacks
-export([start/2, stop/1]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
%% export type
-export_type([config/0]).
-export_type([sql/0, connector/0]).
-export_type([affected_rows/0, insert_id/0, data/0, rows_data/0]).
-export_type([error_code/0, error_status/0, error_message/0]).
%% type
-type config() :: {host, string()} | {port, inet:port_number()} | {user, string()} | {password, string()} | {database, string()} | {encoding, string()}.
-type sql() :: string() | binary().
-type connector() :: pid() | atom().
-type affected_rows() :: non_neg_integer().
-type insert_id() :: non_neg_integer().
-type data() :: undefined | integer() | float() | binary() | [binary()] | calendar:date() | calendar:time() | calendar:datetime().
-type rows_data() :: [[data()]].
-type error_code() :: non_neg_integer().
-type error_status() :: binary().
-type error_message() :: binary().

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

%%%-------------------------------------------------------------------
%%% Records
%%%-------------------------------------------------------------------
%% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html
-record(ok, {
    affected_rows = 0 :: affected_rows(),
    insert_id = 0 :: insert_id(),
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
    code = 0 :: error_code(),
    status = <<>> :: error_status(),
    message = <<>> :: error_message()
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

%% field info
-record(field, {
    catalog = <<>>,
    schema = <<>>,
    table = <<>>,
    origin_table = <<>>,
    name = <<>>,
    origin_name = <<>>,
    metadata = 0,
    character_set = 0,
    length = 0,
    type = 0,
    flags = 0,
    decimals = 0
}).

%% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html
-record(data, {
    fields_info = [#field{}],
    fields_eof = #eof{} :: #eof{},
    rows_data = [] :: rows_data(),
    rows_eof = #eof{} :: #eof{}
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
-spec start_link(Args :: [config()]) -> {ok, pid()} | {error, {already_started, pid()}} | {error, term()}.
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
-spec execute(Sql :: sql()) -> #ok{} | #data{} | #error{}.
execute(Sql) ->
    execute(Sql, ?MODULE).

%% @doc execute
-spec execute(Sql :: sql(), Connector :: connector()) -> #ok{} | #data{} | #error{}.
execute(Sql, Connector) ->
    execute(Sql, Connector, infinity).

%% @doc execute
-spec execute(Sql :: sql(), Connector :: connector(), Timeout :: timeout()) -> #ok{} | #data{} | #error{}.
execute(Sql, Connector, Timeout) ->
    gen_server:call(Connector, {execute, Sql}, Timeout).

%% @doc query
-spec query(Sql :: sql()) -> affected_rows() | insert_id() | rows_data().
query(Sql) ->
    query(Sql, ?MODULE).

%% @doc query
-spec query(Sql :: sql(), Connector :: connector()) -> affected_rows() | insert_id() | rows_data().
query(Sql, Connector) ->
    query(Sql, Connector, infinity).

%% @doc query
-spec query(Sql :: sql(), Connector :: connector(), Timeout :: timeout()) -> affected_rows() | insert_id() | rows_data().
query(Sql, Connector, Timeout) ->
    case execute(Sql, Connector, Timeout) of
        #ok{insert_id = 0, affected_rows = AffectedRows} ->
            AffectedRows;
        #ok{insert_id = InsertId} ->
            InsertId;
        #data{rows_data = RowsData} ->
            RowsData;
        #error{code = Code, status = Status, message = Message} ->
            erlang:error({mysql_error, Sql, {Code, Status, Message}})
    end.

%% @doc select
-spec select(Sql :: sql()) -> rows_data().
select(Sql) ->
    select(Sql, ?MODULE).

%% @doc select
-spec select(Sql :: sql(), Connector :: connector()) -> rows_data().
select(Sql, Connector) ->
    select(Sql, Connector, infinity).

%% @doc select
-spec select(Sql :: sql(), Connector :: connector(), Timeout :: timeout()) -> rows_data().
select(Sql, Connector, Timeout) ->
    case execute(Sql, Connector, Timeout) of
        #data{rows_data = RowsData} ->
            RowsData;
        #error{code = Code, status = Status, message = Message} ->
            erlang:error({mysql_error, Sql, {Code, Status, Message}})
    end.

%% @doc insert
-spec insert(Sql :: sql()) -> affected_rows() | insert_id() | rows_data().
insert(Sql) ->
    insert(Sql, ?MODULE).

%% @doc insert
-spec insert(Sql :: sql(), Connector :: connector()) -> affected_rows() | insert_id() | rows_data().
insert(Sql, Connector) ->
    insert(Sql, Connector, infinity).

%% @doc insert
-spec insert(Sql :: sql(), Connector :: connector(), Timeout :: timeout()) -> affected_rows() | insert_id() | rows_data().
insert(Sql, Connector, Timeout) ->
    case execute(Sql, Connector, Timeout) of
        #ok{insert_id = 0, affected_rows = AffectedRows} ->
            AffectedRows;
        #ok{insert_id = InsertId} ->
            InsertId;
        #data{rows_data = RowsData} ->
            RowsData;
        #error{code = Code, status = Status, message = Message} ->
            erlang:error({mysql_error, Sql, {Code, Status, Message}})
    end.

%% @doc update
-spec update(Sql :: sql()) -> affected_rows().
update(Sql) ->
    update(Sql, ?MODULE).

%% @doc update
-spec update(Sql :: sql(), Connector :: connector()) -> affected_rows().
update(Sql, Connector) ->
    update(Sql, Connector, infinity).

%% @doc update
-spec update(Sql :: sql(), Connector :: connector(), Timeout :: timeout()) -> affected_rows().
update(Sql, Connector, Timeout) ->
    case execute(Sql, Connector, Timeout) of
        #ok{affected_rows = AffectedRows} ->
            AffectedRows;
        #error{code = Code, status = Status, message = Message} ->
            erlang:error({mysql_error, Sql, {Code, Status, Message}})
    end.

%% @doc delete
-spec delete(Sql :: sql()) -> affected_rows() | rows_data().
delete(Sql) ->
    delete(Sql, ?MODULE).

%% @doc delete
-spec delete(Sql :: sql(), Connector :: connector()) -> affected_rows() | rows_data().
delete(Sql, Connector) ->
    delete(Sql, Connector, infinity).

%% @doc delete
-spec delete(Sql :: sql(), Connector :: connector(), Timeout :: timeout()) -> affected_rows() | rows_data().
delete(Sql, Connector, Timeout) ->
    case execute(Sql, Connector, Timeout) of
        #ok{affected_rows = AffectedRows} ->
            AffectedRows;
        #data{rows_data = RowsData} ->
            RowsData;
        #error{code = Code, status = Status, message = Message} ->
            erlang:error({mysql_error, Sql, {Code, Status, Message}})
    end.

%% @doc prepare
-spec prepare(Name :: atom(), Statement :: sql()) -> affected_rows().
prepare(Name, Statement) ->
    prepare(Name, Statement, ?MODULE).

%% @doc prepare
-spec prepare(Name :: atom(), Statement :: sql(), Connector :: connector()) -> affected_rows().
prepare(Name, Statement, Connector) ->
    prepare(Name, Statement, Connector, infinity).

%% @doc prepare
-spec prepare(Name :: atom(), Statement :: sql(), Connector :: connector(), Timeout :: timeout()) -> affected_rows().
prepare(Name, Statement, Connector, Timeout) ->
    query(<<"PREPARE ", (atom_to_binary(Name, utf8))/binary, " FROM '", (iolist_to_binary(Statement))/binary, "'">>, Connector, Timeout).

%% @doc execute prepare
-spec execute_prepare(Name :: atom(), Parameter :: sql()) -> affected_rows() | insert_id() | rows_data().
execute_prepare(Name, Parameter) ->
    execute_prepare(Name, Parameter, ?MODULE).

%% @doc execute prepare
-spec execute_prepare(Name :: atom(), Parameter :: sql(), Connector :: pid() | atom()) -> affected_rows() | insert_id() | rows_data().
execute_prepare(Name, Parameter, Connector) ->
    execute_prepare(Name, Parameter, Connector, infinity).

%% @doc execute prepare
-spec execute_prepare(Name :: atom(), Parameter :: sql(), Connector :: connector(), Timeout :: timeout()) -> affected_rows() | insert_id() | rows_data().
execute_prepare(Name, "", Connector, Timeout) ->
    query(<<"EXECUTE ", (atom_to_binary(Name, utf8))/binary>>, Connector, Timeout);
execute_prepare(Name, Parameter, Connector, Timeout) ->
    query(<<"EXECUTE ", (atom_to_binary(Name, utf8))/binary, " USING ", (iolist_to_binary(Parameter))/binary>>, Connector, Timeout).

%% @doc drop prepare
-spec drop_prepare(Name :: atom()) -> affected_rows().
drop_prepare(Name) ->
    drop_prepare(Name, ?MODULE).

%% @doc drop prepare
-spec drop_prepare(Name :: atom(), Connector :: connector()) -> affected_rows().
drop_prepare(Name, Connector) ->
    drop_prepare(Name, Connector, infinity).

%% @doc drop prepare
-spec drop_prepare(Name :: atom(), Connector :: connector(), Timeout :: timeout()) -> affected_rows().
drop_prepare(Name, Connector, Timeout) ->
    query(<<"DROP PREPARE ", (atom_to_binary(Name, utf8))/binary>>, Connector, Timeout).

%% @doc transaction
-spec transaction(F :: fun(() -> term())) -> term().
transaction(F) ->
    transaction(F, ?MODULE).

%% @doc transaction
-spec transaction(F :: fun(() -> term()), Connector :: connector()) -> term().
transaction(F, Connector) ->
    transaction(F, Connector, infinity).

%% @doc transaction
-spec transaction(F :: fun(() -> term()), Connector :: connector(), Timeout :: timeout()) -> term().
transaction(F, Connector, Timeout) ->
    try
        execute(<<"BEGIN">>, Connector, Timeout),
        Result = F(),
        execute(<<"COMMIT">>, Connector, Timeout),
        Result
    catch Class:Reason ->
        execute(<<"ROLLBACK">>, Connector, Timeout),
        erlang:raise(Class, Reason, [])
    end.

%% @doc extract the affected RowsData from MySQL result on update
-spec get_affected_rows(Result :: #ok{}) -> affected_rows().
get_affected_rows(#ok{affected_rows = AffectedRows}) ->
    AffectedRows.

%% @doc extract the insert id from MySQL result on insert
-spec get_insert_id(Result :: #ok{}) -> insert_id().
get_insert_id(#ok{insert_id = InsertId}) ->
    InsertId.

%% @doc extract the fields info from MySQL result on data received
-spec get_fields_info(Result :: #data{}) -> [#field{}].
get_fields_info(#data{fields_info = FieldsInfo}) ->
    FieldsInfo.

%% @doc extract the data RowsData from MySQL result on data received
-spec get_rows_data(Result :: #data{}) -> rows_data().
get_rows_data(#data{rows_data = RowsData}) ->
    RowsData.

%% @doc extract the error code from MySQL result on error
-spec get_error_code(Result :: #error{}) -> error_code().
get_error_code(#error{code = Code}) ->
    Code.

%% @doc extract the error sql status from MySQL result on error
-spec get_error_status(Result :: #error{}) -> error_status().
get_error_status(#error{status = Status}) ->
    Status.

%% @doc extract the error message from MySQL result on error
-spec get_error_message(Result :: #error{}) -> error_message().
get_error_message(#error{message = Message}) ->
    Message.

%%%===================================================================
%%% application callbacks
%%%===================================================================
%% @doc start
-spec start(StartType :: term(), StartArgs :: term()) -> {ok, pid()} | {error, {already_started, pid()}} | {error, term()}.
start(_, _) ->
    {ok, Args} = application:get_env(?MODULE),
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%% @doc stop
-spec stop(State :: term()) -> ok.
stop(_) ->
    ok.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
%% @doc init
-spec init(Args :: term()) -> {ok, State :: #state{}, Timeout :: timeout()}.
init(Args) ->
    process_flag(trap_exit, true),
    process_flag(trap_exit, true),
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
            %% set database and encoding
            set_base(NewState, Database, Encoding),
            %% succeeded
            {ok, NewState, 60 * 1000};
        {error, Reason} ->
            erlang:error(Reason)
    end.

%% @doc handle_call
-spec handle_call(Request :: term(), From :: term(), State :: #state{}) -> {reply, Reply :: term(), State :: #state{}, Timeout :: timeout()}.
handle_call({execute, Sql}, _From, State) ->
    {reply, execute_sql(State, Sql), State, 60 * 1000};

handle_call(_Request, _From, State) ->
    {reply, ok, State, 60 * 1000}.

%% @doc handle_cast
-spec handle_cast(Request :: term(), State :: #state{}) -> {noreply, NewState :: #state{}, Timeout :: timeout()}.
handle_cast(_Request, State) ->
    {noreply, State, 60 * 1000}.

%% @doc handle_info
-spec handle_info(Info :: term(), State :: #state{}) -> {noreply, NewState :: #state{}, Timeout :: timeout()}.
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
%%% database about part
%%%===================================================================
%% set base
set_base(State, Database, Encoding) ->
    %% change database
    ChangeDatabaseResult = change_database(State, Database),
    not is_record(ChangeDatabaseResult, ok) andalso erlang:error(ChangeDatabaseResult),
    %% set encoding
    SetEncodingResult = set_encoding(State, Encoding),
    not is_record(SetEncodingResult, ok) andalso erlang:error(SetEncodingResult).

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
%%% ping
%%%===================================================================
%% ping
ping(State) ->
    Packet = <<?COM_PING>>,
    %% query packet sequence number start with 0
    send_packet(State, Packet, <<0:8>>),
    %% get result now
    PingResult = handle_execute_result(State, <<1:8>>, <<>>),
    not is_record(PingResult, ok) andalso erlang:error(PingResult).

%%%===================================================================
%%% quit
%%%===================================================================
%% quit
quit(State) ->
    Packet = <<?COM_QUIT>>,
    %% Server closes the connection or returns ERR_Packet.
    send_packet(State, Packet, <<0:8>>).

%%%===================================================================
%%% execute sql request part
%%%===================================================================
%% execute sql
execute_sql(State, Sql) ->
    Packet = <<?COM_QUERY, (iolist_to_binary(Sql))/binary>>,
    %% packet sequence number start with 0
    send_packet(State, Packet, <<0:8>>),
    %% get response now
    handle_execute_result(State, <<1:8>>, <<>>).

%% handle execute result
handle_execute_result(State, Number = <<Seq:8>>, <<Length:24/little, Number:1/binary-unit:8, Packet:Length/binary-unit:8, Rest/binary>>) ->
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
handle_execute_result(_, <<Number:8>>, <<Length:24/little, SequenceNumber:8, _:Length/binary-unit:8, _/binary>>) ->
    %% chaos sequence number
    erlang:error(<<"Got packets out of order (self): ", (integer_to_binary(Number))/binary, " (server): ", (integer_to_binary(SequenceNumber))/binary>>);
handle_execute_result(State, Number, Data) ->
    InData = receive_data(State),
    handle_execute_result(State, Number, <<Data/binary, InData/binary>>).

%% decode fields info, read n field packet, read an eof packet
decode_fields_info(State, Number = <<Seq:8>>, <<Length:24/little, Number:1/binary-unit:8, Packet:Length/binary-unit:8, Rest/binary>>, List) ->
    case Packet of
        <<?EOF:8, PacketRest:4/binary>> ->
            %% eof packet
            %% if (not capabilities & CLIENT_DEPRECATE_EOF)
            %% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
            FieldsInfo = lists:reverse(List),
            decode_rows(State, <<(Seq + 1)>>, Rest, FieldsInfo, PacketRest, []);
        _ ->
            %% column definition
            %% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html
            %% catalog
            {Catalog, CatalogLength} = decode_string(Packet),
            <<_:CatalogLength/binary, CatalogRest/binary>> = Packet,
            %% schema
            {Schema, SchemaLength} = decode_string(CatalogRest),
            <<_:SchemaLength/binary, SchemaRest/binary>> = CatalogRest,
            %% table
            {Table, TableLength} = decode_string(SchemaRest),
            <<_:TableLength/binary, TableRest/binary>> = SchemaRest,
            %% origin table
            {OriginTable, OriginTableLength} = decode_string(TableRest),
            <<_:OriginTableLength/binary, OriginTableRest/binary>> = TableRest,
            %% name
            {Name, NameLength} = decode_string(OriginTableRest),
            <<_:NameLength/binary, NameRest/binary>> = OriginTableRest,
            %% origin name
            {OriginName, OriginNameLength} = decode_string(NameRest),
            <<_:OriginNameLength/binary, OriginNameRest/binary>> = NameRest,
            %% extract packet
            %% character set
            %% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_character_set.html
            %% flags
            %% https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html
            <<Metadata:8/little, CharacterSet:16/little, FieldLength:32/little, Type:8/little, Flags:16/little, Decimals:8/little, _Rest7/binary>> = OriginNameRest,
            %% field info
            Field = #field{
                catalog = Catalog,
                schema = Schema,
                table = Table,
                origin_table = OriginTable,
                name = Name,
                origin_name = OriginName,
                metadata = Metadata,
                character_set = CharacterSet,
                length = FieldLength,
                type = Type,
                flags = Flags,
                decimals = Decimals
            },
            decode_fields_info(State, <<(Seq + 1):8>>, Rest, [Field | List])
    end;
decode_fields_info(_, <<Number:8>>, <<Length:24/little, SequenceNumber:8, _:Length/binary-unit:8, _/binary>>, _) ->
    %% chaos sequence number
    erlang:error(<<"Got packets out of order (self): ", (integer_to_binary(Number))/binary, " (server): ", (integer_to_binary(SequenceNumber))/binary>>);
decode_fields_info(State, Number, Data, List) ->
    InData = receive_data(State),
    decode_fields_info(State, Number, <<Data/binary, InData/binary>>, List).

%% decode RowsData, read n field packet, read an eof packet
decode_rows(State, Number = <<Seq:8>>, <<Length:24/little, Number:1/binary-unit:8, Packet:Length/binary-unit:8, Rest/binary>>, FieldsInfo, FieldsRest, List) ->
    case Packet of
        <<?EOF:8, PacketRest:4/binary>> ->
            %% if capabilities & CLIENT_DEPRECATE_EOF
            %% ok packet
            %% else eof packet
            %% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
            FieldsEof = decode_eof_packet(FieldsRest),
            RowsData = lists:reverse(List),
            RowsEof = decode_eof_packet(PacketRest),
            #data{fields_info = FieldsInfo, fields_eof = FieldsEof, rows_data = RowsData, rows_eof = RowsEof};
        _ ->
            This = decode_fields(FieldsInfo, Packet, []),
            decode_rows(State, <<(Seq + 1):8>>, Rest, FieldsInfo, FieldsRest, [This | List])
    end;
decode_rows(_, <<Number:8>>, <<Length:24/little, SequenceNumber:8, _:Length/binary-unit:8, _/binary>>, _, _, _) ->
    %% chaos sequence number
    erlang:error(<<"Got packets out of order (self): ", (integer_to_binary(Number))/binary, " (server): ", (integer_to_binary(SequenceNumber))/binary>>);
decode_rows(State, Number, Data, FieldsInfo, FieldsRest, List) ->
    InData = receive_data(State),
    decode_rows(State, Number, <<Data/binary, InData/binary>>, FieldsInfo, FieldsRest, List).

%% decode field
decode_fields([], <<>>, List) ->
    lists:reverse(List);
decode_fields([#field{type = Type} | Fields], <<Packet/binary>>, List) ->
    {Column, Length} = decode_string(Packet),
    This = convert_type(Type, Column),
    <<_:Length/binary, Rest/binary>> = Packet,
    decode_fields(Fields, Rest, [This | List]).

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
convert_type(?MYSQL_TYPE_SET,                         Value)     -> binary:split(Value, <<",">>);
convert_type(?MYSQL_TYPE_TINY_BLOB,                   Value)     -> Value;
convert_type(?MYSQL_TYPE_MEDIUM_BLOB,                 Value)     -> Value;
convert_type(?MYSQL_TYPE_LONG_BLOB,                   Value)     -> Value;
convert_type(?MYSQL_TYPE_BLOB,                        Value)     -> Value;
convert_type(?MYSQL_TYPE_VAR_STRING,                  Value)     -> Value;
convert_type(?MYSQL_TYPE_STRING,                      Value)     -> Value;
convert_type(?MYSQL_TYPE_GEOMETRY,                    Value)     -> Value;
convert_type(Type,                                        _)     -> erlang:error({unknown_field_type, Type}).

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
