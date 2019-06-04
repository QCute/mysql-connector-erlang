%%%-------------------------------------------------------------------
%%% @doc
%%% mysql-connector-erlang
%%% * erlang mysql connector
%%% * connect/query/handle by single process
%%% * support version 4.1 or later (8.x caching_sha2_password plugin supported)
%%% * arguments pass by tuple list(config file compatibility) instead maps(otp 17 early supported)
%%% * nice pool compatibility
%%% * quick and easy to integrate in your project
%%% @end
%%%-------------------------------------------------------------------

%% export type
-export_type([config/0]).
-export_type([sql/0, connector/0]).
-export_type([affected_rows/0, insert_id/0, field/0, fields_info/0, data/0, rows_data/0]).
-export_type([error_code/0, error_status/0, error_message/0]).
%% type
-type config() :: {host, string()} | {port, inet:port_number()} | {user, string()} | {password, string()} | {database, string()} | {encoding, string()}.
-type sql() :: list() | binary() | string().
-type connector() :: pid() | atom().
-type affected_rows() :: non_neg_integer().
-type insert_id() :: non_neg_integer().
-type field() :: {Name :: binary(), Type :: non_neg_integer(), Flags :: non_neg_integer(), Decimals :: non_neg_integer()}.
-type fields_info() :: [field()].
-type data() :: undefined | integer() | float() | binary() | calendar:date() | calendar:time() | calendar:datetime().
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

%% https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html
-record(data, {
    fields_info = [] :: fields_info(),
    fields_eof = #eof{} :: #eof{},
    rows_data = [] :: rows_data(),
    rows_eof = #eof{} :: #eof{}
}).

%% mysql connection state
-record(state, {
    socket_type :: gen_tcp | ssl,
    socket :: gen_tcp:socket() | ssl:sslsocket()
}).
