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
%% Includes
-include("mysql_connector.hrl").
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
-spec get_fields_info(Result :: #data{}) -> fields_info().
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
    mysql_connector_startup:connect(Args).

%% @doc handle_call
-spec handle_call(Request :: term(), From :: term(), State :: #state{}) -> {reply, Reply :: term(), State :: #state{}, Timeout :: timeout()}.
handle_call({execute, Sql}, _From, State) ->
    {reply, mysql_connector_executor:execute(State, Sql), State, 60 * 1000};

handle_call(_Request, _From, State) ->
    {reply, ok, State, 60 * 1000}.

%% @doc handle_cast
-spec handle_cast(Request :: term(), State :: #state{}) -> {noreply, NewState :: #state{}, Timeout :: timeout()}.
handle_cast(_Request, State) ->
    {noreply, State, 60 * 1000}.

%% @doc handle_info
-spec handle_info(Info :: term(), State :: #state{}) -> {noreply, NewState :: #state{}, Timeout :: timeout()}.
handle_info(timeout, State) ->
    mysql_connector_executor:ping(State),
    {noreply, State, 60 * 1000};

handle_info(_Info, State) ->
    {noreply, State, 60 * 1000}.

%% @doc terminate
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()), State :: #state{}) -> {ok, NewState :: #state{}}.
terminate(_Reason, State) ->
    mysql_connector_executor:quit(State),
    {ok, State}.

%% @doc code_change
-spec code_change(OldVsn :: (term() | {down, term()}), State :: #state{}, Extra :: term()) -> {ok, NewState :: #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
