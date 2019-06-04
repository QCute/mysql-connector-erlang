-module(test_emysql).
-export([test_emysql/0]).

test_emysql() ->
    io:format("start emysql~n"),
    crypto:start(),
    application:start(emysql),
    emysql:add_pool(test, [{size,1}, {user,"root"}, {password,"root"}, {database,"test"}, {encoding, utf8mb4}]),
    {T, _} = timer:tc(fun() ->
        lists:foreach(fun([T]) -> emysql:execute(test, <<"select * from test.", T/binary>>) end, element(4, emysql:execute(test, "SHOW TABLES")))
    end),
    io:format("emysql: ~p~n", [T]).
