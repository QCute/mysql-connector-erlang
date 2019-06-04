-module(test_mysql).
-export([test_mysql/0]).

test_mysql() ->
    io:format("start mysql~n"),
    {ok, _} = mysql:start_link(1, "localhost", 3306, "root", "root", "test", fun(_, _, _, _) -> ok end, utf8mb4),
    {T, _} = timer:tc(fun() ->
        lists:foreach(fun([T]) -> mysql:fetch(1, <<"select * from test.", T/binary>>) end, element(3, element(2, mysql:fetch(1, "SHOW TABLES"))))
    end),
    io:format("mysql: ~p~n", [T]).
