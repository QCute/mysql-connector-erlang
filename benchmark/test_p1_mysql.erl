-module(test_p1_mysql).
-export([test_p1_mysql/0]).

test_p1_mysql() ->
    io:format("start p1-mysql~n"),
    {ok, _} = p1_mysql:start_link(1, "localhost", 3306, "root", "root", "test", fun(_, _, _) -> ok end),
    {T, _} = timer:tc(fun() ->
        lists:foreach(fun([T]) -> p1_mysql:fetch(1, ["select * from test.", T]) end, element(3, element(2, p1_mysql:fetch(1, "SHOW TABLES"))))
    end),
    io:format("p1_mysql: ~p~n", [T]).
