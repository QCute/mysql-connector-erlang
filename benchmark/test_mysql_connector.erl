-module(test_mysql_connector).
-export([test_mysql_connector/0]).

test_mysql_connector() ->
    io:format("start mysql-connector~n"),
    {ok, M} = mysql_connector:start_link([{host, "localhost"}, {port, 3306}, {user, "root"}, {password, "root"}, {database, "test"}, {encoding, "utf8mb4"}]),
    {T, _} = timer:tc(fun() ->
        lists:foreach(fun([T]) -> mysql_connector:query(<<"select * from test.", T/binary>>, M) end, mysql_connector:query("SHOW TABLES", M))
    end),
    io:format("mysql-connector: ~p~n", [T]).
