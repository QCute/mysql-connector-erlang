-module(test_mysql_otp).
-export([test_mysql_otp/0]).

test_mysql_otp() ->
    io:format("start mysql-otp~n"),
    {ok, P} = mysql:start_link([{host, "localhost"}, {port, 3306}, {user, "root"}, {password, "root"}, {database, "test"}]),
    {T, _} = timer:tc(fun() ->
        lists:foreach(fun([T]) -> mysql:query(P, <<"select * from test.", T/binary>>) end, element(3, mysql:query(P, "SHOW TABLES")))
    end),
    io:format("mysql-otp: ~p~n", [T]).
