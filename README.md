# mysql-connector-erlang
* erlang mysql connector in single file
* connect/query/handle by single process
* support version 4.1 or later (8.x caching_sha2_password plugin supported)
* arguments pass by tuple list(config file compatibility) instead maps(otp 17 early supported)
* nice pool compatibility
* quick and easy to integrate in your project

# quick start
* Add to rebar.config
```
{deps, [
  ...
  {mysql_connector, {git, "https://github.com/QCute/mysql-connector-erlang.git", {branch, "master"}}}
]}.
```

* Add mysql_connector.app start
```
application:start(mysql_connector).
```

* start in project
```
ConnectorArgs = [{host, "localhost"}, {port,3306}, {user,"root"}, {password,"root"}, {database,"test"}, {encoding, "utf8mb4"}],
{ok, Pid} = mysql_connector:start_link(ConnectorArgs),
mysql_connector:select("select 1", Pid).
```

* start with [pool](https://github.com/QCute/volley)
```
ConnectorArgs = [{host, "localhost"}, {port, 3306}, {user, "root"}, {password, "root"}, {database, "test"}], 
PoolArgs = [{worker, {mysql_connector, start_link, [ConnectorArgs]}}, {size, 4}],
%% example use volley 
volley:start_pool(mysql_connector, PoolArgs).
Worker = volley:get(mysql_connector),
mysql_connector:select("select 1", Worker).
```

# Performance

|       Driver                                                     |    Test 1    |    Test 2    |    Test 3    |
|------------------------------------------------------------------|--------------|--------------|--------------|
|[mysq-connector](https://github.com/QCute/mysql-connector-erlang) |  **636052**  |  **630514**  |  **615399**  |
|[mysql-otp](https://github.com/mysql-otp/mysql-otp.git)           |   1220338    |   1011661    |    987828    | 
|[Emysql](https://github.com/Eonblast/Emysql)                      |    640698    |    671094    |    653903    |
|[mysql](https://github.com/dizzyd/erlang-mysql-driver.git)        |    982400    |    838034    |    836162    |
|[p1_mysql](https://github.com/processone/p1_mysql.git)            |   1146160    |   1091125    |   1051280    |
