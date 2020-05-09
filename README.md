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
  {volley, {git, "https://github.com/QCute/mysql-connector-erlang.git", {branch, "master"}}}
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
mysql_connector:select(Pid, "select 1").
```

* start with [pool](https://github.com/QCute/volley)
```
ConnectorArgs = [{host, "localhost"}, {port, 3306}, {user, "root"}, {password, "root"}, {database, "test"}], 
PoolArgs = [{worker, {mysql_connector, start_link, [ConnectorArgs]}}, {size, 4}],
%% example use volley 
volley:start_pool(mysql_connector, PoolArgs).
{ok, Worker} = volley:get(mysql_connector),
mysql_connector:select(Worker, "select 1").
```
