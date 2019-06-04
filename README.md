# mysql-connector-erlang  
* erlang mysql connector in single file
* connect/query/handle by single process  
* support version 4.1 or later (8.x caching_sha2_password plugin supported)  
* arguments pass by tuple list(config file compatibility) instead maps(otp 17 early supported)  
* nice pool compatibility
* quick and easy to integrate in your project

# quick start

* start
```
ConnectorArgs = [{host, "localhost"}, {port,3306}, {user,"root"}, {password,"root"}, {database,"test"}, {encoding, "utf8mb4"}],
{ok, Pid} = mysql_connector:start(ConnectorArgs),
mysql_connector:select(Pid, "select 1").
```

* start with [pool](https://github.com/QCute/volley)
```
ConnectorArgs = [{host, "localhost"}, {port,3306}, {user,"root"}, {password,"root"}, {database,"test"}, 
PoolArgs = [{worker, {?MODULE, start_link, [ConnectorArgs]}}, {size, 4}],
%% example use volley 
mysql_connector:start_pool(volley, start_pool, a_pool, PoolArgs).
{ok, Worker} = volley(an_pool),
mysql_connector:select(Worker, "select 1").
```
