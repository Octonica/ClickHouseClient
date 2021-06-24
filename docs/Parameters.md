# Parameters

ClickHouseClient's implementation of parameters is compliant with ADO.NET. API for working with parameters intended to be familiar to users of
other ADO.NET drivers.

Each command (`Octonica.ClickHouseClient.ClickHouseCommand`) contains a collection of parameters. This collection can be acquired with the
property `Parameters`. Parameters from this collection can be referenced in a query.

## Parameter format

As specified in [Queries with Parameters](https://clickhouse.tech/docs/en/interfaces/cli/#cli-queries-with-parameters) the default format
of the parameter is `{<name>:<data type>}`. However, the type can be derived from the parameter's settings, which allows to omit `:<data type>`
part and declare parameter just as `{<name>}`.

ClickHouseClient also supports parameters in MSSQL-like format: `@<name>`.

Here is an example demonstrating different namestyles of parameters:
```C#
using var connection = new ClickHouseConnection(connectionStr);
connection.Open();

var cmd = connection.CreateCommand("SELECT number/{value:Decimal64(2)} FROM numbers(100000) WHERE number >= @min AND number <= {max}");
cmd.Parameters.AddWithValue("value", 100);
cmd.Parameters.AddWithValue("{min}", 1000);
cmd.Parameters.AddWithValue("@max", 2000);

using var reader = cmd.ExecuteReader();
while(reader.Read())
{
  // Reading the data
}
```

## Parameter settings

The settings of the parameter usually can be detected based on the type of the parameter's value. It is possible to override auto-detected settings
in cases when auto-detection fails or when the required type of the parameter doesn't match to the type of the parameter's value. When settings are
overridden ClickHouseClient will try to convert parameter's value to the requested type.

There are settings inherited from `System.Data.Common.DbParameter`:
1. `DbType`. The type of the parameter. `System.Data.DbType` is a subset of `Octonica.ClickHouseClient.ClickHouseDbType`. For any value of the property
  `ClickHouseDbType` which can't be mapped to the type `System.Data.DbType` this property returns `DbType.Object`;
2. `IsNullable`. Indicates whether the parameter accept `NULL`;
3. `Precision`. Defines the precision for `Decimal` or `DateTime64`;
4. `Scale`. Defines the scale for `Decimal`;
5. `Size`. Defines the size for `FixedString`.

And there are additional settings supported by `Octonica.ClickHouseClient.ClickHouseParameter`:
1. `ArrayRank`. The number of dimensions in the array. Zero for non-arrays;
2. `ClickHouseDbType`. The type of the parameter;
3. `IsArray`. Indicates whether the parameter is an array, i.e. `ArrayRank > 0`;
4. `StringEncoding`. Defines the encoding which will be used for strings;
5. `TimeZone`. Defines the timezone for `DateTime` or `DateTime64`.

## Table-valued parameters

To be fair, a parameter can't be a table. However, ClickHouse allows to pass arbitrary tables with a query. These tables can be referenced in the query
without special syntax.

The tables and parameters are stored separately in `Octonica.ClickHouseClient.ClickHouseCommand`. The collection of tables can be acquired with the property
`TableProviders` of the command.

The basic interface for client-defined table is `Octonica.ClickHouseClient.IClickHouseTableProvider`. A class implementing this interface should provide
a table in a columnar format. There is a default implementation of this interface: `Octonica.ClickHouseClient.ClickHouseTableProvider`. This class allows
to pass a table in a way similar to [ClickHouseColumnWriter](docs/ClickHouseColumnWriter.md).

Here is a simple example demonstrating how to pass a client-defined table to a query:
```C#
using var connection = new ClickHouseConnection(connectionStr);
connection.Open();

var cmd = connection.CreateCommand("SELECT ptable.id, ptable.user, ptable.ip FROM ptable");

var users = new[] {"user1", "user2", "admin1", "admin2"};
var ips = new[] {"1.1.1.1", "2.2.2.2", "127.0.0.1", "::ffff:192.0.2.1"};

var pTableProvider = new ClickHouseTableProvider("ptable", users.Length);
pTableProvider.Columns.AddColumn("id", Enumerable.Range(1, users.Length));
pTableProvider.Columns.AddColumn("user", users);

// The settings of the column are similar to the settings of parameter
var ipColumn = pTableProvider.Columns.AddColumn("ip", ips);
ipColumn.ClickHouseDbType = ClickHouseDbType.IpV6;
ipColumn.IsNullable = false;

cmd.TableProviders.Add(pTableProvider);

using var reader = cmd.ExecuteReader();
while(reader.Read())
{
  // Reading the data
}
```

And here is a bit more practical example demonstrating how a temporary table can be used with `IN` clause:
```C#
using var connection = new ClickHouseConnection(connectionStr);
connection.Open();

var cmd = cn.CreateCommand("SELECT toInt32(number) FROM numbers(100000) WHERE number IN param_table");

var tableProvider = new ClickHouseTableProvider("param_table", 100);
tableProvider.Columns.AddColumn(Enumerable.Range(500, int.MaxValue / 2));

cmd.TableProviders.Add(tableProvider);

using var reader = cmd.ExecuteReader();
while(reader.Read())
{
  // Reading only 100 rows
}
```

## Implementation details

Unfortunately, parameters are not supported by the ClickHouse binary protocol. Which means that it's a client-side feature. ClickHouseClient passes
parameters to the server as a table with one row. The name of this table is unique for each query. It is generated based on `Guid` so there should be
no collision with names of existing tables.

ClickHouseClient analyzes the query and substitutes parameters with `SELECT` subquery. For example, the query
```SQL
SELECT * FROM some_table WHERE id = {id:UInt32}
```
will be transformed before sending to the server to
```SQL
SELECT * FROM some_table WHERE id = (CAST((SELECT _b3dcef95634b4fcfbf67624a39ce2e85.id FROM _b3dcef95634b4fcfbf67624a39ce2e85) AS UInt32))
```
where `_b3dcef95634b4fcfbf67624a39ce2e85` is the name of the table with parameters.