ClickHouse .NET Core driver
===============

This is an implementation of .NET Core driver for ClickHouse in a form of ADO.NET DbProvider API. This driver supports all ADO.NET features (with some exclusions like transaction support).

### Features
* supports binary protocol
* compression (send and recieve)
* timezones
* most clickhouse [column types](docs/TypeMapping.md) are supported ([aggregating ones](https://clickhouse.tech/docs/en/sql_reference/data_types/aggregatefunction/) are under development)
* full support for .net async ADO.NET API
* no unsafe code
* ~~tested~~- used in production
* c# named tuple and record support
* [Dapper](https://dapperlib.github.io/Dapper/) support (example in #19)

### Usage
Install from [NuGet](https://www.nuget.org/packages/Octonica.ClickHouseClient/):
```
dotnet add package Octonica.ClickHouseClient
```

ConnectionString syntax: 
`Host=<host>;Port=<port>;Database=<db>;Password=<pass>`, e.g. `"Host=127.0.0.1;Password=P@ssw0rd; Database=db` additionally, if you want to build a connection string via code you can use `ClickHouseConnectionStringBuilder`.

Entry point for API is ADO .NET DbConnection Class: `Octonica.ClickHouse.ClickHouseConnection`.

### Extended API
In order to provide non-ADO.NET complaint data manipulation functionality, proprietary ClickHouseColumnWriter API exists.
Entry point for API is `ClickHouseConnection#CreateColumnWriter()` method.

#### Simple SELECT async verison
```csharp
var sb = new ClickHouseConnectionStringBuilder();
sb.Host = "192.168.121.143";
using var conn = new ClickHouseConnection(sb);
await conn.OpenAsync();
var currentUser = await conn.CreateCommand("select currentUser()").ExecuteScalarAsync();
```
#### Insert data with parameters
```csharp
var sb = new ClickHouseConnectionStringBuilder();
sb.Host = "127.0.0.1";
using var conn = new ClickHouseConnection(sb);
conn.Open();
using var cmd = conn.CreateCommand("INSERT INTO table_you_just_created SELECT {id}, {dt}");
cmd.Parameters.AddWithValue("id", Guid.NewGuid());
cmd.Parameters.AddWithValue("dt", DateTime.Now, System.Data.DbType.DateTime);
var _ = cmd.ExecuteNonQuery();
```

### Build requirements
In order to build the driver you need to have .NET Core SDK 3.1 or higher.
