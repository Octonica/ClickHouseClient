# ClickHouseColumnWriter

`ClickHouseColumnWriter` is a class dedicated for writing arbitrary large amount of rows to a table.
It writes tables in a columnar layout. It means that the table consists of several columns and
each column contains a list of values (cells) of a particular type. All columns of the table must contain
the same number of cells.

To create a writer call the method `ClickHouseConnection.CreateColumnWriter` (or `ClickHouseConnection.CreateColumnWriterAsync`).
```C#
using var connection = ClickHouseConnection(connectionStr);
connection.Open();

using var writer = connection.CreateColumnWriter("INSERT INTO some_table VALUES");
```

Please, note that the `INSERT` query for the writer must end with `VALUES` keyword, but without actual list of values.

Some methods of `ClickHouseColumnWriter` are similar to methods of `ClickHouseDataReader`: `GetName`, `GetOrdinal`, `GetFieldType`,
`ConfigureColumn` and other methods for manipulating column metadata.

Here is the method for writing tables:
```C#
void WriteTable(IReadOnlyList<object?> columns, int rowCount)
```

A column could be of any type which implements one of interfaces:
* `IReadOnlyList<T>`;
* `IList<T>`;
* `IEnumerable<T>`;
* `IAsyncEnumerable<T>` (supported only by `WriteTableAsync`);
* `IEnumerable`.

The number of columns must be equal to the number of columns in the initial `INSERT` query. Columns may have different number of rows,
but not less than `rowCount`. Columns must be passed in the order defined by the query.

There is an overload of `WriteTable` which distinguishes columns by their names:
```C#
void WriteTable(IReadOnlyDictionary<string, object?> columns, int rowCount)
```

## Examples

Assume there is a table `some_table`.
```C#
using var connection = new ClickHouseConnection(connectionStr);
connection.Open();
var cmd = connection.CreateCommand("CREATE TABLE some_table(id Int32, str Nullable(String), dt DateTime, val Decimal64(4)) ENGINE = Memory");
cmd.ExecuteNonQuery();
```

### Write ordered columns
```C#
var id = new List<Guid>();
var str = new List<string?>();
var dt = new List<DateTime>();
var val = new List<decimal>();

/*
 * Fill lists id, str, dt and val with actual values
 */
 
await using var connection = new ClickHouseConnection(connectionStr);
await connection.OpenAsync();

await using var writer = connection.CreateColumnWriter("INSERT INTO some_table VALUES");

var columns = new object[writer.FieldCount];
columns[writer.GetOrdinal("id")] = id;
columns[writer.GetOrdinal("str")] = str;
columns[writer.GetOrdinal("dt")] = dt;
columns[writer.GetOrdinal("val")] = val;

var rowCount = id.Count;
await writer.WriteTableAsync(columns, rowCount, CancellationToken.None);
```

### Write named columns
```C#
var id = new List<Guid>();
var dt = new List<DateTime>();
var val = new List<decimal>();

/*
 * Fill lists id, dt and val with actual values
 */
 
await using var connection = new ClickHouseConnection(connectionStr);
await connection.OpenAsync();

await using var writer = connection.CreateColumnWriter("INSERT INTO some_table(id, dt, val) VALUES");

var columns = new Dictionary<string, object?>
{
	["id"] = id,
	["dt"] = dt,
	["val"] = val
};

var rowCount = id.Count;
await writer.WriteTableAsync(columns, rowCount, CancellationToken.None);
```