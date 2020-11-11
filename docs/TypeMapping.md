# Type mappings
**ClickHouse type**. The type of the column.

**Default type**. This is the type returned by `Octonica.ClickHouseClient.ClickHouseDataReader.GetFieldType(int ordinal)`. The method `Octonica.ClickHouseClient.ClickHouseDataReader.GetValue(int ordinal)` returns either a value of the default type or `System.DBNull`.

**Supported types**. The value can be converted to one of this types.

**ClickHouseDataReader's method**. The method dedicated to the default type. 

You can get the value of one of supported types by calling `GetFieldValue<T>(int ordinal)` or `GetFieldValue<T>(int ordinal, T? nullValue)`. The latter doesn't throw an error on NULL value.
| ClickHouse type | Default type | Supported types | ClickHouseDataReader's method | 
|---|---|---|---|
| Int8 | sbyte | short, int, long | `GetByte` |
| Int16 | short | int, long | `GetInt16` |
| Int32 | int | long | `GetInt32` |
| Int64 | long | | `GetInt64` |
| UInt8 | byte | ushort, uint, ulong, int, long | |
| UInt16 | ushort | uint, ulong, int, long | `GetUInt16` |
| UInt32 | uint | ulong, long | `GetUInt132` |
| UInt64 | ulong | | `GetUInt64` |
| Float32 | float | double | `GetFloat` |
| Float64 | double | | `GetDouble` |
| Decimal | decimal | | `GetDecimal` |
| Date | System.DateTime | | `GetDateTime` |
| DateTime | System.DateTimeOffset | System.DateTime | `GetDateTimeOffset` |
| DateTime64 | System.DateTimeOffset | System.DateTime | `GetDateTimeOffset` |
| String | string | | `GetString` |
| FixedString | byte[] | string | |
| UUID | System.Guid | | `GetGuid` |
| IPv4 | System.Net.IPAddress | string, int, uint | |
| IPv6 | System.Net.IPAddress | string | |
| Enum8 | string | sbyte, short, int, long | |
| Enum16 | string | short, int, long | |
| Nothing | System.DBNull | | `GetValue` |
| Nullable(T) | T? | | |
| Array(T) | T[] | | |
| Tuple(T1, ... Tn) | System.Tuple<T1, ... Tn> | System.ValueTuple<T1, ... Tn> | |
| LowCardinality<T> | T | | The method for `T` |