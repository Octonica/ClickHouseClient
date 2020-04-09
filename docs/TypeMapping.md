# Type mappings
| clickhouse | .net | DataReader API |
|---|---|---|
| Int8 | sbyte | GetFieldValue\<sbyte\>(), GetInt32()|
| Int16 | short | GetInt16()|
| Int32 | int | GetInt32()|
| Int64 | long | GetInt64()|
| UInt8 | short | GetInt16()|
| UInt16| ushort | GetInt16()|
| UInt32| uint | GetFieldValue\<UInt32\>, GetInt64()|
|~~UInt64~~| ulong | GetFieldValue\<UInt64\>|
| Float32 | float | GetFloat()|
| Float64 | double | GetDouble()|
| String | string | GetString()|
| FixedString | byte[] | GetValue()|