### Octonica.ClickHouseClient v2.2.11, 2023-01-11

#### Bug Fix

* Fix null reference exception when the garbage collector calls the finalizer for `ClickHouseConnection` ([#70](https://github.com/Octonica/ClickHouseClient/issues/70)).

### Octonica.ClickHouseClient v2.2.10, 2022-12-30

#### New Feature

* Add support for the type `Bool` ([#56](https://github.com/Octonica/ClickHouseClient/issues/56)).

#### Bug Fix

* Return the correct non-generic enumerator for `ClickHouseParameterCollection` (PR [#65](https://github.com/Octonica/ClickHouseClient/pull/65)).

#### Improvement

* Remove arguments with default values from constructors of `ClickHouseConnection`. It makes possible to call the constructor `ClickHouseConnection(sting)` using reflection
 ([#54](https://github.com/Octonica/ClickHouseClient/issues/54)).

### Octonica.ClickHouseClient v2.2.9, 2022-04-27

#### New Feature

* New mode of passing parameters to a query - `Interpolate`. In this mode values are interpolated into the query text as constant literals.
 Parameter mode can be set for a connection (the property `ParametersMode` in the connection string), for a command (the property `ClickHouseCommand.ParametersMode`) or
 for a single parameter (`ClickHouseParameter.ParameterMode`)
 ([#49](https://github.com/Octonica/ClickHouseClient/issues/49), PR [#42](https://github.com/Octonica/ClickHouseClient/pull/42)).

#### Improvement

* Set `DateTimeKind.Unspecified` when cast a value of ClickHouse types `Date` and `Date32` to the .NET type `DateTime` (PR [#45](https://github.com/Octonica/ClickHouseClient/pull/45)).

### Octonica.ClickHouseClient v2.2.8, 2022-01-09

#### Bug Fix

* Fix getting a time zone from IANA code. This fix is only applicable to the .NET 6 version of ClickHouseClient running on Windows ([#40](https://github.com/Octonica/ClickHouseClient/issues/40)).

#### Improvement

* Make possible to open a connection to the server with an unrecognized time zone. The `TimeZoneNotFoundException` may be thrown later when reading
  the column of type `DateTime` or `DateTime64` ([#40](https://github.com/Octonica/ClickHouseClient/issues/40)).

#### Miscellaneous

* Default protocol revision is set to 54452. This change was made because the minimal protocol revison with profile events was updated in the ClickHouse v21.12.

### Octonica.ClickHouseClient v2.2.7, 2021-12-04

#### New Feature

* .NET 6.0 support ([#33](https://github.com/Octonica/ClickHouseClient/issues/33)):
  * New API for time zones. Remove dependency from the package TimeZoneConverter;
  * Change mapping of the ClickHouse type `Date` from `DateTime` to `DateOnly`. This affects the behavior of methods `ClickHouseDataReader.GetValue` and `ClickHouseDataReader.GetValues`;
  * Add the method `ClickHouseDataReader.GetDate` for reading values of types `Date` and `Date32`.
* Add methods to the `ClickHouseDataReader` for reading values of well-known types ([#38](https://github.com/Octonica/ClickHouseClient/issues/38)):
  * `GetBigInteger`;
  * `GetIPAddress`;
  * `GetSByte`;
  * `GetUInt16`;
  * `GetUInt32`;
  * `GetUInt64`.
* Add support for the type `Date32` ([#36](https://github.com/Octonica/ClickHouseClient/issues/36)).
* Add support for profile events. Profile events are disabled by default. To enable it set the value of the property `ClickHouseCommand.IgnoreProfileEvents` to `false`.
 Please note that the method `ClickHouseDataReader.NextResult` (or `NextResultAsync`) should be called for switching between regular data and profile events.

#### Bug Fix

* Fix reading empty values of the type `LowCardinality(String)` ([#37](https://github.com/Octonica/ClickHouseClient/issues/37)).

#### Miscellaneous

* Default protocol revision is set to 54450.

### Octonica.ClickHouseClient release v2.1.2, 2021-11-07

#### New Feature

* Add support for Transport Layer Security (TLS) connection ([#35](https://github.com/Octonica/ClickHouseClient/issues/35)).

### Octonica.ClickHouseClient release v2.1.1, 2021-09-16

#### Backward Incompatible Change

* Classes from the namespace `Octonica.ClickHouseClient` that are now sealed and therefore can't be inherited:
  * `ClickHouseColumnWriter`;
  * `ClickHouseCommand`;
  * `ClickHouseConnection`;
  * `ClickHouseConnectionSettings`;
  * `ClickHouseDataReader`;
  * `ClickHouseParameter`;
  * `ClickHouseParameterCollection`;
  * `ClickHouseServerInfo`;
  * `ClickHouseTableColumnCollection`;
  * `ClickHouseTableProvider`;
  * `ClickHouseTableProviderCollection`.
* Classes, enums and interfaces from the namespace `Octonica.ClickHouseClient.Protocol` that are no longer public:
  * `BlockFieldCodes`;
  * `CompressionAlgorithm`;
  * `IClickHouseTableWriter`;
  * `NullableObjTableColumn<TObj>`.
* The class `ClickHouseColumnSettings` was moved from the namespace `Octonica.ClickHouseClient.Protocol` to the namespace `Octonica.ClickHouseClient`.
* The class `Revisions` from the namespace `Octonica.ClickHouseClient.Protocol` was renamed to `ClickHouseProtocolRevisions`.

#### Improvement

* Add XML documentation comments to the NuGet package.

#### Bug Fix

* Fix reading and writing values of the type `Array(LowCardinality(T))` ([#34](https://github.com/Octonica/ClickHouseClient/issues/34)).
* Fix error handling for `ClickHouseColumnWriter`.

### Octonica.ClickHouseClient release v1.3.1, 2021-07-13

#### New Feature

* Values of the type `FixedString` can be converted to the type `char[]`.
* Values of the type `String` can be converted to types `char[]` and `byte[]`.

#### Miscellaneous

* Basic interfaces of the column reader were modified. Despite these interfaces are public,
  they are supposedly used only by an internal part of ClickHouseClient.

### Octonica.ClickHouseClient release v1.2.1, 2021-06-25

### New Feature

* Add support for user-defined tables in queries. New property `ClickHouseCommand.TableProviders` provides access to a collection of user-defined
  tables associated with the command. See the section *'Table-valued parameters'* of [Parameters](docs/Parameters.md) for details ([#24](https://github.com/Octonica/ClickHouseClient/issues/24)).
* Add property `ClickHouseColumnWriter.MaxBlockSize`. This property allows to set the maximal number of rows which can be sent to the server as
  a single block of data. If an input table contains more rows than `MaxBlockSize` it will be sent to the server by parts ([#26](https://github.com/Octonica/ClickHouseClient/issues/26)).
* Add support for the experimental type `Map(key, value)` ([#31](https://github.com/Octonica/ClickHouseClient/issues/31)).
* Add support for long integer types `Int128`, `UInt128`, `Int256` and `UInt256` ([#27](https://github.com/Octonica/ClickHouseClient/issues/27)).

#### Improvement

* Improve performance of reading and writing values of primitive types.
* Improve connection state management and error handling.

### Octonica.ClickHouseClient release v1.1.13, 2021-05-29

#### Bug Fix

* Fix conversion from `System.Guid` to `UUID`. This bug affected `ClickHouseColumnWriter`.
  It caused writing of corrupted values to a column of type `UUID` ([#29](https://github.com/Octonica/ClickHouseClient/issues/29)).

#### New Feature

* Add method `ClickHouseConnection.TryPing`. This method allows to send 'Ping' message and wait for response from the server.

#### Improvement

* Add cast from `UInt8` to `bool`. `ClickHouseDataReader.GetBoolean` no longer throws an exception for values of type `UInt8`.

### Octonica.ClickHouseClient release v1.1.12, 2021-05-19

#### Backward Incompatible Change

* `ClickHouseDataReader.GetField` and `ClickHouseColumnWriter.GetField` now return `typeof(T)` instead of `typeof(Nullable<T>)` for nullable fields.
  It is possible to get original type of a column from field's type info: `ClickClickHouseDataReader.GetFieldTypeInfo(int ordinal).GetFieldType()`.
* Stricter column type check. `ClickHouseColumnWriter` throws an exception when a type of a column is ambiguous
  (for example, a column's type implements both `IReadOnlyList<int>` and `IReadOnlyList<int?>`).

#### New Feature

* Add support for named tuples.
* Add a way to explicitly set a type of a column. The type could be defined in `ClickHouseColumnSettings`. `ClickHouseDataReader` will try to convert
  a column's value to this type. `ClickHouseColumnWriter` will expect a column to be a collection of items of this type.
* Add support for `IReadOnlyList<object>`, `IList<object>`, `IEnumerable<object>` and `IAsyncEnumerable<object>` to `ClickHouseColumnWriter` ([#21](https://github.com/Octonica/ClickHouseClient/issues/21)).

#### Bug Fix

* Add recognition of escape sequences in enum's item names.

### Octonica.ClickHouseClient release v1.1.9, 2021-05-07

#### New Feature

* Parameters in the format `@paramName` are supported in the text of a query ([#19](https://github.com/Octonica/ClickHouseClient/issues/19)).

### Octonica.ClickHouseClient release v1.1.8, 2021-04-25

#### New Feature

* `ClickHouseCommand.ExecuteDbDataReader` supports non-default command behavior ([#18](https://github.com/Octonica/ClickHouseClient/issues/18)).
* Added method `GetTypeArgument` to the interface `IClickHouseTypeInfo`. This method allows to get additional arguments of the type (scale, precision, timezone, size).

### Octonica.ClickHouseClient release v1.1.7, 2021-03-15

#### Bug Fix

* Fixed error handling for `ClickHouseConnection.Open`. The socket was not properly disposed when error occurred during opening a connection.

### Octonica.ClickHouseClient release v1.1.6, 2021-03-08

#### Backward Incompatible Change

* ClickHouseParameter can't be added to several parameter collections. Use the method `ClickHouseParameter.Clone` to create a parameter's copy which doesn't belong to the collection.

#### New Feature

* Octonica.ClickHouseClient for .NET 5.0 was added to NuGet package.
* Added ClickHouseDbProviderFactory which implements DbProviderFactory.
* `ReadOnlyMemory<char>` or `Memory<char>` can be used instead of `string` when writing values to ClickHouse.
* `ReadOnlyMemory<T>` or `Memory<T>` can be used instead of `T[]` (array of `T`) when writing values to ClickHouse.

#### Bug Fix

* Fixed possible race condition when disposing a connection from different threads ([#16](https://github.com/Octonica/ClickHouseClient/issues/16)).

#### Improvement

* Improved implementation of various classes from `System.Data.Common` namespace, such as `DbConnection`, `DbCommand` and `DbParameter`.

### Octonica.ClickHouseClient release v1.0.17, 2020-12-10

#### Bug Fix

* Fixed execution of queries which affect large (greater than 2^31) number of rows ([#15](https://github.com/Octonica/ClickHouseClient/issues/15)).
* Fixed comparison of parameter's names in ClickHouseParameterCollection.

#### Improvement

* Added public method `ClickHouseParameter.IsValidParameterName` which allows to check if the string can be used as the name of a parameter.

### Octonica.ClickHouseClient release v1.0.14, 2020-12-02

#### Bug Fix

* The driver was incompatible with ClickHouse v2.10 and higher.
* Fixed writing columns from a source which contains more rows than `rowCount`.
* Fixed writing columns from a source which implements `IList<T>` but doesn't implement `IReadOnlyList<T>`.

### Octonica.ClickHouseClient release v1.0.13, 2020-11-11

#### Backward Incompatible Change

* The default name of the client changed from `Octonica.ClickHouse` to `Octonica.ClickHouseClient`.

#### New Feature

* Added type `DateTime64`.
* Implemented methods `NextResult` and `NextResultAsync` in `ClickHouseDataReader`. These methods can be used to read totals and extremes ([#11](https://github.com/Octonica/ClickHouseClient/issues/11)).
* Added `Extremes` property to `ClickHouseCommand`. It allows to toggle `extremes` setting for the query.
* Added `TimeZone` property to `ClickHouseParameter`. It allows to specify the timezone for datetime types.
* Array can be used as the value of command parameter. Added properties `IsArray` and `ArrayRank` to `ClickHouseParameter` ([#14](https://github.com/Octonica/ClickHouseClient/issues/14)).

#### Bug Fix

* The type `UInt64` was mapped to the type `UInt32` in the command parameter.

#### Improvement

* Detection of attempts to connect to ClickHouse server with HTTP protocol ([#10](https://github.com/Octonica/ClickHouseClient/issues/10)).
* `ReadWriteTimeout` is respected in async network operations if `cancellationToken` is not defined (i.e. `CanellationToken.None`).

#### Miscellaneous

* Default protocol revision is set to 54441.
