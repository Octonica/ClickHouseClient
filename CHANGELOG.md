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
