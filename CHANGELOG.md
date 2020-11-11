### Octonica.ClickHouseClient relese v1.0.13, 2020-11-11

#### Backward Incompatible Change

* The default name of the client changed from `Octonica.ClickHouse` to `Octonica.ClickHouseClient`.

#### New Feature

* Added type `DateTime64`.
* Implemented methods `NextResult` and `NextResultAsync` in `ClickHouseDataReader`. These methods can be used to read totals and extremes (#11).
* Added `Extremes` property to `ClickHouseCommand`. It allows to toggle `extremes` setting for the query.
* Added `TimeZone` property to `ClickHouseParameter`. It allows to specify the timezone for datetime types.
* Array can be used as the value of command parameter. Added properties `IsArray` and `ArrayRank` to `ClickHouseParameter` (#14).

#### Bug Fix

* The type `UInt64` was mapped to the type `UInt32` in the command parameter.

#### Improvement

* Detection of attempts to connect to ClickHouse server with HTTP protocol (#10).
* `ReadWriteTimeout` is respected in async network operations if `cancellationToken` is not defined (i.e. `CanellationToken.None`).

#### Miscellaneous

* Default protocol revision is set to 54441.
