#region License Apache 2.0
/* Copyright 2020-2021, 2024 Octonica
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#endregion

using System.Data;

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// Specifies the data type of a field (column) or a <see cref="ClickHouseParameter"/> object.
    /// </summary>
    public enum ClickHouseDbType
    {
        /// <summary>
        /// The type is not supported by the client. An encoding should be defined explicitly via <see cref="ClickHouseParameter.StringEncoding"/>
        /// or <see cref="ClickHouseColumnSettings.StringEncoding"/>.
        /// </summary>
        /// <remarks>This value corresponds to <see cref="DbType.AnsiString"/>.</remarks>
        AnsiString = DbType.AnsiString,

        /// <summary>
        /// An array of bytes.
        /// </summary>
        /// <remarks>This value corresponds to <see cref="DbType.Binary"/>.</remarks>
        Binary = DbType.Binary,

        /// <summary>
        /// An 8-bit unsigned integer ranging in value from 0 to 255.
        /// </summary>
        /// <remarks>This value corresponds to <see cref="DbType.Byte"/>.</remarks>
        Byte = DbType.Byte,

        /// <summary>
        /// A simple type representing Boolean values of <see langword="true"/> or <see langword="false"/>.        
        /// </summary>
        /// <remarks>This value corresponds to <see cref="DbType.Boolean"/>.</remarks>
        Boolean = DbType.Boolean,

        /// <summary>
        /// The ClickHouse type Decimal(18, 4).
        /// </summary>
        /// <remarks>This value corresponds to <see cref="DbType.Currency"/>.</remarks>
        Currency = DbType.Currency,

        /// <summary>
        /// A type representing a date value without a time.
        /// </summary>
        /// <remarks>This value corresponds to <see cref="DbType.Date"/>.</remarks>
        Date = DbType.Date,

        /// <summary>
        /// A type representing a date and time value.
        /// </summary>
        /// <remarks>This value corresponds to <see cref="DbType.DateTime"/>.</remarks>
        DateTime = DbType.DateTime,

        /// <summary>
        /// The ClickHouse type Decimal(38, 9).
        /// </summary>
        /// <remarks>This value corresponds to <see cref="DbType.Decimal"/>.</remarks>
        Decimal = DbType.Decimal,

        /// <inheritdoc cref="DbType.Double"/>
        /// <remarks>This value corresponds to <see cref="DbType.Double"/>.</remarks>
        Double = DbType.Double,

        /// <summary>
        /// The ClickHouse type UUID.
        /// </summary>
        /// <remarks>This value corresponds to <see cref="DbType.Guid"/>.</remarks>
        Guid = DbType.Guid,

        /// <inheritdoc cref="DbType.Int16"/>
        /// <remarks>This value corresponds to <see cref="DbType.Int16"/>.</remarks>
        Int16 = DbType.Int16,

        /// <inheritdoc cref="DbType.Int32"/>
        /// <remarks>This value corresponds to <see cref="DbType.Int32"/>.</remarks>
        Int32 = DbType.Int32,

        /// <inheritdoc cref="DbType.Int64"/>
        /// <remarks>This value corresponds to <see cref="DbType.Int64"/>.</remarks>
        Int64 = DbType.Int64,

        /// <summary>
        /// A general type representing a value of either an unknown type or the ClickHouse type Nothing.
        /// </summary>
        /// <remarks>This value corresponds to <see cref="DbType.Object"/>.</remarks>
        Object = DbType.Object,

        /// <inheritdoc cref="DbType.SByte"/>
        /// <remarks>This value corresponds to <see cref="DbType.SByte"/>.</remarks>
        SByte = DbType.SByte,

        /// <inheritdoc cref="DbType.Single"/>
        /// <remarks>This value corresponds to <see cref="DbType.Single"/>.</remarks>
        Single = DbType.Single,

        /// <summary>
        /// A variable-length string.
        /// </summary>
        /// <remarks>This value corresponds to <see cref="DbType.String"/>.</remarks>
        String = DbType.String,

        /// <summary>
        /// The type is not supported by the client.
        /// </summary>
        /// <remarks>This value corresponds to <see cref="DbType.Time"/>.</remarks>
        Time = DbType.Time,

        /// <inheritdoc cref="DbType.UInt16"/>
        /// <remarks>This value corresponds to <see cref="DbType.UInt16"/>.</remarks>
        UInt16 = DbType.UInt16,

        /// <inheritdoc cref="DbType.UInt32"/>
        /// <remarks>This value corresponds to <see cref="DbType.UInt32"/>.</remarks>
        UInt32 = DbType.UInt32,

        /// <inheritdoc cref="DbType.UInt64"/>
        /// <remarks>This value corresponds to <see cref="DbType.UInt64"/>.</remarks>
        UInt64 = DbType.UInt64,

        /// <summary>
        /// A type representing numeric values with the specified precision (from 1 to 76) and scale (from 0 to the precision).
        /// </summary>
        /// <remarks>This value corresponds to <see cref="DbType.VarNumeric"/>.</remarks>
        VarNumeric = DbType.VarNumeric,

        /// <summary>
        /// The type is not supported by the client.
        /// </summary>
        /// <remarks>This value corresponds to <see cref="DbType.AnsiStringFixedLength"/>.</remarks>
        AnsiStringFixedLength = DbType.AnsiStringFixedLength,

        /// <summary>
        /// A fixed-length string.
        /// </summary>
        /// <remarks>This value corresponds to <see cref="DbType.StringFixedLength"/>.</remarks>
        StringFixedLength = DbType.StringFixedLength,

        /// <summary>
        /// The type is not supported by the client.
        /// </summary>
        /// <remarks>This value corresponds to <see cref="DbType.Xml"/>.</remarks>
        Xml = DbType.Xml,

        /// <summary>
        /// <seealso cref="DateTime64"/> with an accuracy of 100 nanoseconds.
        /// </summary>
        /// <remarks>This value corresponds to <see cref="DbType.DateTime2"/>.</remarks>
        DateTime2 = DbType.DateTime2,

        /// <summary>
        /// A type representing a date and time value with time zone awareness.
        /// </summary>
        /// <remarks>This value corresponds to <see cref="DbType.DateTimeOffset"/>.</remarks>
        DateTimeOffset = DbType.DateTimeOffset,

        /// <summary>
        /// It's not a valid code for any type. This value is used as the delimiter between <see cref="DbType"/> and ClickHouse-specific type codes.
        /// </summary>
        ClickHouseSpecificTypeDelimiterCode = 0x3FFF,

        /// <summary>
        /// A type representing an IP v4 address.
        /// </summary>
        IpV4 = ClickHouseSpecificTypeDelimiterCode + 1,

        /// <summary>
        /// A type representing an IP v6 address.
        /// </summary>
        IpV6 = ClickHouseSpecificTypeDelimiterCode + 2,

        /// <summary>
        /// A type representing a variable-length array of values.
        /// </summary>
        Array = ClickHouseSpecificTypeDelimiterCode + 3,

        /// <summary>
        /// A type representing a fixed-length set of values.
        /// </summary>
        Tuple = ClickHouseSpecificTypeDelimiterCode + 4,

        /// <summary>
        /// A type representing NULL value.
        /// </summary>
        Nothing = ClickHouseSpecificTypeDelimiterCode + 5,

        /// <summary>
        /// A type representing Enum value.
        /// </summary>
        Enum = ClickHouseSpecificTypeDelimiterCode + 6,

        /// <summary>
        /// A type representing a date and time value with defined sub-second precision.
        /// Supported range of values: [1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999]
        /// </summary>
        DateTime64 = ClickHouseSpecificTypeDelimiterCode + 7,

        /// <summary>
        /// The ClickHouse type Map(key, value). This type is not supported by <see cref="ClickHouseParameter"/>.
        /// </summary>
        Map = ClickHouseSpecificTypeDelimiterCode + 8,

        /// <summary>
        /// An integral type representing signed 128-bit integers with values between -170141183460469231731687303715884105728 and 170141183460469231731687303715884105727.
        /// </summary>
        Int128 = ClickHouseSpecificTypeDelimiterCode + 9,

        /// <summary>
        /// An integral type representing unsigned 128-bit integers with values between 0 and 340282366920938463463374607431768211455.
        /// </summary>
        UInt128 = ClickHouseSpecificTypeDelimiterCode + 10,

        /// <summary>
        /// An integral type representing signed 256-bit integers with values between -57896044618658097711785492504343953926634992332820282019728792003956564819968 and 57896044618658097711785492504343953926634992332820282019728792003956564819967.
        /// </summary>
        Int256 = ClickHouseSpecificTypeDelimiterCode + 11,

        /// <summary>
        /// An integral type representing unsigned 256-bit integers with values between 0 and 115792089237316195423570985008687907853269984665640564039457584007913129639935.
        /// </summary>
        UInt256 = ClickHouseSpecificTypeDelimiterCode + 12,

        /// <summary>
        /// A type representing a date value without a time. Supports the date range same with <see cref="DateTime64"/>.
        /// Stored in four bytes as the number of days since 1970-01-01.
        /// </summary>
        Date32 = ClickHouseSpecificTypeDelimiterCode + 13,

        /// <summary>
        /// This type represents a union of other data types. Type Variant(T1, T2, ..., TN) means that each row of this type
        /// has a value of either type T1 or T2 or ... or TN or none of them (NULL value).
        /// </summary>
        Variant = ClickHouseSpecificTypeDelimiterCode + 14,
    }
}
