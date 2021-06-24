#region License Apache 2.0
/* Copyright 2020-2021 Octonica
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
    public enum ClickHouseDbType
    {
        /// <summary>
        /// Not supported
        /// </summary>
        AnsiString = DbType.AnsiString,
        Binary = DbType.Binary,
        Byte = DbType.Byte,
        Boolean = DbType.Boolean,
        Currency = DbType.Currency,
        Date = DbType.Date,
        DateTime = DbType.DateTime,
        Decimal = DbType.Decimal,
        Double = DbType.Double,
        Guid = DbType.Guid,
        Int16 = DbType.Int16,
        Int32 = DbType.Int32,
        Int64 = DbType.Int64,
        Object = DbType.Object,
        SByte = DbType.SByte,
        Single = DbType.Single,
        String = DbType.String,

        /// <summary>
        /// Not supported
        /// </summary>
        Time = DbType.Time,
        UInt16 = DbType.UInt16,
        UInt32 = DbType.UInt32,
        UInt64 = DbType.UInt64,
        VarNumeric = DbType.VarNumeric,

        /// <summary>
        /// Not supported
        /// </summary>
        AnsiStringFixedLength = DbType.AnsiStringFixedLength,
        StringFixedLength = DbType.StringFixedLength,

        /// <summary>
        /// Not supported
        /// </summary>
        Xml = DbType.Xml,

        /// <summary>
        /// <seealso cref="DateTime64"/> with an accuracy of 100 nanoseconds.
        /// </summary>
        DateTime2 = DbType.DateTime2,
        DateTimeOffset = DbType.DateTimeOffset,

        /// <summary>
        /// It's not a valid code for any type. This value is used as the delimiter between <see cref="DbType"/> and ClickHouse-specific type codes.
        /// </summary>
        ClickHouseSpecificTypeDelimiterCode = 0x3FFF,

        IpV4 = ClickHouseSpecificTypeDelimiterCode + 1,
        IpV6 = ClickHouseSpecificTypeDelimiterCode + 2,

        Array = ClickHouseSpecificTypeDelimiterCode + 3,
        Tuple = ClickHouseSpecificTypeDelimiterCode + 4,

        Nothing = ClickHouseSpecificTypeDelimiterCode + 5,

        Enum = ClickHouseSpecificTypeDelimiterCode + 6,

        DateTime64 = ClickHouseSpecificTypeDelimiterCode + 7,

        Map = ClickHouseSpecificTypeDelimiterCode + 8,
    }
}
