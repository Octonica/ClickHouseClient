#region License Apache 2.0
/* Copyright 2021-2022 Octonica
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

using System;
using NodaTime;

namespace Octonica.ClickHouseClient.Types
{
    /// <summary>
    /// Represents a set of properties describing a type of a column.
    /// </summary>
    public interface IClickHouseColumnTypeDescriptor
    {
        /// <summary>
        /// Gets the type of the column.
        /// </summary>
        ClickHouseDbType? ClickHouseDbType { get; }

        /// <summary>
        /// Gets the type of the column's value (i.e. the type of column's cells).
        /// </summary>
        Type ValueType { get; }

        /// <summary>
        /// Gets the value indicating whether the column can contain NULLs.
        /// </summary>
        bool? IsNullable { get; }

        /// <summary>
        /// Gets the size. This value is applied to the ClickHouse type FixedString.
        /// </summary>
        int Size { get; }

        /// <summary>
        /// Gets the precision. This value is applied to ClickHouse types Decimal and DateTime64.
        /// </summary>
        byte? Precision { get; }

        /// <summary>
        /// Gets the scale. This value is applied to the ClickHouse type Decimal.
        /// </summary>
        byte? Scale { get; }

        /// <summary>
        /// Gets the time zone. This value is applied to ClickHouse types DateTime and DateTime64.
        /// </summary>
        DateTimeZone? TimeZone { get; }

        /// <summary>
        /// Gets the rank (a number of dimensions) of an array.
        /// </summary>
        int? ArrayRank { get; }
    }
}
