#region License Apache 2.0
/* Copyright 2021 Octonica
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

using Octonica.ClickHouseClient.Protocol;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// Represents a configurable column descriptor.
    /// </summary>
    public class ClickHouseTableColumn : IClickHouseColumnDescriptor
    {
        /// <inheritdoc/>
        public string ColumnName { get; }

        /// <summary>
        /// Gets or sets the settings that should be applied when writing the column.
        /// </summary>
        public ClickHouseColumnSettings? Settings { get; set; }

        /// <summary>
        /// Gets or sets the type of the column.
        /// </summary>
        public ClickHouseDbType? ClickHouseDbType { get; set; }

        /// <summary>
        /// Gets or sets the type of the column's value (i.e. the type of column's cells).
        /// </summary>
        public Type ValueType { get; }

        /// <summary>
        /// Gets or sets the value indicating whether the column can contain NULLs.
        /// </summary>
        public bool? IsNullable { get; set; }

        /// <summary>
        /// Gets or sets the size. This value is applied to the ClickHouse type FixedString.
        /// </summary>
        public int Size { get; set; }

        /// <summary>
        /// Gets or sets the precision. This value is applied to ClickHouse types Decimal and DateTime64.
        /// </summary>
        public byte? Precision { get; set; }

        /// <summary>
        /// Gets or sets the scale. This value is applied to the ClickHouse type Decimal.
        /// </summary>
        public byte? Scale { get; set; }

        /// <summary>
        /// Gets or sets the time zone. This value is applied to ClickHouse types DateTime and DateTime64.
        /// </summary>
        public TimeZoneInfo? TimeZone { get; set; }

        /// <summary>
        /// Gets or sets the rank (a number of dimensions) of an array.
        /// </summary>
        public int? ArrayRank { get; set; }

        /// <summary>
        /// Gets the object representing a column. It must implement one of interfaces:
        /// <see cref="IReadOnlyList{T}"/>,
        /// <see cref="IList{T}"/>,
        /// <see cref="IAsyncEnumerable{T}"/>,
        /// <see cref="IEnumerable{T}"/> or
        /// <see cref="IEnumerable"/>.
        /// </summary>
        public object Value { get; }

        /// <summary>
        /// Initializes a new instance of <see cref="ClickHouseTableColumn"/> class with the specified name.
        /// </summary>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="value">
        /// The object representing a column. It must implement one of interfaces:
        /// <see cref="IReadOnlyList{T}"/>,
        /// <see cref="IList{T}"/>,
        /// <see cref="IAsyncEnumerable{T}"/>,
        /// <see cref="IEnumerable{T}"/> or
        /// <see cref="IEnumerable"/>.
        /// </param>
        /// <param name="valueType">The type of the column's values.</param>
        public ClickHouseTableColumn(string columnName, object value, Type valueType)
        {
            if (string.IsNullOrWhiteSpace(columnName))
                throw new ArgumentException("The name of a column must be a non-empty string", nameof(columnName));

            ColumnName = columnName;
            Value = value ?? throw new ArgumentNullException(nameof(value));
            ValueType = valueType ?? throw new ArgumentNullException(nameof(valueType));
        }
    }
}
