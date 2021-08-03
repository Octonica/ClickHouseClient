#region License Apache 2.0
/* Copyright 2019-2021 Octonica
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
using System.Text;
using Octonica.ClickHouseClient.Types;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient.Protocol
{
    /// <summary>
    /// Represents additional column settings that affect the behavior of <see cref="ClickHouseDataReader"/> and <see cref="ClickHouseColumnWriter"/>.
    /// </summary>
    public class ClickHouseColumnSettings
    {
        private ITypeDispatcher? _columnTypeDispatcher;

        /// <summary>
        /// Gets encoding applied to strings when reading from the database or writing to the database.
        /// </summary>
        public Encoding? StringEncoding { get; }

        /// <summary>
        /// Gets the converter applied to enums.
        /// </summary>
        public IClickHouseEnumConverter? EnumConverter { get; }

        /// <summary>
        /// Gets the explicitly defined type of the column. This value overrides the type of the field for <see cref="ClickHouseDataReader"/>
        /// and <see cref="ClickHouseColumnWriter"/>. <see cref="ClickHouseDataReader"/> will try to convert a column's value to this type.
        /// <see cref="ClickHouseColumnWriter"/> will expect a collection of items of this type as input.
        /// </summary>
        public Type? ColumnType { get; }

        /// <summary>
        /// Initializes a new instance of <see cref="ClickHouseConnectionSettings"/> class with the specified encoding.
        /// </summary>
        /// <param name="stringEncoding">The encoding applied to strings when reading from the database or writing to the database.</param>
        public ClickHouseColumnSettings(Encoding stringEncoding)
        {
            StringEncoding = stringEncoding ?? throw new ArgumentNullException(nameof(stringEncoding));
        }

        /// <summary>
        /// Initializes a new instance of <see cref="ClickHouseConnectionSettings"/> class with the specified enum converter.
        /// </summary>
        /// <param name="enumConverter">The converter applied to enums.</param>
        public ClickHouseColumnSettings(IClickHouseEnumConverter enumConverter)
        {
            EnumConverter = enumConverter ?? throw new ArgumentNullException(nameof(enumConverter));
        }

        /// <summary>
        /// Initializes a new instance of <see cref="ClickHouseConnectionSettings"/> class with the specified column type.
        /// </summary>
        /// <param name="columnType">
        /// The explicitly defined type of the column. This value overrides the type of the field for <see cref="ClickHouseDataReader"/>
        /// and <see cref="ClickHouseColumnWriter"/>. <see cref="ClickHouseDataReader"/> will try to convert a column's value to this type.
        /// <see cref="ClickHouseColumnWriter"/> will expect a collection of items of this type as input.
        /// </param>
        public ClickHouseColumnSettings(Type columnType)
        {
            ColumnType = columnType ?? throw new ArgumentException(nameof(columnType));
        }

        /// <summary>
        /// Initializes a new instance of <see cref="ClickHouseConnectionSettings"/> class with multiple specified parameters.
        /// </summary>
        /// <param name="stringEncoding">The encoding applied to strings when reading from the database or writing to the database.</param>
        /// <param name="enumConverter">The converter applied to enums.</param>
        /// <param name="columnType">
        /// The explicitly defined type of the column. This value overrides the type of the field for <see cref="ClickHouseDataReader"/>
        /// and <see cref="ClickHouseColumnWriter"/>. <see cref="ClickHouseDataReader"/> will try to convert a column's value to this type.
        /// <see cref="ClickHouseColumnWriter"/> will expect a collection of items of this type as input.
        /// </param>
        public ClickHouseColumnSettings(Encoding? stringEncoding = null, IClickHouseEnumConverter? enumConverter = null, Type? columnType = null)
        {
            StringEncoding = stringEncoding;
            EnumConverter = enumConverter;
            ColumnType = columnType;
        }

        internal ITypeDispatcher? GetColumnTypeDispatcher()
        {
            if (ColumnType == null)
                return null;

            return _columnTypeDispatcher ??= TypeDispatcher.Create(ColumnType);
        }
    }
}
