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
    public class ClickHouseColumnSettings
    {
        private ITypeDispatcher? _columnTypeDispatcher;

        public Encoding? StringEncoding { get; }

        public IClickHouseEnumConverter? EnumConverter { get; }

        /// <summary>
        /// Overrides the type of the field for <see cref="ClickHouseDataReader"/> and <see cref="ClickHouseColumnWriter"/>.
        /// <see cref="ClickHouseDataReader"/> will try to convert a column's value to this type. <see cref="ClickHouseColumnWriter"/> will expect a collection of
        /// items of this type as input.
        /// </summary>
        public Type? ColumnType { get; }

        public ClickHouseColumnSettings(Encoding stringEncoding)
        {
            StringEncoding = stringEncoding ?? throw new ArgumentNullException(nameof(stringEncoding));
        }

        public ClickHouseColumnSettings(IClickHouseEnumConverter enumConverter)
        {
            EnumConverter = enumConverter ?? throw new ArgumentNullException(nameof(enumConverter));
        }

        /// <summary>
        /// <inheritdoc cref="ColumnType"/>
        /// </summary>
        public ClickHouseColumnSettings(Type columnType)
        {
            ColumnType = columnType ?? throw new ArgumentException(nameof(columnType));
        }

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
