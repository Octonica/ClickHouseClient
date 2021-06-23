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

namespace Octonica.ClickHouseClient
{
    public class ClickHouseTableColumn : IClickHouseColumnDescriptor
    {
        public string ColumnName { get; }

        public ClickHouseColumnSettings? Settings { get; set; }

        public ClickHouseDbType? ClickHouseDbType { get; set; }

        public Type ValueType { get; }

        public bool? IsNullable { get; set; }

        public int Size { get; set; }

        public byte? Precision { get; set; }

        public byte? Scale { get; set; }

        public TimeZoneInfo? TimeZone { get; set; }

        public int? ArrayRank { get; set; }

        public object Value { get; }

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
