#region License Apache 2.0
/* Copyright 2019-2020 Octonica
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

using System.Collections.ObjectModel;

namespace Octonica.ClickHouseClient.Protocol
{
    internal class BlockHeader
    {
        private static readonly ReadOnlyCollection<ColumnInfo> EmptyColumns = new(new ColumnInfo[0]);

        public string? TableName { get; }

        public ReadOnlyCollection<ColumnInfo> Columns { get; }

        public int RowCount { get; }

        public BlockHeader(string? tableName, ReadOnlyCollection<ColumnInfo>? columns, int rowCount)
        {
            TableName = tableName;
            Columns = columns ?? EmptyColumns;
            RowCount = rowCount;
        }
    }
}
