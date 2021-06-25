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

using System;
using System.Collections.Generic;

namespace Octonica.ClickHouseClient
{
    public class ClickHouseTableProvider : IClickHouseTableProvider
    {
        public string TableName { get; }

        public int ColumnCount => Columns.Count;

        public int RowCount { get; }

        public ClickHouseTableColumnCollection Columns { get; } = new ClickHouseTableColumnCollection();

        public ClickHouseTableProvider(string tableName, int rowCount)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentNullException("The name of a table must be a non-empty string.", nameof(tableName));
            if (rowCount < 0)
                throw new ArgumentException("The number of rows must be a positive number.", nameof(rowCount));

            TableName = tableName;
            RowCount = rowCount;
        }

        public ClickHouseTableColumn AddColumn(object column)
        {
            return Columns.AddColumn(column);
        }

        public ClickHouseTableColumn AddColumn(object column, Type columnType)
        {
            return Columns.AddColumn(column, columnType);
        }

        public ClickHouseTableColumn AddColumn(string columnName, object column)
        {
            return Columns.AddColumn(columnName, column);
        }

        public ClickHouseTableColumn AddColumn(string columnName, object column, Type columnType)
        {
            return Columns.AddColumn(columnName, column, columnType);
        }

        public ClickHouseTableColumn AddColumn<T>(IReadOnlyList<T> column)
        {
            return Columns.AddColumn(column);
        }

        public ClickHouseTableColumn AddColumn<T>(string columnName, IReadOnlyList<T> column)
        {
            return Columns.AddColumn(columnName, column);
        }

        object IClickHouseTableProvider.GetColumn(int index)
        {
            return Columns[index].Value;
        }

        IClickHouseColumnDescriptor IClickHouseTableProvider.GetColumnDescriptor(int index)
        {
            return Columns[index];
        }
    }
}
