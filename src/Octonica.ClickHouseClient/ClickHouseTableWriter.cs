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

using Octonica.ClickHouseClient.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Octonica.ClickHouseClient
{
    internal sealed class ClickHouseTableWriter : IClickHouseTableWriter
    {
        public string TableName { get; }

        public int RowCount { get; }

        public IReadOnlyList<IClickHouseColumnWriter> Columns { get; }

        public ClickHouseTableWriter(string tableName, int rowCount, IEnumerable<IClickHouseColumnWriter> columns)
        {
            if (columns == null)
            {
                throw new ArgumentNullException(nameof(columns));
            }

            TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            RowCount = rowCount;
            Columns = columns.ToList().AsReadOnly();
        }

        public ClickHouseTableWriter(string tableName, int rowCount, IReadOnlyList<IClickHouseColumnWriter> columns)
        {
            TableName = tableName;
            RowCount = rowCount;
            Columns = columns;
        }
    }
}
