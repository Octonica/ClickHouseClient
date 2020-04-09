#region License Apache 2.0
/* Copyright 2019-2020 Octonica LLC
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
using System.Collections.ObjectModel;
using Octonica.ClickHouseClient.Protocol;

namespace Octonica.ClickHouseClient
{
    internal sealed class ClickHouseTable
    {
        public BlockHeader Header { get; }

        public ReadOnlyCollection<IClickHouseTableColumn> Columns { get; }

        public ClickHouseTable(BlockHeader header, ReadOnlyCollection<IClickHouseTableColumn> columns)
        {
            Header = header ?? throw new ArgumentNullException(nameof(header));
            Columns = columns ?? throw new ArgumentNullException(nameof(columns));
        }
    }
}
