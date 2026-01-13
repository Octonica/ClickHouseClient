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

namespace Octonica.ClickHouseClient.Types
{
    internal static class ClickHouseTableColumnHelper
    {
        public static Type? TryGetValueType(IClickHouseTableColumn column)
        {
            return column.TryDipatch(ClickHouseTableColumnValueTypeDispatcher.Instance, out Type? type) ? type : null;
        }

        private sealed class ClickHouseTableColumnValueTypeDispatcher : IClickHouseTableColumnDispatcher<Type>
        {
            public static readonly ClickHouseTableColumnValueTypeDispatcher Instance = new();

            private ClickHouseTableColumnValueTypeDispatcher()
            {
            }

            public Type Dispatch<T>(IClickHouseTableColumn<T> column)
            {
                return typeof(T);
            }
        }
    }
}
