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

namespace Octonica.ClickHouseClient.Types
{
    internal class StructureTableColumn<T> : IClickHouseTableColumn<T>
        where T : struct
    {
        private readonly ReadOnlyMemory<T> _buffer;

        public int RowCount => _buffer.Length;

        public StructureTableColumn(ReadOnlyMemory<T> buffer)
        {
            _buffer = buffer;
        }

        public bool IsNull(int index)
        {
            return false;
        }

        public T GetValue(int index)
        {
            return _buffer.Span[index];
        }

        public virtual IClickHouseTableColumn<TAs>? TryReinterpret<TAs>()
        {
            if (typeof(TAs) == typeof(T?))
                return (IClickHouseTableColumn<TAs>) (object) new NullableStructTableColumn<T>(null, this);

            return null;
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            return GetValue(index);
        }

        bool IClickHouseTableColumn.TryDipatch<TRes>(IClickHouseTableColumnDispatcher<TRes> dispatcher, out TRes dispatchedValue)
        {
            dispatchedValue = dispatcher.Dispatch(this);
            return true;
        }
    }
}
