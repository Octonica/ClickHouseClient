#region License Apache 2.0
/* Copyright 2020-2021, 2024 Octonica
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
using System.Net;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class IpV6TableColumn : IClickHouseTableColumn<IPAddress>
    {
        private readonly ReadOnlyMemory<byte> _buffer;

        public int RowCount => _buffer.Length / 16;

        public IPAddress DefaultValue => IPAddress.IPv6Any;

        public IpV6TableColumn(ReadOnlyMemory<byte> buffer)
        {
            _buffer = buffer;
        }

        public bool IsNull(int index)
        {
            return false;
        }

        public IPAddress GetValue(int index)
        {
            return new IPAddress(_buffer.Span.Slice(index * 16, 16));
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            return GetValue(index);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            return typeof(T) == typeof(string)
                ? (IClickHouseTableColumn<T>)(object)new ReinterpretedTableColumn<IPAddress, string>(this, v => v.ToString())
                : this as IClickHouseTableColumn<T>;
        }

        bool IClickHouseTableColumn.TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, out T dispatchedValue)
        {
            dispatchedValue = dispatcher.Dispatch(this);
            return true;
        }
    }
}
