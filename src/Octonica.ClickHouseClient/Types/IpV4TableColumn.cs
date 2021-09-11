#region License Apache 2.0
/* Copyright 2020-2021 Octonica
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
using System.Runtime.InteropServices;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class IpV4TableColumn: IClickHouseTableColumn<IPAddress>
    {
        private readonly ReadOnlyMemory<byte> _buffer;

        public int RowCount => _buffer.Length / sizeof(uint);

        public IpV4TableColumn(ReadOnlyMemory<byte> buffer)
        {
            _buffer = buffer;
        }

        public bool IsNull(int index)
        {
            return false;
        }

        public IPAddress GetValue(int index)
        {
            Span<int> address = stackalloc int[1];
            var addressSpan = MemoryMarshal.AsBytes(address);
            _buffer.Slice(index * sizeof(uint), sizeof(uint)).Span.CopyTo(addressSpan);
            
            address[0] = IPAddress.NetworkToHostOrder(address[0]);
            return new IPAddress(addressSpan);
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            return GetValue(index);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            IClickHouseTableColumn? result = null;
            if (typeof(T) == typeof(IPAddress))
            {
                result = this;
            }
            else if (typeof(T) == typeof(string))
            {
                result = new ReinterpretedTableColumn<IPAddress, string>(this, v => v.ToString());
            }
            if (typeof(T) == typeof(uint))
            {
                result = new ReinterpretedTableColumn<uint>(this, new RawIpV4TableColumn<uint>(_buffer, m => BitConverter.ToUInt32(m.Span)));
            }
            else if (typeof(T) == typeof(uint?))
            {
                result = new ReinterpretedTableColumn<uint?>(this, new NullableStructTableColumn<uint>(null, new RawIpV4TableColumn<uint>(_buffer, m => BitConverter.ToUInt32(m.Span))));
            }
            else if (typeof(T) == typeof(int))
            {
                result = new ReinterpretedTableColumn<int>(this, new RawIpV4TableColumn<int>(_buffer, m => BitConverter.ToInt32(m.Span)));
            }
            else if (typeof(T) == typeof(int?))
            {
                result = new ReinterpretedTableColumn<int?>(this, new NullableStructTableColumn<int>(null, new RawIpV4TableColumn<int>(_buffer, m => BitConverter.ToInt32(m.Span))));
            }

            return (IClickHouseTableColumn<T>?) result;
        }

        bool IClickHouseTableColumn.TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, out T dispatchedValue)
        {
            dispatchedValue = dispatcher.Dispatch(this);
            return true;
        }

        private sealed class RawIpV4TableColumn<TStruct> : IClickHouseTableColumn<TStruct>
            where TStruct : struct
        {
            private readonly ReadOnlyMemory<byte> _buffer;
            private readonly Func<ReadOnlyMemory<byte>, TStruct> _getValue;

            public int RowCount => _buffer.Length / sizeof(uint);

            public RawIpV4TableColumn(ReadOnlyMemory<byte> buffer, Func<ReadOnlyMemory<byte>, TStruct> getValue)
            {
                _buffer = buffer;
                _getValue = getValue;
            }

            public bool IsNull(int index)
            {
                return false;
            }

            public TStruct GetValue(int index)
            {
                return _getValue(_buffer.Slice(index * sizeof(uint), sizeof(uint)));
            }

            object IClickHouseTableColumn.GetValue(int index)
            {
                return GetValue(index);
            }

            public IClickHouseTableColumn<T>? TryReinterpret<T>()
            {
                return this as IClickHouseTableColumn<T>;
            }

            bool IClickHouseTableColumn.TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, out T dispatchedValue)
            {
                dispatchedValue = dispatcher.Dispatch(this);
                return true;
            }
        }
    }
}
