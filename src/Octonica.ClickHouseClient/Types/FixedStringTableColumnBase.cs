#region License Apache 2.0
/* Copyright 2019-2021, 2024 Octonica
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
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace Octonica.ClickHouseClient.Types
{
    internal abstract class FixedStringTableColumnBase<TOut> : IClickHouseTableColumn<TOut>, IClickHouseArrayTableColumn<byte>
    {
        private readonly Memory<byte> _buffer;
        private readonly int _rowSize;
        private readonly Encoding _encoding;

        public int RowCount { get; }

        public abstract TOut DefaultValue { get; }

        protected FixedStringTableColumnBase(Memory<byte> buffer, int rowSize, Encoding encoding)
        {
            _buffer = buffer;
            _rowSize = rowSize;
            _encoding = encoding;
            RowCount = _buffer.Length / _rowSize;
        }

        public bool IsNull(int index)
        {
            return false;
        }

        [return: NotNull]
        public TOut GetValue(int index)
        {
            return GetValue(_encoding, _buffer.Span.Slice(index * _rowSize, _rowSize));
        }

        [return: NotNull]
        protected abstract TOut GetValue(Encoding encoding, ReadOnlySpan<byte> span);

        object IClickHouseTableColumn.GetValue(int index)
        {
            return GetValue(index);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            if (typeof(T) == typeof(string))
                return (IClickHouseTableColumn<T>)(object)new FixedStringDecodedTableColumn(_buffer, _rowSize, _encoding);
            if (typeof(T) == typeof(byte[]))
                return (IClickHouseTableColumn<T>)(object)new FixedStringTableColumn(_buffer, _rowSize, _encoding);
            if (typeof(T) == typeof(char[]))
                return (IClickHouseTableColumn<T>)(object)new FixedStringDecodedCharArrayTableColumn(_buffer, _rowSize, _encoding);

            return null;
        }

        bool IClickHouseTableColumn.TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, out T dispatchedValue)
        {
            dispatchedValue = dispatcher.Dispatch(this);
            return true;
        }

        public int CopyTo(int index, Span<byte> buffer, int dataOffset)
        {
            if (dataOffset < 0 || dataOffset > _rowSize)
                throw new ArgumentOutOfRangeException(nameof(dataOffset));

            var length = Math.Min(_rowSize - dataOffset, buffer.Length);
            _buffer.Span.Slice(index * _rowSize + dataOffset, length).CopyTo(buffer);
            return length;
        }
    }
}
