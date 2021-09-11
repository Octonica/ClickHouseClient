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
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace Octonica.ClickHouseClient.Types
{
    internal abstract class StringTableColumnBase<TOut> : IClickHouseTableColumn<TOut>, IClickHouseArrayTableColumn<byte>
    {
        private readonly Encoding _encoding;
        private readonly List<(int segmentIndex, int offset, int length)> _layouts;
        private readonly List<Memory<byte>> _segments;

        public int RowCount => _layouts.Count;

        protected StringTableColumnBase(Encoding encoding, List<(int segmentIndex, int offset, int length)> layouts, List<Memory<byte>> segments)
        {
            _encoding = encoding ?? throw new ArgumentNullException(nameof(encoding));
            _layouts = layouts ?? throw new ArgumentNullException(nameof(layouts));
            _segments = segments ?? throw new ArgumentNullException(nameof(segments));
        }

        public bool IsNull(int index)
        {
            return false;
        }

        [return: NotNull]
        public TOut GetValue(int index)
        {
            var(segmentIndex, offset, length) = _layouts[index];
            if (length == 0)
                return GetValue(_encoding, Span<byte>.Empty);

            var span = _segments[segmentIndex].Slice(offset, length).Span;
            return GetValue(_encoding, span);
        }

        [return: NotNull]
        protected abstract TOut GetValue(Encoding encoding, ReadOnlySpan<byte> span);

        object IClickHouseTableColumn.GetValue(int index)
        {
            return GetValue(index);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            if (typeof(T) == typeof(byte[]))
                return (IClickHouseTableColumn<T>)(object)new StringByteArrayTableColumn(_encoding, _layouts, _segments);
            if (typeof(T) == typeof(string))
                return (IClickHouseTableColumn<T>)(object)new StringTableColumn(_encoding, _layouts, _segments);
            if (typeof(T) == typeof(char[]))
                return (IClickHouseTableColumn<T>)(object)new StringCharArrayTableColumn(_encoding, _layouts, _segments);

            return null;
        }

        bool IClickHouseTableColumn.TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, out T dispatchedValue)
        {
            dispatchedValue = dispatcher.Dispatch(this);
            return true;
        }

        public int CopyTo(int index, Span<byte> buffer, int dataOffset)
        {
            var (segmentIndex, offset, length) = _layouts[index];
            if (dataOffset < 0 || dataOffset > length)
                throw new ArgumentOutOfRangeException(nameof(dataOffset));

            if (length == 0)
                return 0;

            var maxLength = Math.Min(length - dataOffset, buffer.Length);
            var slice = _segments[segmentIndex].Slice(offset + dataOffset, maxLength);
            slice.Span.CopyTo(buffer);
            return maxLength;
        }
    }
}
