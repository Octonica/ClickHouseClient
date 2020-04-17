#region License Apache 2.0
/* Copyright 2020 Octonica
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
using System.Buffers;
using System.Net;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;

namespace Octonica.ClickHouseClient.Types
{
    internal abstract class IpColumnReaderBase : IClickHouseColumnReader
    {
        private readonly int _rowCount;
        private readonly Memory<byte> _buffer;

        private int _position;

        public int ElementSize { get; }

        protected IpColumnReaderBase(int rowCount, int elementSize)
        {
            _rowCount = rowCount;
            ElementSize = elementSize;
            _buffer = new Memory<byte>(new byte[_rowCount * elementSize]);
        }

        public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
        {
            if (_position >= _rowCount)
                throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");

            var byteSize = Math.Min(ElementSize * (_rowCount - _position), (int) (sequence.Length - sequence.Length % ElementSize));
            var elementCount = byteSize / ElementSize;
            if (elementCount == 0)
                return new SequenceSize(0, 0);

            sequence.Slice(0, byteSize).CopyTo(_buffer.Slice(_position * ElementSize).Span);

            _position += elementCount;
            return new SequenceSize(byteSize, elementCount);
        }

        public SequenceSize Skip(ReadOnlySequence<byte> sequence, int maxElementsCount, ref object? skipContext)
        {
            var count = Math.Min(maxElementsCount, (int) sequence.Length / ElementSize);
            return new SequenceSize(count * ElementSize, count);
        }

        public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
        {
            return EndRead(_buffer.Slice(0, _position * ElementSize));
        }

        protected abstract IClickHouseTableColumn<IPAddress> EndRead(ReadOnlyMemory<byte> buffer);
    }
}
