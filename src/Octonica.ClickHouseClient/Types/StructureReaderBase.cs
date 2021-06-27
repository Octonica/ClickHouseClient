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
using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;

namespace Octonica.ClickHouseClient.Types
{
    public abstract class StructureReaderBase<TIn, TOut> : IClickHouseColumnReader
        where TIn : struct
    {
        private readonly int _rowCount;

        private int _position;
        private readonly TIn[]? _buffer;

        protected int ElementSize { get; }

        protected virtual bool BitwiseCopyAllowed => false;

        public StructureReaderBase(int elementSize, int rowCount)
        {
            ElementSize = elementSize;
            _rowCount = rowCount;

            if (rowCount > 0)
                _buffer = new TIn[rowCount];
        }

        public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
        {
            if (_position >= _rowCount)
                throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");

            var byteSize = Math.Min(ElementSize * (_rowCount - _position), (int) (sequence.Length - sequence.Length % ElementSize));
            var elementCount = byteSize / ElementSize;
            if (elementCount == 0)
                return new SequenceSize(0, 0);

            int count;
            if (BitwiseCopyAllowed)
            {
                var targetBytes = MemoryMarshal.AsBytes(new Span<TIn>(_buffer, _position, elementCount));
                Debug.Assert(byteSize == targetBytes.Length);
                sequence.Slice(0, byteSize).CopyTo(targetBytes);
                count = elementCount;
            }
            else
            {
                count = CopyTo(sequence.Slice(0, byteSize), ((Span<TIn>) _buffer).Slice(_position, elementCount));
                Debug.Assert(count >= 0 && count <= elementCount);
            }

            _position += count;
            return new SequenceSize(count * ElementSize, count);
        }

        private int CopyTo(ReadOnlySequence<byte> source, Span<TIn> target)
        {
            Span<byte> tmpSpan = stackalloc byte[ElementSize];
            int count = 0;
            for (var slice = source; !slice.IsEmpty; slice = slice.Slice(ElementSize), count++)
            {
                if (slice.FirstSpan.Length >= ElementSize)
                    target[count] = ReadElement(slice.FirstSpan);
                else
                {
                    slice.Slice(0, ElementSize).CopyTo(tmpSpan);
                    target[count] = ReadElement(tmpSpan);
                }
            }

            return count;
        }

        protected abstract TIn ReadElement(ReadOnlySpan<byte> source);

        protected abstract IClickHouseTableColumn<TOut> EndRead(ClickHouseColumnSettings? settings, ReadOnlyMemory<TIn> buffer);

        public IClickHouseTableColumn<TOut> EndRead(ClickHouseColumnSettings? settings)
        {
            return EndRead(settings, ((ReadOnlyMemory<TIn>)_buffer).Slice(0, _position));
        }

        IClickHouseTableColumn IClickHouseColumnReader.EndRead(ClickHouseColumnSettings? settings)
        {
            return EndRead(settings);
        }
    }

    public abstract class StructureReaderBase<T> : StructureReaderBase<T, T>
        where T : struct
    {
        public StructureReaderBase(int elementSize, int rowCount)
            : base(elementSize, rowCount)
        {
        }

        protected override IClickHouseTableColumn<T> EndRead(ClickHouseColumnSettings? settings, ReadOnlyMemory<T> buffer)
        {
            return new StructureTableColumn<T>(buffer);
        }
    }
}
