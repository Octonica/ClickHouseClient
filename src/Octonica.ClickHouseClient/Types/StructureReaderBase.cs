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

using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Octonica.ClickHouseClient.Types
{
    /// <summary>
    /// Represents a base class capable of reading columns of value types.
    /// </summary>
    /// <typeparam name="TIn">The type of the column that must be a value type (struct).</typeparam>
    /// <typeparam name="TOut">The type of the output column.</typeparam>
    public abstract class StructureReaderBase<TIn, TOut> : IClickHouseColumnReader
        where TIn : struct
    {
        private readonly int _rowCount;

        private int _position;
        private readonly TIn[]? _buffer;

        /// <summary>
        /// Gets the size of a single element in bytes.
        /// </summary>
        protected int ElementSize { get; }

        /// <summary>
        /// Gets the value indicating whether bytes from an input buffer can be copied to the column's buffer bitwise. The default is <see langword="false"/>.
        /// </summary>
        protected virtual bool BitwiseCopyAllowed => false;

        /// <summary>
        /// Initializes <see cref="StructureWriterBase{TIn, TOut}"/> with specified parameters.
        /// </summary>
        /// <param name="elementSize">The size of a single element in bytes.</param>
        /// <param name="rowCount">The number of rows that the reader should read.</param>
        protected StructureReaderBase(int elementSize, int rowCount)
        {
            ElementSize = elementSize;
            _rowCount = rowCount;

            if (rowCount > 0)
            {
                _buffer = new TIn[rowCount];
            }
        }

        /// <summary>
        /// Reads as much elements as possible from the provided binary buffer.
        /// </summary>
        /// <param name="sequence">The binary buffer.</param>
        /// <returns>The <see cref="SequenceSize"/> that contains the number of bytes and the number of elements which were read.</returns>
        public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
        {
            if (_position >= _rowCount)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");
            }

            int byteSize = Math.Min(ElementSize * (_rowCount - _position), (int)(sequence.Length - (sequence.Length % ElementSize)));
            int elementCount = byteSize / ElementSize;
            if (elementCount == 0)
            {
                return new SequenceSize(0, 0);
            }

            int count;
            if (BitwiseCopyAllowed)
            {
                Span<byte> targetBytes = MemoryMarshal.AsBytes(new Span<TIn>(_buffer, _position, elementCount));
                Debug.Assert(byteSize == targetBytes.Length);
                sequence.Slice(0, byteSize).CopyTo(targetBytes);
                count = elementCount;
            }
            else
            {
                count = CopyTo(sequence.Slice(0, byteSize), ((Span<TIn>)_buffer).Slice(_position, elementCount));
                Debug.Assert(count >= 0 && count <= elementCount);
            }

            _position += count;
            return new SequenceSize(count * ElementSize, count);
        }

        private int CopyTo(ReadOnlySequence<byte> source, Span<TIn> target)
        {
            Span<byte> tmpSpan = stackalloc byte[ElementSize];
            int count = 0;
            for (ReadOnlySequence<byte> slice = source; !slice.IsEmpty; slice = slice.Slice(ElementSize), count++)
            {
                if (slice.FirstSpan.Length >= ElementSize)
                {
                    target[count] = ReadElement(slice.FirstSpan);
                }
                else
                {
                    slice.Slice(0, ElementSize).CopyTo(tmpSpan);
                    target[count] = ReadElement(tmpSpan);
                }
            }

            return count;
        }

        /// <summary>
        /// When overriden in a derived class reads a single element from the provided binary buffer.
        /// </summary>
        /// <param name="source">The binary buffer.</param>
        /// <returns>The decoded value.</returns>
        protected abstract TIn ReadElement(ReadOnlySpan<byte> source);

        /// <summary>
        /// When overriden in a derived class creates a column for <see cref="ClickHouseDataReader"/> with the specified settings.
        /// </summary>
        /// <param name="settings">The settings of the column.</param>
        /// <param name="buffer">The buffer that contains the column's rows.</param>
        /// <returns>A column for <see cref="ClickHouseDataReader"/>.</returns>
        protected abstract IClickHouseTableColumn<TOut> EndRead(ClickHouseColumnSettings? settings, ReadOnlyMemory<TIn> buffer);

        /// <inheritdoc/>
        public IClickHouseTableColumn<TOut> EndRead(ClickHouseColumnSettings? settings)
        {
            return EndRead(settings, ((ReadOnlyMemory<TIn>)_buffer)[.._position]);
        }

        IClickHouseTableColumn IClickHouseColumnReader.EndRead(ClickHouseColumnSettings? settings)
        {
            return EndRead(settings);
        }
    }

    /// <summary>
    /// Represents a base class capable of reading columns of value types.
    /// </summary>
    /// <typeparam name="T">The type of the column that must be a value type (struct).</typeparam>
    public abstract class StructureReaderBase<T> : StructureReaderBase<T, T>
        where T : struct
    {
        /// <summary>
        /// Initializes <see cref="StructureWriterBase{TIn, TOut}"/> with specified parameters.
        /// </summary>
        /// <param name="elementSize">The size of a single element in bytes.</param>
        /// <param name="rowCount">The number of rows that the reader should read.</param>
        public StructureReaderBase(int elementSize, int rowCount)
            : base(elementSize, rowCount)
        {
        }

        /// <summary>
        /// Creates a column for <see cref="ClickHouseDataReader"/> from the provided buffer. The column settings are ignored.
        /// </summary>
        /// <param name="settings">The settings of the column. This argument is ignored by this method.</param>
        /// <param name="buffer">The buffer that contains the column's rows.</param>
        /// <returns>A column for <see cref="ClickHouseDataReader"/>.</returns>
        protected override IClickHouseTableColumn<T> EndRead(ClickHouseColumnSettings? settings, ReadOnlyMemory<T> buffer)
        {
            return new StructureTableColumn<T>(buffer);
        }
    }
}
