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

using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Octonica.ClickHouseClient.Types
{
    /// <summary>
    /// Represents a base class capable of writing columns of value types.
    /// </summary>
    /// <typeparam name="T">The type of the column that must be a value type (struct).</typeparam>
    public abstract class StructureWriterBase<T> : IClickHouseColumnWriter
        where T : struct
    {
        private readonly IReadOnlyList<T> _rows;

        private int _position;

        /// <summary>
        /// Gets the size of a single element in bytes.
        /// </summary>
        protected int ElementSize { get; }

        /// <inheritdoc/>
        public string ColumnName { get; }

        /// <inheritdoc/>
        public string ColumnType { get; }

        /// <summary>
        /// Gets the value indicating whether elements from the column can be copied to an output buffer bitwise. The default is <see langword="false"/>.
        /// </summary>
        protected virtual bool BitwiseCopyAllowed => false;

        /// <summary>
        /// Initializes <see cref="StructureWriterBase{T}"/> with specified arguments.
        /// </summary>
        /// <param name="columnName">The name of the column to write data to.</param>
        /// <param name="columnType">The full name of the ClickHouse type of the column.</param>
        /// <param name="elementSize">The size of a single element in bytes.</param>
        /// <param name="rows">The list of rows that the writer should write.</param>
        protected StructureWriterBase(string columnName, string columnType, int elementSize, IReadOnlyList<T> rows)
        {
            ElementSize = elementSize;
            _rows = rows;
            ColumnName = columnName;
            ColumnType = columnType;
        }

        /// <inheritdoc/>
        public SequenceSize WriteNext(Span<byte> writeTo)
        {
            int elementsCount = Math.Min(_rows.Count - _position, writeTo.Length / ElementSize);

            if (BitwiseCopyAllowed)
            {
                Span<T> targetSpan = MemoryMarshal.Cast<byte, T>(writeTo[..(elementsCount * ElementSize)]);
                int length = _rows.CopyTo(targetSpan, _position);
                Debug.Assert(length == elementsCount);
                _position += length;
            }
            else
            {
                for (int i = 0; i < elementsCount; i++, _position++)
                {
                    WriteElement(writeTo[(i * ElementSize)..], _rows[_position]);
                }
            }

            return new SequenceSize(elementsCount * ElementSize, elementsCount);
        }

        /// <summary>
        /// Writes a single value to the target span.
        /// </summary>
        /// <param name="writeTo">The buffer to write a single value to. It's guaranteed that the size of the buffer is not less than the size of an element.</param>
        /// <param name="value">The value that should be written.</param>
        protected abstract void WriteElement(Span<byte> writeTo, in T value);
    }

    /// <summary>
    /// Represents a base class capable of converting column's values to a value type and writing them to a column.
    /// </summary>
    /// <typeparam name="TIn">The type of the input data.</typeparam>
    /// <typeparam name="TOut">The type of the column that must be a value type (struct).</typeparam>
    public abstract class StructureWriterBase<TIn, TOut> : IClickHouseColumnWriter
        where TOut : struct
    {
        private readonly IReadOnlyList<TIn> _rows;

        private int _position;

        /// <summary>
        /// Gets the size of a single element in bytes.
        /// </summary>
        protected int ElementSize { get; }

        /// <inheritdoc/>
        public string ColumnName { get; }

        /// <inheritdoc/>
        public string ColumnType { get; }

        /// <summary>
        /// Initializes <see cref="StructureWriterBase{TIn, TOut}"/> with specified arguments.
        /// </summary>
        /// <param name="columnName">The name of the column to write data to.</param>
        /// <param name="columnType">The full name of the ClickHouse type of the column.</param>
        /// <param name="elementSize">The size of a single element in bytes.</param>
        /// <param name="rows">The list of rows that the writer should write.</param>
        protected StructureWriterBase(string columnName, string columnType, int elementSize, IReadOnlyList<TIn> rows)
        {
            ElementSize = elementSize;
            _rows = rows;
            ColumnName = columnName;
            ColumnType = columnType;
        }

        /// <inheritdoc/>
        public SequenceSize WriteNext(Span<byte> writeTo)
        {
            int elementsCount = Math.Min(_rows.Count - _position, writeTo.Length / ElementSize);

            Span<TOut> targetSpan = MemoryMarshal.Cast<byte, TOut>(writeTo[..(elementsCount * ElementSize)]);
            _position += _rows.Map(Convert).CopyTo(targetSpan, _position);

            return new SequenceSize(elementsCount * ElementSize, elementsCount);
        }

        /// <summary>
        /// When overriden in a derived type converts a single element of the type <typeparamref name="TIn"/>
        /// to a value of the type <typeparamref name="TOut"/>.
        /// </summary>
        /// <param name="value">The value that should be converted.</param>
        /// <returns>The value converted to the type <typeparamref name="TOut"/>.</returns>
        protected abstract TOut Convert(TIn value);
    }
}
