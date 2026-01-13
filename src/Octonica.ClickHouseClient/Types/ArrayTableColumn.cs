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

using Octonica.ClickHouseClient.Utils;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class ArrayTableColumn : IClickHouseTableColumn
    {
        private readonly IClickHouseTableColumn _column;
        private readonly List<(int offset, int length)> _ranges;

        public int RowCount => _ranges.Count;

        public ArrayTableColumn(IClickHouseTableColumn column, List<(int offset, int length)> ranges)
        {
            _column = column ?? throw new ArgumentNullException(nameof(column));
            _ranges = ranges ?? throw new ArgumentNullException(nameof(ranges));
        }

        public bool IsNull(int index)
        {
            return false;
        }

        public object GetValue(int index)
        {
            (int offset, int length) = _ranges[index];
            object?[] result = new object?[length];
            if (result.Length == 0)
            {
                return result;
            }

            for (int i = 0; i < result.Length; i++)
            {
                result[i] = _column.IsNull(offset + i) ? null : _column.GetValue(offset + i);
            }

            return result;
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            Type? elementType;
            Type type = typeof(T);
            return !type.IsArray || (elementType = type.GetElementType()) == null
                ? null
                : (IClickHouseTableColumn<T>?)TypeDispatcher.Dispatch(elementType, new ArrayTableColumnTypeDispatcher(_column, _ranges));
        }

        bool IClickHouseTableColumn.TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, [MaybeNullWhen(false)] out T dispatchedValue)
        {
            dispatchedValue = default;
            return false;
        }
    }

    internal sealed class ArrayTableColumn<TElement> : IClickHouseTableColumn<TElement[]>, IClickHouseArrayTableColumn<TElement>
    {
        private readonly IClickHouseTableColumn<TElement> _column;
        private readonly List<(int offset, int length)> _ranges;

        public int RowCount => _ranges.Count;

        public TElement[] DefaultValue { get; }

        public ArrayTableColumn(IClickHouseTableColumn<TElement> column, List<(int offset, int length)> ranges)
        {
            _column = column ?? throw new ArgumentNullException(nameof(column));
            _ranges = ranges ?? throw new ArgumentNullException(nameof(ranges));
            DefaultValue = Array.Empty<TElement>();
        }

        public bool IsNull(int index)
        {
            return false;
        }

        public TElement[] GetValue(int index)
        {
            (int offset, int length) = _ranges[index];
            if (length == 0)
            {
                return Array.Empty<TElement>();
            }

            TElement[] result = new TElement[length];
            for (int i = 0; i < result.Length; i++)
            {
                result[i] = _column.GetValue(offset + i);
            }

            return result;
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            Type? elementType;
            Type type = typeof(T);
            return !type.IsArray || (elementType = type.GetElementType()) == null
                ? null
                : (IClickHouseTableColumn<T>?)TypeDispatcher.Dispatch(elementType, new ArrayTableColumnTypeDispatcher(_column, _ranges));
        }

        IClickHouseArrayTableColumn<T>? IClickHouseTableColumn.TryReinterpretAsArray<T>()
        {
            IClickHouseTableColumn<T>? reinterpretedColumn = _column as IClickHouseTableColumn<T> ?? _column.TryReinterpret<T>();
            return reinterpretedColumn == null
                ? null
                : (IClickHouseArrayTableColumn<T>)new ReinterpretedArrayTableColumn<T>(this, new ArrayTableColumn<T>(reinterpretedColumn, _ranges));
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            return GetValue(index);
        }

        public int CopyTo(int index, Span<TElement> buffer, int dataOffset)
        {
            (int offset, int length) range = _ranges[index];
            if (dataOffset < 0 || dataOffset > range.length)
            {
                throw new ArgumentOutOfRangeException(nameof(dataOffset));
            }

            int length = Math.Min(range.length - dataOffset, buffer.Length);
            for (int i = 0; i < length; i++)
            {
                buffer[dataOffset + i] = _column.GetValue(range.offset + i);
            }

            return length;
        }

        bool IClickHouseTableColumn.TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, [MaybeNullWhen(false)] out T dispatchedValue)
        {
            dispatchedValue = dispatcher.Dispatch(this);
            return true;
        }
    }

    internal sealed class ArrayTableColumnTypeDispatcher : ITypeDispatcher<IClickHouseTableColumn?>
    {
        private readonly IClickHouseTableColumn _column;
        private readonly List<(int offset, int length)> _ranges;

        public ArrayTableColumnTypeDispatcher(IClickHouseTableColumn column, List<(int offset, int length)> ranges)
        {
            _column = column ?? throw new ArgumentNullException(nameof(column));
            _ranges = ranges;
        }

        public IClickHouseTableColumn? Dispatch<T>()
        {
            IClickHouseTableColumn<T>? reinterpretedColumn = _column as IClickHouseTableColumn<T> ?? _column.TryReinterpret<T>();
            if (reinterpretedColumn == null)
            {
                return null;
            }

            ArrayTableColumn<T> reinterpretedArray = new(reinterpretedColumn, _ranges);
            return reinterpretedArray;
        }
    }
}
