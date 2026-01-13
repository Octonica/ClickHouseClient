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

using Octonica.ClickHouseClient.Exceptions;
using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class LowCardinalityTableColumn : IClickHouseTableColumn
    {
        private readonly ReadOnlyMemory<byte> _keys;
        private readonly int _keySize;
        private readonly IClickHouseTableColumn _values;
        private readonly bool _isNullable;

        public int RowCount { get; }

        public LowCardinalityTableColumn(ReadOnlyMemory<byte> keys, int keySize, IClickHouseTableColumn values, bool isNullable)
        {
            _keys = keys;
            _keySize = keySize;
            _values = values;
            _isNullable = isNullable;
            RowCount = _keys.Length / _keySize;
        }

        public bool IsNull(int index)
        {
            if (!_isNullable)
            {
                return false;
            }

            int valueIndex = GetValueIndex(index);
            return valueIndex == 0;
        }

        public object GetValue(int index)
        {
            int valueIndex = GetValueIndex(index);
            return valueIndex == 0 && _isNullable ? DBNull.Value : _values.GetValue(valueIndex);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            IClickHouseTableColumn<T>? reinterpretedValues = _values as IClickHouseTableColumn<T> ?? _values.TryReinterpret<T>();
            return reinterpretedValues == null ? null : (IClickHouseTableColumn<T>)new LowCardinalityTableColumn<T>(_keys, _keySize, reinterpretedValues, _isNullable);
        }

        IClickHouseArrayTableColumn<T>? IClickHouseTableColumn.TryReinterpretAsArray<T>()
        {
            IClickHouseArrayTableColumn<T>? reinterpretedValues = _values as IClickHouseArrayTableColumn<T> ?? _values.TryReinterpretAsArray<T>();
            return reinterpretedValues == null
                ? null
                : (IClickHouseArrayTableColumn<T>)new LowCardinalityArrayTableColumn<T>(this, _keys, _keySize, reinterpretedValues, _isNullable);
        }

        bool IClickHouseTableColumn.TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, [MaybeNullWhen(false)] out T dispatchedValue)
        {
            dispatchedValue = default;
            return false;
        }

        private int GetValueIndex(int index)
        {
            if (index < 0 || index > RowCount)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            int valueIndex = 0;
            Span<byte> valueIndexBytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref valueIndex, 1))[.._keySize];
            _keys.Slice(index * _keySize, _keySize).Span.CopyTo(valueIndexBytes);

            return valueIndex;
        }
    }

    internal sealed class LowCardinalityTableColumn<TValue> : IClickHouseTableColumn<TValue>
    {
        private readonly ReadOnlyMemory<byte> _keys;
        private readonly int _keySize;
        private readonly IClickHouseTableColumn<TValue> _values;
        private readonly bool _isNullable;

        public int RowCount { get; }

        public TValue DefaultValue => _values.DefaultValue;

        public LowCardinalityTableColumn(ReadOnlyMemory<byte> keys, int keySize, IClickHouseTableColumn<TValue> values, bool isNullable)
        {
            _keys = keys;
            _keySize = keySize;
            _values = values;
            _isNullable = isNullable;
            RowCount = _keys.Length / _keySize;
        }

        public bool IsNull(int index)
        {
            if (!_isNullable)
            {
                return false;
            }

            int valueIndex = GetValueIndex(index);
            return valueIndex == 0;
        }

        public TValue GetValue(int index)
        {
            int valueIndex = GetValueIndex(index);
            if (valueIndex == 0 && _isNullable)
            {
                TValue? defaultValue = default;
                return defaultValue is not null
                    ? throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, $"Can't convert NULL to \"{typeof(TValue)}\".")
                    : defaultValue!;
            }

            return _values.GetValue(valueIndex);
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            int valueIndex = GetValueIndex(index);
            return valueIndex == 0 && _isNullable ? DBNull.Value : ((IClickHouseTableColumn)_values).GetValue(valueIndex);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            IClickHouseTableColumn<T>? reinterpretedValues = _values as IClickHouseTableColumn<T> ?? _values.TryReinterpret<T>();
            return reinterpretedValues == null ? null : (IClickHouseTableColumn<T>)new LowCardinalityTableColumn<T>(_keys, _keySize, reinterpretedValues, _isNullable);
        }

        IClickHouseArrayTableColumn<T>? IClickHouseTableColumn.TryReinterpretAsArray<T>()
        {
            IClickHouseArrayTableColumn<T>? reinterpretedValues = _values as IClickHouseArrayTableColumn<T> ?? _values.TryReinterpretAsArray<T>();
            return reinterpretedValues == null
                ? null
                : (IClickHouseArrayTableColumn<T>)new LowCardinalityArrayTableColumn<T>(this, _keys, _keySize, reinterpretedValues, _isNullable);
        }

        bool IClickHouseTableColumn.TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, out T dispatchedValue)
        {
            dispatchedValue = dispatcher.Dispatch(this);
            return true;
        }

        private int GetValueIndex(int index)
        {
            if (index < 0 || index > RowCount)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            int valueIndex = 0;
            Span<byte> valueIndexBytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref valueIndex, 1))[.._keySize];
            _keys.Slice(index * _keySize, _keySize).Span.CopyTo(valueIndexBytes);

            return valueIndex;
        }
    }

    internal sealed class LowCardinalityArrayTableColumn<TElement> : IClickHouseArrayTableColumn<TElement>
    {
        private readonly IClickHouseTableColumn _reinterpretationRoot;
        private readonly ReadOnlyMemory<byte> _keys;
        private readonly int _keySize;
        private readonly IClickHouseArrayTableColumn<TElement> _values;
        private readonly bool _isNullable;

        public int RowCount { get; }

        public LowCardinalityArrayTableColumn(IClickHouseTableColumn reinterpretationRoot, ReadOnlyMemory<byte> keys, int keySize, IClickHouseArrayTableColumn<TElement> values, bool isNullable)
        {
            _reinterpretationRoot = reinterpretationRoot;
            _keys = keys;
            _keySize = keySize;
            _values = values;
            _isNullable = isNullable;
            RowCount = _keys.Length / _keySize;
        }

        public int CopyTo(int index, Span<TElement> buffer, int dataOffset)
        {
            int valueIndex = GetValueIndex(index);
            return valueIndex == 0 && _isNullable
                ? throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Can't copy NULL value to the buffer.")
                : _values.CopyTo(valueIndex, buffer, dataOffset);
        }

        public object GetValue(int index)
        {
            int valueIndex = GetValueIndex(index);
            return valueIndex == 0 && _isNullable ? DBNull.Value : _values.GetValue(valueIndex);
        }

        public bool IsNull(int index)
        {
            if (!_isNullable)
            {
                return false;
            }

            int valueIndex = GetValueIndex(index);
            return valueIndex == 0;
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            return _reinterpretationRoot as IClickHouseTableColumn<T> ?? _reinterpretationRoot.TryReinterpret<T>();
        }

        IClickHouseArrayTableColumn<T>? IClickHouseTableColumn.TryReinterpretAsArray<T>()
        {
            return _reinterpretationRoot as IClickHouseArrayTableColumn<T> ?? _reinterpretationRoot.TryReinterpretAsArray<T>();
        }

        bool IClickHouseTableColumn.TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, [MaybeNullWhen(false)] out T dispatchedValue)
        {
            dispatchedValue = default;
            return false;
        }

        private int GetValueIndex(int index)
        {
            if (index < 0 || index > RowCount)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            int valueIndex = 0;
            Span<byte> valueIndexBytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref valueIndex, 1))[.._keySize];
            _keys.Slice(index * _keySize, _keySize).Span.CopyTo(valueIndexBytes);

            return valueIndex;
        }
    }
}
