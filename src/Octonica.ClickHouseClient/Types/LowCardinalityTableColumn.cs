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
using System.Runtime.InteropServices;
using Octonica.ClickHouseClient.Exceptions;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class LowCardinalityTableColumn : IClickHouseTableColumn
    {
        private readonly ReadOnlyMemory<byte> _keys;
        private readonly int _keySize;
        private readonly IClickHouseTableColumn _values;

        public int RowCount { get; }

        public LowCardinalityTableColumn(ReadOnlyMemory<byte> keys, int keySize, IClickHouseTableColumn values)
        {
            _keys = keys;
            _keySize = keySize;
            _values = values;
            RowCount = _keys.Length / _keySize;
        }

        public bool IsNull(int index)
        {
            if (index < 0 || index > RowCount)
                throw new ArgumentOutOfRangeException(nameof(index));

            var valueIndex = GetValueIndex(index);
            return valueIndex == 0;
        }

        public object GetValue(int index)
        {
            var valueIndex = GetValueIndex(index);
            if (valueIndex == 0)
                return DBNull.Value;

            return _values.GetValue(valueIndex);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            var reinterpretedValues = _values as IClickHouseTableColumn<T> ?? _values.TryReinterpret<T>();
            if (reinterpretedValues == null)
                return null;

            return new LowCardinalityTableColumn<T>(_keys, _keySize, reinterpretedValues);
        }

        IClickHouseArrayTableColumn<T>? IClickHouseTableColumn.TryReinterpretAsArray<T>()
        {
            var reinterpretedValues = _values as IClickHouseArrayTableColumn<T> ?? _values.TryReinterpretAsArray<T>();
            if (reinterpretedValues == null)
                return null;

            return new LowCardinalityArrayTableColumn<T>(this, _keys, _keySize, reinterpretedValues);
        }

        private int GetValueIndex(int index)
        {
            if (index < 0 || index > RowCount)
                throw new ArgumentOutOfRangeException(nameof(index));

            int valueIndex = 0;
            var valueIndexBytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref valueIndex, 1)).Slice(0, _keySize);
            _keys.Slice(index * _keySize, _keySize).Span.CopyTo(valueIndexBytes);

            return valueIndex;
        }
    }

    internal sealed class LowCardinalityTableColumn<TValue> : IClickHouseTableColumn<TValue>
    {
        private readonly ReadOnlyMemory<byte> _keys;
        private readonly int _keySize;
        private readonly IClickHouseTableColumn<TValue> _values;

        public int RowCount { get; }
    
        public LowCardinalityTableColumn(ReadOnlyMemory<byte> keys, int keySize, IClickHouseTableColumn<TValue> values)
        {
            _keys = keys;
            _keySize = keySize;
            _values = values;
            RowCount = _keys.Length / _keySize;
        }

        public bool IsNull(int index)
        {
            if (index < 0 || index>RowCount)
                throw new ArgumentOutOfRangeException(nameof(index));

            var valueIndex = GetValueIndex(index);
            return valueIndex == 0;
        }

        public TValue GetValue(int index)
        {
            var valueIndex = GetValueIndex(index);
            if (valueIndex == 0)
            {
                var defaultValue = default(TValue);
                if (!(defaultValue is null))
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, $"Can't convert NULL to \"{typeof(TValue)}\".");

                return defaultValue!;
            }

            return _values.GetValue(valueIndex);
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            var valueIndex = GetValueIndex(index);
            if (valueIndex == 0)
                return DBNull.Value;

            return ((IClickHouseTableColumn) _values).GetValue(valueIndex);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            var reinterpretedValues = _values as IClickHouseTableColumn<T> ?? _values.TryReinterpret<T>();
            if (reinterpretedValues == null)
                return null;

            return new LowCardinalityTableColumn<T>(_keys, _keySize, reinterpretedValues);
        }

        IClickHouseArrayTableColumn<T>? IClickHouseTableColumn.TryReinterpretAsArray<T>()
        {
            var reinterpretedValues = _values as IClickHouseArrayTableColumn<T> ?? _values.TryReinterpretAsArray<T>();
            if (reinterpretedValues == null)
                return null;

            return new LowCardinalityArrayTableColumn<T>(this, _keys, _keySize, reinterpretedValues);
        }

        private int GetValueIndex(int index)
        {
            if (index < 0 || index > RowCount)
                throw new ArgumentOutOfRangeException(nameof(index));

            int valueIndex = 0;
            var valueIndexBytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref valueIndex, 1)).Slice(0, _keySize);
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

        public int RowCount { get; }

        public LowCardinalityArrayTableColumn(IClickHouseTableColumn reinterpretationRoot, ReadOnlyMemory<byte> keys, int keySize, IClickHouseArrayTableColumn<TElement> values)
        {
            _reinterpretationRoot = reinterpretationRoot;
            _keys = keys;
            _keySize = keySize;
            _values = values;
            RowCount = _keys.Length / _keySize;
        }

        public int CopyTo(int index, Span<TElement> buffer, int dataOffset)
        {
            var valueIndex = GetValueIndex(index);
            if (valueIndex == 0)
                throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Can't copy NULL value to the buffer.");

            return _values.CopyTo(valueIndex, buffer, dataOffset);
        }

        public object GetValue(int index)
        {
            var valueIndex = GetValueIndex(index);
            if (valueIndex == 0)
                return DBNull.Value;

            return _values.GetValue(valueIndex);
        }

        public bool IsNull(int index)
        {
            var valueIndex = GetValueIndex(index);
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

        private int GetValueIndex(int index)
        {
            if (index < 0 || index > RowCount)
                throw new ArgumentOutOfRangeException(nameof(index));

            int valueIndex = 0;
            var valueIndexBytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref valueIndex, 1)).Slice(0, _keySize);
            _keys.Slice(index * _keySize, _keySize).Span.CopyTo(valueIndexBytes);

            return valueIndex;
        }
    }
}
