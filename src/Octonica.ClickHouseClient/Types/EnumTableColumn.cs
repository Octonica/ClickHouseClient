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
using System.Collections.Generic;
using System.Diagnostics;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class EnumTableColumn<TKey> : IClickHouseTableColumn<string>
        where TKey : struct
    {
        private readonly IClickHouseTableColumn<TKey> _internalColumn;
        private readonly IReadOnlyDictionary<TKey, string> _valueMap;

        public int RowCount => _internalColumn.RowCount;

        public EnumTableColumn(IClickHouseTableColumn<TKey> internalColumn, IReadOnlyDictionary<TKey, string> valueMap)
        {
            _internalColumn = internalColumn;
            _valueMap = valueMap;
        }

        public bool IsNull(int index)
        {
            Debug.Assert(!_internalColumn.IsNull(index));
            return false;
        }

        public string GetValue(int index)
        {
            var value = _internalColumn.GetValue(index);
            if (!_valueMap.TryGetValue(value, out var strValue))
                throw new InvalidCastException($"There is no string representation for the value {value} in the enum.");

            return strValue;
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            return GetValue(index);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            var internalReinterpreted = _internalColumn as IClickHouseTableColumn<T> ?? _internalColumn.TryReinterpret<T>();
            if (internalReinterpreted != null)
                return new ReinterpretedTableColumn<T>(this, internalReinterpreted);

            return null;
        }

        bool IClickHouseTableColumn.TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, out T dispatchedValue)
        {
            dispatchedValue = dispatcher.Dispatch(this);
            return true;
        }
    }

    internal sealed class EnumTableColumn<TKey, TEnum> : IClickHouseTableColumn<TEnum>
        where TKey : struct
        where TEnum : Enum
    {
        private readonly IClickHouseTableColumn<TKey> _internalColumn;
        private readonly IReadOnlyDictionary<TKey, TEnum> _enumMap;
        private readonly IReadOnlyDictionary<TKey, string> _stringMap;

        public int RowCount => _internalColumn.RowCount;

        public EnumTableColumn(IClickHouseTableColumn<TKey> internalColumn, IReadOnlyDictionary<TKey, TEnum> enumMap, IReadOnlyDictionary<TKey, string> stringMap)
        {
            _internalColumn = internalColumn;
            _enumMap = enumMap;
            _stringMap = stringMap;
        }

        public bool IsNull(int index)
        {
            Debug.Assert(!_internalColumn.IsNull(index));
            return false;
        }

        public TEnum GetValue(int index)
        {
            var value = _internalColumn.GetValue(index);
            if (!_enumMap.TryGetValue(value, out var strValue))
            {
                if (_stringMap.TryGetValue(value, out var nativeValue))
                    throw new InvalidCastException($"The value '{nativeValue}'={value} of the enum can't be converted to the type '{typeof(Enum).FullName}'.");

                throw new InvalidCastException($"The value {value} doesn't belong to the enum.");
            }

            return strValue;
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            return GetValue(index);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            IClickHouseTableColumn<T>? reinterpretedColumn;
            if (typeof(T) == typeof(string))
                reinterpretedColumn = (IClickHouseTableColumn<T>) (object) new EnumTableColumn<TKey>(_internalColumn, _stringMap);
            else
                reinterpretedColumn = _internalColumn.TryReinterpret<T>();

            if (reinterpretedColumn == null)
                return null;

            return new ReinterpretedTableColumn<T>(this, reinterpretedColumn);
        }

        bool IClickHouseTableColumn.TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, out T dispatchedValue)
        {
            dispatchedValue = dispatcher.Dispatch(this);
            return true;
        }
    }
}
