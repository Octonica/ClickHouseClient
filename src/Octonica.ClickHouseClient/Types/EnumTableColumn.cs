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

        public string DefaultValue { get; }

        public EnumTableColumn(IClickHouseTableColumn<TKey> internalColumn, IReadOnlyDictionary<TKey, string> valueMap)
        {
            _internalColumn = internalColumn;
            _valueMap = valueMap;

            if (!_valueMap.TryGetValue(_internalColumn.DefaultValue, out string? defaultStr))
            {
                defaultStr = string.Empty;
            }

            DefaultValue = defaultStr;
        }

        public bool IsNull(int index)
        {
            Debug.Assert(!_internalColumn.IsNull(index));
            return false;
        }

        public string GetValue(int index)
        {
            TKey value = _internalColumn.GetValue(index);
            return !_valueMap.TryGetValue(value, out string? strValue)
                ? throw new InvalidCastException($"There is no string representation for the value {value} in the enum.")
                : strValue;
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            return GetValue(index);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            IClickHouseTableColumn<T>? internalReinterpreted = _internalColumn as IClickHouseTableColumn<T> ?? _internalColumn.TryReinterpret<T>();
            return internalReinterpreted != null ? new ReinterpretedTableColumn<T>(this, internalReinterpreted) : (IClickHouseTableColumn<T>?)null;
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

        public TEnum DefaultValue { get; }

        public EnumTableColumn(IClickHouseTableColumn<TKey> internalColumn, IReadOnlyDictionary<TKey, TEnum> enumMap, IReadOnlyDictionary<TKey, string> stringMap)
        {
            _internalColumn = internalColumn;
            _enumMap = enumMap;
            _stringMap = stringMap;

            if (!_enumMap.TryGetValue(_internalColumn.DefaultValue, out TEnum? defaultEnum))
            {
                defaultEnum = default;
            }

            Debug.Assert(defaultEnum != null);
            DefaultValue = defaultEnum;
        }

        public bool IsNull(int index)
        {
            Debug.Assert(!_internalColumn.IsNull(index));
            return false;
        }

        public TEnum GetValue(int index)
        {
            TKey value = _internalColumn.GetValue(index);
            if (!_enumMap.TryGetValue(value, out TEnum? strValue))
            {
                if (_stringMap.TryGetValue(value, out string? nativeValue))
                {
                    throw new InvalidCastException($"The value '{nativeValue}'={value} of the enum can't be converted to the type '{typeof(Enum).FullName}'.");
                }

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
            IClickHouseTableColumn<T>? reinterpretedColumn = typeof(T) == typeof(string)
                ? (IClickHouseTableColumn<T>)(object)new EnumTableColumn<TKey>(_internalColumn, _stringMap)
                : _internalColumn.TryReinterpret<T>();
            return reinterpretedColumn == null ? null : (IClickHouseTableColumn<T>)new ReinterpretedTableColumn<T>(this, reinterpretedColumn);
        }

        bool IClickHouseTableColumn.TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, out T dispatchedValue)
        {
            dispatchedValue = dispatcher.Dispatch(this);
            return true;
        }
    }
}
