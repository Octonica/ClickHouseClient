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
using System.Collections.Generic;
using System.Diagnostics;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class EnumTableColumn<TValue> : IClickHouseTableColumn<string>
        where TValue : struct
    {
        private readonly IClickHouseTableColumn<TValue> _internalColumn;
        private readonly IReadOnlyDictionary<TValue, string> _reversedEnumMap;

        public int RowCount => _internalColumn.RowCount;

        public EnumTableColumn(IClickHouseTableColumn<TValue> internalColumn, IReadOnlyDictionary<TValue, string> reversedEnumMap)
        {
            _internalColumn = internalColumn;
            _reversedEnumMap = reversedEnumMap;
        }

        public bool IsNull(int index)
        {
            Debug.Assert(!_internalColumn.IsNull(index));
            return false;
        }

        public string GetValue(int index)
        {
            var value = _internalColumn.GetValue(index);
            if (!_reversedEnumMap.TryGetValue(value, out var strValue))
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
    }
}
