#region License Apache 2.0
/* Copyright 2024 Octonica
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
using System.Diagnostics.CodeAnalysis;

namespace Octonica.ClickHouseClient.Types
{
    internal class VariantTableColumn : IClickHouseTableColumn
    {
        private readonly List<IClickHouseTableColumn> _values;
        private readonly byte[] _columnIndices;
        private readonly int[] _valueIndices;

        public int RowCount => _columnIndices.Length;

        public VariantTableColumn(List<IClickHouseTableColumn> values, byte[] columnIndices, int[] valueIndices)
        {
            Debug.Assert(columnIndices.Length == valueIndices.Length);

            _values = values;
            _columnIndices = columnIndices;
            _valueIndices = valueIndices;
        }

        public object GetValue(int index)
        {
            var columnIndex = _columnIndices[index];
            // 0xFF stands for NULL
            if (columnIndex == 0xFF)
                return DBNull.Value;

            return _values[columnIndex].GetValue(_valueIndices[index]);
        }

        public bool IsNull(int index)
        {
            return _columnIndices[index] == 0xFF;
        }

        public bool TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, [MaybeNullWhen(false)] out T dispatchedValue)
        {
            dispatchedValue = default;
            return false;
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            return null;
        }
    }
}
