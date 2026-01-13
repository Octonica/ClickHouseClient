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
using System.Diagnostics.CodeAnalysis;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class SparseColumn<T> : IClickHouseTableColumn<T>
    {
        private readonly IClickHouseTableColumn<T> _valuesColumn;
        private readonly List<int> _offsets;
        private readonly bool _trailingDefaults;

        private int _lastHit = 0;

        public int RowCount { get; }

        public T DefaultValue { get; }

        public SparseColumn(IClickHouseTableColumn<T> valuesColumn, int rowCount, List<int> offsets, bool trailingDefaults)
        {
            _valuesColumn = valuesColumn;
            RowCount = rowCount;
            _offsets = offsets;
            _trailingDefaults = trailingDefaults;
            DefaultValue = _valuesColumn.DefaultValue;
        }

        private SparseColumn(IClickHouseTableColumn<T> valuesColumn, int rowCount, List<int> offsets, bool trailingDefaults, int lastHit)
        {
            _valuesColumn = valuesColumn;
            RowCount = rowCount;
            _offsets = offsets;
            _trailingDefaults = trailingDefaults;
            _lastHit = lastHit;
            DefaultValue = _valuesColumn.DefaultValue;
        }

        public T GetValue(int index)
        {
            int valueIndex = GetValueIndex(index);
            return valueIndex < 0 ? DefaultValue : _valuesColumn.GetValue(valueIndex);
        }

        public bool IsNull(int index)
        {
            int valueIndex = GetValueIndex(index);
            return valueIndex < 0 ? DefaultValue is null : _valuesColumn.IsNull(valueIndex);
        }

        public bool TryDipatch<TOut>(IClickHouseTableColumnDispatcher<TOut> dispatcher, [MaybeNullWhen(false)] out TOut dispatchedValue)
        {
            dispatchedValue = dispatcher.Dispatch(this);
            return true;
        }

        public IClickHouseTableColumn<TAs>? TryReinterpret<TAs>()
        {
            IClickHouseTableColumn<TAs>? valuesReinterpreted = _valuesColumn.TryReinterpret<TAs>();
            return valuesReinterpreted == null
                ? null
                : (IClickHouseTableColumn<TAs>)new SparseColumn<TAs>(valuesReinterpreted, RowCount, _offsets, _trailingDefaults, _lastHit);
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            return (object?)GetValue(index) ?? DBNull.Value;
        }

        private int GetValueIndex(int index)
        {
            if (index < 0 || index >= RowCount)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            if (_offsets.Count == 0)
            {
                return -1;
            }

            int lastHit = _lastHit;
            int lastHitIdx = _offsets[lastHit];
            if (index > lastHitIdx)
            {
                int next = lastHit + 1;
                if (next == _offsets.Count)
                {
                    return _trailingDefaults ? -1 : _offsets.Count + (index - lastHitIdx);
                }

                int nextIdx = _offsets[next];
                if (index < nextIdx)
                {
                    return -1; // The most expected case
                }

                if (index == nextIdx)
                {
                    _lastHit = next;
                    return next;
                }
            }
            else if (index == lastHitIdx)
            {
                return lastHit;
            }
            else if (lastHit == 0)
            {
                // index < lastHitIdx
                return -1;
            }

            lastHit = _offsets.BinarySearch(index);
            if (lastHit >= 0)
            {
                _lastHit = lastHit;
                return lastHit;
            }

            lastHit = ~lastHit;
            _lastHit = Math.Max(lastHit - 1, 0);
            return lastHit < _offsets.Count || _trailingDefaults ? -1 : _offsets.Count + (index - _offsets[^1]);
        }
    }
}
