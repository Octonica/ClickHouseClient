#region License Apache 2.0
/* Copyright 2021 Octonica
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

using System.Collections.Generic;
using System.Diagnostics;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class KeyValuePairTableColumn<TKey, TValue> : TupleTableColumnBase, IClickHouseTableColumn<KeyValuePair<TKey, TValue>>
    {
        private readonly IClickHouseTableColumn<TKey> _keyColumn;
        private readonly IClickHouseTableColumn<TValue> _valueColumn;

        public KeyValuePairTableColumn(int rowCount, IClickHouseTableColumn<TKey> keyColumn, IClickHouseTableColumn<TValue> valueColumn)
            : base(rowCount)
        {
            _keyColumn = keyColumn;
            _valueColumn = valueColumn;
        }

        protected override object GetTupleValue(int index)
        {
            return GetValue(index);
        }

        public new KeyValuePair<TKey, TValue> GetValue(int index)
        {
            CheckIndex(index);

            return new KeyValuePair<TKey, TValue>(_keyColumn.GetValue(index), _valueColumn.GetValue(index));            
        }

        public override IEnumerable<IClickHouseTableColumn> GetColumns()
        {
            yield return _keyColumn;
            yield return _valueColumn;
        }

        protected override T Dispatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher)
        {
            return dispatcher.Dispatch(this);
        }

        internal class Reinterpreter : ReinterpreterBase
        {
            public override TupleTableColumnBase? TryReinterpret(int rowCount, IReadOnlyList<IClickHouseTableColumn> columns)
            {
                Debug.Assert(columns.Count == 2);

                var keyColumn = TryReinterpret<TKey>(columns[0]);
                if (keyColumn == null)
                    return null;

                var valueColumn = TryReinterpret<TValue>(columns[1]);
                if (valueColumn == null)
                    return null;

                return new KeyValuePairTableColumn<TKey, TValue>(rowCount, keyColumn, valueColumn);
            }
        }
    }
}
