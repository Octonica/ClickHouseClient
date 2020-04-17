#region License Apache 2.0
/* Copyright 2019-2020 Octonica
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

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class ReinterpretedTableColumn<TFrom, TTo> : IClickHouseTableColumn<TTo>
    {
        private readonly IClickHouseTableColumn? _reinterpretationRoot;
        private readonly IClickHouseTableColumn<TFrom> _sourceColumn;
        private readonly Func<TFrom, TTo> _reinterpret;

        public int RowCount => _sourceColumn.RowCount;

        public ReinterpretedTableColumn(IClickHouseTableColumn<TFrom> sourceColumn, Func<TFrom, TTo> reinterpret)
        {
            _sourceColumn = sourceColumn ?? throw new ArgumentNullException(nameof(sourceColumn));
            _reinterpret = reinterpret ?? throw new ArgumentNullException(nameof(reinterpret));
        }

        public ReinterpretedTableColumn(IClickHouseTableColumn reinterpretationRoot, IClickHouseTableColumn<TFrom> sourceColumn, Func<TFrom, TTo> reinterpret)
        {
            _reinterpretationRoot = reinterpretationRoot ?? throw new ArgumentNullException(nameof(reinterpretationRoot));
            _sourceColumn = sourceColumn ?? throw new ArgumentNullException(nameof(sourceColumn));
            _reinterpret = reinterpret ?? throw new ArgumentNullException(nameof(reinterpret));
        }

        public bool IsNull(int index)
        {
            return _sourceColumn.IsNull(index);
        }

        public TTo GetValue(int index)
        {
            var value = _sourceColumn.GetValue(index);
            return _reinterpret(value);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            return (_reinterpretationRoot ?? _sourceColumn).TryReinterpret<T>();
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            var sourceValue = ((IClickHouseTableColumn) _sourceColumn).GetValue(index);
            if (sourceValue == DBNull.Value)
                return sourceValue;

            var reinterpreted = _reinterpret((TFrom) sourceValue);
            return reinterpreted!;
        }
    }

    internal sealed class ReinterpretedTableColumn<TValue> : IClickHouseTableColumn<TValue>
    {
        private readonly IClickHouseTableColumn _reinterpretationRoot;
        private readonly IClickHouseTableColumn<TValue> _column;

        public int RowCount => _column.RowCount;

        public ReinterpretedTableColumn(IClickHouseTableColumn reinterpretationRoot, IClickHouseTableColumn<TValue> column)
        {
            _reinterpretationRoot = reinterpretationRoot ?? throw new ArgumentNullException(nameof(reinterpretationRoot));
            _column = column ?? throw new ArgumentNullException(nameof(column));
        }

        public bool IsNull(int index)
        {
            return _column.IsNull(index);
        }

        public TValue GetValue(int index)
        {
            return _column.GetValue(index);
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            return ((IClickHouseTableColumn) _column).GetValue(index);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            return _reinterpretationRoot.TryReinterpret<T>();
        }
    }
}
