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

using Octonica.ClickHouseClient.Utils;
using System;
using System.Diagnostics.CodeAnalysis;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class ReinterpretedObjectTableColumn<TRes> : IClickHouseReinterpretedTableColumn<TRes>
    {
        private readonly IClickHouseTableColumn _column;
        private readonly Func<object, TRes> _reinterpret;

        public int RowCount => _column.RowCount;

        TRes IClickHouseTableColumn<TRes>.DefaultValue => throw new NotSupportedException("The default value is not supported for the column of type Object.");

        public ReinterpretedObjectTableColumn(IClickHouseTableColumn column, Func<object, TRes> reinterpret)
        {
            _column = column ?? throw new ArgumentNullException(nameof(column));
            _reinterpret = reinterpret ?? throw new ArgumentNullException(nameof(reinterpret));
        }

        public bool IsNull(int index)
        {
            return _column.IsNull(index);
        }

        public TRes GetValue(int index)
        {
            object value = _column.GetValue(index);
            TRes? converted = _reinterpret(value);
            return converted;
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            object value = _column.GetValue(index);
            TRes? converted = _reinterpret(value);
            return converted is null ? DBNull.Value : converted;
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            return (_column as IClickHouseTableColumn<T>) ?? _column.TryReinterpret<T>();
        }

        IClickHouseArrayTableColumn<T>? IClickHouseTableColumn.TryReinterpretAsArray<T>()
        {
            return (_column as IClickHouseArrayTableColumn<T>) ?? _column.TryReinterpretAsArray<T>();
        }

        public bool TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, [MaybeNullWhen(false)] out T dispatchedValue)
        {
            dispatchedValue = dispatcher.Dispatch(this);
            return true;
        }

        public IClickHouseReinterpretedTableColumn<TResult> Chain<TResult>(Func<TRes, TResult> convert)
        {
            return new ReinterpretedObjectTableColumn<TResult>(_column, FunctionHelper.Combine(_reinterpret, convert));
        }
    }
}