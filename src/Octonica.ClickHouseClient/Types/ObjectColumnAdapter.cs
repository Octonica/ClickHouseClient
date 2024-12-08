#region License Apache 2.0
/* Copyright 2019-2021, 2024 Octonica
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
    internal sealed class ObjectColumnAdapter : IClickHouseReinterpretedTableColumn<object>
    {
        private readonly IClickHouseTableColumn _tableColumn;

        public int RowCount => _tableColumn.RowCount;

        public object DefaultValue => throw new NotSupportedException("The default value is not supported for the column of type Object.");

        public ObjectColumnAdapter(IClickHouseTableColumn tableColumn)
        {
            _tableColumn = tableColumn;
        }

        public bool IsNull(int index)
        {
            return _tableColumn.IsNull(index);
        }

        public object GetValue(int index)
        {
            return _tableColumn.GetValue(index);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            return _tableColumn.TryReinterpret<T>();
        }

        bool IClickHouseTableColumn.TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, out T dispatchedValue)
        {
            dispatchedValue = dispatcher.Dispatch(this);
            return true;
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            return GetValue(index);
        }

        public IClickHouseReinterpretedTableColumn<TResult> Chain<TResult>(Func<object, TResult> reinterpret)
        {
            return new ReinterpretedObjectTableColumn<TResult>(_tableColumn, reinterpret);
        }
    }
}
