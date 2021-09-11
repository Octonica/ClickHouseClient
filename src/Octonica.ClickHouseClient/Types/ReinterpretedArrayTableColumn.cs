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

using System;
using System.Diagnostics.CodeAnalysis;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class ReinterpretedArrayTableColumn<TElement> : IClickHouseArrayTableColumn<TElement>
    {
        private readonly IClickHouseTableColumn _reinterpretationRoot;
        private readonly IClickHouseArrayTableColumn<TElement> _arrayColumn;

        public int RowCount => _arrayColumn.RowCount;

        public ReinterpretedArrayTableColumn(IClickHouseTableColumn reinterpretationRoot, IClickHouseArrayTableColumn<TElement> arrayColumn)
        {
            _reinterpretationRoot = reinterpretationRoot;
            _arrayColumn = arrayColumn;
        }

        public int CopyTo(int index, Span<TElement> buffer, int dataOffset)
        {
            return _arrayColumn.CopyTo(index, buffer, dataOffset);
        }

        public object GetValue(int index)
        {
            return _arrayColumn.GetValue(index);
        }

        public bool IsNull(int index)
        {
            return _arrayColumn.IsNull(index);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            return _reinterpretationRoot as IClickHouseTableColumn<T> ?? _reinterpretationRoot.TryReinterpret<T>();
        }

        IClickHouseArrayTableColumn<T>? IClickHouseTableColumn.TryReinterpretAsArray<T>()
        {
            return _reinterpretationRoot as IClickHouseArrayTableColumn<T> ?? _reinterpretationRoot.TryReinterpretAsArray<T>();
        }

        bool IClickHouseTableColumn.TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, [MaybeNullWhen(false)] out T dispatchedValue)
        {
            dispatchedValue = default;
            return false;
        }
    }
}
