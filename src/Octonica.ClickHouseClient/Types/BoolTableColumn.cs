#region License Apache 2.0
/* Copyright 2022, 2024 Octonica
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
    internal sealed class BoolTableColumn : IClickHouseTableColumn<bool>
    {
        private readonly ReadOnlyMemory<byte> _buffer;

        public int RowCount => _buffer.Length;

        public bool DefaultValue => false;

        public BoolTableColumn(ReadOnlyMemory<byte> buffer)
        {
            _buffer = buffer;
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            return GetValue(index);
        }

        public bool GetValue(int index)
        {
            return _buffer.Span[index] != 0;
        }

        public bool IsNull(int index)
        {
            return false;
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            // Inherit type cast logic from UInt8
            UInt8TableColumn uint8Column = new(_buffer);
            IClickHouseTableColumn<T>? reinterpreted = uint8Column as IClickHouseTableColumn<T> ?? uint8Column.TryReinterpret<T>();
            return reinterpreted == null ? null : (IClickHouseTableColumn<T>)new ReinterpretedTableColumn<T>(this, reinterpreted);
        }

        public bool TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, [MaybeNullWhen(false)] out T dispatchedValue)
        {
            dispatchedValue = dispatcher.Dispatch(this);
            return true;
        }
    }
}
