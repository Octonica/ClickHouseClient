#region License Apache 2.0
/* Copyright 2021, 2024 Octonica
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
using System.Numerics;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class BigIntegerTableColumn : IClickHouseTableColumn<BigInteger>
    {
        private readonly byte[] _rawData;
        private readonly int _elementByteSize;
        private readonly bool _isUnsigned;

        public int RowCount { get; }

        public BigInteger DefaultValue => BigInteger.Zero;

        public BigIntegerTableColumn(byte[] rawData, int rowCount, int elementByteSize, bool isUnsigned)
        {
            _rawData = rawData;
            RowCount = rowCount;
            _elementByteSize = elementByteSize;
            _isUnsigned = isUnsigned;
        }

        public BigInteger GetValue(int index)
        {
            if (index < 0 || index >= RowCount)
                throw new IndexOutOfRangeException();

            var slice = ((ReadOnlySpan<byte>)_rawData).Slice(index * _elementByteSize, _elementByteSize);
            return new BigInteger(slice, _isUnsigned);
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            return GetValue(index);
        }

        public bool IsNull(int index)
        {
            return false;
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            if (typeof(T) == typeof(BigInteger?))
                return (IClickHouseTableColumn<T>)(object)new NullableStructTableColumn<BigInteger>(null, this);

            return null;
        }

        bool IClickHouseTableColumn.TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, out T dispatchedValue)
        {
            dispatchedValue = dispatcher.Dispatch(this);
            return true;
        }
    }
}
