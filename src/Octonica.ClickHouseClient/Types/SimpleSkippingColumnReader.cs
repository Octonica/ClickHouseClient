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

using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using System;
using System.Buffers;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class SimpleSkippingColumnReader : IClickHouseColumnReaderBase
    {
        private readonly int _elementSize;
        private readonly int _rowCount;

        private int _position;

        public SimpleSkippingColumnReader(int elementSize, int rowCount)
        {
            _elementSize = elementSize;
            _rowCount = rowCount;
        }

        public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
        {
            if (_position >= _rowCount)
                throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");

            var elementCount = (int)Math.Min(_rowCount - _position, sequence.Length / _elementSize);
            var byteCount = elementCount * _elementSize;

            _position += elementCount;
            return new SequenceSize(byteCount, elementCount);
        }
    }
}
