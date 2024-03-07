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

using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using System;
using System.Buffers;
using System.Diagnostics;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class CustomSerializationSkippingColumnReader : IClickHouseColumnReaderBase
    {
        private readonly int _rowCount;
        private readonly IClickHouseColumnTypeInfo _typeInfo;

        private ClickHouseColumnSerializationMode _mode;
        private int _sparseRowPostion;
        private int _expectedBaseRowCount = -1;
        private int _valueRowCount;
        private IClickHouseColumnReaderBase? _baseReader;

        public CustomSerializationSkippingColumnReader(IClickHouseColumnTypeInfo typeInfo, int rowCount, ClickHouseColumnSerializationMode mode)
        {
            _typeInfo = typeInfo;
            _mode = mode;
            _rowCount = rowCount;
        }

        public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
        {
            switch (_mode)
            {
                case ClickHouseColumnSerializationMode.Custom:
                    if (sequence.IsEmpty)
                        return SequenceSize.Empty;

                    // The prefix consists of a single byte encoding the serialization mode
                    var mode = (ClickHouseColumnSerializationMode)sequence.FirstSpan[0];
                    if (mode == ClickHouseColumnSerializationMode.Sparse || mode == ClickHouseColumnSerializationMode.Default)
                    {
                        _mode = mode;
                        var result = ReadNext(sequence.Slice(1));
                        return result.AddBytes(1);
                    }

                    throw new ClickHouseException(ClickHouseErrorCodes.ProtocolUnexpectedResponse, $"Expected one of serialization modes: sparse or default. Received value: {mode}.");

                case ClickHouseColumnSerializationMode.Default:
                    _baseReader ??= _typeInfo.CreateSkippingColumnReader(_rowCount);
                    return _baseReader.ReadNext(sequence);

                case ClickHouseColumnSerializationMode.Sparse:
                    return SkipSparse(sequence);

                default:
                    throw new InvalidOperationException($"Internal error. Unexpected column serialization mode: {_mode}.");
            }
        }

        private SequenceSize SkipSparse(ReadOnlySequence<byte> sequence)
        {
            if (_expectedBaseRowCount == 0)
                throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");

            if (_expectedBaseRowCount > 0)
            {
                Debug.Assert(_baseReader != null);

                var result = _baseReader.ReadNext(sequence);
                _expectedBaseRowCount -= result.Elements;

                if (_expectedBaseRowCount < 0)
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");

                if (_expectedBaseRowCount == 0)
                {
                    // Calculate the number of rows with default values
                    int defaultRowsCount = _rowCount - _valueRowCount;
                    return result.AddElements(defaultRowsCount);
                }

                return result;
            }

            const ulong endOfGranuleFlag = 1ul << 62;
            var seq = sequence;
            int totalBytes = 0;
            while (true)
            {
                if (!ClickHouseBinaryProtocolReader.TryRead7BitInteger(seq, out var group_size, out var bytesRead))
                    return new SequenceSize(totalBytes, 0);

                totalBytes += bytesRead;
                seq = seq.Slice(bytesRead);

                var endOfGranule = (group_size & endOfGranuleFlag) == endOfGranuleFlag;
                group_size &= ~endOfGranuleFlag;
                _sparseRowPostion += checked((int)group_size);

                if (_sparseRowPostion >= _rowCount)
                {
                    _sparseRowPostion = _rowCount;
                    if (endOfGranule)
                        break;

                    // It may be ok, but non-final group with an overflow looks suspicious
                    Debug.Fail("Unexpected group after the end of the column");
                    continue;
                }

                ++_sparseRowPostion;
                ++_valueRowCount;

                if (endOfGranule)
                {
                    _valueRowCount += _rowCount - _sparseRowPostion;
                    break;
                }
            }

            Debug.Assert(_rowCount >= _sparseRowPostion);
            Debug.Assert(_expectedBaseRowCount == -1);
            _expectedBaseRowCount = _valueRowCount;

            _baseReader = _typeInfo.CreateSkippingColumnReader(_expectedBaseRowCount);
            if (_expectedBaseRowCount == 0)
            {
                // The column consists only of default values
                return new SequenceSize(totalBytes, _rowCount);
            }

            Debug.Assert(_expectedBaseRowCount > 0);
            var baseResult = ReadNext(seq);
            return baseResult.AddBytes(totalBytes);
        }
    }
}
