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
using System.Collections.Generic;
using System.Diagnostics;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class CustomSerializationColumnReader : IClickHouseColumnReader, IClickHouseTableColumnDispatcher<IClickHouseTableColumn>
    {
        private readonly int _rowCount;
        private readonly IClickHouseColumnTypeInfo _typeInfo;

        private ClickHouseColumnSerializationMode _mode;
        private bool _trailingDefaults;
        private int _sparseRowPostion;
        private List<int>? _offsets;
        private int _expectedBaseRowCount = -1;
        private IClickHouseColumnReader? _baseReader;

        public CustomSerializationColumnReader(IClickHouseColumnTypeInfo typeInfo, int rowCount, ClickHouseColumnSerializationMode mode)
        {
            _typeInfo = typeInfo;
            _mode = mode;
            _rowCount = rowCount;
        }

        public IClickHouseTableColumn? EndRead(ClickHouseColumnSettings? settings)
        {
            if (_baseReader == null)
            {
                return _typeInfo.CreateColumnReader(0).EndRead(settings);
            }

            IClickHouseTableColumn columnInfo = _baseReader.EndRead(settings)!;
            return _mode == ClickHouseColumnSerializationMode.Sparse
                ? columnInfo.TryDipatch(this, out IClickHouseTableColumn? dispatchedColumn)
                    ? dispatchedColumn!
                    : throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Sparse column reader error. A type of the column was not dispatched.")
                : columnInfo;
        }

        IClickHouseTableColumn IClickHouseTableColumnDispatcher<IClickHouseTableColumn>.Dispatch<T>(IClickHouseTableColumn<T> column)
        {
            Debug.Assert(_offsets != null);
            return new SparseColumn<T>(column, _rowCount, _offsets, _trailingDefaults);
        }

        public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
        {
            switch (_mode)
            {
                case ClickHouseColumnSerializationMode.Custom:
                    if (sequence.IsEmpty)
                    {
                        return SequenceSize.Empty;
                    }

                    // The prefix consists of a single byte encoding the serialization mode
                    ClickHouseColumnSerializationMode mode = (ClickHouseColumnSerializationMode)sequence.FirstSpan[0];
                    if (mode is ClickHouseColumnSerializationMode.Sparse or ClickHouseColumnSerializationMode.Default)
                    {
                        _mode = mode;
                        SequenceSize result = ReadNext(sequence.Slice(1));
                        return result.AddBytes(1);
                    }

                    throw new ClickHouseException(ClickHouseErrorCodes.ProtocolUnexpectedResponse, $"Expected one of serialization modes: sparse or default. Received value: {mode}.");

                case ClickHouseColumnSerializationMode.Default:
                    _baseReader ??= _typeInfo.CreateColumnReader(_rowCount);
                    return _baseReader.ReadNext(sequence);

                case ClickHouseColumnSerializationMode.Sparse:
                    return ReadSparse(sequence);

                default:
                    throw new InvalidOperationException($"Internal error. Unexpected column serialization mode: {_mode}.");
            }
        }

        private SequenceSize ReadSparse(ReadOnlySequence<byte> sequence)
        {
            if (_expectedBaseRowCount == 0)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");
            }

            if (_expectedBaseRowCount > 0)
            {
                Debug.Assert(_offsets != null);
                Debug.Assert(_baseReader != null);

                SequenceSize result = _baseReader.ReadNext(sequence);
                _expectedBaseRowCount -= result.Elements;

                if (_expectedBaseRowCount < 0)
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");
                }

                if (_expectedBaseRowCount == 0)
                {
                    // Calculate the number of rows with default values
                    int defaultRowsCount = _rowCount - _offsets.Count;
                    if (!_trailingDefaults)
                    {
                        int lastNonDefaultIdx = _offsets.Count == 0 ? -1 : _offsets[^1];
                        defaultRowsCount -= _rowCount - (lastNonDefaultIdx + 1);
                    }

                    return result.AddElements(defaultRowsCount);
                }

                return result;
            }

            if (_offsets == null)
            {
                // The default ratio from the docs
                // https://clickhouse.com/docs/en/operations/settings/merge-tree-settings#ratio_of_defaults_for_sparse_serialization
                const double defaultRatioForSparseSerialization = 0.9375;
                int capacity = Math.Max(16, (int)(_rowCount * (1 - defaultRatioForSparseSerialization)));
                _offsets = new List<int>(capacity);
            }

            const ulong endOfGranuleFlag = 1ul << 62;
            ReadOnlySequence<byte> seq = sequence;
            int totalBytes = 0;
            while (true)
            {
                if (!ClickHouseBinaryProtocolReader.TryRead7BitInteger(seq, out ulong group_size, out int bytesRead))
                {
                    return new SequenceSize(totalBytes, 0);
                }

                totalBytes += bytesRead;
                seq = seq.Slice(bytesRead);

                bool endOfGranule = (group_size & endOfGranuleFlag) == endOfGranuleFlag;
                group_size &= ~endOfGranuleFlag;
                _sparseRowPostion += checked((int)group_size);

                if (_sparseRowPostion >= _rowCount)
                {
                    _trailingDefaults = true;
                    _sparseRowPostion = _rowCount;
                    if (endOfGranule)
                    {
                        break;
                    }

                    // It may be ok, but non-final group with an overflow looks suspicious
                    Debug.Fail("Unexpected group after the end of the column");
                    continue;
                }

                _offsets.Add(_sparseRowPostion++);

                if (endOfGranule)
                {
                    break;
                }
            }

            Debug.Assert(_rowCount >= _sparseRowPostion);
            Debug.Assert(_expectedBaseRowCount == -1);

            _expectedBaseRowCount = _offsets.Count;
            if (!_trailingDefaults)
            {
                _expectedBaseRowCount += _rowCount - _sparseRowPostion;
            }

            _baseReader = _typeInfo.CreateColumnReader(_expectedBaseRowCount);
            if (_expectedBaseRowCount == 0)
            {
                // The column consists only of default values
                return new SequenceSize(totalBytes, _rowCount);
            }

            Debug.Assert(_expectedBaseRowCount > 0);
            SequenceSize baseResult = ReadSparse(seq);
            return baseResult.AddBytes(totalBytes);
        }
    }
}
