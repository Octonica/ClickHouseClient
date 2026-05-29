#region License Apache 2.0
/* Copyright 2026 Octonica
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

namespace Octonica.ClickHouseClient.Types;

internal sealed class ReplicatedSerializationSkippingColumnReader : IClickHouseColumnReaderBase
{
    private readonly IClickHouseColumnTypeInfo _columnType;
    private readonly int _rowCount;

    private int _keySize;
    private int _indicesOffset;

    private IClickHouseColumnReaderBase? _baseReader;
    private int _baseRowCount;
    private int _baseOffset;

    public ReplicatedSerializationSkippingColumnReader(IClickHouseColumnTypeInfo columnType, int rowCount)
    {
        _columnType = columnType;
        _rowCount = rowCount;
    }

    public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
    {
        if (_baseReader != null)
        {
            var result = _baseReader.ReadNext(sequence);
            if (_baseOffset + result.Elements == _baseRowCount)
                result = new SequenceSize(result.Bytes, _rowCount - _baseOffset);

            _baseOffset += result.Elements;
            return result;
        }

        int totalBytes = 0;
        var seq = sequence;
        int bytesRead;
        if (_keySize == 0)
        {
            if (!ClickHouseBinaryProtocolReader.TryRead7BitInteger(seq, out var indexRowCount, out bytesRead))
                return SequenceSize.Empty;

            if (checked((int)indexRowCount) != _rowCount)
                throw new ClickHouseException(ClickHouseErrorCodes.ProtocolUnexpectedResponse, $"Unexpected number of index elements ({indexRowCount}) when reading a column with 'replicated' serialization. This number doesn't much expected row count ({_rowCount}).");

            totalBytes += bytesRead;
            seq = sequence.Slice(totalBytes);
            if (seq.IsEmpty)
                return SequenceSize.Empty;

            _keySize = seq.FirstSpan[0];
            switch (_keySize)
            {
                case 1:
                case 2:
                case 4:
                    break;

                default:
                    throw new ClickHouseException(ClickHouseErrorCodes.ProtocolUnexpectedResponse, $"Unexpected key ({_keySize}) size when reading a column with 'replicated' serialization.");
            }

            ++totalBytes;
        }

        var indicesLength = _rowCount * _keySize;
        seq = sequence.Slice(totalBytes);
        var len = Math.Min(indicesLength - _indicesOffset, checked((int)seq.Length));

        _indicesOffset += len;
        totalBytes += len;

        if (_indicesOffset < indicesLength)
            return new SequenceSize(totalBytes, 0);

        seq = sequence.Slice(totalBytes);
        if (!ClickHouseBinaryProtocolReader.TryRead7BitInteger(seq, out var realRowCount, out bytesRead))
            return new SequenceSize(totalBytes, 0);

        totalBytes += bytesRead;
        seq = sequence.Slice(totalBytes);

        _baseRowCount = checked((int)realRowCount);
        _baseReader = _columnType.CreateSkippingColumnReader(_baseRowCount);
        var baseResult = ReadNext(seq);
        return baseResult.AddBytes(totalBytes);
    }
}
