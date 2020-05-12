#region License Apache 2.0
/* Copyright 2020 Octonica
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
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class LowCardinalityTypeInfo : IClickHouseColumnTypeInfo
    {
        private readonly IClickHouseColumnTypeInfo? _baseType;

        public string ComplexTypeName { get; }

        public string TypeName => "LowCardinality";

        public int GenericArgumentsCount => _baseType == null ? 0 : 1;

        public LowCardinalityTypeInfo()
        {
            _baseType = null;
            ComplexTypeName = TypeName;
        }

        private LowCardinalityTypeInfo(IClickHouseColumnTypeInfo baseType)
        {
            _baseType = baseType ?? throw new ArgumentNullException(nameof(baseType));
            ComplexTypeName = $"{TypeName}({_baseType.ComplexTypeName})";
        }

        public IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            if (_baseType == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            return new LowCardinalityColumnReader(rowCount, _baseType);
        }

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (_baseType == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            // Writing values as is. Let the database do de-duplication
            return _baseType.CreateColumnWriter(columnName, rows, columnSettings);
        }

        public IClickHouseColumnTypeInfo GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            if (_baseType != null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, "The type is already fully specified.");

            if (options.Count > 1)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"Too many arguments in the definition of \"{TypeName}\".");

            var baseType = typeInfoProvider.GetTypeInfo(options[0]);
            return new LowCardinalityTypeInfo(baseType);
        }

        public Type GetFieldType()
        {
            if (_baseType == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            return _baseType.GetFieldType();
        }

        public ClickHouseDbType GetDbType()
        {
            if (_baseType == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            return _baseType.GetDbType();
        }

        public IClickHouseTypeInfo GetGenericArgument(int index)
        {
            if (_baseType == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            if (index != 0)
                throw new IndexOutOfRangeException();

            return _baseType;
        }

        private class LowCardinalityColumnReader : IClickHouseColumnReader
        {
            private readonly int _rowCount;
            private readonly IClickHouseColumnTypeInfo _baseType;

            private IClickHouseColumnReader? _baseColumnReader;
            private int _baseRowCount;

            private int _keySize;
            private byte[]? _buffer;
            private int _position;

            public LowCardinalityColumnReader(int rowCount, IClickHouseColumnTypeInfo baseType)
            {
                _rowCount = rowCount;

                if (baseType is NullableTypeInfo nullableBaseType)
                {
                    if (nullableBaseType.UnderlyingType == null)
                        throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{baseType.ComplexTypeName}\" is not fully specified.");

                    // LowCardinality column stores NULL as the key 0
                    _baseType = nullableBaseType.UnderlyingType;
                }
                else
                {
                    _baseType = baseType;
                }
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                if (_position >= _rowCount)
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");

                var result = new SequenceSize(0, 0);
                var slice = sequence;
                if (_baseColumnReader == null)
                {
                    var header = TryReadHeader(slice);
                    if (header == null)
                        return result;

                    _baseRowCount = header.Value.keyCount;
                    _keySize = header.Value.keySize;
                    _baseColumnReader = _baseType.CreateColumnReader(_baseRowCount);

                    slice = slice.Slice(header.Value.bytesRead);
                    result = result.AddBytes(header.Value.bytesRead);
                }

                if (_baseRowCount > 0)
                {
                    var baseResult = _baseColumnReader.ReadNext(slice);
                    _baseRowCount -= baseResult.Elements;

                    slice = slice.Slice(baseResult.Bytes);
                    result = result.AddBytes(baseResult.Bytes);
                }

                if (_baseRowCount > 0)
                    return result;

                if (_buffer == null)
                {
                    if (slice.Length < sizeof(ulong))
                        return result;

                    ulong length = 0;
                    slice.Slice(0, sizeof(ulong)).CopyTo(MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref length, 1)));

                    if ((int) length != _rowCount)
                        throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, $"Internal error. Row count check failed: {_rowCount} rows expected, {length} rows detected.");

                    _buffer = new byte[_rowCount * _keySize];

                    slice = slice.Slice(sizeof(ulong));
                    result = result.AddBytes(sizeof(ulong));
                }

                var elementCount = Math.Min(_rowCount - _position, (int) (slice.Length / _keySize));
                var byteCount = elementCount * _keySize;
                slice.Slice(0, byteCount).CopyTo(new Span<byte>(_buffer, _position * _keySize, byteCount));

                _position += elementCount;
                result += new SequenceSize(byteCount, elementCount);

                return result;
            }

            public SequenceSize Skip(ReadOnlySequence<byte> sequence, int maxElementsCount, ref object? skipContext)
            {
                var slice = sequence;
                var result = new SequenceSize(0, 0);
                LowCardinalityColumnReaderSkipContext knownSkipContext;
                if (skipContext == null)
                {
                    var header = TryReadHeader(slice);
                    if (header == null)
                        return result;

                    result = result.AddBytes(header.Value.bytesRead);
                    slice = slice.Slice(header.Value.bytesRead);

                    var baseReader = _baseType.CreateColumnReader(0);
                    skipContext = knownSkipContext = new LowCardinalityColumnReaderSkipContext(baseReader, header.Value.keyCount, header.Value.keySize);
                }
                else
                {
                    knownSkipContext = (LowCardinalityColumnReaderSkipContext) skipContext;
                }

                result += knownSkipContext.SkipNext(slice, maxElementsCount);
                return result;
            }

            public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
            {
                var valuesColumn = (_baseColumnReader ?? _baseType.CreateColumnReader(0)).EndRead(settings);
                
                if (_buffer == null)
                    return new LowCardinalityTableColumn(ReadOnlyMemory<byte>.Empty, 1, valuesColumn);

                return new LowCardinalityTableColumn(new ReadOnlyMemory<byte>(_buffer, 0, _position * _keySize), _keySize, valuesColumn);
            }

            private static (int keySize, int keyCount, int bytesRead)? TryReadHeader(ReadOnlySequence<byte> sequence)
            {
                Span<ulong> headerValues = stackalloc ulong[3];
                var headerBytes = MemoryMarshal.AsBytes(headerValues);
                if (sequence.Length < headerBytes.Length)
                    return null;

                sequence.Slice(0, headerBytes.Length).CopyTo(headerBytes);

                // https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeLowCardinality.cpp
                // Dictionary is written as number N and N keys after them.
                // Dictionary can be shared for continuous range of granules, so some marks may point to the same position.
                // Shared dictionary is stored in state and is read once.
                //
                // SharedDictionariesWithAdditionalKeys = 1,

                if (headerValues[0] != 1)
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, $"Internal error. Unexpected dictionary version: {headerBytes[0]}.");

                var keySizeCode = unchecked((byte)headerValues[1]);
                int keySize;
                switch (keySizeCode)
                {
                    case 0:
                        keySize = 1;
                        break;
                    case 1:
                        keySize = 2;
                        break;
                    case 3:
                        keySize = 4;
                        break;
                    case 4:
                        throw new NotSupportedException("64-bit keys are not supported.");
                    default:
                        throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, $"Internal error. Unexpected size of a key: {keySizeCode}.");
                }

                // There are several control flags, but the client always receives 0x2|0x4
                //
                // https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeLowCardinality.cpp
                // 0x1 Need to read dictionary if it wasn't.
                // 0x2 Need to read additional keys. Additional keys are stored before indexes as value N and N keys after them.
                // 0x4 Need to update dictionary. It means that previous granule has different dictionary.

                var flags = headerValues[1] >> 8;
                if (flags != (0x2 | 0x4))
                    throw new NotSupportedException("Received combination of flags is not supported.");

                var keyCount = (int) headerValues[2];
                return (keySize, keyCount, headerBytes.Length);
            }
        }

        private sealed class LowCardinalityColumnReaderSkipContext
        {
            private readonly IClickHouseColumnReader _baseReader;
            private readonly int _baseRowCount;
            private readonly int _keySize;

            private object? _baseSkipContext;
            private int _basePosition;
            private bool _headerSkipped;
            
            public LowCardinalityColumnReaderSkipContext(IClickHouseColumnReader baseReader, int baseRowCount, int keySize)
            {
                _baseReader = baseReader;
                _baseRowCount = baseRowCount;
                _keySize = keySize;
            }

            public SequenceSize SkipNext(ReadOnlySequence<byte> sequence, int maxElementsCount)
            {
                var result = new SequenceSize(0, 0);
                if (_basePosition < _baseRowCount)
                {
                    var baseResult = _baseReader.Skip(sequence, _baseRowCount - _basePosition, ref _baseSkipContext);

                    _basePosition += baseResult.Elements;
                    result = result.AddBytes(baseResult.Bytes);

                    if (_basePosition < _baseRowCount)
                        return result;
                }

                if (!_headerSkipped)
                {
                    if (sequence.Length - result.Bytes < sizeof(ulong))
                        return result;

                    result = result.AddBytes(sizeof(ulong));
                    _headerSkipped = true;
                }

                var elementCount = Math.Min(((int) sequence.Length - result.Bytes) / _keySize, maxElementsCount);
                result += new SequenceSize(elementCount * _keySize, elementCount);

                return result;
            }
        }
    }
}
