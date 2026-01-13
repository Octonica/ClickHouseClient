#region License Apache 2.0
/* Copyright 2020-2021, 2023-2024 Octonica
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
using Octonica.ClickHouseClient.Utils;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;

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
            (IClickHouseColumnTypeInfo baseType, bool isNullable) = GetBaseTypeInfo();
            return new LowCardinalityColumnReader(rowCount, baseType, isNullable);
        }

        IClickHouseColumnReader IClickHouseColumnTypeInfo.CreateColumnReader(int rowCount, ClickHouseColumnSerializationMode serializationMode)
        {
            return serializationMode == ClickHouseColumnSerializationMode.Default
                ? CreateColumnReader(rowCount)
                : throw new NotSupportedException($"Custom serialization for {TypeName} type is not supported by ClickHouseClient.");
        }

        public IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            (IClickHouseColumnTypeInfo baseType, _) = GetBaseTypeInfo();
            return new LowCardinalitySkippingColumnReader(rowCount, baseType);
        }

        IClickHouseColumnReaderBase IClickHouseColumnTypeInfo.CreateSkippingColumnReader(int rowCount, ClickHouseColumnSerializationMode serializationMode)
        {
            return serializationMode == ClickHouseColumnSerializationMode.Default
                ? CreateSkippingColumnReader(rowCount)
                : throw new NotSupportedException($"Custom serialization for {TypeName} type is not supported by ClickHouseClient.");
        }

        private (IClickHouseColumnTypeInfo baseType, bool isNullable) GetBaseTypeInfo()
        {
            if (_baseType == null)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");
            }

            if (_baseType is NullableTypeInfo nullableBaseType)
            {
                if (nullableBaseType.UnderlyingType == null)
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{_baseType.ComplexTypeName}\" is not fully specified.");
                }

                // LowCardinality column stores NULL as the key 0
                return (nullableBaseType.UnderlyingType, true);
            }

            return (_baseType, false);
        }

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (typeof(T) == typeof(string))
            {
                // In most cases values of type T can be inserted into a column of type LowCardinality(T).
                // However, a column of type Map(LowCardinality(String), TValue) doesn't accept keys of type String.
                // https://github.com/Octonica/ClickHouseClient/issues/86
                return CreateColumnWriter(columnName, (IReadOnlyList<string>)rows, StringComparer.Ordinal, string.Empty, columnSettings);
            }

            if (_baseType == null)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");
            }

            // Writing values as is. Let the database do de-duplication
            return _baseType.CreateColumnWriter(columnName, rows, columnSettings);
        }

        private LowCardinalityColumnWriter CreateColumnWriter<T>(
            string columnName,
            IReadOnlyList<T> rows,
            IEqualityComparer<T> equalityComparer,
            T defaultValue,
            ClickHouseColumnSettings? columnSettings)
            where T : notnull
        {
            (IClickHouseColumnTypeInfo? baseType, bool isNullable) = GetBaseTypeInfo();

            Dictionary<T, int> map = new(equalityComparer);
            List<int> indices = new(rows.Count);
            List<T> uniqueValues = [];
            if (isNullable)
            {
                uniqueValues.Add(defaultValue);
            }

            // Reserve the first non-nullable position in the dictionary for the defualt value
            map.Add(defaultValue, uniqueValues.Count);
            uniqueValues.Add(defaultValue);

            foreach (T? row in rows)
            {
                if (isNullable && row is null)
                {
                    indices.Add(0);
                    continue;
                }

                if (!map.TryGetValue(row, out int index))
                {
                    index = uniqueValues.Count;
                    map.Add(row, index);
                    uniqueValues.Add(row);
                }

                indices.Add(index);
            }

            if (indices.Count == 0)
            {
                uniqueValues.Clear();
            }

            KeySizeCode keySizeCode = uniqueValues.Count > ushort.MaxValue
                ? KeySizeCode.UInt32
                : uniqueValues.Count > byte.MaxValue ? KeySizeCode.UInt16 : KeySizeCode.UInt8;
            Debug.Assert(_baseType != null);
            IClickHouseColumnWriter baseWriter = baseType.CreateColumnWriter(columnName, uniqueValues, columnSettings);
            return new LowCardinalityColumnWriter(baseWriter, uniqueValues.Count, ComplexTypeName, indices, keySizeCode);
        }

        public IClickHouseParameterWriter<T> CreateParameterWriter<T>()
        {
            return _baseType == null
                ? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.")
                : _baseType.CreateParameterWriter<T>();
        }

        public IClickHouseColumnTypeInfo GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            if (_baseType != null)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, "The type is already fully specified.");
            }

            if (options.Count > 1)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"Too many arguments in the definition of \"{TypeName}\".");
            }

            IClickHouseColumnTypeInfo baseType = typeInfoProvider.GetTypeInfo(options[0]);
            return new LowCardinalityTypeInfo(baseType);
        }

        public Type GetFieldType()
        {
            return _baseType == null
                ? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.")
                : _baseType.GetFieldType();
        }

        public ClickHouseDbType GetDbType()
        {
            return _baseType == null
                ? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.")
                : _baseType.GetDbType();
        }

        public IClickHouseTypeInfo GetGenericArgument(int index)
        {
            return _baseType == null
                ? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.")
                : index != 0 ? throw new IndexOutOfRangeException() : (IClickHouseTypeInfo)_baseType;
        }

        private sealed class LowCardinalityColumnReader : IClickHouseColumnReader
        {
            private readonly int _rowCount;
            private readonly IClickHouseColumnTypeInfo _baseType;
            private readonly bool _isNullable;

            private IClickHouseColumnReader? _baseColumnReader;
            private int _baseRowCount;

            private int _keySize;
            private byte[]? _buffer;
            private int _position;

            public LowCardinalityColumnReader(int rowCount, IClickHouseColumnTypeInfo baseType, bool isNullable)
            {
                _rowCount = rowCount;
                _baseType = baseType;
                _isNullable = isNullable;
            }

            SequenceSize IClickHouseColumnReaderBase.ReadPrefix(ReadOnlySequence<byte> sequence)
            {
                return ReadPrefix(sequence);
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                if (_position >= _rowCount)
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");
                }

                SequenceSize result = new(0, 0);
                ReadOnlySequence<byte> slice = sequence;
                if (_baseColumnReader == null)
                {
                    (int keySize, int keyCount, int bytesRead)? header = TryReadHeader(slice);
                    if (header == null)
                    {
                        return result;
                    }

                    _baseRowCount = header.Value.keyCount;
                    _keySize = header.Value.keySize;
                    _baseColumnReader = _baseType.CreateColumnReader(_baseRowCount);

                    slice = slice.Slice(header.Value.bytesRead);
                    result = result.AddBytes(header.Value.bytesRead);
                }

                if (_baseRowCount > 0)
                {
                    SequenceSize baseResult = _baseColumnReader.ReadNext(slice);
                    _baseRowCount -= baseResult.Elements;

                    slice = slice.Slice(baseResult.Bytes);
                    result = result.AddBytes(baseResult.Bytes);
                }

                if (_baseRowCount > 0)
                {
                    return result;
                }

                if (_buffer == null)
                {
                    if (slice.Length < sizeof(ulong))
                    {
                        return result;
                    }

                    ulong length = 0;
                    slice.Slice(0, sizeof(ulong)).CopyTo(MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref length, 1)));

                    if ((int)length != _rowCount)
                    {
                        throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, $"Internal error. Row count check failed: {_rowCount} rows expected, {length} rows detected.");
                    }

                    _buffer = new byte[_rowCount * _keySize];

                    slice = slice.Slice(sizeof(ulong));
                    result = result.AddBytes(sizeof(ulong));
                }

                int elementCount = Math.Min(_rowCount - _position, (int)(slice.Length / _keySize));
                int byteCount = elementCount * _keySize;
                slice.Slice(0, byteCount).CopyTo(new Span<byte>(_buffer, _position * _keySize, byteCount));

                _position += elementCount;
                result += new SequenceSize(byteCount, elementCount);

                return result;
            }

            public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
            {
                ReadOnlyMemory<byte> keys;
                int keySize;
                if (_buffer == null)
                {
                    keys = ReadOnlyMemory<byte>.Empty;
                    keySize = 1;
                }
                else
                {
                    keys = new ReadOnlyMemory<byte>(_buffer, 0, _position * _keySize);
                    keySize = _keySize;
                }

                IClickHouseTableColumn valuesColumn = (_baseColumnReader ?? _baseType.CreateColumnReader(0)).EndRead(settings)!;
                if (!valuesColumn.TryDipatch(new LowCardinalityTableColumnDispatcher(keys, keySize, _isNullable), out IClickHouseTableColumn? result))
                {
                    result = new LowCardinalityTableColumn(keys, keySize, valuesColumn, _isNullable);
                }

                return result!;
            }

            public static SequenceSize ReadPrefix(ReadOnlySequence<byte> sequence)
            {
                if (sequence.Length < sizeof(ulong))
                {
                    return SequenceSize.Empty;
                }

                ulong version = 0;
                sequence.Slice(0, sizeof(ulong)).CopyTo(MemoryMarshal.Cast<ulong, byte>(MemoryMarshal.CreateSpan(ref version, 1)));

                // https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeLowCardinality.cpp
                // Dictionary is written as number N and N keys after them.
                // Dictionary can be shared for continuous range of granules, so some marks may point to the same position.
                // Shared dictionary is stored in state and is read once.
                //
                // SharedDictionariesWithAdditionalKeys = 1,

                return version != 1
                    ? throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, $"Unexpected dictionary version received: {version}.")
                    : new SequenceSize(sizeof(ulong), 1);
            }

            public static (int keySize, int keyCount, int bytesRead)? TryReadHeader(ReadOnlySequence<byte> sequence)
            {
                Span<ulong> headerValues = stackalloc ulong[2];
                Span<byte> headerBytes = MemoryMarshal.AsBytes(headerValues);
                if (sequence.Length < headerBytes.Length)
                {
                    return null;
                }

                sequence.Slice(0, headerBytes.Length).CopyTo(headerBytes);

                KeySizeCode keySizeCode = (KeySizeCode)unchecked((byte)headerValues[0]);
                int keySize = keySizeCode switch
                {
                    KeySizeCode.UInt8 => 1,
                    KeySizeCode.UInt16 => 2,
                    KeySizeCode.UInt32 => 4,
                    KeySizeCode.UInt64 => throw new NotSupportedException("64-bit keys are not supported."),
                    _ => throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, $"Internal error. Unexpected size of a key: {keySizeCode}."),
                };

                // There are several control flags, but the client always receives 0x2|0x4
                //
                // https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeLowCardinality.cpp
                // 0x1 Need to read dictionary if it wasn't.
                // 0x2 Need to read additional keys. Additional keys are stored before indexes as value N and N keys after them.
                // 0x4 Need to update dictionary. It means that previous granule has different dictionary.

                ulong flags = headerValues[0] >> 8;
                if (flags != (0x2 | 0x4))
                {
                    throw new NotSupportedException("Received combination of flags is not supported.");
                }

                int keyCount = checked((int)headerValues[1]);
                return (keySize, keyCount, headerBytes.Length);
            }
        }

        private sealed class LowCardinalitySkippingColumnReader : IClickHouseColumnReaderBase
        {
            private readonly int _rowCount;
            private readonly IClickHouseColumnTypeInfo _baseType;

            private IClickHouseColumnReaderBase? _baseReader;
            private int _baseRowCount;
            private int _keySize;

            private int _basePosition;
            private bool _headerSkipped;
            private int _position;

            public LowCardinalitySkippingColumnReader(int rowCount, IClickHouseColumnTypeInfo baseType)
            {
                _rowCount = rowCount;
                _baseType = baseType;
            }

            SequenceSize IClickHouseColumnReaderBase.ReadPrefix(ReadOnlySequence<byte> sequence)
            {
                return LowCardinalityColumnReader.ReadPrefix(sequence);
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                ReadOnlySequence<byte> slice = sequence;
                SequenceSize result = new(0, 0);
                if (_baseReader == null)
                {
                    (int keySize, int keyCount, int bytesRead)? header = LowCardinalityColumnReader.TryReadHeader(slice);
                    if (header == null)
                    {
                        return result;
                    }

                    result = result.AddBytes(header.Value.bytesRead);
                    slice = slice.Slice(header.Value.bytesRead);

                    _baseRowCount = header.Value.keyCount;
                    _keySize = header.Value.keySize;

                    _baseReader = _baseType.CreateSkippingColumnReader(_baseRowCount);
                }

                if (_basePosition < _baseRowCount)
                {
                    SequenceSize baseResult = _baseReader.ReadNext(slice);

                    _basePosition += baseResult.Elements;
                    result = result.AddBytes(baseResult.Bytes);

                    if (_basePosition < _baseRowCount)
                    {
                        return result;
                    }
                }

                if (!_headerSkipped)
                {
                    if (sequence.Length - result.Bytes < sizeof(ulong))
                    {
                        return result;
                    }

                    ulong length = 0;
                    sequence.Slice(result.Bytes, sizeof(ulong)).CopyTo(MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref length, 1)));

                    if ((int)length != _rowCount)
                    {
                        throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, $"Internal error. Row count check failed: {_rowCount} rows expected, {length} rows detected.");
                    }

                    result = result.AddBytes(sizeof(ulong));
                    _headerSkipped = true;
                }

                int maxElementsCount = _rowCount - _position;
                if (maxElementsCount <= 0)
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");
                }

                int elementCount = (int)Math.Min((sequence.Length - result.Bytes) / _keySize, maxElementsCount);
                _position += elementCount;
                result += new SequenceSize(elementCount * _keySize, elementCount);

                return result;
            }
        }

        private sealed class LowCardinalityColumnWriter : IClickHouseColumnWriter
        {
            private readonly IClickHouseColumnWriter _baseWriter;
            private readonly IReadOnlyList<int> _indices;
            private readonly KeySizeCode _keySizeCode;

            private bool _headerWritten;
            private int _baseRowCount;
            private int _position = -1;

            public string ColumnName => _baseWriter.ColumnName;

            public string ColumnType { get; }

            public LowCardinalityColumnWriter(IClickHouseColumnWriter baseWriter, int baseRowCount, string columnType, IReadOnlyList<int> indices, KeySizeCode keySizeCode)
            {
                ColumnType = columnType;
                _indices = indices;
                _keySizeCode = keySizeCode;
                _baseWriter = baseWriter;
                _baseRowCount = baseRowCount;
            }

            SequenceSize IClickHouseColumnWriter.WritePrefix(Span<byte> writeTo)
            {
                if (writeTo.Length < sizeof(ulong))
                {
                    return SequenceSize.Empty;
                }

                Span<ulong> writeToVersion = MemoryMarshal.Cast<byte, ulong>(writeTo[..sizeof(ulong)]);
                writeToVersion[0] = 1; // Version

                return new SequenceSize(sizeof(ulong), 1);
            }

            public SequenceSize WriteNext(Span<byte> writeTo)
            {
                Span<byte> targetSpan = writeTo;
                SequenceSize result = new(0, 0);
                if (!_headerWritten)
                {
                    Span<ulong> headerValues = stackalloc ulong[2];
                    headerValues[0] = ((0x2 | 0x4) << 8) | (ulong)_keySizeCode; // The size of and element and flags
                    headerValues[1] = (ulong)_baseRowCount;

                    Span<byte> headerBytes = MemoryMarshal.AsBytes(headerValues);
                    if (headerBytes.Length > targetSpan.Length)
                    {
                        return result;
                    }

                    headerBytes.CopyTo(targetSpan);
                    targetSpan = targetSpan[headerBytes.Length..];
                    result = result.AddBytes(headerBytes.Length);
                    _headerWritten = true;
                }

                if (_baseRowCount > 0)
                {
                    SequenceSize baseResult = _baseWriter.WriteNext(targetSpan);

                    _baseRowCount -= baseResult.Elements;
                    result = result.AddBytes(baseResult.Bytes);
                    if (_baseRowCount > 0)
                    {
                        return result;
                    }

                    if (_baseRowCount != 0)
                    {
                        throw new ClickHouseException(ClickHouseErrorCodes.InternalError, $"Internal error. Row count check failed: written {-_baseRowCount} more rows then expected.");
                    }

                    targetSpan = targetSpan[baseResult.Bytes..];
                }

                if (_position < 0)
                {
                    ulong length = (ulong)_indices.Count;
                    ReadOnlySpan<byte> lengthSpan = MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan(ref length, 1));
                    if (lengthSpan.Length > targetSpan.Length)
                    {
                        return result;
                    }

                    lengthSpan.CopyTo(targetSpan);
                    result = result.AddBytes(lengthSpan.Length);
                    targetSpan = targetSpan[lengthSpan.Length..];

                    _position = 0;
                }

                int size = _indices.Count - _position;
                int byteSize;
                switch (_keySizeCode)
                {
                    case KeySizeCode.UInt8:
                        size = Math.Min(targetSpan.Length, size);
                        byteSize = size;
                        for (int i = 0; i < size; _position++, i++)
                        {
                            targetSpan[i] = (byte)_indices[_position];
                        }

                        break;

                    case KeySizeCode.UInt16:
                        size = Math.Min(targetSpan.Length / 2, size);
                        byteSize = size * 2;
                        Span<ushort> ushortSpan = MemoryMarshal.Cast<byte, ushort>(targetSpan[..byteSize]);

                        for (int i = 0; i < size; _position++, i++)
                        {
                            ushortSpan[i] = (ushort)_indices[_position];
                        }

                        break;

                    case KeySizeCode.UInt32:
                        size = Math.Min(targetSpan.Length / 4, size);
                        byteSize = size * 4;
                        Span<int> intSpan = MemoryMarshal.Cast<byte, int>(targetSpan[..byteSize]);

                        for (int i = 0; i < size; _position++, i++)
                        {
                            intSpan[i] = _indices[_position];
                        }

                        break;

                    default:
                        throw new InvalidOperationException($"Internal error. Unexpected key size code: {_keySizeCode}.");
                }

                result += new SequenceSize(byteSize, size);
                return result;
            }
        }

        private sealed class LowCardinalityTableColumnDispatcher : IClickHouseTableColumnDispatcher<IClickHouseTableColumn>
        {
            private readonly ReadOnlyMemory<byte> _keys;
            private readonly int _keySize;
            private readonly bool _isNullable;

            public LowCardinalityTableColumnDispatcher(ReadOnlyMemory<byte> keys, int keySize, bool isNullable)
            {
                _keys = keys;
                _keySize = keySize;
                _isNullable = isNullable;
            }

            public IClickHouseTableColumn Dispatch<T>(IClickHouseTableColumn<T> column)
            {
                return new LowCardinalityTableColumn<T>(_keys, _keySize, column, _isNullable);
            }
        }

        private enum KeySizeCode
        {
            UInt8 = 0,
            UInt16 = 1,
            UInt32 = 2,
            UInt64 = 3
        }
    }
}
