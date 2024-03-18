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
using Octonica.ClickHouseClient.Utils;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Octonica.ClickHouseClient.Types
{
    internal class VariantTypeInfo : IClickHouseColumnTypeInfo
    {
        private readonly List<IClickHouseColumnTypeInfo>? _types;

        public string ComplexTypeName { get; }

        public string TypeName => "Variant";

        public int GenericArgumentsCount => _types?.Count ?? 0;

        public VariantTypeInfo()
        {
            ComplexTypeName = TypeName;
        }

        private VariantTypeInfo(string complexTypeName, List<IClickHouseColumnTypeInfo> types)
        {
            ComplexTypeName = complexTypeName;
            _types = types;
        }

        public IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            if (_types == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            return new VariantColumnReader(rowCount, this);
        }

        IClickHouseColumnReader IClickHouseColumnTypeInfo.CreateColumnReader(int rowCount, ClickHouseColumnSerializationMode serializationMode)
        {
            if (serializationMode != ClickHouseColumnSerializationMode.Default)
                throw new NotSupportedException($"Custom serialization for \"{ComplexTypeName}\" is not supported by ClickHouseClient.");

            return CreateColumnReader(rowCount);
        }

        public IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            if (_types == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            return new VariantSkippingColumnReader(rowCount, this);
        }

        IClickHouseColumnReaderBase IClickHouseColumnTypeInfo.CreateSkippingColumnReader(int rowCount, ClickHouseColumnSerializationMode serializationMode)
        {
            if (serializationMode != ClickHouseColumnSerializationMode.Default)
                throw new NotSupportedException($"Custom serialization for \"{ComplexTypeName}\" is not supported by ClickHouseClient.");

            return CreateSkippingColumnReader(rowCount);
        }

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (_types == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            var valueHandlers = new List<IValueHandler>(_types.Count);
            var nonGenericHandlerType = typeof(ValueHandler<>);
            foreach (var type in _types)
            {
                var clrType = type.GetFieldType();
                var handler = (IValueHandler?)Activator.CreateInstance(nonGenericHandlerType.MakeGenericType(clrType));
                Debug.Assert(handler != null);
                valueHandlers.Add(handler);
            }

            var indices = new byte[rows.Count];
            int count = 0;
            foreach (var value in rows)
            {
                if (value == null || value is DBNull)
                {
                    indices[count++] = 0xFF;
                    continue;
                }

                bool handled = false;
                for (int i = 0; i < valueHandlers.Count; i++)
                {
                    if (valueHandlers[i].TryHandle(value))
                    {
                        indices[count++] = checked((byte)i);
                        handled = true;
                        break;
                    }
                }

                if (!handled)
                {
                    var allowedTypeList = string.Join(", ", _types.Select(t => $"\"{t.GetFieldType().Name}\""));
                    throw new ClickHouseException(
                        ClickHouseErrorCodes.TypeNotSupported,
                        $"Column \"{columnName}\". A value of type \"{value.GetType().Name}\" can't be written to the column of type \"{ComplexTypeName}\". Allowed types are: {allowedTypeList}.");
                }
            }

            Debug.Assert(count == indices.Length);
            var rowCounts = new List<int>(valueHandlers.Count);
            var writers = new List<IClickHouseColumnWriter>(valueHandlers.Count);
            for (int i = 0; i < valueHandlers.Count; i++)
            {
                IValueHandler? handler = valueHandlers[i];
                rowCounts.Add(handler.RowCount);
                writers.Add(handler.CreateColumnWriter(columnName, _types[i], columnSettings));
            }

            return new VariantColumnWriter(columnName, ComplexTypeName, indices, rowCounts, writers);
        }

        public IClickHouseParameterWriter<T> CreateParameterWriter<T>()
        {
            throw new NotSupportedException($"Parameters of type \"{ComplexTypeName}\" are not supported.");
        }

        public ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Variant;
        }

        public IClickHouseColumnTypeInfo GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            if (_types != null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, "The type is already fully specified.");

            var types = new List<IClickHouseColumnTypeInfo>(options.Count);
            var typeNameBuilder = new StringBuilder(TypeName).Append('(');
            bool isFirst = true;
            foreach(var option in options)
            {
                if (isFirst)
                    isFirst = false;
                else
                    typeNameBuilder.Append(", ");

                var type = typeInfoProvider.GetTypeInfo(option);
                types.Add(type);
                typeNameBuilder.Append(type.ComplexTypeName);
            }

            var complexTypeName = typeNameBuilder.Append(')').ToString();
            return new VariantTypeInfo(complexTypeName, types);
        }

        public Type GetFieldType()
        {
            return typeof(object);
        }

        public IClickHouseTypeInfo GetGenericArgument(int index)
        {
            if (_types == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            return _types[index];
        }

        private abstract class VariantColumnReaderBase<TReader>
            where TReader:IClickHouseColumnReaderBase
        {
            private readonly List<byte[]> _prefixes;

            private IClickHouseColumnReaderBase? _prefixReader;
            private int _readerPosition;
            private int _rowPosition;

            protected readonly int RowCount;
            protected readonly VariantTypeInfo TypeInfo;
            protected readonly List<TReader> Readers;

            public VariantColumnReaderBase(int rowCount, VariantTypeInfo typeInfo)
            {
                Debug.Assert(typeInfo._types != null);

                Readers = new List<TReader>(typeInfo._types.Count);
                _prefixes = new List<byte[]>(typeInfo._types.Count);
                RowCount = rowCount;
                TypeInfo = typeInfo;
            }

            public SequenceSize ReadPrefix(ReadOnlySequence<byte> sequence)
            {
                var types = TypeInfo._types;
                Debug.Assert(types != null);
                var totalBytes = 0;
                for (int i = _prefixes.Count; i < types.Count; i++)
                {
                    var slice = sequence.Slice(totalBytes);
                    var reader = _prefixReader ?? types[i].CreateSkippingColumnReader(0);
                    var prefixSize = reader.ReadPrefix(slice);
                    if (prefixSize.Elements == 0)
                    {
                        _prefixReader = reader;
                        return new SequenceSize(totalBytes, 0);
                    }

                    _prefixReader = null;
                    totalBytes += prefixSize.Bytes;
                    if (prefixSize.Bytes > 0)
                    {
                        var prefix = new byte[prefixSize.Bytes];
                        slice.Slice(0, prefix.Length).CopyTo(prefix);
                        _prefixes.Add(prefix);
                    }
                    else
                    {
                        _prefixes.Add(Array.Empty<byte>());
                    }
                }

                return new SequenceSize(totalBytes, 1);
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence, int[] rowCounts)
            {
                var seq = sequence;
                int totalBytes = 0;
                var totalElements = 0;
                Debug.Assert(TypeInfo._types != null);
                for (; _readerPosition < rowCounts.Length; _readerPosition++)
                {
                    TReader reader;
                    if (Readers.Count == _readerPosition)
                    {
                        reader = CreateReader(TypeInfo._types[_readerPosition], rowCounts[_readerPosition]);

                        var prefixBytes = _prefixes[_readerPosition];
                        if (prefixBytes.Length > 0)
                        {
                            var prefixSize = reader.ReadPrefix(new ReadOnlySequence<byte>(prefixBytes));

                            if (prefixSize.Bytes == 0 && prefixSize.Elements == 0)
                                throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Failed to read the column prefix.");

                            if (prefixSize.Bytes != prefixBytes.Length)
                                throw new ClickHouseException(ClickHouseErrorCodes.InternalError, $"Internal error. The column prefix' size is {prefixBytes.Length}, but the number of consumed bytes is {prefixSize.Bytes}.");

                            if (prefixSize.Elements != 1)
                                throw new ClickHouseException(ClickHouseErrorCodes.InternalError, $"Internal error. Received an unexpected number of column prefixes: {prefixSize.Elements}.");
                        }

                        Readers.Add(reader);
                    }
                    else
                    {
                        reader = Readers[^1];
                    }

                    SequenceSize size;
                    if (rowCounts[_readerPosition] == 0)
                        size = SequenceSize.Empty;
                    else
                        size = reader.ReadNext(seq);

                    totalBytes += size.Bytes;
                    _rowPosition += size.Elements;

                    if (_readerPosition == rowCounts.Length - 1)
                    {
                        totalElements += size.Elements;
                        if (_rowPosition == rowCounts[_readerPosition])
                            totalElements += RowCount - rowCounts[_readerPosition];
                    }

                    if (_rowPosition < rowCounts[_readerPosition])
                        break;

                    _rowPosition = 0;
                    seq = seq.Slice(size.Bytes);
                }

                return new SequenceSize(totalBytes, totalElements);
            }

            protected abstract TReader CreateReader(IClickHouseColumnTypeInfo typeInfo, int rowCount);
        }

        private sealed class VariantColumnReader : VariantColumnReaderBase<IClickHouseColumnReader>, IClickHouseColumnReader
        {
            private readonly byte[] _typeIndices;
            private readonly int[] _elementIndices;
            private readonly int[] _rowCounts;

            private int _indexPosition;

            public VariantColumnReader(int rowCount, VariantTypeInfo typeInfo)
                :base(rowCount, typeInfo)
            {
                Debug.Assert(typeInfo._types != null);

                _typeIndices = rowCount == 0 ? Array.Empty<byte>() : new byte[rowCount];
                _elementIndices = new int[rowCount];
                _rowCounts = new int[typeInfo._types.Count];
            }

            SequenceSize IClickHouseColumnReaderBase.ReadPrefix(ReadOnlySequence<byte> sequence)
            {
                return ReadPrefix(sequence);
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                int totalBytes = 0;
                var seq = sequence;
                if (_indexPosition < _typeIndices.Length)
                {
                    var length = Math.Min(_typeIndices.Length - _indexPosition, (int)seq.Length);
                    seq.Slice(0, length).CopyTo(((Span<byte>)_typeIndices).Slice(_indexPosition));

                    totalBytes += length;

                    for (int i = 0; i < length; i++, _indexPosition++)
                    {
                        var typeIndex = _typeIndices[_indexPosition];
                        // 0xFF stands for NULL
                        if (typeIndex != 0xFF)
                            _elementIndices[_indexPosition] = _rowCounts[typeIndex]++;
                    }

                    if (_indexPosition < _typeIndices.Length)
                        return new SequenceSize(totalBytes, 0);

                    seq = seq.Slice(length);
                }

                var result = ReadNext(seq, _rowCounts);
                result = result.AddBytes(totalBytes);
                return result;
            }

            protected override IClickHouseColumnReader CreateReader(IClickHouseColumnTypeInfo typeInfo, int rowCount)
            {
                return typeInfo.CreateColumnReader(rowCount);
            }

            public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
            {
                if (_typeIndices.Length == 0)
                    return new StructureTableColumn<int>(ReadOnlyMemory<int>.Empty);

                Debug.Assert(TypeInfo._types != null);
                var columns = new List<IClickHouseTableColumn>(_rowCounts.Length);
                for (int i = 0; i < TypeInfo._types.Count; i++)
                {
                    if (i < Readers.Count)
                    {
                        columns.Add(Readers[i].EndRead(settings));
                    }
                    else
                    {
                        var reader = TypeInfo._types[i].CreateColumnReader(0);
                        columns.Add(reader.EndRead(settings));
                    }
                }

                return new VariantTableColumn(columns, _typeIndices, _elementIndices);
            }
        }

        private sealed class VariantSkippingColumnReader : VariantColumnReaderBase<IClickHouseColumnReaderBase>, IClickHouseColumnReaderBase
        {
            private readonly int[] _rowCounts;

            private int _indexPosition;

            public VariantSkippingColumnReader(int rowCount, VariantTypeInfo typeInfo)
                : base(rowCount, typeInfo)
            {
                Debug.Assert(typeInfo._types != null);
                _rowCounts = new int[typeInfo._types.Count];
            }

            SequenceSize IClickHouseColumnReaderBase.ReadPrefix(ReadOnlySequence<byte> sequence)
            {
                return ReadPrefix(sequence);
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                int totalBytes = 0;
                var seq = sequence;
                if (_indexPosition < RowCount)
                {
                    var length = Math.Min(RowCount - _indexPosition, (int)seq.Length);
                    var typeIndices = new byte[length];
                    seq.Slice(0, length).CopyTo(typeIndices);

                    totalBytes += length;

                    for (int i = 0; i < length; i++, _indexPosition++)
                    {
                        // 0xFF stands for NULL
                        if (typeIndices[i] != 0xFF)
                            ++_rowCounts[typeIndices[i]];
                    }

                    if (_indexPosition < RowCount)
                        return new SequenceSize(totalBytes, 0);

                    seq = seq.Slice(length);
                }

                var result = ReadNext(seq, _rowCounts);
                result = result.AddBytes(totalBytes);
                return result;
            }

            protected override IClickHouseColumnReaderBase CreateReader(IClickHouseColumnTypeInfo typeInfo, int rowCount)
            {
                return typeInfo.CreateSkippingColumnReader(rowCount);
            }
        }

        private interface IValueHandler
        {
            int RowCount { get; }

            bool TryHandle(object value);

            IClickHouseColumnWriter CreateColumnWriter(string columnName, IClickHouseColumnTypeInfo typeInfo, ClickHouseColumnSettings? columnSettings);
        }

        private sealed class ValueHandler<T> : IValueHandler
        {
            private readonly List<T> _rows = new List<T>();

            public int RowCount => _rows.Count;

            public IClickHouseColumnWriter CreateColumnWriter(string columnName, IClickHouseColumnTypeInfo typeInfo, ClickHouseColumnSettings? columnSettings)
            {
                return typeInfo.CreateColumnWriter(columnName + "." + typeInfo.ComplexTypeName, _rows, columnSettings);
            }

            public bool TryHandle(object value)
            {
                if(value is T typedValue)
                {
                    _rows.Add(typedValue);
                    return true;
                }

                return false;
            }
        }

        private sealed class VariantColumnWriter : IClickHouseColumnWriter
        {
            private readonly byte[] _indices;
            private readonly List<int> _rowCounts;
            private readonly List<IClickHouseColumnWriter> _columnWriters;

            private int _prefixPosition;
            private int _indexPosition;
            private int _columnPosition;
            private int _rowPosition;

            public string ColumnName { get; }

            public string ColumnType { get; }

            public VariantColumnWriter(string columnName, string columnType, byte[] indices, List<int> rowCounts, List<IClickHouseColumnWriter> columnWriters)
            {
                ColumnName = columnName;
                ColumnType = columnType;
                _indices = indices;
                _rowCounts = rowCounts;
                _columnWriters = columnWriters;
            }

            SequenceSize IClickHouseColumnWriter.WritePrefix(Span<byte> writeTo)
            {
                var totalBytes = 0;
                for (; _prefixPosition < _columnWriters.Count; _prefixPosition++)
                {
                    var prefixSize = _columnWriters[_prefixPosition].WritePrefix(writeTo.Slice(totalBytes));
                    totalBytes += prefixSize.Bytes;
                    if (prefixSize.Elements == 0)
                        return new SequenceSize(totalBytes, 0);
                }

                return new SequenceSize(totalBytes, 1);
            }

            public SequenceSize WriteNext(Span<byte> writeTo)
            {
                var totalBytes = 0;
                var span = writeTo;
                if (_indexPosition < _indices.Length)
                {
                    var length = Math.Min(writeTo.Length, _indices.Length - _indexPosition);
                    ((ReadOnlySpan<byte>)_indices).Slice(_indexPosition, length).CopyTo(span.Slice(0, length));
                    totalBytes += length;
                    _indexPosition += length;

                    if (_indexPosition < _indices.Length)
                        return new SequenceSize(totalBytes, 0);

                    span = span.Slice(length);
                }

                var totalElements = 0;
                for (; _columnPosition < _columnWriters.Count; _columnPosition++)
                {
                    SequenceSize size;
                    if (_rowCounts[_columnPosition] == 0)
                        size = SequenceSize.Empty;
                    else
                        size = _columnWriters[_columnPosition].WriteNext(span);

                    _rowPosition += size.Elements;
                    totalBytes += size.Bytes;

                    if (_columnPosition == _columnWriters.Count - 1)
                    {
                        totalElements += size.Elements;
                        if (_rowPosition == _rowCounts[_columnPosition])
                            totalElements += _indices.Length - _rowPosition;
                    }

                    if (_rowPosition < _rowCounts[_columnPosition])
                        break;

                    _rowPosition = 0;
                    span = span.Slice(size.Bytes);
                }

                return new SequenceSize(totalBytes, totalElements);
            }
        }
    }
}
