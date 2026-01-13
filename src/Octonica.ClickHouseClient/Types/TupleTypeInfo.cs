#region License Apache 2.0
/* Copyright 2019-2021, 2023-2024 Octonica
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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class TupleTypeInfo : IClickHouseColumnTypeInfo
    {
        private readonly List<IClickHouseColumnTypeInfo>? _elementTypes;
        private readonly List<string>? _elementNames;

        public string ComplexTypeName { get; }

        public string TypeName => "Tuple";

        public int GenericArgumentsCount => _elementTypes?.Count ?? 0;

        public TupleTypeInfo()
        {
            ComplexTypeName = TypeName;
            _elementTypes = null;
        }

        private TupleTypeInfo(string complexTypeName, List<IClickHouseColumnTypeInfo> elementTypes, List<string>? elementNames)
        {
            if (elementNames != null && elementTypes.Count != elementNames.Count)
            {
                throw new ArgumentException("The number of elements must be equal to the number of element's types.", nameof(elementNames));
            }

            ComplexTypeName = complexTypeName;
            _elementTypes = elementTypes;
            _elementNames = elementNames;
        }

        public IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            if (_elementTypes == null)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");
            }

            List<IClickHouseColumnReader> elementReaders = _elementTypes.Select(t => t.CreateColumnReader(rowCount)).ToList();
            return new TupleColumnReader(rowCount, elementReaders);
        }

        IClickHouseColumnReader IClickHouseColumnTypeInfo.CreateColumnReader(int rowCount, ClickHouseColumnSerializationMode serializationMode)
        {
            return serializationMode == ClickHouseColumnSerializationMode.Default
                ? CreateColumnReader(rowCount)
                : serializationMode == ClickHouseColumnSerializationMode.Custom
                ? _elementTypes == null
                    ? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.")
                    : (IClickHouseColumnReader)new TupleCustomSerializationColumnReader(this, rowCount)
                : throw new NotSupportedException($"The serialization mode {serializationMode} for {TypeName} type is not supported by ClickHouseClient.");
        }

        public IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            if (_elementTypes == null)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");
            }

            List<IClickHouseColumnReaderBase> elementReaders = _elementTypes.Select(t => t.CreateSkippingColumnReader(rowCount)).ToList();
            return new TupleSkippingColumnReader(rowCount, elementReaders);
        }

        IClickHouseColumnReaderBase IClickHouseColumnTypeInfo.CreateSkippingColumnReader(int rowCount, ClickHouseColumnSerializationMode serializationMode)
        {
            return serializationMode == ClickHouseColumnSerializationMode.Default
                ? CreateSkippingColumnReader(rowCount)
                : serializationMode == ClickHouseColumnSerializationMode.Custom
                ? _elementTypes == null
                    ? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.")
                    : (IClickHouseColumnReaderBase)new TupleCustomSerializationSkippingColumnReader(this, rowCount)
                : throw new NotSupportedException($"The serialization mode {serializationMode} for {TypeName} type is not supported by ClickHouseClient.");
        }

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            return _elementTypes == null
                ? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.")
                : (IClickHouseColumnWriter)TupleColumnWriter.CreateColumnWriter(columnName, ComplexTypeName, _elementTypes, rows, columnSettings);
        }

        public IClickHouseParameterWriter<T> CreateParameterWriter<T>()
        {
            // TODO: ClickHouseDbType.Tuple is not supported in DefaultTypeInfoProvider.GetTypeInfo
            if (_elementTypes == null)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");
            }

            Type tupeType = typeof(T);
            return tupeType == typeof(DBNull)
                ? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values.")
                : TupleColumnWriter.CreateParameterWriter<T>(this, ComplexTypeName, _elementTypes, false);
        }

        public IClickHouseColumnTypeInfo GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            if (_elementTypes != null)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, "The type is already fully specified.");
            }

            StringBuilder complexTypeNameBuilder = new StringBuilder(TypeName).Append('(');
            List<IClickHouseColumnTypeInfo> elementTypes = new(options.Count);
            List<string>? elementNames = null;
            foreach (ReadOnlyMemory<char> option in options)
            {
                if (elementTypes.Count > 0)
                {
                    _ = complexTypeNameBuilder.Append(", ");
                }

                int identifierLen = ClickHouseSyntaxHelper.GetIdentifierLiteralLength(option.Span);
                if (identifierLen == option.Span.Length)
                {
                    identifierLen = -1;
                }

                if (identifierLen < 0)
                {
                    if (elementNames != null)
                    {
                        throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, "A tuple can be either named or not. Mixing of named and unnamed arguments is not allowed.");
                    }

                    IClickHouseColumnTypeInfo typeInfo = typeInfoProvider.GetTypeInfo(option);
                    elementTypes.Add(typeInfo);
                }
                else
                {
                    if (elementNames == null)
                    {
                        if (elementTypes.Count > 0)
                        {
                            throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, "A tuple can be either named or not. Mixing of named and unnamed arguments is not allowed.");
                        }

                        elementNames = new List<string>(options.Count);
                    }

                    string name = ClickHouseSyntaxHelper.GetIdentifier(option.Span[..identifierLen]);
                    IClickHouseColumnTypeInfo typeInfo = typeInfoProvider.GetTypeInfo(option[(identifierLen + 1)..]);

                    elementTypes.Add(typeInfo);
                    elementNames.Add(name);

                    _ = complexTypeNameBuilder.Append(option[..identifierLen]).Append(' ');
                }

                _ = complexTypeNameBuilder.Append(elementTypes[^1].ComplexTypeName);
            }

            string complexTypeName = complexTypeNameBuilder.Append(')').ToString();
            return new TupleTypeInfo(complexTypeName, elementTypes, elementNames);
        }

        public Type GetFieldType()
        {
            return _elementTypes == null
                ? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.")
                : MakeTupleType(_elementTypes.Select(elt => elt.GetFieldType()).ToArray());
        }

        public ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Tuple;
        }

        public IClickHouseTypeInfo GetGenericArgument(int index)
        {
            return _elementTypes == null
                ? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.")
                : (IClickHouseTypeInfo)_elementTypes[index];
        }

        public object GetTypeArgument(int index)
        {
            return _elementTypes == null
                ? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.")
                : _elementNames == null
                ? _elementTypes[index]
                : new KeyValuePair<string, IClickHouseTypeInfo>(_elementNames[index], _elementTypes[index]);
        }

        public static Type MakeTupleType(IReadOnlyList<Type> genericArgs)
        {
            Type tupleType;
            switch (genericArgs.Count)
            {
                case 0:
                    throw new ArgumentException("No type arguments specified.", nameof(genericArgs));
                case 1:
                    tupleType = typeof(Tuple<>);
                    break;
                case 2:
                    tupleType = typeof(Tuple<,>);
                    break;
                case 3:
                    tupleType = typeof(Tuple<,,>);
                    break;
                case 4:
                    tupleType = typeof(Tuple<,,,>);
                    break;
                case 5:
                    tupleType = typeof(Tuple<,,,,>);
                    break;
                case 6:
                    tupleType = typeof(Tuple<,,,,,>);
                    break;
                case 7:
                    tupleType = typeof(Tuple<,,,,,,>);
                    break;
                default:
                    tupleType = typeof(Tuple<,,,,,,,>);
                    Type restType = MakeTupleType(genericArgs.Slice(7));

                    Type[] tuple8ArgsArr = new Type[8];
                    for (int i = 0; i < 7; i++)
                    {
                        tuple8ArgsArr[i] = genericArgs[i];
                    }

                    tuple8ArgsArr[7] = restType;
                    return tupleType.MakeGenericType(tuple8ArgsArr);
            }

            Type[] genericArgsArr = genericArgs as Type[] ?? genericArgs.ToArray();
            return tupleType.MakeGenericType(genericArgsArr);
        }

        private static void AddParameterWriter<TTuple, TItem>(List<IClickHouseParameterWriter<TTuple>> writers, IClickHouseColumnTypeInfo elementType, Func<TTuple, TItem> getItem)
        {
            IClickHouseParameterWriter<TItem> elementWriter = elementType.CreateParameterWriter<TItem>();
            TupleItemParameterWriter<TTuple, TItem> writer = new(elementWriter, getItem);
            writers.Add(writer);
        }

        private static IClickHouseParameterWriter<TTuple> CreateParameterWriter<TTuple>(TupleTypeInfo typeInfo, IReadOnlyList<IClickHouseParameterWriter<TTuple>> itemWriters, bool isRest)
        {
            return new TupleParameterWriter<TTuple>(typeInfo, itemWriters, isRest);
        }

        private sealed class TupleCustomSerializationColumnReader : IClickHouseColumnReader
        {
            private readonly TupleTypeInfo _typeInfo;
            private readonly int _rowCount;

            private TupleColumnReader? _realReader;

            public TupleCustomSerializationColumnReader(TupleTypeInfo typeInfo, int rowCount)
            {
                _typeInfo = typeInfo;
                _rowCount = rowCount;
            }

            SequenceSize IClickHouseColumnReaderBase.ReadPrefix(ReadOnlySequence<byte> sequence)
            {
                if (_realReader != null)
                {
                    return ((IClickHouseColumnReaderBase)_realReader).ReadPrefix(sequence);
                }

                List<IClickHouseColumnTypeInfo>? elementTypes = _typeInfo._elementTypes;
                Debug.Assert(elementTypes != null);

                if (sequence.Length < elementTypes.Count + 1)
                {
                    return SequenceSize.Empty;
                }

                ClickHouseColumnSerializationMode mode = (ClickHouseColumnSerializationMode)sequence.FirstSpan[0];
                if (mode != ClickHouseColumnSerializationMode.Default)
                {
                    throw new NotSupportedException($"The serialization mode {mode} is not supported by {_typeInfo.TypeName} type. Only the default mode is supported.");
                }

                ReadOnlySequence<byte> seq = sequence.Slice(1);
                List<(IClickHouseColumnTypeInfo type, ClickHouseColumnSerializationMode mode)> typeModes = new(elementTypes.Count);
                foreach (IClickHouseColumnTypeInfo type in elementTypes)
                {
                    mode = (ClickHouseColumnSerializationMode)seq.FirstSpan[0];
                    if (mode is not ClickHouseColumnSerializationMode.Default and not ClickHouseColumnSerializationMode.Sparse)
                    {
                        throw new NotSupportedException($"Invalid serialization mode ({mode}) for an elment of tuple.");
                    }

                    typeModes.Add((type, mode));
                    seq = seq.Slice(1);
                }

                List<IClickHouseColumnReader> readers = typeModes.Select(tm => tm.type.CreateColumnReader(_rowCount, tm.mode)).ToList();
                _realReader = new TupleColumnReader(_rowCount, readers);
                SequenceSize result = ((IClickHouseColumnReaderBase)_realReader).ReadPrefix(seq);
                return result.AddBytes(elementTypes.Count + 1);
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                return _realReader == null
                    ? throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Detected an attempt to read the column before reading its prefix.")
                    : _realReader.ReadNext(sequence);
            }

            public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
            {
                return _realReader == null ? _typeInfo.CreateColumnReader(0).EndRead(settings)! : _realReader.EndRead(settings)!;
            }
        }

        private sealed class TupleColumnReader : IClickHouseColumnReader
        {
            private readonly int _rowCount;
            private readonly IReadOnlyList<IClickHouseColumnReader> _readers;

            private int _prefixPosition;
            private int _readerPosition;
            private int _position;

            public TupleColumnReader(int rowCount, IReadOnlyList<IClickHouseColumnReader> readers)
            {
                _rowCount = rowCount;
                _readers = readers;
            }

            SequenceSize IClickHouseColumnReaderBase.ReadPrefix(ReadOnlySequence<byte> sequence)
            {
                return ReadPrefix(sequence, _readers, ref _prefixPosition);
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                bool isLastColumn = _readerPosition == _readers.Count - 1;
                if (!isLastColumn && _position >= _rowCount)
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");
                }

                IClickHouseColumnReader currentReader = _readers[_readerPosition];
                SequenceSize result = new(0, 0);
                while (!isLastColumn)
                {
                    if (_position < _rowCount)
                    {
                        int elementsCount = _rowCount - _position;
                        SequenceSize size = currentReader.ReadNext(sequence.Slice(result.Bytes));

                        result = new SequenceSize(result.Bytes + size.Bytes, result.Elements);
                        _position += size.Elements;
                        if (size.Elements < elementsCount)
                        {
                            return result;
                        }
                    }

                    _position = 0;
                    currentReader = _readers[++_readerPosition];
                    isLastColumn = _readerPosition == _readers.Count - 1;
                }

                SequenceSize lastColumnSize = currentReader.ReadNext(sequence.Slice(result.Bytes));
                _position += lastColumnSize.Elements;

                return new SequenceSize(result.Bytes + lastColumnSize.Bytes, lastColumnSize.Elements);
            }

            public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
            {
                List<IClickHouseTableColumn> columns = new(_readers.Count);
                foreach (IClickHouseColumnReader reader in _readers)
                {
                    IClickHouseTableColumn column = reader.EndRead(settings)!;
                    columns.Add(column);
                }

                if (columns.Count == 0)
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Tuple reader produced no columns.");
                }

                return TupleTableColumnBase.MakeTupleColumn(columns[^1].RowCount, columns);
            }

            public static SequenceSize ReadPrefix(ReadOnlySequence<byte> sequence, IReadOnlyList<IClickHouseColumnReaderBase> elementReaders, ref int prefixPosition)
            {
                int totalBytes = 0;
                IClickHouseColumnReaderBase prefixReader = elementReaders[prefixPosition];
                while (prefixPosition < elementReaders.Count - 1)
                {
                    SequenceSize prefixSize = prefixReader.ReadPrefix(sequence);
                    totalBytes += prefixSize.Bytes;

                    if (prefixSize.Elements == 0)
                    {
                        return new SequenceSize(totalBytes, 0);
                    }

                    if (prefixSize.Elements != 1)
                    {
                        throw new ClickHouseException(ClickHouseErrorCodes.InternalError, $"Internal error. Received an unexpected number of column prefixes: {prefixSize.Elements}.");
                    }

                    prefixReader = elementReaders[++prefixPosition];
                }

                SequenceSize lastPrefixSize = prefixReader.ReadPrefix(sequence);
                return lastPrefixSize.AddBytes(totalBytes);
            }
        }

        private sealed class TupleCustomSerializationSkippingColumnReader : IClickHouseColumnReaderBase
        {
            private readonly TupleTypeInfo _typeInfo;
            private readonly int _rowCount;

            private TupleSkippingColumnReader? _realReader;

            public TupleCustomSerializationSkippingColumnReader(TupleTypeInfo typeInfo, int rowCount)
            {
                _typeInfo = typeInfo;
                _rowCount = rowCount;
            }

            SequenceSize IClickHouseColumnReaderBase.ReadPrefix(ReadOnlySequence<byte> sequence)
            {
                if (_realReader != null)
                {
                    return ((IClickHouseColumnReaderBase)_realReader).ReadPrefix(sequence);
                }

                List<IClickHouseColumnTypeInfo>? elementTypes = _typeInfo._elementTypes;
                Debug.Assert(elementTypes != null);

                if (sequence.Length < elementTypes.Count + 1)
                {
                    return SequenceSize.Empty;
                }

                ClickHouseColumnSerializationMode mode = (ClickHouseColumnSerializationMode)sequence.FirstSpan[0];
                if (mode != ClickHouseColumnSerializationMode.Default)
                {
                    throw new NotSupportedException($"The serialization mode {mode} is not supported by {_typeInfo.TypeName} type. Only the default mode is supported.");
                }

                ReadOnlySequence<byte> seq = sequence.Slice(1);
                List<(IClickHouseColumnTypeInfo type, ClickHouseColumnSerializationMode mode)> typeModes = new(elementTypes.Count);
                foreach (IClickHouseColumnTypeInfo type in elementTypes)
                {
                    mode = (ClickHouseColumnSerializationMode)seq.FirstSpan[0];
                    if (mode is not ClickHouseColumnSerializationMode.Default and not ClickHouseColumnSerializationMode.Sparse)
                    {
                        throw new NotSupportedException($"Invalid serialization mode ({mode}) for an elment of tuple.");
                    }

                    typeModes.Add((type, mode));
                    seq = seq.Slice(1);
                }

                List<IClickHouseColumnReaderBase> readers = typeModes.Select(tm => tm.type.CreateSkippingColumnReader(_rowCount, tm.mode)).ToList();
                _realReader = new TupleSkippingColumnReader(_rowCount, readers);
                SequenceSize result = ((IClickHouseColumnReaderBase)_realReader).ReadPrefix(seq);
                return result.AddBytes(elementTypes.Count + 1);
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                return _realReader == null
                    ? throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Detected an attempt to read the column before reading its prefix.")
                    : _realReader.ReadNext(sequence);
            }
        }

        private sealed class TupleSkippingColumnReader : IClickHouseColumnReaderBase
        {
            private readonly int _rowCount;
            private readonly IReadOnlyList<IClickHouseColumnReaderBase> _elementReaders;

            private int _prefixPosition;
            private int _elementReaderIndex;

            private int _position;

            public TupleSkippingColumnReader(int rowCount, IReadOnlyList<IClickHouseColumnReaderBase> elementReaders)
            {
                _rowCount = rowCount;
                _elementReaders = elementReaders;
            }

            SequenceSize IClickHouseColumnReaderBase.ReadPrefix(ReadOnlySequence<byte> sequence)
            {
                return TupleColumnReader.ReadPrefix(sequence, _elementReaders, ref _prefixPosition);
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                SequenceSize result = new(0, 0);
                IClickHouseColumnReaderBase elementReader = _elementReaders[_elementReaderIndex];
                while (_elementReaderIndex < _elementReaders.Count - 1)
                {
                    SequenceSize actualCount = elementReader.ReadNext(sequence.Slice(result.Bytes));

                    result = result.AddBytes(actualCount.Bytes);
                    _position += actualCount.Elements;
                    if (_position < _rowCount)
                    {
                        return result;
                    }

                    _position = 0;
                    elementReader = _elementReaders[++_elementReaderIndex];
                }

                SequenceSize lastColumnCount = elementReader.ReadNext(sequence.Slice(result.Bytes));
                _position += lastColumnCount.Elements;

                return lastColumnCount.AddBytes(result.Bytes);
            }
        }

        private sealed class TupleColumnWriter : IClickHouseColumnWriter
        {
            private readonly int _rowCount;
            private readonly List<IClickHouseColumnWriter> _columns;

            public string ColumnName { get; }

            public string ColumnType { get; }

            private int _prefixPosition;

            private int _currentWriterIdx;
            private int _currentWriterPosition;

            private TupleColumnWriter(string columnName, List<IClickHouseColumnWriter> columns, int rowCount)
            {
                _rowCount = rowCount;
                ColumnName = columnName;
                _columns = columns;

                StringBuilder typeNameBuilder = _columns.Aggregate(new StringBuilder("Tuple("), (b, c) => b.Append(c.ColumnType).Append(','));
                typeNameBuilder[^1] = ')';
                ColumnType = typeNameBuilder.ToString();
            }

            SequenceSize IClickHouseColumnWriter.WritePrefix(Span<byte> writeTo)
            {
                int totalBytes = 0;
                for (; _prefixPosition < _columns.Count; _prefixPosition++)
                {
                    Span<byte> slice = writeTo[totalBytes..];
                    SequenceSize size = _columns[_prefixPosition].WritePrefix(slice);

                    totalBytes += size.Bytes;
                    if (size.Elements == 0)
                    {
                        break;
                    }
                }

                return _prefixPosition == _columns.Count ? new SequenceSize(totalBytes, 1) : new SequenceSize(totalBytes, 0);
            }

            public SequenceSize WriteNext(Span<byte> writeTo)
            {
                SequenceSize result = new(0, 0);

                while (_currentWriterIdx < _columns.Count - 1)
                {
                    IClickHouseColumnWriter columnWriter = _columns[_currentWriterIdx];
                    if (_currentWriterPosition < _rowCount)
                    {
                        int expectedElementsCount = _rowCount - _currentWriterPosition;
                        SequenceSize actualCount = columnWriter.WriteNext(writeTo[result.Bytes..]);

                        _currentWriterPosition += actualCount.Elements;
                        result = result.AddBytes(actualCount.Bytes);
                        if (actualCount.Elements < expectedElementsCount)
                        {
                            return result;
                        }
                    }

                    ++_currentWriterIdx;
                    _currentWriterPosition = 0;
                }

                SequenceSize lastColumnCount = _columns[^1].WriteNext(writeTo[result.Bytes..]);
                _currentWriterPosition += lastColumnCount.Elements;

                return result.Add(lastColumnCount);
            }

            public static TupleColumnWriter CreateColumnWriter<T>(string columnName, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
            {
                ITupleWriterFactory factory = CreateWriterFactory(typeof(T), columnType, elementTypes);

                try
                {
                    return factory.CreateColumnWriter(columnName, elementTypes, rows, columnSettings);
                }
                catch (Exception ex)
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{columnType}\".", ex);
                }
            }

            public static IClickHouseParameterWriter<T> CreateParameterWriter<T>(TupleTypeInfo typeInfo, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, bool isRest)
            {
                ITupleWriterFactory factory = CreateWriterFactory(typeof(T), columnType, elementTypes);

                object writer;
                try
                {
                    writer = factory.CreateParameterWriter(typeInfo, elementTypes, isRest);
                }
                catch (Exception ex)
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{columnType}\".", ex);
                }

                return (IClickHouseParameterWriter<T>)writer;
            }

            private static ITupleWriterFactory CreateWriterFactory(Type tupleType, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes)
            {
                Type? factoryType = null;
                if (tupleType.IsGenericType)
                {
                    Type listItemTypeDef = tupleType.GetGenericTypeDefinition();
                    switch (elementTypes.Count)
                    {
                        case 1:
                            if (listItemTypeDef == typeof(Tuple<>))
                            {
                                factoryType = typeof(TupleColumnFactory<>);
                            }
                            else if (listItemTypeDef == typeof(ValueTuple<>))
                            {
                                factoryType = typeof(ValueTupleColumnFactory<>);
                            }

                            break;
                        case 2:
                            if (listItemTypeDef == typeof(Tuple<,>))
                            {
                                factoryType = typeof(TupleColumnFactory<,>);
                            }
                            else if (listItemTypeDef == typeof(ValueTuple<,>))
                            {
                                factoryType = typeof(ValueTupleColumnFactory<,>);
                            }
                            else if (listItemTypeDef == typeof(KeyValuePair<,>))
                            {
                                factoryType = typeof(KeyValuePairColumnFactory<,>);
                            }

                            break;
                        case 3:
                            if (listItemTypeDef == typeof(Tuple<,,>))
                            {
                                factoryType = typeof(TupleColumnFactory<,,>);
                            }
                            else if (listItemTypeDef == typeof(ValueTuple<,,>))
                            {
                                factoryType = typeof(ValueTupleColumnFactory<,,>);
                            }

                            break;
                        case 4:
                            if (listItemTypeDef == typeof(Tuple<,,,>))
                            {
                                factoryType = typeof(TupleColumnFactory<,,,>);
                            }
                            else if (listItemTypeDef == typeof(ValueTuple<,,,>))
                            {
                                factoryType = typeof(ValueTupleColumnFactory<,,,>);
                            }

                            break;
                        case 5:
                            if (listItemTypeDef == typeof(Tuple<,,,,>))
                            {
                                factoryType = typeof(TupleColumnFactory<,,,,>);
                            }
                            else if (listItemTypeDef == typeof(ValueTuple<,,,,>))
                            {
                                factoryType = typeof(ValueTupleColumnFactory<,,,,>);
                            }

                            break;
                        case 6:
                            if (listItemTypeDef == typeof(Tuple<,,,,,>))
                            {
                                factoryType = typeof(TupleColumnFactory<,,,,,>);
                            }
                            else if (listItemTypeDef == typeof(ValueTuple<,,,,,>))
                            {
                                factoryType = typeof(ValueTupleColumnFactory<,,,,,>);
                            }

                            break;
                        case 7:
                            if (listItemTypeDef == typeof(Tuple<,,,,,,>))
                            {
                                factoryType = typeof(TupleColumnFactory<,,,,,,>);
                            }
                            else if (listItemTypeDef == typeof(ValueTuple<,,,,,,>))
                            {
                                factoryType = typeof(ValueTupleColumnFactory<,,,,,,>);
                            }

                            break;
                        default:
                            if (elementTypes.Count >= 8)
                            {
                                if (listItemTypeDef == typeof(Tuple<,,,,,,,>))
                                {
                                    factoryType = typeof(TupleColumnFactory<,,,,,,,>);
                                }
                                else if (listItemTypeDef == typeof(ValueTuple<,,,,,,,>))
                                {
                                    factoryType = typeof(ValueTupleColumnFactory<,,,,,,,>);
                                }
                            }

                            break;
                    }

                    if (factoryType != null)
                    {
                        Type[] args = tupleType.GetGenericArguments();
                        factoryType = factoryType.MakeGenericType(args);
                    }
                }

                if (factoryType == null)
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{tupleType}\" can't be converted to the ClickHouse type \"{columnType}\".");
                }

                ITupleWriterFactory? factory;
                try
                {
                    factory = (ITupleWriterFactory?)Activator.CreateInstance(factoryType);
                }
                catch (Exception ex)
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{tupleType}\" can't be converted to the ClickHouse type \"{columnType}\".", ex);
                }

                Debug.Assert(factory != null);
                return factory;
            }

            private interface ITupleWriterFactory
            {
                TupleColumnWriter CreateColumnWriter(string columnName, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings);

                object CreateParameterWriter(TupleTypeInfo typeInfo, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, bool isRest);
            }

            private sealed class TupleColumnFactory<T1> : ITupleWriterFactory
            {
                public TupleColumnWriter CreateColumnWriter(string columnName, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    IReadOnlyList<Tuple<T1>> rows = (IReadOnlyList<Tuple<T1>>)untypedRows;
                    List<IClickHouseColumnWriter> columns =
                    [
                        elementTypes[0].CreateColumnWriter(columnName, rows.Map(t => t.Item1), columnSettings)
                    ];

                    return new TupleColumnWriter(columnName, columns, rows.Count);
                }

                public object CreateParameterWriter(TupleTypeInfo typeInfo, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, bool isRest)
                {
                    List<IClickHouseParameterWriter<Tuple<T1>>> elementWriters = new(elementTypes.Count);
                    AddParameterWriter(elementWriters, elementTypes[0], t => t.Item1);
                    return TupleTypeInfo.CreateParameterWriter(typeInfo, elementWriters, isRest);
                }
            }

            private sealed class TupleColumnFactory<T1, T2> : ITupleWriterFactory
            {
                public TupleColumnWriter CreateColumnWriter(string columnName, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    IReadOnlyList<Tuple<T1, T2>> rows = (IReadOnlyList<Tuple<T1, T2>>)untypedRows;
                    List<IClickHouseColumnWriter> columns =
                    [
                        elementTypes[0].CreateColumnWriter(columnName, rows.Map(t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, rows.Map(t => t.Item2), columnSettings)
                    ];

                    return new TupleColumnWriter(columnName, columns, rows.Count);
                }

                public object CreateParameterWriter(TupleTypeInfo typeInfo, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, bool isRest)
                {
                    List<IClickHouseParameterWriter<Tuple<T1, T2>>> elementWriters = new(elementTypes.Count);

                    AddParameterWriter(elementWriters, elementTypes[0], t => t.Item1);
                    AddParameterWriter(elementWriters, elementTypes[1], t => t.Item2);

                    return TupleTypeInfo.CreateParameterWriter(typeInfo, elementWriters, isRest);
                }
            }

            private sealed class TupleColumnFactory<T1, T2, T3> : ITupleWriterFactory
            {
                public TupleColumnWriter CreateColumnWriter(string columnName, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    IReadOnlyList<Tuple<T1, T2, T3>> rows = (IReadOnlyList<Tuple<T1, T2, T3>>)untypedRows;
                    List<IClickHouseColumnWriter> columns =
                    [
                        elementTypes[0].CreateColumnWriter(columnName, rows.Map(t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, rows.Map(t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, rows.Map(t => t.Item3), columnSettings)
                    ];

                    return new TupleColumnWriter(columnName, columns, rows.Count);
                }

                public object CreateParameterWriter(TupleTypeInfo typeInfo, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, bool isRest)
                {
                    List<IClickHouseParameterWriter<Tuple<T1, T2, T3>>> elementWriters = new(elementTypes.Count);

                    AddParameterWriter(elementWriters, elementTypes[0], t => t.Item1);
                    AddParameterWriter(elementWriters, elementTypes[1], t => t.Item2);
                    AddParameterWriter(elementWriters, elementTypes[2], t => t.Item3);

                    return TupleTypeInfo.CreateParameterWriter(typeInfo, elementWriters, isRest);
                }
            }

            private sealed class TupleColumnFactory<T1, T2, T3, T4> : ITupleWriterFactory
            {
                public TupleColumnWriter CreateColumnWriter(string columnName, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    IReadOnlyList<Tuple<T1, T2, T3, T4>> rows = (IReadOnlyList<Tuple<T1, T2, T3, T4>>)untypedRows;
                    List<IClickHouseColumnWriter> columns =
                    [
                        elementTypes[0].CreateColumnWriter(columnName, rows.Map(t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, rows.Map(t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, rows.Map(t => t.Item3), columnSettings),
                        elementTypes[3].CreateColumnWriter(columnName, rows.Map(t => t.Item4), columnSettings)
                    ];

                    return new TupleColumnWriter(columnName, columns, rows.Count);
                }

                public object CreateParameterWriter(TupleTypeInfo typeInfo, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, bool isRest)
                {
                    List<IClickHouseParameterWriter<Tuple<T1, T2, T3, T4>>> elementWriters = new(elementTypes.Count);

                    AddParameterWriter(elementWriters, elementTypes[0], t => t.Item1);
                    AddParameterWriter(elementWriters, elementTypes[1], t => t.Item2);
                    AddParameterWriter(elementWriters, elementTypes[2], t => t.Item3);
                    AddParameterWriter(elementWriters, elementTypes[3], t => t.Item4);

                    return TupleTypeInfo.CreateParameterWriter(typeInfo, elementWriters, isRest);
                }
            }

            private sealed class TupleColumnFactory<T1, T2, T3, T4, T5> : ITupleWriterFactory
            {
                public TupleColumnWriter CreateColumnWriter(string columnName, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    IReadOnlyList<Tuple<T1, T2, T3, T4, T5>> rows = (IReadOnlyList<Tuple<T1, T2, T3, T4, T5>>)untypedRows;
                    List<IClickHouseColumnWriter> columns =
                    [
                        elementTypes[0].CreateColumnWriter(columnName, rows.Map(t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, rows.Map(t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, rows.Map(t => t.Item3), columnSettings),
                        elementTypes[3].CreateColumnWriter(columnName, rows.Map(t => t.Item4), columnSettings),
                        elementTypes[4].CreateColumnWriter(columnName, rows.Map(t => t.Item5), columnSettings)
                    ];

                    return new TupleColumnWriter(columnName, columns, rows.Count);
                }

                public object CreateParameterWriter(TupleTypeInfo typeInfo, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, bool isRest)
                {
                    List<IClickHouseParameterWriter<Tuple<T1, T2, T3, T4, T5>>> elementWriters = new(elementTypes.Count);

                    AddParameterWriter(elementWriters, elementTypes[0], t => t.Item1);
                    AddParameterWriter(elementWriters, elementTypes[1], t => t.Item2);
                    AddParameterWriter(elementWriters, elementTypes[2], t => t.Item3);
                    AddParameterWriter(elementWriters, elementTypes[3], t => t.Item4);
                    AddParameterWriter(elementWriters, elementTypes[4], t => t.Item5);

                    return TupleTypeInfo.CreateParameterWriter(typeInfo, elementWriters, isRest);
                }
            }

            private sealed class TupleColumnFactory<T1, T2, T3, T4, T5, T6> : ITupleWriterFactory
            {
                public TupleColumnWriter CreateColumnWriter(string columnName, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    IReadOnlyList<Tuple<T1, T2, T3, T4, T5, T6>> rows = (IReadOnlyList<Tuple<T1, T2, T3, T4, T5, T6>>)untypedRows;
                    List<IClickHouseColumnWriter> columns =
                    [
                        elementTypes[0].CreateColumnWriter(columnName, rows.Map(t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, rows.Map(t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, rows.Map(t => t.Item3), columnSettings),
                        elementTypes[3].CreateColumnWriter(columnName, rows.Map(t => t.Item4), columnSettings),
                        elementTypes[4].CreateColumnWriter(columnName, rows.Map(t => t.Item5), columnSettings),
                        elementTypes[5].CreateColumnWriter(columnName, rows.Map(t => t.Item6), columnSettings)
                    ];

                    return new TupleColumnWriter(columnName, columns, rows.Count);
                }

                public object CreateParameterWriter(TupleTypeInfo typeInfo, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, bool isRest)
                {
                    List<IClickHouseParameterWriter<Tuple<T1, T2, T3, T4, T5, T6>>> elementWriters = new(elementTypes.Count);

                    AddParameterWriter(elementWriters, elementTypes[0], t => t.Item1);
                    AddParameterWriter(elementWriters, elementTypes[1], t => t.Item2);
                    AddParameterWriter(elementWriters, elementTypes[2], t => t.Item3);
                    AddParameterWriter(elementWriters, elementTypes[3], t => t.Item4);
                    AddParameterWriter(elementWriters, elementTypes[4], t => t.Item5);
                    AddParameterWriter(elementWriters, elementTypes[5], t => t.Item6);

                    return TupleTypeInfo.CreateParameterWriter(typeInfo, elementWriters, isRest);
                }
            }

            private sealed class TupleColumnFactory<T1, T2, T3, T4, T5, T6, T7> : ITupleWriterFactory
            {
                public TupleColumnWriter CreateColumnWriter(string columnName, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    IReadOnlyList<Tuple<T1, T2, T3, T4, T5, T6, T7>> rows = (IReadOnlyList<Tuple<T1, T2, T3, T4, T5, T6, T7>>)untypedRows;
                    List<IClickHouseColumnWriter> columns =
                    [
                        elementTypes[0].CreateColumnWriter(columnName, rows.Map(t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, rows.Map(t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, rows.Map(t => t.Item3), columnSettings),
                        elementTypes[3].CreateColumnWriter(columnName, rows.Map(t => t.Item4), columnSettings),
                        elementTypes[4].CreateColumnWriter(columnName, rows.Map(t => t.Item5), columnSettings),
                        elementTypes[5].CreateColumnWriter(columnName, rows.Map(t => t.Item6), columnSettings),
                        elementTypes[6].CreateColumnWriter(columnName, rows.Map(t => t.Item7), columnSettings)
                    ];

                    return new TupleColumnWriter(columnName, columns, rows.Count);
                }

                public object CreateParameterWriter(TupleTypeInfo typeInfo, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, bool isRest)
                {
                    List<IClickHouseParameterWriter<Tuple<T1, T2, T3, T4, T5, T6, T7>>> elementWriters = new(elementTypes.Count);

                    AddParameterWriter(elementWriters, elementTypes[0], t => t.Item1);
                    AddParameterWriter(elementWriters, elementTypes[1], t => t.Item2);
                    AddParameterWriter(elementWriters, elementTypes[2], t => t.Item3);
                    AddParameterWriter(elementWriters, elementTypes[3], t => t.Item4);
                    AddParameterWriter(elementWriters, elementTypes[4], t => t.Item5);
                    AddParameterWriter(elementWriters, elementTypes[5], t => t.Item6);
                    AddParameterWriter(elementWriters, elementTypes[6], t => t.Item7);

                    return TupleTypeInfo.CreateParameterWriter(typeInfo, elementWriters, isRest);
                }
            }

            private sealed class TupleColumnFactory<T1, T2, T3, T4, T5, T6, T7, TRest> : ITupleWriterFactory
                where TRest : notnull
            {
                public TupleColumnWriter CreateColumnWriter(string columnName, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    IReadOnlyList<Tuple<T1, T2, T3, T4, T5, T6, T7, TRest>> rows = (IReadOnlyList<Tuple<T1, T2, T3, T4, T5, T6, T7, TRest>>)untypedRows;

                    IReadOnlyList<IClickHouseColumnTypeInfo> subColumns = elementTypes.Slice(7);
                    string subType = "Tuple(" + string.Join(", ", subColumns.Select(c => c.ComplexTypeName)) + ")";
                    TupleColumnWriter lastColumn = TupleColumnWriter.CreateColumnWriter(columnName, subType, subColumns, rows.Map(t => t.Rest), columnSettings);

                    List<IClickHouseColumnWriter> columns =
                    [
                        elementTypes[0].CreateColumnWriter(columnName, rows.Map(t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, rows.Map(t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, rows.Map(t => t.Item3), columnSettings),
                        elementTypes[3].CreateColumnWriter(columnName, rows.Map(t => t.Item4), columnSettings),
                        elementTypes[4].CreateColumnWriter(columnName, rows.Map(t => t.Item5), columnSettings),
                        elementTypes[5].CreateColumnWriter(columnName, rows.Map(t => t.Item6), columnSettings),
                        elementTypes[6].CreateColumnWriter(columnName, rows.Map(t => t.Item7), columnSettings),
                        .. lastColumn._columns,
                    ];

                    return new TupleColumnWriter(columnName, columns, rows.Count);
                }

                public object CreateParameterWriter(TupleTypeInfo typeInfo, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, bool isRest)
                {
                    List<IClickHouseParameterWriter<Tuple<T1, T2, T3, T4, T5, T6, T7, TRest>>> elementWriters = new(elementTypes.Count);

                    AddParameterWriter(elementWriters, elementTypes[0], t => t.Item1);
                    AddParameterWriter(elementWriters, elementTypes[1], t => t.Item2);
                    AddParameterWriter(elementWriters, elementTypes[2], t => t.Item3);
                    AddParameterWriter(elementWriters, elementTypes[3], t => t.Item4);
                    AddParameterWriter(elementWriters, elementTypes[4], t => t.Item5);
                    AddParameterWriter(elementWriters, elementTypes[5], t => t.Item6);
                    AddParameterWriter(elementWriters, elementTypes[6], t => t.Item7);

                    IReadOnlyList<IClickHouseColumnTypeInfo> restElements = elementTypes.Slice(7);
                    string restType = "Tuple(" + string.Join(", ", restElements.Select(c => c.ComplexTypeName)) + ")";
                    IClickHouseParameterWriter<TRest> restWriter = CreateParameterWriter<TRest>(typeInfo, restType, restElements, true);

                    elementWriters.Add(new TupleItemParameterWriter<Tuple<T1, T2, T3, T4, T5, T6, T7, TRest>, TRest>(restWriter, t => t.Rest));
                    return TupleTypeInfo.CreateParameterWriter(typeInfo, elementWriters, isRest);
                }
            }

            private sealed class ValueTupleColumnFactory<T1> : ITupleWriterFactory
            {
                public TupleColumnWriter CreateColumnWriter(string columnName, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    IReadOnlyList<ValueTuple<T1>> rows = (IReadOnlyList<ValueTuple<T1>>)untypedRows;
                    List<IClickHouseColumnWriter> columns =
                    [
                        elementTypes[0].CreateColumnWriter(columnName, rows.Map(t => t.Item1), columnSettings)
                    ];

                    return new TupleColumnWriter(columnName, columns, rows.Count);
                }

                public object CreateParameterWriter(TupleTypeInfo typeInfo, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, bool isRest)
                {
                    List<IClickHouseParameterWriter<ValueTuple<T1>>> elementWriters = new(elementTypes.Count);
                    AddParameterWriter(elementWriters, elementTypes[0], t => t.Item1);
                    return TupleTypeInfo.CreateParameterWriter(typeInfo, elementWriters, isRest);
                }
            }

            private sealed class ValueTupleColumnFactory<T1, T2> : ITupleWriterFactory
            {
                public TupleColumnWriter CreateColumnWriter(string columnName, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    IReadOnlyList<(T1, T2)> rows = (IReadOnlyList<ValueTuple<T1, T2>>)untypedRows;
                    List<IClickHouseColumnWriter> columns =
                    [
                        elementTypes[0].CreateColumnWriter(columnName, rows.Map(t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, rows.Map(t => t.Item2), columnSettings)
                    ];

                    return new TupleColumnWriter(columnName, columns, rows.Count);
                }

                public object CreateParameterWriter(TupleTypeInfo typeInfo, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, bool isRest)
                {
                    List<IClickHouseParameterWriter<(T1, T2)>> elementWriters = new(elementTypes.Count);

                    AddParameterWriter(elementWriters, elementTypes[0], t => t.Item1);
                    AddParameterWriter(elementWriters, elementTypes[1], t => t.Item2);

                    return TupleTypeInfo.CreateParameterWriter(typeInfo, elementWriters, isRest);
                }
            }

            private sealed class ValueTupleColumnFactory<T1, T2, T3> : ITupleWriterFactory
            {
                public TupleColumnWriter CreateColumnWriter(string columnName, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    IReadOnlyList<(T1, T2, T3)> rows = (IReadOnlyList<ValueTuple<T1, T2, T3>>)untypedRows;
                    List<IClickHouseColumnWriter> columns =
                    [
                        elementTypes[0].CreateColumnWriter(columnName, rows.Map(t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, rows.Map(t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, rows.Map(t => t.Item3), columnSettings)
                    ];

                    return new TupleColumnWriter(columnName, columns, rows.Count);
                }

                public object CreateParameterWriter(TupleTypeInfo typeInfo, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, bool isRest)
                {
                    List<IClickHouseParameterWriter<(T1, T2, T3)>> elementWriters = new(elementTypes.Count);

                    AddParameterWriter(elementWriters, elementTypes[0], t => t.Item1);
                    AddParameterWriter(elementWriters, elementTypes[1], t => t.Item2);
                    AddParameterWriter(elementWriters, elementTypes[2], t => t.Item3);

                    return TupleTypeInfo.CreateParameterWriter(typeInfo, elementWriters, isRest);
                }
            }

            private sealed class ValueTupleColumnFactory<T1, T2, T3, T4> : ITupleWriterFactory
            {
                public TupleColumnWriter CreateColumnWriter(string columnName, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    IReadOnlyList<(T1, T2, T3, T4)> rows = (IReadOnlyList<ValueTuple<T1, T2, T3, T4>>)untypedRows;
                    List<IClickHouseColumnWriter> columns =
                    [
                        elementTypes[0].CreateColumnWriter(columnName, rows.Map(t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, rows.Map(t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, rows.Map(t => t.Item3), columnSettings),
                        elementTypes[3].CreateColumnWriter(columnName, rows.Map(t => t.Item4), columnSettings)
                    ];

                    return new TupleColumnWriter(columnName, columns, rows.Count);
                }

                public object CreateParameterWriter(TupleTypeInfo typeInfo, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, bool isRest)
                {
                    List<IClickHouseParameterWriter<(T1, T2, T3, T4)>> elementWriters = new(elementTypes.Count);

                    AddParameterWriter(elementWriters, elementTypes[0], t => t.Item1);
                    AddParameterWriter(elementWriters, elementTypes[1], t => t.Item2);
                    AddParameterWriter(elementWriters, elementTypes[2], t => t.Item3);
                    AddParameterWriter(elementWriters, elementTypes[3], t => t.Item4);

                    return TupleTypeInfo.CreateParameterWriter(typeInfo, elementWriters, isRest);
                }
            }

            private sealed class ValueTupleColumnFactory<T1, T2, T3, T4, T5> : ITupleWriterFactory
            {
                public TupleColumnWriter CreateColumnWriter(string columnName, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    IReadOnlyList<(T1, T2, T3, T4, T5)> rows = (IReadOnlyList<ValueTuple<T1, T2, T3, T4, T5>>)untypedRows;
                    List<IClickHouseColumnWriter> columns =
                    [
                        elementTypes[0].CreateColumnWriter(columnName, rows.Map(t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, rows.Map(t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, rows.Map(t => t.Item3), columnSettings),
                        elementTypes[3].CreateColumnWriter(columnName, rows.Map(t => t.Item4), columnSettings),
                        elementTypes[4].CreateColumnWriter(columnName, rows.Map(t => t.Item5), columnSettings)
                    ];

                    return new TupleColumnWriter(columnName, columns, rows.Count);
                }

                public object CreateParameterWriter(TupleTypeInfo typeInfo, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, bool isRest)
                {
                    List<IClickHouseParameterWriter<(T1, T2, T3, T4, T5)>> elementWriters = new(elementTypes.Count);

                    AddParameterWriter(elementWriters, elementTypes[0], t => t.Item1);
                    AddParameterWriter(elementWriters, elementTypes[1], t => t.Item2);
                    AddParameterWriter(elementWriters, elementTypes[2], t => t.Item3);
                    AddParameterWriter(elementWriters, elementTypes[3], t => t.Item4);
                    AddParameterWriter(elementWriters, elementTypes[4], t => t.Item5);

                    return TupleTypeInfo.CreateParameterWriter(typeInfo, elementWriters, isRest);
                }
            }

            private sealed class ValueTupleColumnFactory<T1, T2, T3, T4, T5, T6> : ITupleWriterFactory
            {
                public TupleColumnWriter CreateColumnWriter(string columnName, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    IReadOnlyList<(T1, T2, T3, T4, T5, T6)> rows = (IReadOnlyList<ValueTuple<T1, T2, T3, T4, T5, T6>>)untypedRows;
                    List<IClickHouseColumnWriter> columns =
                    [
                        elementTypes[0].CreateColumnWriter(columnName, rows.Map(t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, rows.Map(t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, rows.Map(t => t.Item3), columnSettings),
                        elementTypes[3].CreateColumnWriter(columnName, rows.Map(t => t.Item4), columnSettings),
                        elementTypes[4].CreateColumnWriter(columnName, rows.Map(t => t.Item5), columnSettings),
                        elementTypes[5].CreateColumnWriter(columnName, rows.Map(t => t.Item6), columnSettings)
                    ];

                    return new TupleColumnWriter(columnName, columns, rows.Count);
                }

                public object CreateParameterWriter(TupleTypeInfo typeInfo, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, bool isRest)
                {
                    List<IClickHouseParameterWriter<(T1, T2, T3, T4, T5, T6)>> elementWriters = new(elementTypes.Count);

                    AddParameterWriter(elementWriters, elementTypes[0], t => t.Item1);
                    AddParameterWriter(elementWriters, elementTypes[1], t => t.Item2);
                    AddParameterWriter(elementWriters, elementTypes[2], t => t.Item3);
                    AddParameterWriter(elementWriters, elementTypes[3], t => t.Item4);
                    AddParameterWriter(elementWriters, elementTypes[4], t => t.Item5);
                    AddParameterWriter(elementWriters, elementTypes[5], t => t.Item6);

                    return TupleTypeInfo.CreateParameterWriter(typeInfo, elementWriters, isRest);
                }
            }

            private sealed class ValueTupleColumnFactory<T1, T2, T3, T4, T5, T6, T7> : ITupleWriterFactory
            {
                public TupleColumnWriter CreateColumnWriter(string columnName, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    IReadOnlyList<(T1, T2, T3, T4, T5, T6, T7)> rows = (IReadOnlyList<ValueTuple<T1, T2, T3, T4, T5, T6, T7>>)untypedRows;
                    List<IClickHouseColumnWriter> columns =
                    [
                        elementTypes[0].CreateColumnWriter(columnName, rows.Map(t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, rows.Map(t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, rows.Map(t => t.Item3), columnSettings),
                        elementTypes[3].CreateColumnWriter(columnName, rows.Map(t => t.Item4), columnSettings),
                        elementTypes[4].CreateColumnWriter(columnName, rows.Map(t => t.Item5), columnSettings),
                        elementTypes[5].CreateColumnWriter(columnName, rows.Map(t => t.Item6), columnSettings),
                        elementTypes[6].CreateColumnWriter(columnName, rows.Map(t => t.Item7), columnSettings)
                    ];

                    return new TupleColumnWriter(columnName, columns, rows.Count);
                }

                public object CreateParameterWriter(TupleTypeInfo typeInfo, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, bool isRest)
                {
                    List<IClickHouseParameterWriter<(T1, T2, T3, T4, T5, T6, T7)>> elementWriters = new(elementTypes.Count);

                    AddParameterWriter(elementWriters, elementTypes[0], t => t.Item1);
                    AddParameterWriter(elementWriters, elementTypes[1], t => t.Item2);
                    AddParameterWriter(elementWriters, elementTypes[2], t => t.Item3);
                    AddParameterWriter(elementWriters, elementTypes[3], t => t.Item4);
                    AddParameterWriter(elementWriters, elementTypes[4], t => t.Item5);
                    AddParameterWriter(elementWriters, elementTypes[5], t => t.Item6);
                    AddParameterWriter(elementWriters, elementTypes[6], t => t.Item7);

                    return TupleTypeInfo.CreateParameterWriter(typeInfo, elementWriters, isRest);
                }
            }

            private sealed class ValueTupleColumnFactory<T1, T2, T3, T4, T5, T6, T7, TRest> : ITupleWriterFactory
                where TRest : struct
            {
                public TupleColumnWriter CreateColumnWriter(string columnName, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    IReadOnlyList<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>> rows = (IReadOnlyList<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>>)untypedRows;

                    IReadOnlyList<IClickHouseColumnTypeInfo> subColumns = elementTypes.Slice(7);
                    string subType = "Tuple(" + string.Join(", ", subColumns.Select(c => c.ComplexTypeName)) + ")";
                    TupleColumnWriter lastColumn = TupleColumnWriter.CreateColumnWriter(columnName, subType, subColumns, rows.Map(t => t.Rest), columnSettings);

                    List<IClickHouseColumnWriter> columns =
                    [
                        elementTypes[0].CreateColumnWriter(columnName, rows.Map(t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, rows.Map(t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, rows.Map(t => t.Item3), columnSettings),
                        elementTypes[3].CreateColumnWriter(columnName, rows.Map(t => t.Item4), columnSettings),
                        elementTypes[4].CreateColumnWriter(columnName, rows.Map(t => t.Item5), columnSettings),
                        elementTypes[5].CreateColumnWriter(columnName, rows.Map(t => t.Item6), columnSettings),
                        elementTypes[6].CreateColumnWriter(columnName, rows.Map(t => t.Item7), columnSettings),
                        .. lastColumn._columns,
                    ];

                    return new TupleColumnWriter(columnName, columns, rows.Count);
                }

                public object CreateParameterWriter(TupleTypeInfo typeInfo, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, bool isRest)
                {
                    List<IClickHouseParameterWriter<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>>> elementWriters = new(elementTypes.Count);

                    AddParameterWriter(elementWriters, elementTypes[0], t => t.Item1);
                    AddParameterWriter(elementWriters, elementTypes[1], t => t.Item2);
                    AddParameterWriter(elementWriters, elementTypes[2], t => t.Item3);
                    AddParameterWriter(elementWriters, elementTypes[3], t => t.Item4);
                    AddParameterWriter(elementWriters, elementTypes[4], t => t.Item5);
                    AddParameterWriter(elementWriters, elementTypes[5], t => t.Item6);
                    AddParameterWriter(elementWriters, elementTypes[6], t => t.Item7);

                    IReadOnlyList<IClickHouseColumnTypeInfo> restElements = elementTypes.Slice(7);
                    string restType = "Tuple(" + string.Join(", ", restElements.Select(c => c.ComplexTypeName)) + ")";
                    IClickHouseParameterWriter<TRest> restWriter = CreateParameterWriter<TRest>(typeInfo, restType, restElements, true);

                    elementWriters.Add(new TupleItemParameterWriter<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>, TRest>(restWriter, t => t.Rest));
                    return TupleTypeInfo.CreateParameterWriter(typeInfo, elementWriters, isRest);
                }
            }

            private sealed class KeyValuePairColumnFactory<TKey, TValue> : ITupleWriterFactory
            {
                public TupleColumnWriter CreateColumnWriter(string columnName, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    IReadOnlyList<KeyValuePair<TKey, TValue>> rows = (IReadOnlyList<KeyValuePair<TKey, TValue>>)untypedRows;

                    List<IClickHouseColumnWriter> columns =
                    [
                        elementTypes[0].CreateColumnWriter(columnName, rows.Map(p => p.Key), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, rows.Map(p => p.Value), columnSettings)
                    ];

                    return new TupleColumnWriter(columnName, columns, rows.Count);
                }

                public object CreateParameterWriter(TupleTypeInfo typeInfo, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, bool isRest)
                {
                    List<IClickHouseParameterWriter<KeyValuePair<TKey, TValue>>> writers = new(2);
                    AddParameterWriter(writers, elementTypes[0], pair => pair.Key);
                    AddParameterWriter(writers, elementTypes[1], pair => pair.Value);

                    return TupleTypeInfo.CreateParameterWriter(typeInfo, writers, isRest);
                }
            }
        }

        private sealed class TupleItemParameterWriter<TTuple, TItem> : IClickHouseParameterWriter<TTuple>
        {
            private readonly IClickHouseParameterWriter<TItem> _itemWriter;
            private readonly Func<TTuple, TItem> _getItem;

            public TupleItemParameterWriter(IClickHouseParameterWriter<TItem> itemWriter, Func<TTuple, TItem> getItem)
            {
                _itemWriter = itemWriter;
                _getItem = getItem;
            }

            public bool TryCreateParameterValueWriter(TTuple value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter)
            {
                return _itemWriter.TryCreateParameterValueWriter(_getItem(value), isNested, out valueWriter);
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, TTuple value)
            {
                return _itemWriter.Interpolate(queryBuilder, _getItem(value));
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseColumnTypeInfo, Func<StringBuilder, Func<StringBuilder, StringBuilder>, StringBuilder>, StringBuilder> writeValue)
            {
                return _itemWriter.Interpolate(queryBuilder, typeInfoProvider, writeValue);
            }
        }

        private sealed class TupleParameterWriter<T> : IClickHouseParameterWriter<T>
        {
            private readonly TupleTypeInfo _type;
            private readonly IReadOnlyList<IClickHouseParameterWriter<T>> _itemWriters;
            private readonly bool _isRest;

            public TupleParameterWriter(TupleTypeInfo type, IReadOnlyList<IClickHouseParameterWriter<T>> itemWriters, bool isRest)
            {
                _type = type;
                _itemWriters = itemWriters;
                _isRest = isRest;
            }

            public bool TryCreateParameterValueWriter(T value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter)
            {
                valueWriter = null;
                return false;
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, T value)
            {
                if (!_isRest)
                {
                    _ = queryBuilder.Append("tuple(");
                }

                bool isFirst = true;
                foreach (IClickHouseParameterWriter<T> itemWriter in _itemWriters)
                {
                    if (isFirst)
                    {
                        isFirst = false;
                    }
                    else
                    {
                        _ = queryBuilder.Append(',');
                    }

                    _ = itemWriter.Interpolate(queryBuilder, value);
                }

                if (!_isRest)
                {
                    _ = queryBuilder.Append(')');
                }

                return queryBuilder;
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseColumnTypeInfo, Func<StringBuilder, Func<StringBuilder, StringBuilder>, StringBuilder>, StringBuilder> writeValue)
            {
                Debug.Assert(!_isRest);
                return writeValue(queryBuilder, _type, FunctionHelper.Apply);
            }
        }
    }
}
