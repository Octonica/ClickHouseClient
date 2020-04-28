#region License Apache 2.0
/* Copyright 2019-2020 Octonica
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
using System.Diagnostics;
using System.Linq;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class TupleTypeInfo : IClickHouseColumnTypeInfo
    {
        private readonly List<IClickHouseColumnTypeInfo>? _elementTypes;

        public string ComplexTypeName { get; }

        public string TypeName => "Tuple";

        public int GenericArgumentsCount => _elementTypes?.Count ?? 0;

        public TupleTypeInfo()
        {
            ComplexTypeName = TypeName;
            _elementTypes = null;
        }

        private TupleTypeInfo(List<IClickHouseColumnTypeInfo> elementTypes)
        {
            ComplexTypeName = TypeName + "(" + string.Join(", ", elementTypes.Select(t => t.ComplexTypeName)) + ")";
            _elementTypes = elementTypes;
        }

        public IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            if (_elementTypes == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            return new TupleColumnReader(rowCount, _elementTypes);
        }

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (_elementTypes == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            return TupleColumnWriter.CreateColumnWriter(columnName, ComplexTypeName, _elementTypes, rows, columnSettings);
        }

        public IClickHouseColumnTypeInfo GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            if (_elementTypes != null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, "The type is already fully specified.");

            var elementTypes = new List<IClickHouseColumnTypeInfo>(options.Count);
            elementTypes.AddRange(options.Select(typeInfoProvider.GetTypeInfo));

            return new TupleTypeInfo(elementTypes);
        }

        public Type GetFieldType()
        {
            if (_elementTypes == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            return MakeTupleType(_elementTypes.Select(elt => elt.GetFieldType()).ToArray());
        }

        public ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Tuple;
        }

        public IClickHouseTypeInfo GetGenericArgument(int index)
        {
            if (_elementTypes == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            return _elementTypes[index];
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
                    var restType = MakeTupleType(new ReadOnlyListSpan<Type>(genericArgs, 7));

                    var tuple8ArgsArr = new Type[8];
                    for (int i = 0; i < 7; i++)
                        tuple8ArgsArr[i] = genericArgs[i];

                    tuple8ArgsArr[7] = restType;
                    return tupleType.MakeGenericType(tuple8ArgsArr);
            }

            var genericArgsArr = genericArgs as Type[] ?? genericArgs.ToArray();
            return tupleType.MakeGenericType(genericArgsArr);
        }

        private sealed class TupleColumnReader : IClickHouseColumnReader
        {
            private readonly int _rowCount;
            private readonly List<IClickHouseColumnTypeInfo> _elementTypes;

            private readonly List<IClickHouseColumnReader> _readers;
            private int _position;

            public TupleColumnReader(int rowCount, List<IClickHouseColumnTypeInfo> elementTypes)
            {
                _rowCount = rowCount;
                _elementTypes = elementTypes;

                _readers = new List<IClickHouseColumnReader>(_elementTypes.Count) {_elementTypes[0].CreateColumnReader(_rowCount)};
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                var isLastColumn = _readers.Count == _elementTypes.Count;
                if (!isLastColumn && _position >= _rowCount)
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");

                var currentReader = _readers[^1];
                var result = new SequenceSize(0, 0);
                while (!isLastColumn)
                {
                    if (_position < _rowCount)
                    {
                        var elementsCount = _rowCount - _position;
                        var size = currentReader.ReadNext(sequence.Slice(result.Bytes));

                        result = new SequenceSize(result.Bytes + size.Bytes, result.Elements);
                        _position += size.Elements;
                        if (size.Elements < elementsCount)
                            return result;
                    }

                    _position = 0;
                    currentReader = _elementTypes[_readers.Count].CreateColumnReader(_rowCount);
                    _readers.Add(currentReader);
                    isLastColumn = _readers.Count == _elementTypes.Count;
                }

                var lastColumnSize = currentReader.ReadNext(sequence.Slice(result.Bytes));
                _position += lastColumnSize.Elements;

                return new SequenceSize(result.Bytes + lastColumnSize.Bytes, lastColumnSize.Elements);
            }

            public SequenceSize Skip(ReadOnlySequence<byte> sequence, int maxElementsCount, ref object? skipContext)
            {
                TupleSkipContext tupleSkipContext;
                if (skipContext != null)
                    tupleSkipContext = (TupleSkipContext) skipContext;
                else
                    skipContext = tupleSkipContext = new TupleSkipContext(maxElementsCount, _elementTypes);

                return tupleSkipContext.Skip(sequence, maxElementsCount);
            }

            public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
            {
                var columns = new List<IClickHouseTableColumn>(_elementTypes.Count);
                for (int i = 0; i < _elementTypes.Count; i++)
                {
                    var reader = i < _readers.Count ? _readers[i] : _elementTypes[i].CreateColumnReader(0);
                    columns.Add(reader.EndRead(settings));
                }

                return TupleTableColumnBase.MakeTupleColumn(columns[^1].RowCount, columns);
            }
        }

        private class TupleSkipContext
        {
            private readonly int _rowCount;
            private readonly List<IClickHouseColumnTypeInfo> _elementTypes;

            private readonly List<IClickHouseColumnReader> _readers;

            private int _position;
            private object? _currentReaderSkipContext;

            public TupleSkipContext(int rowCount, List<IClickHouseColumnTypeInfo> elementTypes)
            {
                _rowCount = rowCount;
                _elementTypes = elementTypes;

                _readers = new List<IClickHouseColumnReader>(elementTypes.Count) {_elementTypes[0].CreateColumnReader(0)};
            }

            public SequenceSize Skip(ReadOnlySequence<byte> sequence, int maxElementsCount)
            {
                Debug.Assert(maxElementsCount == _rowCount - _position);

                var result = new SequenceSize(0, 0);
                while (_readers.Count != _elementTypes.Count)
                {
                    var expectedCount = _rowCount - _position;
                    var actualCount = _readers[^1].Skip(sequence.Slice(result.Bytes), expectedCount, ref _currentReaderSkipContext);

                    result = result.AddBytes(actualCount.Bytes);
                    _position += actualCount.Elements;
                    if (_position < _rowCount)
                        return result;

                    _position = 0;
                    _currentReaderSkipContext = null;
                    _readers.Add(_elementTypes[_readers.Count].CreateColumnReader(0));
                }

                var lastColumnCount = _readers[^1].Skip(sequence.Slice(result.Bytes), maxElementsCount, ref _currentReaderSkipContext);
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

            private int _currentWriterIdx;
            private int _currentWriterPosition;

            private TupleColumnWriter(string columnName, string columnType, List<IClickHouseColumnWriter> columns, int rowCount)
            {
                _rowCount = rowCount;
                ColumnName = columnName;
                ColumnType = columnType;
                _columns = columns;
            }

            public SequenceSize WriteNext(Span<byte> writeTo)
            {
                var result = new SequenceSize(0, 0);

                while (_currentWriterIdx < _columns.Count - 1)
                {
                    var columnWriter = _columns[_currentWriterIdx];
                    if (_currentWriterPosition < _rowCount)
                    {
                        var expectedElementsCount = _rowCount - _currentWriterPosition;
                        var actualCount = columnWriter.WriteNext(writeTo.Slice(result.Bytes));

                        _currentWriterPosition += actualCount.Elements;
                        result = result.AddBytes(actualCount.Bytes);
                        if (actualCount.Elements < expectedElementsCount)
                            return result;
                    }

                    ++_currentWriterIdx;
                    _currentWriterPosition = 0;
                }

                var lastColumnCount = _columns[^1].WriteNext(writeTo.Slice(result.Bytes));
                _currentWriterPosition += lastColumnCount.Elements;

                return result.Add(lastColumnCount);
            }

            public static TupleColumnWriter CreateColumnWriter<T>(string columnName, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
            {
                var listItemType = typeof(T);
                Type? factoryType = null;
                if (listItemType.IsGenericType)
                {
                    var listItemTypeDef = listItemType.GetGenericTypeDefinition();
                    switch (elementTypes.Count)
                    {
                        case 1:
                            if (listItemTypeDef == typeof(Tuple<>))
                                factoryType = typeof(TupleColumnFactory<>);
                            else if (listItemTypeDef == typeof(ValueTuple<>))
                                factoryType = typeof(ValueTupleColumnFactory<>);
                            break;
                        case 2:
                            if (listItemTypeDef == typeof(Tuple<,>))
                                factoryType = typeof(TupleColumnFactory<,>);
                            else if (listItemTypeDef == typeof(ValueTuple<,>))
                                factoryType = typeof(ValueTupleColumnFactory<,>);
                            break;
                        case 3:
                            if (listItemTypeDef == typeof(Tuple<,,>))
                                factoryType = typeof(TupleColumnFactory<,,>);
                            else if (listItemTypeDef == typeof(ValueTuple<,,>))
                                factoryType = typeof(ValueTupleColumnFactory<,,>);
                            break;
                        case 4:
                            if (listItemTypeDef == typeof(Tuple<,,,>))
                                factoryType = typeof(TupleColumnFactory<,,,>);
                            else if (listItemTypeDef == typeof(ValueTuple<,,,>))
                                factoryType = typeof(ValueTupleColumnFactory<,,,>);
                            break;
                        case 5:
                            if (listItemTypeDef == typeof(Tuple<,,,,>))
                                factoryType = typeof(TupleColumnFactory<,,,,>);
                            else if (listItemTypeDef == typeof(ValueTuple<,,,,>))
                                factoryType = typeof(ValueTupleColumnFactory<,,,,>);
                            break;
                        case 6:
                            if (listItemTypeDef == typeof(Tuple<,,,,,>))
                                factoryType = typeof(TupleColumnFactory<,,,,,>);
                            else if (listItemTypeDef == typeof(ValueTuple<,,,,,>))
                                factoryType = typeof(ValueTupleColumnFactory<,,,,,>);
                            break;
                        case 7:
                            if (listItemTypeDef == typeof(Tuple<,,,,,,>))
                                factoryType = typeof(TupleColumnFactory<,,,,,,>);
                            else if (listItemTypeDef == typeof(ValueTuple<,,,,,,>))
                                factoryType = typeof(ValueTupleColumnFactory<,,,,,,>);
                            break;
                        default:
                            if (elementTypes.Count >= 8)
                            {
                                if (listItemTypeDef == typeof(Tuple<,,,,,,,>))
                                    factoryType = typeof(TupleColumnFactory<,,,,,,,>);
                                else if (listItemTypeDef == typeof(ValueTuple<,,,,,,,>))
                                    factoryType = typeof(ValueTupleColumnFactory<,,,,,,,>);
                            }

                            break;
                    }


                    if (factoryType != null)
                    {
                        var args = listItemType.GetGenericArguments();
                        factoryType = factoryType.MakeGenericType(args);
                    }
                }

                if (factoryType == null)
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{columnType}\".");

                try
                {
                    var factory = (IColumnFactory) Activator.CreateInstance(factoryType)!;
                    return factory.Create(columnName, columnType, elementTypes, rows, columnSettings);
                }
                catch (Exception ex)
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{columnType}\".", ex);
                }
            }

            private static MappedReadOnlyList<TIn, TOut> MapList<TIn, TOut>(IReadOnlyList<TIn> list, Func<TIn, TOut> map)
            {
                return new MappedReadOnlyList<TIn, TOut>(list, map);
            }

            private interface IColumnFactory
            {
                TupleColumnWriter Create(string columnName, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings);
            }

            private sealed class TupleColumnFactory<T1> : IColumnFactory
            {
                public TupleColumnWriter Create(string columnName, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    var rows = (IReadOnlyList<Tuple<T1>>) untypedRows;
                    var columns = new List<IClickHouseColumnWriter>(1)
                    {
                        elementTypes[0].CreateColumnWriter(columnName, MapList(rows, t => t.Item1), columnSettings)
                    };

                    return new TupleColumnWriter(columnName, columnType, columns, rows.Count);
                }
            }

            private sealed class TupleColumnFactory<T1, T2> : IColumnFactory
            {
                public TupleColumnWriter Create(string columnName, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    var rows = (IReadOnlyList<Tuple<T1, T2>>) untypedRows;
                    var columns = new List<IClickHouseColumnWriter>(2)
                    {
                        elementTypes[0].CreateColumnWriter(columnName, MapList(rows, t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, MapList(rows, t => t.Item2), columnSettings)
                    };

                    return new TupleColumnWriter(columnName, columnType, columns, rows.Count);
                }
            }

            private sealed class TupleColumnFactory<T1, T2, T3> : IColumnFactory
            {
                public TupleColumnWriter Create(string columnName, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    var rows = (IReadOnlyList<Tuple<T1, T2, T3>>) untypedRows;
                    var columns = new List<IClickHouseColumnWriter>(3)
                    {
                        elementTypes[0].CreateColumnWriter(columnName, MapList(rows, t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, MapList(rows, t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, MapList(rows, t => t.Item3), columnSettings)
                    };

                    return new TupleColumnWriter(columnName, columnType, columns, rows.Count);
                }
            }

            private sealed class TupleColumnFactory<T1, T2, T3, T4> : IColumnFactory
            {
                public TupleColumnWriter Create(string columnName, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    var rows = (IReadOnlyList<Tuple<T1, T2, T3, T4>>) untypedRows;
                    var columns = new List<IClickHouseColumnWriter>(4)
                    {
                        elementTypes[0].CreateColumnWriter(columnName, MapList(rows, t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, MapList(rows, t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, MapList(rows, t => t.Item3), columnSettings),
                        elementTypes[3].CreateColumnWriter(columnName, MapList(rows, t => t.Item4), columnSettings)
                    };

                    return new TupleColumnWriter(columnName, columnType, columns, rows.Count);
                }
            }

            private sealed class TupleColumnFactory<T1, T2, T3, T4, T5> : IColumnFactory
            {
                public TupleColumnWriter Create(string columnName, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    var rows = (IReadOnlyList<Tuple<T1, T2, T3, T4, T5>>) untypedRows;
                    var columns = new List<IClickHouseColumnWriter>(5)
                    {
                        elementTypes[0].CreateColumnWriter(columnName, MapList(rows, t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, MapList(rows, t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, MapList(rows, t => t.Item3), columnSettings),
                        elementTypes[3].CreateColumnWriter(columnName, MapList(rows, t => t.Item4), columnSettings),
                        elementTypes[4].CreateColumnWriter(columnName, MapList(rows, t => t.Item5), columnSettings)
                    };

                    return new TupleColumnWriter(columnName, columnType, columns, rows.Count);
                }
            }

            private sealed class TupleColumnFactory<T1, T2, T3, T4, T5, T6> : IColumnFactory
            {
                public TupleColumnWriter Create(string columnName, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    var rows = (IReadOnlyList<Tuple<T1, T2, T3, T4, T5, T6>>) untypedRows;
                    var columns = new List<IClickHouseColumnWriter>(6)
                    {
                        elementTypes[0].CreateColumnWriter(columnName, MapList(rows, t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, MapList(rows, t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, MapList(rows, t => t.Item3), columnSettings),
                        elementTypes[3].CreateColumnWriter(columnName, MapList(rows, t => t.Item4), columnSettings),
                        elementTypes[4].CreateColumnWriter(columnName, MapList(rows, t => t.Item5), columnSettings),
                        elementTypes[5].CreateColumnWriter(columnName, MapList(rows, t => t.Item6), columnSettings)
                    };
                    
                    return new TupleColumnWriter(columnName, columnType, columns, rows.Count);
                }
            }

            private sealed class TupleColumnFactory<T1, T2, T3, T4, T5, T6, T7> : IColumnFactory
            {
                public TupleColumnWriter Create(string columnName, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    var rows = (IReadOnlyList<Tuple<T1, T2, T3, T4, T5, T6, T7>>) untypedRows;
                    var columns = new List<IClickHouseColumnWriter>(7)
                    {
                        elementTypes[0].CreateColumnWriter(columnName, MapList(rows, t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, MapList(rows, t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, MapList(rows, t => t.Item3), columnSettings),
                        elementTypes[3].CreateColumnWriter(columnName, MapList(rows, t => t.Item4), columnSettings),
                        elementTypes[4].CreateColumnWriter(columnName, MapList(rows, t => t.Item5), columnSettings),
                        elementTypes[5].CreateColumnWriter(columnName, MapList(rows, t => t.Item6), columnSettings),
                        elementTypes[6].CreateColumnWriter(columnName, MapList(rows, t => t.Item7), columnSettings)
                    };

                    return new TupleColumnWriter(columnName, columnType, columns, rows.Count);
                }
            }

            private sealed class TupleColumnFactory<T1, T2, T3, T4, T5, T6, T7, TRest> : IColumnFactory
            {
                public TupleColumnWriter Create(string columnName, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
#pragma warning disable CS8714 // The type cannot be used as type parameter in the generic type or method. Nullability of type argument doesn't match 'notnull' constraint.
                    var rows = (IReadOnlyList<Tuple<T1, T2, T3, T4, T5, T6, T7, TRest>>) untypedRows;
#pragma warning restore CS8714

                    var subColumns = new ReadOnlyListSpan<IClickHouseColumnTypeInfo>(elementTypes, 7);
                    var subType = "Tuple(" + string.Join(", ", subColumns.Select(c => c.ComplexTypeName)) + ")";
                    var lastColumn = CreateColumnWriter(columnName, subType, subColumns, MapList(rows, t => t.Rest), columnSettings);

                    var columns = new List<IClickHouseColumnWriter>(7 + lastColumn._columns.Count)
                    {
                        elementTypes[0].CreateColumnWriter(columnName, MapList(rows, t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, MapList(rows, t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, MapList(rows, t => t.Item3), columnSettings),
                        elementTypes[3].CreateColumnWriter(columnName, MapList(rows, t => t.Item4), columnSettings),
                        elementTypes[4].CreateColumnWriter(columnName, MapList(rows, t => t.Item5), columnSettings),
                        elementTypes[5].CreateColumnWriter(columnName, MapList(rows, t => t.Item6), columnSettings),
                        elementTypes[6].CreateColumnWriter(columnName, MapList(rows, t => t.Item7), columnSettings)
                    };
                    columns.AddRange(lastColumn._columns);

                    return new TupleColumnWriter(columnName, columnType, columns, rows.Count);
                }
            }

            private sealed class ValueTupleColumnFactory<T1> : IColumnFactory
            {
                public TupleColumnWriter Create(string columnName, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    var rows = (IReadOnlyList<ValueTuple<T1>>)untypedRows;
                    var columns = new List<IClickHouseColumnWriter>(1)
                    {
                        elementTypes[0].CreateColumnWriter(columnName, MapList(rows, t => t.Item1), columnSettings)
                    };

                    return new TupleColumnWriter(columnName, columnType, columns, rows.Count);
                }
            }

            private sealed class ValueTupleColumnFactory<T1, T2> : IColumnFactory
            {
                public TupleColumnWriter Create(string columnName, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    var rows = (IReadOnlyList<ValueTuple<T1, T2>>)untypedRows;
                    var columns = new List<IClickHouseColumnWriter>(2)
                    {
                        elementTypes[0].CreateColumnWriter(columnName, MapList(rows, t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, MapList(rows, t => t.Item2), columnSettings)
                    };

                    return new TupleColumnWriter(columnName, columnType, columns, rows.Count);
                }
            }

            private sealed class ValueTupleColumnFactory<T1, T2, T3> : IColumnFactory
            {
                public TupleColumnWriter Create(string columnName, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    var rows = (IReadOnlyList<ValueTuple<T1, T2, T3>>)untypedRows;
                    var columns = new List<IClickHouseColumnWriter>(3)
                    {
                        elementTypes[0].CreateColumnWriter(columnName, MapList(rows, t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, MapList(rows, t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, MapList(rows, t => t.Item3), columnSettings)
                    };

                    return new TupleColumnWriter(columnName, columnType, columns, rows.Count);
                }
            }

            private sealed class ValueTupleColumnFactory<T1, T2, T3, T4> : IColumnFactory
            {
                public TupleColumnWriter Create(string columnName, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    var rows = (IReadOnlyList<ValueTuple<T1, T2, T3, T4>>)untypedRows;
                    var columns = new List<IClickHouseColumnWriter>(4)
                    {
                        elementTypes[0].CreateColumnWriter(columnName, MapList(rows, t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, MapList(rows, t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, MapList(rows, t => t.Item3), columnSettings),
                        elementTypes[3].CreateColumnWriter(columnName, MapList(rows, t => t.Item4), columnSettings)
                    };

                    return new TupleColumnWriter(columnName, columnType, columns, rows.Count);
                }
            }

            private sealed class ValueTupleColumnFactory<T1, T2, T3, T4, T5> : IColumnFactory
            {
                public TupleColumnWriter Create(string columnName, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    var rows = (IReadOnlyList<ValueTuple<T1, T2, T3, T4, T5>>)untypedRows;
                    var columns = new List<IClickHouseColumnWriter>(5)
                    {
                        elementTypes[0].CreateColumnWriter(columnName, MapList(rows, t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, MapList(rows, t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, MapList(rows, t => t.Item3), columnSettings),
                        elementTypes[3].CreateColumnWriter(columnName, MapList(rows, t => t.Item4), columnSettings),
                        elementTypes[4].CreateColumnWriter(columnName, MapList(rows, t => t.Item5), columnSettings)
                    };

                    return new TupleColumnWriter(columnName, columnType, columns, rows.Count);
                }
            }

            private sealed class ValueTupleColumnFactory<T1, T2, T3, T4, T5, T6> : IColumnFactory
            {
                public TupleColumnWriter Create(string columnName, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    var rows = (IReadOnlyList<ValueTuple<T1, T2, T3, T4, T5, T6>>)untypedRows;
                    var columns = new List<IClickHouseColumnWriter>(6)
                    {
                        elementTypes[0].CreateColumnWriter(columnName, MapList(rows, t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, MapList(rows, t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, MapList(rows, t => t.Item3), columnSettings),
                        elementTypes[3].CreateColumnWriter(columnName, MapList(rows, t => t.Item4), columnSettings),
                        elementTypes[4].CreateColumnWriter(columnName, MapList(rows, t => t.Item5), columnSettings),
                        elementTypes[5].CreateColumnWriter(columnName, MapList(rows, t => t.Item6), columnSettings)
                    };

                    return new TupleColumnWriter(columnName, columnType, columns, rows.Count);
                }
            }

            private sealed class ValueTupleColumnFactory<T1, T2, T3, T4, T5, T6, T7> : IColumnFactory
            {
                public TupleColumnWriter Create(string columnName, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    var rows = (IReadOnlyList<ValueTuple<T1, T2, T3, T4, T5, T6, T7>>)untypedRows;
                    var columns = new List<IClickHouseColumnWriter>(7)
                    {
                        elementTypes[0].CreateColumnWriter(columnName, MapList(rows, t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, MapList(rows, t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, MapList(rows, t => t.Item3), columnSettings),
                        elementTypes[3].CreateColumnWriter(columnName, MapList(rows, t => t.Item4), columnSettings),
                        elementTypes[4].CreateColumnWriter(columnName, MapList(rows, t => t.Item5), columnSettings),
                        elementTypes[5].CreateColumnWriter(columnName, MapList(rows, t => t.Item6), columnSettings),
                        elementTypes[6].CreateColumnWriter(columnName, MapList(rows, t => t.Item7), columnSettings)
                    };

                    return new TupleColumnWriter(columnName, columnType, columns, rows.Count);
                }
            }

            private sealed class ValueTupleColumnFactory<T1, T2, T3, T4, T5, T6, T7, TRest> : IColumnFactory
                where TRest : struct
            {
                public TupleColumnWriter Create(string columnName, string columnType, IReadOnlyList<IClickHouseColumnTypeInfo> elementTypes, object untypedRows, ClickHouseColumnSettings? columnSettings)
                {
                    var rows = (IReadOnlyList<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>>) untypedRows;

                    var subColumns = new ReadOnlyListSpan<IClickHouseColumnTypeInfo>(elementTypes, 7);
                    var subType = "Tuple(" + string.Join(", ", subColumns.Select(c => c.ComplexTypeName)) + ")";
                    var lastColumn = CreateColumnWriter(columnName, subType, subColumns, MapList(rows, t => t.Rest), columnSettings);

                    var columns = new List<IClickHouseColumnWriter>(7 + lastColumn._columns.Count)
                    {
                        elementTypes[0].CreateColumnWriter(columnName, MapList(rows, t => t.Item1), columnSettings),
                        elementTypes[1].CreateColumnWriter(columnName, MapList(rows, t => t.Item2), columnSettings),
                        elementTypes[2].CreateColumnWriter(columnName, MapList(rows, t => t.Item3), columnSettings),
                        elementTypes[3].CreateColumnWriter(columnName, MapList(rows, t => t.Item4), columnSettings),
                        elementTypes[4].CreateColumnWriter(columnName, MapList(rows, t => t.Item5), columnSettings),
                        elementTypes[5].CreateColumnWriter(columnName, MapList(rows, t => t.Item6), columnSettings),
                        elementTypes[6].CreateColumnWriter(columnName, MapList(rows, t => t.Item7), columnSettings)
                    };
                    columns.AddRange(lastColumn._columns);

                    return new TupleColumnWriter(columnName, columnType, columns, rows.Count);
                }
            }
        }
    }
}
