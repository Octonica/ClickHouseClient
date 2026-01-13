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
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class ArrayTypeInfo : IClickHouseColumnTypeInfo
    {
        private readonly IClickHouseColumnTypeInfo? _elementTypeInfo;

        public string ComplexTypeName { get; }

        public string TypeName { get; } = "Array";

        public int GenericArgumentsCount => _elementTypeInfo == null ? 0 : 1;

        public ArrayTypeInfo()
        {
            ComplexTypeName = TypeName;
        }

        private ArrayTypeInfo(IClickHouseColumnTypeInfo elementTypeInfo)
        {
            _elementTypeInfo = elementTypeInfo ?? throw new ArgumentNullException(nameof(elementTypeInfo));
            ComplexTypeName = $"{TypeName}({_elementTypeInfo.ComplexTypeName})";
        }

        public IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return _elementTypeInfo == null
                ? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.")
                : (IClickHouseColumnReader)new ArrayColumnReader(rowCount, _elementTypeInfo);
        }

        IClickHouseColumnReader IClickHouseColumnTypeInfo.CreateColumnReader(int rowCount, ClickHouseColumnSerializationMode serializationMode)
        {
            return serializationMode != ClickHouseColumnSerializationMode.Default
                ? throw new NotSupportedException($"Custom serialization for {ComplexTypeName} is not supported by ClickHouseClient.")
                : CreateColumnReader(rowCount);
        }

        public IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            return _elementTypeInfo == null
                ? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.")
                : (IClickHouseColumnReaderBase)new ArraySkippingColumnReader(rowCount, _elementTypeInfo);
        }

        IClickHouseColumnReaderBase IClickHouseColumnTypeInfo.CreateSkippingColumnReader(int rowCount, ClickHouseColumnSerializationMode serializationMode)
        {
            return serializationMode != ClickHouseColumnSerializationMode.Default
                ? throw new NotSupportedException($"Custom serialization for {ComplexTypeName} is not supported by ClickHouseClient.")
                : CreateSkippingColumnReader(rowCount);
        }

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (_elementTypeInfo == null)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");
            }

            Type rowType = typeof(T);
            Type? elementType = null;
            if (rowType.IsArray)
            {
                int rank = rowType.GetArrayRank();
                if (rank > 1)
                {
                    elementType = rowType.GetElementType()!;
                    Debug.Assert(elementType != null);

                    (Func<Array, object> createList, Type listElementType) = MultiDimensionalArrayReadOnlyListAdapter.Dispatch(elementType, rowType.GetArrayRank());
                    MultiDimensionalArrayColumnWriterDispatcher mdaDispatcher = new(
                        columnName,
                        (IReadOnlyList<Array>)rows,
                        columnSettings,
                        _elementTypeInfo,
                        createList);

                    return TypeDispatcher.Dispatch(listElementType, mdaDispatcher);
                }
            }

            foreach (Type? genericItf in rowType.GetInterfaces().Where(itf => itf.IsGenericType))
            {
                if (genericItf.GetGenericTypeDefinition() != typeof(IReadOnlyList<>))
                {
                    continue;
                }

                if (elementType == null)
                {
                    elementType = genericItf.GetGenericArguments()[0];
                }
                else
                {
                    Type elementTypeCandidate = genericItf.GetGenericArguments()[0];

                    if (elementType.IsAssignableFrom(elementTypeCandidate))
                    {
                        elementType = elementTypeCandidate;
                    }
                    else if (!elementTypeCandidate.IsAssignableFrom(elementType))
                    {
                        throw new ClickHouseException(
                            ClickHouseErrorCodes.TypeNotSupported,
                            $"Can't detect a type of the array's element. Candidates are: \"{elementType}\" and \"{elementTypeCandidate}\".");
                    }
                }
            }

            ArrayColumnWriterDispatcherBase dispatcher;
            if (elementType == null)
            {
                Type rowGenericTypeDef = rowType.GetGenericTypeDefinition();
                if (rowGenericTypeDef == typeof(ReadOnlyMemory<>))
                {
                    elementType = rowType.GetGenericArguments()[0]!;
                    dispatcher = new ReadOnlyColumnWriterDispatcher(columnName, rows, columnSettings, _elementTypeInfo);
                }
                else if (rowGenericTypeDef == typeof(Memory<>))
                {
                    elementType = rowType.GetGenericArguments()[0]!;
                    dispatcher = new MemoryColumnWriterDispatcher(columnName, rows, columnSettings, _elementTypeInfo);
                }
                else
                {
                    throw new ClickHouseException(
                        ClickHouseErrorCodes.TypeNotSupported,
                        $"Can't detect a type of the array's element. The type \"{typeof(T)}\" doesn't implement \"{typeof(IReadOnlyList<>)}\".");
                }
            }
            else
            {
                dispatcher = new ArrayColumnWriterDispatcher(columnName, rows, columnSettings, _elementTypeInfo);
            }

            try
            {
                return TypeDispatcher.Dispatch(elementType, dispatcher);
            }
            catch (ClickHouseException ex) when (ex.ErrorCode == ClickHouseErrorCodes.TypeNotSupported)
            {
                throw new ClickHouseException(ex.ErrorCode, $"The type \"{rowType}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\". See the inner exception for details.", ex);
            }
        }

        public IClickHouseColumnTypeInfo GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            if (options.Count > 1)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"Too many arguments in the definition of \"{TypeName}\".");
            }

            IClickHouseColumnTypeInfo elementTypeInfo = typeInfoProvider.GetTypeInfo(options[0]);
            return new ArrayTypeInfo(elementTypeInfo);
        }

        public Type GetFieldType()
        {
            return _elementTypeInfo == null ? typeof(object[]) : _elementTypeInfo.GetFieldType().MakeArrayType();
        }

        public ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Array;
        }

        public IClickHouseTypeInfo GetGenericArgument(int index)
        {
            return _elementTypeInfo == null
                ? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.")
                : index != 0 ? throw new IndexOutOfRangeException() : (IClickHouseTypeInfo)_elementTypeInfo;
        }

        public IClickHouseParameterWriter<T> CreateParameterWriter<T>()
        {
            if (_elementTypeInfo == null)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");
            }

            Type type = typeof(T);
            if (type == typeof(DBNull))
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{ComplexTypeName}\" does not allow null values.");
            }

            if (type == typeof(string))
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
            }

            Type? elementType = null;
            foreach (Type itf in type.GetInterfaces())
            {
                if (!itf.IsGenericType)
                {
                    continue;
                }

                Type typeDef = itf.GetGenericTypeDefinition();
                if (typeDef == typeof(ICollection<>) || typeDef == typeof(IReadOnlyCollection<>))
                {
                    Type genericArg = itf.GetGenericArguments()[0];
                    if (elementType == null || elementType.IsAssignableFrom(genericArg))
                    {
                        elementType = genericArg;
                    }
                    else if (!genericArg.IsAssignableFrom(elementType))
                    {
                        throw new ClickHouseException(
                            ClickHouseErrorCodes.TypeNotSupported,
                            $"Can't detect a type of the array's element. Candidates are: \"{elementType}\" and \"{genericArg}\".");
                    }
                }
            }

            ArrayParameterWriterDispatcher<T> dispatcher;
            if (elementType == null)
            {
                int arrayRank;
                if (!type.IsArray || (arrayRank = type.GetArrayRank()) == 1)
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
                }

                IClickHouseColumnTypeInfo? elementTypeInfo = _elementTypeInfo;
                for (int i = 1; i < arrayRank; i++)
                {
                    if (elementTypeInfo is NullableTypeInfo nti)
                    {
                        elementTypeInfo = nti.UnderlyingType;
                        --i;
                    }
                    else
                    {
                        elementTypeInfo = elementTypeInfo is ArrayTypeInfo ati
                            ? ati._elementTypeInfo
                            : throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The multidimensional array value can not be converted type \"{ComplexTypeName}\": dimension number mismatch.");
                    }

                    if (elementTypeInfo == null)
                    {
                        throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");
                    }
                }

                elementType = type.GetElementType();
                Debug.Assert(elementType != null);
                dispatcher = new ArrayParameterWriterDispatcher<T>(this, elementTypeInfo, true);
            }
            else
            {
                dispatcher = new ArrayParameterWriterDispatcher<T>(this, _elementTypeInfo, false);
            }

            IClickHouseParameterWriter<T> writer = TypeDispatcher.Dispatch(elementType, dispatcher);
            return writer;
        }

        private abstract class ArrayColumnReaderBase<TElementColumnReader> : IClickHouseColumnReaderBase
            where TElementColumnReader : class, IClickHouseColumnReaderBase
        {
            private readonly int _rowCount;

            // A part of the inner column's header exposed outside of the beginning of the current column
            private byte[]? _prefix;

            private int _elementPosition;

            protected IClickHouseColumnTypeInfo ElementType { get; }

            protected List<(int offset, int length)> Ranges { get; }

            protected TElementColumnReader? ElementColumnReader { get; private set; }

            protected int Position { get; private set; }

            public ArrayColumnReaderBase(int rowCount, IClickHouseColumnTypeInfo elementType)
            {
                _rowCount = rowCount;
                ElementType = elementType;
                Ranges = new List<(int offset, int length)>(_rowCount);
            }

            SequenceSize IClickHouseColumnReaderBase.ReadPrefix(ReadOnlySequence<byte> sequence)
            {
                // We can't read the prefix right now, so we keep it in the buffer until we get the real reader
                IClickHouseColumnReaderBase skippingReader = ElementType.CreateSkippingColumnReader(0);
                ReadOnlySequence<byte> updSeq = sequence;
                if (_prefix != null)
                {
                    SimpleReadOnlySequenceSegment<byte> segment = new(_prefix, updSeq);
                    updSeq = new ReadOnlySequence<byte>(segment, 0, segment.LastSegment, segment.LastSegment.Memory.Length);
                }

                SequenceSize result = skippingReader.ReadPrefix(updSeq);
                int bufferSize = result.Bytes;
                if (_prefix != null)
                {
                    result = result.AddBytes(-_prefix.Length);
                }

                if (result.Bytes < 0)
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Byte offset calculation error. The length of the prefix is negative.");
                }

                if (result.Bytes == 0)
                {
                    return result;
                }

                Array.Resize(ref _prefix, bufferSize);
                updSeq.Slice(0, bufferSize).CopyTo(_prefix);
                return result;
            }

            SequenceSize IClickHouseColumnReaderBase.ReadNext(ReadOnlySequence<byte> sequence)
            {
                if (Position >= _rowCount)
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");
                }

                int bytesCount = 0;
                ReadOnlySequence<byte> slice = sequence;
                if (ElementColumnReader == null)
                {
                    ulong totalLength = Ranges.Aggregate((ulong)0, (acc, r) => acc + (ulong)r.length);

                    Span<byte> sizeSpan = stackalloc byte[sizeof(ulong)];
                    for (int i = Ranges.Count; i < _rowCount; i++)
                    {
                        if (slice.Length < sizeSpan.Length)
                        {
                            return new SequenceSize(bytesCount, 0);
                        }

                        ulong length;
                        if (slice.FirstSpan.Length >= sizeSpan.Length)
                        {
                            length = BitConverter.ToUInt64(slice.FirstSpan);
                        }
                        else
                        {
                            slice.Slice(0, sizeSpan.Length).CopyTo(sizeSpan);
                            length = BitConverter.ToUInt64(sizeSpan);
                        }

                        slice = slice.Slice(sizeSpan.Length);
                        bytesCount += sizeSpan.Length;

                        int offset = checked((int)totalLength);
                        int rangeLength = checked((int)(length - totalLength));
                        Ranges.Add((offset, rangeLength));

                        totalLength = length;
                    }

                    ElementColumnReader = CreateElementColumnReader(checked((int)totalLength));

                    if (totalLength == 0)
                    {
                        // Special case for an empty array
                        SequenceSize result = new(bytesCount, _rowCount - Position);
                        Position = _rowCount;
                        return result;
                    }
                }

                if (_prefix != null)
                {
                    SequenceSize prefixSize = ElementColumnReader.ReadPrefix(new ReadOnlySequence<byte>(_prefix));
                    if (prefixSize.Bytes == 0 && prefixSize.Elements == 0)
                    {
                        throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Failed to read the column prefix.");
                    }

                    if (prefixSize.Bytes != _prefix.Length)
                    {
                        throw new ClickHouseException(ClickHouseErrorCodes.InternalError, $"Internal error. The column prefix' size is {_prefix.Length}, but the number of consumed bytes is {prefixSize.Bytes}.");
                    }

                    if (prefixSize.Elements != 1)
                    {
                        throw new ClickHouseException(ClickHouseErrorCodes.InternalError, $"Internal error. Received an unexpected number of column prefixes: {prefixSize.Elements}.");
                    }

                    _prefix = null;
                }

                SequenceSize elementsSize = ElementColumnReader.ReadNext(slice);
                _elementPosition += elementsSize.Elements;
                int elementsCount = 0;
                while (Position < _rowCount)
                {
                    (int offset, int length) = Ranges[Position];
                    if (length + offset > _elementPosition)
                    {
                        break;
                    }

                    ++elementsCount;
                    ++Position;
                }

                return new SequenceSize(bytesCount + elementsSize.Bytes, elementsCount);
            }

            protected abstract TElementColumnReader CreateElementColumnReader(int totalLength);
        }

        private sealed class ArrayColumnReader : ArrayColumnReaderBase<IClickHouseColumnReader>, IClickHouseColumnReader
        {
            public ArrayColumnReader(int rowCount, IClickHouseColumnTypeInfo elementType)
                : base(rowCount, elementType)
            {
            }

            protected override IClickHouseColumnReader CreateElementColumnReader(int totalLength)
            {
                return ElementType.CreateColumnReader(totalLength);
            }

            public IClickHouseTableColumn? EndRead(ClickHouseColumnSettings? settings)
            {
                IClickHouseColumnReader elementColumnReader = ElementColumnReader ?? ElementType.CreateColumnReader(0);
                IClickHouseTableColumn column = elementColumnReader.EndRead(settings)!;
                List<(int offset, int length)> ranges = Position == Ranges.Count ? Ranges : Ranges.Take(Position).ToList();

                if (!column.TryDipatch(new ArrayTableColumnDipatcher(ranges), out IClickHouseTableColumn? result))
                {
                    result = new ArrayTableColumn(column, ranges);
                }

                return result!;
            }
        }

        private sealed class ArraySkippingColumnReader : ArrayColumnReaderBase<IClickHouseColumnReaderBase>
        {
            public ArraySkippingColumnReader(int rowCount, IClickHouseColumnTypeInfo elementType)
                : base(rowCount, elementType)
            {
            }

            protected override IClickHouseColumnReaderBase CreateElementColumnReader(int totalLength)
            {
                return ElementType.CreateSkippingColumnReader(totalLength);
            }
        }

        private sealed class ArrayColumnWriterDispatcher : ArrayColumnWriterDispatcherBase
        {
            public ArrayColumnWriterDispatcher(string columnName, object rows, ClickHouseColumnSettings? columnSettings, IClickHouseColumnTypeInfo elementTypeInfo)
                : base(columnName, rows, columnSettings, elementTypeInfo)
            {
            }

            protected override ArrayLinearizedList<T> ToList<T>(object rows)
            {
                return new ArrayLinearizedList<T>(new ReadOnlyCollectionList<T>((IReadOnlyList<IReadOnlyList<T>?>)rows));
            }
        }

        private sealed class MemoryColumnWriterDispatcher : ArrayColumnWriterDispatcherBase
        {
            public MemoryColumnWriterDispatcher(string columnName, object rows, ClickHouseColumnSettings? columnSettings, IClickHouseColumnTypeInfo elementTypeInfo)
                : base(columnName, rows, columnSettings, elementTypeInfo)
            {
            }

            protected override ArrayLinearizedList<T> ToList<T>(object rows)
            {
                return new ArrayLinearizedList<T>(new MemoryCollectionList<T>((IReadOnlyList<Memory<T>>)rows));
            }
        }

        private sealed class ReadOnlyColumnWriterDispatcher : ArrayColumnWriterDispatcherBase
        {
            public ReadOnlyColumnWriterDispatcher(string columnName, object rows, ClickHouseColumnSettings? columnSettings, IClickHouseColumnTypeInfo elementTypeInfo)
                : base(columnName, rows, columnSettings, elementTypeInfo)
            {
            }

            protected override ArrayLinearizedList<T> ToList<T>(object rows)
            {
                return new ArrayLinearizedList<T>(new ReadOnlyMemoryCollectionList<T>((IReadOnlyList<ReadOnlyMemory<T>>)rows));
            }
        }

        private abstract class ArrayColumnWriterDispatcherBase : ITypeDispatcher<IClickHouseColumnWriter>
        {
            private readonly string _columnName;
            private readonly object _rows;
            private readonly ClickHouseColumnSettings? _columnSettings;
            private readonly IClickHouseColumnTypeInfo _elementTypeInfo;

            protected ArrayColumnWriterDispatcherBase(string columnName, object rows, ClickHouseColumnSettings? columnSettings, IClickHouseColumnTypeInfo elementTypeInfo)
            {
                _columnName = columnName;
                _rows = rows;
                _columnSettings = columnSettings;
                _elementTypeInfo = elementTypeInfo;
            }

            public IClickHouseColumnWriter Dispatch<T>()
            {
                ArrayLinearizedList<T> linearizedList = ToList<T>(_rows);
                IClickHouseColumnWriter elementColumnWriter = _elementTypeInfo.CreateColumnWriter(_columnName, linearizedList, _columnSettings);
                string columnType = $"Array({elementColumnWriter.ColumnType})";
                return new ArrayColumnWriter<T>(columnType, linearizedList, elementColumnWriter);
            }

            protected abstract ArrayLinearizedList<T> ToList<T>(object rows);
        }

        private sealed class MultiDimensionalArrayColumnWriterDispatcher : ITypeDispatcher<IClickHouseColumnWriter>
        {
            private readonly string _columnName;
            private readonly IReadOnlyList<Array> _rows;
            private readonly ClickHouseColumnSettings? _columnSettings;
            private readonly IClickHouseColumnTypeInfo _elementTypeInfo;
            private readonly Func<Array, object> _dispatchArray;

            public MultiDimensionalArrayColumnWriterDispatcher(
                string columnName,
                IReadOnlyList<Array> rows,
                ClickHouseColumnSettings? columnSettings,
                IClickHouseColumnTypeInfo elementTypeInfo,
                Func<Array, object> dispatchArray)
            {
                _columnName = columnName;
                _rows = rows;
                _columnSettings = columnSettings;
                _elementTypeInfo = elementTypeInfo;
                _dispatchArray = dispatchArray;
            }

            public IClickHouseColumnWriter Dispatch<T>()
            {
                IReadOnlyListExt<IReadOnlyList<T>> mappedRows = MappedReadOnlyList<Array, IReadOnlyList<T>>.Map(_rows, arr => (IReadOnlyList<T>)_dispatchArray(arr));
                ArrayLinearizedList<T> linearizedList = new(new ReadOnlyCollectionList<T>(mappedRows));
                IClickHouseColumnWriter elementColumnWriter = _elementTypeInfo.CreateColumnWriter(_columnName, linearizedList, _columnSettings);
                string columnType = $"Array({elementColumnWriter.ColumnType})";
                return new ArrayColumnWriter<T>(columnType, linearizedList, elementColumnWriter);
            }
        }

        private sealed class ArrayColumnWriter<T> : IClickHouseColumnWriter
        {
            private readonly ArrayLinearizedList<T> _rows;
            private readonly IClickHouseColumnWriter _elementColumnWriter;

            public string ColumnName { get; }

            public string ColumnType { get; }

            private int _headerPosition;
            private int _position;
            private int _elementPosition;

            public ArrayColumnWriter(string columnType, ArrayLinearizedList<T> rows, IClickHouseColumnWriter elementColumnWriter)
            {
                _rows = rows;
                _elementColumnWriter = elementColumnWriter;
                ColumnName = elementColumnWriter.ColumnName;
                ColumnType = columnType;
            }

            SequenceSize IClickHouseColumnWriter.WritePrefix(Span<byte> writeTo)
            {
                return _elementColumnWriter.WritePrefix(writeTo);
            }

            public SequenceSize WriteNext(Span<byte> writeTo)
            {
                int bytesCount = 0;
                Span<byte> span = writeTo;
                for (; _headerPosition < _rows.ListLengths.Count; _headerPosition++)
                {
                    if (!BitConverter.TryWriteBytes(span, (ulong)_rows.ListLengths[_headerPosition]))
                    {
                        return new SequenceSize(bytesCount, 0);
                    }

                    span = span[sizeof(ulong)..];
                    bytesCount += sizeof(ulong);
                }

                if (_rows.Count == 0)
                {
                    // There are no actual values, only an empty array or a bunch of empty arrays
                    Debug.Assert(_position == 0);
                    _position = _rows.ListLengths.Count;
                    return new SequenceSize(bytesCount, _position);
                }

                SequenceSize elementSize = _elementColumnWriter.WriteNext(span);
                _elementPosition += elementSize.Elements;

                int elementsCount = 0;
                while (_position < _rows.ListLengths.Count)
                {
                    if (_rows.ListLengths[_position] > _elementPosition)
                    {
                        break;
                    }

                    ++elementsCount;
                    ++_position;
                }

                return new SequenceSize(elementSize.Bytes + bytesCount, elementsCount);
            }
        }

        private sealed class ArrayLinearizedList<T> : IReadOnlyList<T>
        {
            private readonly ICollectionList<T> _listOfLists;

            public List<int> ListLengths { get; }

            public int Count { get; }

            public ArrayLinearizedList(ICollectionList<T> listOfLists)
            {
                _listOfLists = listOfLists;
                ListLengths = new List<int>(_listOfLists.Count);

                int offset = 0;
                foreach (int listLength in _listOfLists.GetListLengths())
                {
                    offset += listLength;
                    ListLengths.Add(offset);
                }

                Count = offset;
            }

            public IEnumerator<T> GetEnumerator()
            {
                return _listOfLists.GetItems().GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public T this[int index]
            {
                get
                {
                    if (index < 0 || index >= Count)
                    {
                        throw new IndexOutOfRangeException();
                    }

                    int listIndex = ListLengths.BinarySearch(index);
                    int elementIndex;
                    if (listIndex < 0)
                    {
                        listIndex = ~listIndex;
                        elementIndex = listIndex == 0 ? index : index - ListLengths[listIndex - 1];
                    }
                    else
                    {
                        elementIndex = 0;
                        listIndex++;
                    }

                    for (; listIndex < _listOfLists.Count; listIndex++)
                    {
                        if (elementIndex < 0)
                        {
                            break;
                        }

                        int listLength = _listOfLists.GetLength(listIndex);
                        if (elementIndex < listLength)
                        {
                            return _listOfLists[listIndex, elementIndex];
                        }

                        elementIndex = index - ListLengths[listIndex];
                    }

                    throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error: data structure is corrupted.");
                }
            }
        }

        private sealed class ArrayTableColumnDipatcher : IClickHouseTableColumnDispatcher<IClickHouseTableColumn>
        {
            private readonly List<(int offset, int length)> _ranges;

            public ArrayTableColumnDipatcher(List<(int offset, int length)> ranges)
            {
                _ranges = ranges;
            }

            public IClickHouseTableColumn Dispatch<T>(IClickHouseTableColumn<T> column)
            {
                return new ArrayTableColumn<T>(column, _ranges);
            }
        }

        private sealed class ArrayParameterWriterDispatcher<TArray> : ITypeDispatcher<IClickHouseParameterWriter<TArray>>
        {
            private readonly ArrayTypeInfo _arrayType;
            private readonly IClickHouseColumnTypeInfo _elementType;
            private readonly bool _isMultidimensional;

            public ArrayParameterWriterDispatcher(ArrayTypeInfo arrayType, IClickHouseColumnTypeInfo elementType, bool isMultidimensional)
            {
                _arrayType = arrayType;
                _elementType = elementType;
                _isMultidimensional = isMultidimensional;
            }

            public IClickHouseParameterWriter<TArray> Dispatch<T>()
            {
                IClickHouseParameterWriter<T> elementWriter = _elementType.CreateParameterWriter<T>();

                return _isMultidimensional
                    ? new MultidimensionalArralParameterWriter<TArray, T>(_arrayType, elementWriter)
                    : new ArrayParameterWriter<TArray, T>(_arrayType, elementWriter);
            }
        }

        private sealed class MultidimensionalArralParameterWriter<TArray, TElement> : IClickHouseParameterWriter<TArray>
        {
            private readonly ArrayTypeInfo _arrayType;
            private readonly IClickHouseParameterWriter<TElement> _elementWriter;

            public MultidimensionalArralParameterWriter(ArrayTypeInfo arrayType, IClickHouseParameterWriter<TElement> elementWriter)
            {
                _arrayType = arrayType;
                _elementWriter = elementWriter;
            }

            public bool TryCreateParameterValueWriter(TArray value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter)
            {
                if (value is null)
                {
                    throw new ArgumentNullException(nameof(value));
                }

                IEnumerator enumerator = ((IEnumerable)value).GetEnumerator();
                if (!TryCreateElementWriters((Array)(object)value, enumerator, 0, out List<IClickHouseParameterValueWriter>? elementWriters))
                {
                    valueWriter = null;
                    return false;
                }

                valueWriter = new ArrayLiteralValueWriter(elementWriters);
                return true;
            }

            private bool TryCreateElementWriters(Array array, IEnumerator enumerator, int dimension, [NotNullWhen(true)] out List<IClickHouseParameterValueWriter>? writers)
            {
                int rankLength = array.GetLength(dimension);
                writers = new List<IClickHouseParameterValueWriter>(rankLength);
                if (dimension == array.Rank - 1)
                {
                    for (int i = 0; i < rankLength; ++i)
                    {
                        if (!enumerator.MoveNext())
                        {
                            throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error: unexpected iterator out of bound.");
                        }

                        if (!_elementWriter.TryCreateParameterValueWriter((TElement)enumerator.Current!, true, out IClickHouseParameterValueWriter? elementWriter))
                        {
                            return false;
                        }

                        writers.Add(elementWriter);
                    }
                }
                else
                {
                    int nextDimension = dimension + 1;
                    for (int i = 0; i < rankLength; ++i)
                    {
                        if (!TryCreateElementWriters(array, enumerator, nextDimension, out List<IClickHouseParameterValueWriter>? elementWriters))
                        {
                            return false;
                        }

                        writers.Add(new ArrayLiteralValueWriter(elementWriters));
                    }
                }

                return true;
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, TArray value)
            {
                if (value is null)
                {
                    throw new ArgumentNullException(nameof(value));
                }

                IEnumerator enumerator = ((IEnumerable)value).GetEnumerator();
                return Interpolate(queryBuilder, (Array)(object)value, enumerator, 0);
            }

            private StringBuilder Interpolate(StringBuilder queryBuilder, Array array, IEnumerator enumerator, int dimension)
            {
                int rankLength = array.GetLength(dimension);
                _ = queryBuilder.Append('[');
                if (dimension == array.Rank - 1)
                {
                    for (int i = 0; i < rankLength; ++i)
                    {
                        if (!enumerator.MoveNext())
                        {
                            throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error: unexpected iterator out of bound.");
                        }

                        if (i > 0)
                        {
                            _ = queryBuilder.Append(',');
                        }

                        _ = _elementWriter.Interpolate(queryBuilder, (TElement)enumerator.Current!);
                    }
                }
                else
                {
                    int nextDimension = dimension + 1;
                    for (int i = 0; i < rankLength; ++i)
                    {
                        if (i > 0)
                        {
                            _ = queryBuilder.Append(',');
                        }

                        _ = Interpolate(queryBuilder, array, enumerator, nextDimension);
                    }
                }

                return queryBuilder.Append(']');
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseColumnTypeInfo, Func<StringBuilder, Func<StringBuilder, StringBuilder>, StringBuilder>, StringBuilder> writeValue)
            {
                return _elementWriter.Interpolate(queryBuilder, typeInfoProvider, (qb, typeInfo, writeElement) =>
                {
                    int rank = 1;
                    IClickHouseColumnTypeInfo? elementTypeInfo = _arrayType._elementTypeInfo;
                    while (elementTypeInfo is ArrayTypeInfo elementArray)
                    {
                        elementTypeInfo = elementArray._elementTypeInfo;
                        ++rank;
                    }

                    Debug.Assert(elementTypeInfo != null);
                    if (elementTypeInfo.ComplexTypeName == typeInfo.ComplexTypeName)
                    {
                        return writeValue(qb, _arrayType, FunctionHelper.Apply);
                    }

                    ArrayTypeInfo updArrayTypeInfo = new(typeInfo);
                    for (int i = rank - 1; i > 0; i--)
                    {
                        updArrayTypeInfo = new ArrayTypeInfo(updArrayTypeInfo);
                    }

                    return writeValue(qb, updArrayTypeInfo, (qb2, realWrite) =>
                    {
                        for (int i = 1; i <= rank; i++)
                        {
                            _ = qb2.AppendFormat(CultureInfo.InvariantCulture, "arrayMap(_elt{0} -> ", i);
                        }

                        _ = writeElement(qb2, b => b.AppendFormat(CultureInfo.InvariantCulture, "_elt{0}", rank));

                        for (int i = rank - 1; i > 0; i--)
                        {
                            _ = qb2.AppendFormat(CultureInfo.InvariantCulture, ", _elt{0})", i);
                        }

                        _ = qb2.Append(", ");
                        _ = realWrite(qb2);
                        _ = qb2.Append(')');
                        return qb2;
                    });
                });
            }
        }

        private sealed class ArrayParameterWriter<TArray, TElement> : IClickHouseParameterWriter<TArray>
        {
            private readonly ArrayTypeInfo _type;
            private readonly IClickHouseParameterWriter<TElement> _elementWriter;

            public ArrayParameterWriter(ArrayTypeInfo type, IClickHouseParameterWriter<TElement> elementWriter)
            {
                _type = type;
                _elementWriter = elementWriter;
            }

            public bool TryCreateParameterValueWriter(TArray value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter)
            {
                if (value is null)
                {
                    throw new ArgumentNullException(nameof(value));
                }

                List<IClickHouseParameterValueWriter> elementWriters = new();
                foreach (TElement? element in (IEnumerable<TElement>)value)
                {
                    if (!_elementWriter.TryCreateParameterValueWriter(element, true, out IClickHouseParameterValueWriter? elementWriter))
                    {
                        valueWriter = null;
                        return false;
                    }

                    elementWriters.Add(elementWriter);
                }

                valueWriter = new ArrayLiteralValueWriter(elementWriters);
                return true;
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, TArray value)
            {
                if (value is null)
                {
                    throw new ArgumentNullException(nameof(value));
                }

                IClickHouseColumnTypeInfo? elementTypeInfo = _type._elementTypeInfo;
                Debug.Assert(elementTypeInfo != null);

                _ = queryBuilder.Append('[');

                bool isFirst = true;
                foreach (TElement? element in (IEnumerable<TElement>)value)
                {
                    if (isFirst)
                    {
                        isFirst = false;
                    }
                    else
                    {
                        _ = queryBuilder.Append(',');
                    }

                    _ = _elementWriter.Interpolate(queryBuilder, element);
                }

                return queryBuilder.Append(']');
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseColumnTypeInfo, Func<StringBuilder, Func<StringBuilder, StringBuilder>, StringBuilder>, StringBuilder> writeValue)
            {
                return _elementWriter.Interpolate(queryBuilder, typeInfoProvider, (qb, typeInfo, writeElement) =>
                {
                    IClickHouseColumnTypeInfo? elementTypeInfo = _type._elementTypeInfo;
                    Debug.Assert(elementTypeInfo != null);
                    if (elementTypeInfo.ComplexTypeName == typeInfo.ComplexTypeName)
                    {
                        return writeValue(qb, _type, FunctionHelper.Apply);
                    }

                    ArrayTypeInfo updArrayTypeInfo = new(typeInfo);
                    return writeValue(qb, updArrayTypeInfo, (qb2, realWrite) =>
                    {
                        _ = qb2.Append("arrayMap(_elt -> ");
                        _ = writeElement(qb2, b => b.Append("_elt"));
                        _ = qb2.Append(", ");
                        _ = realWrite(qb2);
                        return qb2.Append(')');
                    });
                });
            }
        }

        private sealed class ArrayLiteralValueWriter : IClickHouseParameterValueWriter
        {
            private readonly List<IClickHouseParameterValueWriter> _elementWriters;

            public int Length { get; }

            public ArrayLiteralValueWriter(List<IClickHouseParameterValueWriter> elementWriters)
            {
                Length =
                    2 + // []
                    elementWriters.Aggregate(0, (l, w) => w.Length + l) + // length of elements
                    Math.Max(0, elementWriters.Count - 1); // comas
                _elementWriters = elementWriters;
            }

            public int Write(Memory<byte> buffer)
            {
                int count = 0;
                buffer.Span[count++] = (byte)'[';

                bool isFirst = true;
                foreach (IClickHouseParameterValueWriter writer in _elementWriters)
                {
                    if (isFirst)
                    {
                        isFirst = false;
                    }
                    else
                    {
                        buffer.Span[count++] = (byte)',';
                    }

                    count += writer.Write(buffer[count..]);
                }

                buffer.Span[count++] = (byte)']';
                Debug.Assert(count == Length);

                return count;
            }
        }
    }
}
