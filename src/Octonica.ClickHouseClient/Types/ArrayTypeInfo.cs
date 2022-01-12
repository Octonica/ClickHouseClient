#region License Apache 2.0
/* Copyright 2019-2021 Octonica
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
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class ArrayTypeInfo : IClickHouseColumnTypeInfo
    {
        private readonly IClickHouseColumnTypeInfo? _elementTypeInfo;
        
        // Part of the header of the element's column may be located before the beginning of the array
        private readonly int _exposedElementHeaderSize;

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
            _exposedElementHeaderSize = GetExposedElementHeaderSize(elementTypeInfo);
            ComplexTypeName = $"{TypeName}({_elementTypeInfo.ComplexTypeName})";
        }

        public IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            if (_elementTypeInfo == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            return new ArrayColumnReader(rowCount, _elementTypeInfo, _exposedElementHeaderSize);
        }

        public IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            if (_elementTypeInfo == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            return new ArraySkippingColumnReader(rowCount, _elementTypeInfo, _exposedElementHeaderSize);
        }

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (_elementTypeInfo == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            var rowType = typeof(T);
            Type? elementType = null;
            if (rowType.IsArray)
            {
                var rank = rowType.GetArrayRank();
                if (rank > 1)
                {
                    elementType = rowType.GetElementType()!;
                    Debug.Assert(elementType != null);

                    var listAdapterInfo = MultiDimensionalArrayReadOnlyListAdapter.Dispatch(elementType, rowType.GetArrayRank());
                    var mdaDispatcher = new MultiDimensionalArrayColumnWriterDispatcher(
                        columnName,
                        (IReadOnlyList<Array>) rows,
                        columnSettings,
                        _elementTypeInfo,
                        listAdapterInfo.createList);

                    return TypeDispatcher.Dispatch(listAdapterInfo.listElementType, mdaDispatcher);
                }
            }

            foreach (var genericItf in rowType.GetInterfaces().Where(itf => itf.IsGenericType))
            {
                if (genericItf.GetGenericTypeDefinition() != typeof(IReadOnlyList<>))
                    continue;

                if (elementType == null)
                {
                    elementType = genericItf.GetGenericArguments()[0];
                }
                else
                {
                    var elementTypeCandidate = genericItf.GetGenericArguments()[0];

                    if (elementType.IsAssignableFrom(elementTypeCandidate))
                        elementType = elementTypeCandidate;
                    else if (!elementTypeCandidate.IsAssignableFrom(elementType))
                        throw new ClickHouseException(
                            ClickHouseErrorCodes.TypeNotSupported,
                            $"Can't detect a type of the array's element. Candidates are: \"{elementType}\" and \"{elementTypeCandidate}\".");
                }
            }

            ArrayColumnWriterDispatcherBase dispatcher;
            if (elementType == null)
            {
                var rowGenericTypeDef = rowType.GetGenericTypeDefinition();
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
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"Too many arguments in the definition of \"{TypeName}\".");

            var elementTypeInfo = typeInfoProvider.GetTypeInfo(options[0]);
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
            if (_elementTypeInfo == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            if (index != 0)
                throw new IndexOutOfRangeException();

            return _elementTypeInfo;
        }

        private static int GetExposedElementHeaderSize(IClickHouseTypeInfo elementType)
        {
            var type = elementType;
            while (type.TypeName == "Array")
                type = type.GetGenericArgument(0);

            if (type.TypeName == "LowCardinality")
                return sizeof(ulong);

            return 0;
        }

        public void FormatValue(StringBuilder queryStringBuilder, object? value)
        {
            if (_elementTypeInfo == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");
            
            if (value == null || value is DBNull)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{ComplexTypeName}\" does not allow null values");

            if (value is string || !(value is IEnumerable enumerable))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{value.GetType()}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            if (value is Array array && array.Rank > 1)
            {
                FormatMultiDimensionalArray(array, _elementTypeInfo, 0, array.GetEnumerator());
                
                void FormatMultiDimensionalArray(Array array, IClickHouseColumnTypeInfo dimensionElementType, int dimension, IEnumerator enumerator)
                {
                    var rankLength = array.GetLength(dimension);
                    queryStringBuilder.Append('[');
                    if (dimension == array.Rank - 1)
                    {
                        for (var i = 0; i < rankLength; ++i)
                        {
                            if (i > 0)
                                queryStringBuilder.Append(',');
                            
                            if (!enumerator.MoveNext())
                            {
                                throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error: unexpected iterator out of bound.");
                            }
                            
                            dimensionElementType.FormatValue(queryStringBuilder, enumerator.Current);
                        }
                    }
                    else
                    {
                        var nextDimension = dimension + 1;
                        IClickHouseColumnTypeInfo nextDimensionElementType;
                        var tmp = dimensionElementType;
                        while (tmp is NullableTypeInfo nti)
                            tmp = nti.UnderlyingType;
                        if (tmp == null)
                            throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");
                        if (tmp is ArrayTypeInfo ati)
                            nextDimensionElementType = ati._elementTypeInfo ?? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");
                        else
                            throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The multidimensional array value can not be converted type \"{ComplexTypeName}\": dimension number mismatch.");
                        
                        for (var i = 0; i < rankLength; ++i)
                        {
                            if (i > 0)
                                queryStringBuilder.Append(',');
                            
                            FormatMultiDimensionalArray(array, nextDimensionElementType, nextDimension, enumerator);
                        }
                    }
                    queryStringBuilder.Append(']');
                }
            }
            else
            {
                queryStringBuilder.Append('[');
                var needPrecedingComma = false;
                foreach (var element in enumerable)
                {
                    if (needPrecedingComma)
                        queryStringBuilder.Append(',');
                    else
                        needPrecedingComma = true;
                    _elementTypeInfo.FormatValue(queryStringBuilder, element);
                }
                queryStringBuilder.Append(']');
            }
        }

        private abstract class ArrayColumnReaderBase<TElementColumnReader> : IClickHouseColumnReaderBase
            where TElementColumnReader : class, IClickHouseColumnReaderBase
        {
            private readonly int _rowCount;

            // A part of the inner column's header exposed outside of the beginning of the current column
            private readonly byte[]? _exposedHeader;

            private int _elementPosition;
            private int _exposedHeaderPosition;

            protected IClickHouseColumnTypeInfo ElementType { get; }

            protected List<(int offset, int length)> Ranges { get; }

            protected TElementColumnReader? ElementColumnReader { get; private set; }

            protected int Position { get; private set; }

            public ArrayColumnReaderBase(int rowCount, IClickHouseColumnTypeInfo elementType, int exposedElementHeaderSize)
            {
                _rowCount = rowCount;
                ElementType = elementType;

                if (exposedElementHeaderSize > 0)
                    _exposedHeader = new byte[exposedElementHeaderSize];

                Ranges = new List<(int offset, int length)>(_rowCount);
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                if (Position >= _rowCount)
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");

                int bytesCount = 0;
                var slice = sequence;

                if (ElementColumnReader == null)
                {
                    if (_exposedHeader != null && _exposedHeaderPosition < _exposedHeader.Length)
                    {
                        bytesCount = (int)Math.Min(slice.Length, _exposedHeader.Length - _exposedHeaderPosition);
                        slice.Slice(0, bytesCount).CopyTo(((Span<byte>)_exposedHeader).Slice(_exposedHeaderPosition, bytesCount));
                        _exposedHeaderPosition += bytesCount;
                        slice = slice.Slice(bytesCount);
                    }

                    var totalLength = Ranges.Aggregate((ulong)0, (acc, r) => acc + (ulong)r.length);

                    Span<byte> sizeSpan = stackalloc byte[sizeof(ulong)];
                    for (int i = Ranges.Count; i < _rowCount; i++)
                    {
                        if (slice.Length < sizeSpan.Length)
                            return new SequenceSize(bytesCount, 0);

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

                        var offset = checked((int)totalLength);
                        var rangeLength = checked((int)(length - totalLength));
                        Ranges.Add((offset, rangeLength));

                        totalLength = length;
                    }

                    ElementColumnReader = CreateElementColumnReader(checked((int)totalLength));
                    _exposedHeaderPosition = 0;

                    if (totalLength == 0)
                    {
                        // Special case for an empty array
                        var result = new SequenceSize(bytesCount, _rowCount - Position);
                        Position = _rowCount;
                        return result;
                    }
                }

                SequenceSize elementsSize;
                if (_exposedHeader != null && _exposedHeaderPosition < _exposedHeader.Length)
                {
                    var segment = new SimpleReadOnlySequenceSegment<byte>(((ReadOnlyMemory<byte>)_exposedHeader).Slice(_exposedHeaderPosition), slice);
                    elementsSize = ElementColumnReader.ReadNext(new ReadOnlySequence<byte>(segment, 0, segment.LastSegment, segment.LastSegment.Memory.Length));

                    var exposedHeaderByteCount = Math.Min(elementsSize.Bytes, _exposedHeader.Length - _exposedHeaderPosition);
                    _exposedHeaderPosition += exposedHeaderByteCount;
                    elementsSize = elementsSize.AddBytes(-exposedHeaderByteCount);
                }
                else
                {
                    elementsSize = ElementColumnReader.ReadNext(slice);
                }

                _elementPosition += elementsSize.Elements;
                var elementsCount = 0;
                while (Position < _rowCount)
                {
                    var currentRange = Ranges[Position];
                    if (currentRange.length + currentRange.offset > _elementPosition)
                        break;

                    ++elementsCount;
                    ++Position;
                }

                return new SequenceSize(bytesCount + elementsSize.Bytes, elementsCount);
            }

            protected abstract TElementColumnReader CreateElementColumnReader(int totalLength);
        }

        private sealed class ArrayColumnReader : ArrayColumnReaderBase<IClickHouseColumnReader>, IClickHouseColumnReader
        {
            public ArrayColumnReader(int rowCount, IClickHouseColumnTypeInfo elementType, int exposedElementHeaderSize)
                : base(rowCount, elementType, exposedElementHeaderSize)
            {
            }

            protected override IClickHouseColumnReader CreateElementColumnReader(int totalLength)
            {
                return ElementType.CreateColumnReader(totalLength);
            }

            public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
            {
                var elementColumnReader = ElementColumnReader ?? ElementType.CreateColumnReader(0);
                var column = elementColumnReader.EndRead(settings);
                var ranges = Position == Ranges.Count ? Ranges : Ranges.Take(Position).ToList();

                if (!column.TryDipatch(new ArrayTableColumnDipatcher(ranges), out var result))
                    result = new ArrayTableColumn(column, ranges);

                return result;
            }
        }

        private sealed class ArraySkippingColumnReader : ArrayColumnReaderBase<IClickHouseColumnReaderBase>
        {
            public ArraySkippingColumnReader(int rowCount, IClickHouseColumnTypeInfo elementType, int exposedElementHeaderSize)
                : base(rowCount, elementType, exposedElementHeaderSize)
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
                return new ArrayLinearizedList<T>((IReadOnlyList<IReadOnlyList<T>?>) rows);
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
                return new ArrayLinearizedList<T>(MappedReadOnlyList<Memory<T>, IReadOnlyList<T>?>.Map((IReadOnlyList<Memory<T>>) rows, m => new ReadOnlyMemoryList<T>(m)));
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
                return new ArrayLinearizedList<T>(MappedReadOnlyList<ReadOnlyMemory<T>, IReadOnlyList<T>?>.Map((IReadOnlyList<ReadOnlyMemory<T>>) rows, m => new ReadOnlyMemoryList<T>(m)));
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
                var linearizedList = ToList<T>(_rows);
                var elementColumnWriter = _elementTypeInfo.CreateColumnWriter(_columnName, linearizedList, _columnSettings);
                var columnType = $"Array({elementColumnWriter.ColumnType})";
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
                var mappedRows = MappedReadOnlyList<Array, IReadOnlyList<T>>.Map(_rows, arr => (IReadOnlyList<T>) _dispatchArray(arr));
                var linearizedList = new ArrayLinearizedList<T>(mappedRows);
                var elementColumnWriter = _elementTypeInfo.CreateColumnWriter(_columnName, linearizedList, _columnSettings);
                var columnType = $"Array({elementColumnWriter.ColumnType})";
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

            public SequenceSize WriteNext(Span<byte> writeTo)
            {
                int bytesCount = 0;
                var span = writeTo;
                for (; _headerPosition < _rows.ListLengths.Count; _headerPosition++)
                {
                    if (!BitConverter.TryWriteBytes(span, (ulong) _rows.ListLengths[_headerPosition]))
                        return new SequenceSize(bytesCount, 0);

                    span = span.Slice(sizeof(ulong));
                    bytesCount += sizeof(ulong);
                }

                var elementSize = _elementColumnWriter.WriteNext(span);
                _elementPosition += elementSize.Elements;

                var elementsCount = 0;
                while (_position < _rows.ListLengths.Count)
                {
                    if (_rows.ListLengths[_position] > _elementPosition)
                        break;

                    ++elementsCount;
                    ++_position;
                }

                return new SequenceSize(elementSize.Bytes + bytesCount, elementsCount);
            }
        }

        private sealed class ArrayLinearizedList<T> : IReadOnlyList<T>
        {
            private readonly IReadOnlyList<IReadOnlyList<T>?> _listOfLists;

            public List<int> ListLengths { get; }

            public int Count { get; }

            public ArrayLinearizedList(IReadOnlyList<IReadOnlyList<T>?> listOfLists)
            {
                _listOfLists = listOfLists;
                ListLengths = new List<int>(_listOfLists.Count);

                int offset = 0;
                foreach (var list in _listOfLists)
                {
                    offset += list?.Count ?? 0;
                    ListLengths.Add(offset);
                }

                Count = offset;
            }

            public IEnumerator<T> GetEnumerator()
            {
                return _listOfLists.SelectMany(list => list ?? Enumerable.Empty<T>()).GetEnumerator();
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
                        throw new IndexOutOfRangeException();

                    var listIndex = ListLengths.BinarySearch(index);
                    int elementIndex;
                    if (listIndex < 0)
                    {
                        listIndex = ~listIndex;
                        if (listIndex == 0)
                        {
                            elementIndex = index;
                        }
                        else
                        {
                            elementIndex = index - ListLengths[listIndex - 1];
                        }
                    }
                    else
                    {
                        elementIndex = 0;
                        listIndex++;
                    }

                    for (; listIndex < _listOfLists.Count; listIndex++)
                    {
                        if (elementIndex < 0)
                            break;

                        var list = _listOfLists[listIndex];
                        if (list != null && elementIndex < list.Count)
                            return list[elementIndex];

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
    }
}
