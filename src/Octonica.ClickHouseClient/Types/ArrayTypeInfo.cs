#region License Apache 2.0
/* Copyright 2019-2020 Octonica LLC
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
using System.Linq;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class ArrayTypeInfo : IClickHouseTypeInfo
    {
        public string ComplexTypeName { get; }

        public string TypeName { get; } = "Array";

        public IClickHouseTypeInfo? ElementTypeInfo { get; }

        public ArrayTypeInfo()
        {
            ComplexTypeName = TypeName;
        }

        private ArrayTypeInfo(IClickHouseTypeInfo elementTypeInfo)
        {
            ElementTypeInfo = elementTypeInfo ?? throw new ArgumentNullException(nameof(elementTypeInfo));
            ComplexTypeName = $"{TypeName}({ElementTypeInfo.ComplexTypeName})";
        }

        public IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            if (ElementTypeInfo == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            return new ArrayColumnReader(rowCount, ElementTypeInfo);
        }

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (ElementTypeInfo == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            Type? elementType = null;
            foreach (var genericItf in typeof(T).GetInterfaces().Where(itf => itf.IsGenericType))
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
                        throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"Can't detect a type of the array's element. Candidates are: \"{elementType}\" and \"{elementTypeCandidate}\".");
                }
            }

            if (elementType == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"Can't detect a type of the array's element. The type \"{typeof(T)}\" doesn't implement \"{typeof(IReadOnlyList<>)}\".");

            var dispatcher = new ArrayColumnWriterDispatcher(columnName, ComplexTypeName, rows, columnSettings, ElementTypeInfo);
            return TypeDispatcher.Dispatch(elementType, dispatcher);
        }

        public IClickHouseTypeInfo GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            if (options.Count > 1)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"Too many arguments in the definition of \"{TypeName}\".");

            var elementTypeInfo = typeInfoProvider.GetTypeInfo(options[0]);
            return new ArrayTypeInfo(elementTypeInfo);
        }

        public Type GetFieldType()
        {
            return ElementTypeInfo == null ? typeof(object[]) : ElementTypeInfo.GetFieldType().MakeArrayType();
        }

        private sealed class ArrayColumnReader : IClickHouseColumnReader
        {
            private readonly int _rowCount;
            private readonly IClickHouseTypeInfo _elementType;
            private readonly List<(int offset, int length)> _ranges;

            private IClickHouseColumnReader? _elementColumnReader;

            private int _position;
            private int _elementPosition;

            public ArrayColumnReader(int rowCount, IClickHouseTypeInfo elementType)
            {
                _rowCount = rowCount;
                _elementType = elementType;

                _ranges = new List<(int offset, int length)>(_rowCount);
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                if (_position >= _rowCount)
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");

                int bytesCount = 0;
                var slice = sequence;

                if (_elementColumnReader == null)
                {
                    var totalLength = _ranges.Aggregate((ulong) 0, (acc, r) => acc + (ulong) r.length);

                    Span<byte> sizeSpan = stackalloc byte[sizeof(ulong)];
                    for (int i = _ranges.Count; i < _rowCount; i++)
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

                        var offset = checked((int) totalLength);
                        var rangeLength = checked((int) (length - totalLength));
                        _ranges.Add((offset, rangeLength));

                        totalLength = length;
                    }

                    _elementColumnReader = _elementType.CreateColumnReader(checked((int) totalLength));

                    if (totalLength == 0)
                    {
                        // Special case for an empty array
                        var result = new SequenceSize(bytesCount, _rowCount - _position);
                        _position = _rowCount;
                        return result;
                    }
                }

                var elementsSize = _elementColumnReader.ReadNext(slice);

                _elementPosition += elementsSize.Elements;
                var elementsCount = 0;
                while (_position < _rowCount)
                {
                    var currentRange = _ranges[_position];
                    if (currentRange.length + currentRange.offset > _elementPosition)
                        break;

                    ++elementsCount;
                    ++_position;
                }

                return new SequenceSize(bytesCount + elementsSize.Bytes, elementsCount);
            }

            public SequenceSize Skip(ReadOnlySequence<byte> sequence, int maxElementsCount, ref object? skipContext)
            {
                ArrayColumnSkipContext typedSkipContext;
                if (skipContext != null)
                    typedSkipContext = (ArrayColumnSkipContext) skipContext;
                else
                    skipContext = typedSkipContext = new ArrayColumnSkipContext(_elementType, maxElementsCount);

                return typedSkipContext.Skip(sequence, maxElementsCount);
            }

            public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
            {
                var elementColumnReader = _elementColumnReader ?? _elementType.CreateColumnReader(0);
                var column = elementColumnReader.EndRead(settings);
                var ranges = _position == _ranges.Count ? _ranges : _ranges.Take(_position).ToList();

                var columnType = column.GetType();
                Type? recognizedElementType = null;
                foreach (var itf in columnType.GetInterfaces().Where(i => i.IsGenericType))
                {
                    var typeDef = itf.GetGenericTypeDefinition();
                    if (typeDef != typeof(IClickHouseTableColumn<>))
                        continue;

                    if (recognizedElementType == null)
                    {
                        recognizedElementType = itf.GenericTypeArguments[0];
                    }
                    else
                    {
                        recognizedElementType = null;
                        break;
                    }
                }

                if (recognizedElementType != null)
                {
                    var reinterpretedColumn = TypeDispatcher.Dispatch(recognizedElementType, new ArrayTableColumnTypeDispatcher(column, ranges));
                    if (reinterpretedColumn != null)
                        return reinterpretedColumn;
                }

                return new ArrayTableColumn(column, ranges);
            }
        }

        private sealed class ArrayColumnSkipContext
        {
            private readonly IClickHouseTypeInfo _elementType;
            private readonly List<(int offset, int length)> _ranges;

            private IClickHouseColumnReader? _elementColumnReader;
            private object? _elementColumnReaderSkipContext;

            private int _position;
            private int _elementPosition;

            public ArrayColumnSkipContext(IClickHouseTypeInfo elementType, int initialCapacity)
            {
                _elementType = elementType;
                _ranges = new List<(int offset, int length)>(initialCapacity);
            }

            public SequenceSize Skip(ReadOnlySequence<byte> sequence, int maxElementsCount)
            { 
                int bytesCount = 0;
                var slice = sequence;

                if (_elementColumnReader == null)
                {
                    var totalLength = _ranges.Aggregate((ulong) 0, (acc, r) => acc + (ulong) r.length);

                    Span<byte> sizeSpan = stackalloc byte[sizeof(ulong)];
                    for (int i = _ranges.Count; i < maxElementsCount; i++)
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

                        var offset = checked((int) totalLength);
                        var rangeLength = checked((int) (length - totalLength));
                        _ranges.Add((offset, rangeLength));

                        totalLength = length;
                    }

                    _elementColumnReader = _elementType.CreateColumnReader(checked((int) totalLength));
                }

                if (maxElementsCount > _ranges.Count - _position)
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");

                var maxElementRange = _ranges[_position + maxElementsCount - 1];
                var elementsSize = _elementColumnReader.Skip(slice, maxElementRange.offset + maxElementRange.length - _elementPosition, ref _elementColumnReaderSkipContext);

                _elementPosition += elementsSize.Elements;
                var elementsCount = 0;
                while (_position < _ranges.Count)
                {
                    var currentRange = _ranges[_position];
                    if (currentRange.length + currentRange.offset > _elementPosition)
                        break;

                    ++elementsCount;
                    ++_position;
                }

                return new SequenceSize(bytesCount + elementsSize.Bytes, elementsCount);
            }
        }

        private sealed class ArrayColumnWriterDispatcher : ITypeDispatcher<IClickHouseColumnWriter>
        {
            private readonly string _columnName;
            private readonly string _columnType;
            private readonly object _rows;
            private readonly ClickHouseColumnSettings? _columnSettings;
            private readonly IClickHouseTypeInfo _elementTypeInfo;

            public ArrayColumnWriterDispatcher(string columnName, string columnType, object rows, ClickHouseColumnSettings? columnSettings, IClickHouseTypeInfo elementTypeInfo)
            {
                _columnName = columnName;
                _columnType = columnType;
                _rows = rows;
                _columnSettings = columnSettings;
                _elementTypeInfo = elementTypeInfo;
            }

            public IClickHouseColumnWriter Dispatch<T>()
            {
                var linearizedList = new ArrayLinearizedList<T>((IReadOnlyList<IReadOnlyList<T>?>) _rows);
                var elementColumnWriter = _elementTypeInfo.CreateColumnWriter(_columnName, linearizedList, _columnSettings);
                return new ArrayColumnWriter<T>(_columnType, linearizedList, elementColumnWriter);
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
    }
}
