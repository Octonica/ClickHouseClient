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
using System.Collections.Generic;
using System.Text;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class StringTypeInfo : SimpleTypeInfo
    {
        public StringTypeInfo()
            : base("String")
        {
        }

        public override IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new StringColumnReader(rowCount);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (typeof(T) == typeof(string))
                return new StringColumnWriter(columnName, ComplexTypeName, (IReadOnlyList<string>) rows, columnSettings?.StringEncoding ?? Encoding.UTF8);

            if (typeof(T) == typeof(char[]))
            {
                var mappedList = new MappedReadOnlyList<char[]?, ReadOnlyMemory<char>>((IReadOnlyList<char[]?>)rows, m => m.AsMemory());
                return new StringSpanColumnWriter(columnName, ComplexTypeName, mappedList, columnSettings?.StringEncoding ?? Encoding.UTF8);
            }

            if (typeof(T) == typeof(ReadOnlyMemory<char>))
                return new StringSpanColumnWriter(columnName, ComplexTypeName, (IReadOnlyList<ReadOnlyMemory<char>>) rows, columnSettings?.StringEncoding ?? Encoding.UTF8);

            if (typeof(T) == typeof(Memory<char>))
            {
                var mappedList = new MappedReadOnlyList<Memory<char>, ReadOnlyMemory<char>>((IReadOnlyList<Memory<char>>) rows, m => m);
                return new StringSpanColumnWriter(columnName, ComplexTypeName, mappedList, columnSettings?.StringEncoding ?? Encoding.UTF8);
            }

            if (typeof(T) == typeof(byte[]))
                return new BinaryStringColumnWriter(columnName, ComplexTypeName, (IReadOnlyList<byte[]>) rows);

            if (typeof(T) == typeof(ReadOnlyMemory<byte>))
                return new BinaryStringSpanColumnWriter(columnName, ComplexTypeName, (IReadOnlyList<ReadOnlyMemory<byte>>) rows);

            if (typeof(T) == typeof(Memory<byte>))
            {
                var mappedList = new MappedReadOnlyList<Memory<byte>, ReadOnlyMemory<byte>>((IReadOnlyList<Memory<byte>>) rows, m => m);
                return new BinaryStringSpanColumnWriter(columnName, ComplexTypeName, mappedList);
            }

            throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
        }

        public override Type GetFieldType()
        {
            return typeof(string);
        }

        public override ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.String;
        }

        private class StringColumnReader : IClickHouseColumnReader
        {
            private readonly int _rowCount;
            private readonly int _bufferSize;
            
            private readonly List<(int segmentIndex, int offset, int length)> _layouts;
            private readonly List<Memory<byte>> _segments = new List<Memory<byte>>(1);

            private int _position;

            public StringColumnReader(int rowCount)
            {
                _rowCount = rowCount;
                _bufferSize = 4096; //TODO: from settings
                _layouts = new List<(int segmentIndex, int offset, int length)>(rowCount);
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                if (_layouts.Count >= _rowCount)
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");

                int expectedElementsCount = _rowCount - _layouts.Count;
                int elementsCount = 0, bytesCount = 0;
                while (elementsCount < expectedElementsCount)
                {
                    var slice = sequence.Slice(bytesCount);
                    if (!ClickHouseBinaryProtocolReader.TryRead7BitInteger(slice, out var longSize, out var bytesRead))
                        break;

                    var size = (int) longSize;
                    if (slice.Length - bytesRead < size)
                        break;

                    if (size == 0)
                    {
                        _layouts.Add((0, 0, 0));
                    }
                    else
                    {
                        var lastSegment = _segments.Count == 0 ? default : _segments[^1];
                        lastSegment = lastSegment.Slice(_position);
                        if (lastSegment.Length < size)
                        {
                            lastSegment = new Memory<byte>(new byte[Math.Max(_bufferSize, size)]);
                            _position = 0;
                            _segments.Add(lastSegment);
                        }

                        var stringBytes = slice.Slice(bytesRead, size);
                        stringBytes.CopyTo(lastSegment.Span);

                        _layouts.Add((_segments.Count - 1, _position, size));
                        _position += size;
                    }

                    ++elementsCount;
                    bytesCount += size + bytesRead;
                }

                return new SequenceSize(bytesCount, elementsCount);
            }

            public SequenceSize Skip(ReadOnlySequence<byte> sequence, int maxElementsCount, ref object? skipContext)
            {
                int offset = 0;
                int count = 0;
                while (count < maxElementsCount)
                {
                    var slice = sequence.Slice(offset);
                    if (!ClickHouseBinaryProtocolReader.TryRead7BitInteger(slice, out var size, out var bytesRead))
                        break;

                    var totalLength = bytesRead + (int) size;
                    if (slice.Length < totalLength)
                        break;

                    offset += totalLength;
                    ++count;
                }

                return new SequenceSize(offset, count);
            }

            public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
            {
                return new StringTableColumn(settings?.StringEncoding ?? Encoding.UTF8, _layouts, _segments);
            }
        }

        private sealed class BinaryStringColumnWriter : StringColumnWriterBase
        {
            private readonly IReadOnlyList<byte[]> _rows;

            protected override int RowCount => _rows.Count;

            public BinaryStringColumnWriter(string columnName, string columnType, IReadOnlyList<byte[]> rows)
                : base(columnName, columnType)
            {
                _rows = rows;
            }

            protected override int GetByteCount(int rowIndex)
            {
                return _rows[rowIndex]?.Length ?? 0;
            }

            protected override void WriteBytes(int rowIndex, Span<byte> writeTo)
            {
                _rows[rowIndex].CopyTo(writeTo);
            }
        }

        private sealed class BinaryStringSpanColumnWriter : StringColumnWriterBase
        {
            private readonly IReadOnlyList<ReadOnlyMemory<byte>> _rows;

            protected override int RowCount => _rows.Count;

            public BinaryStringSpanColumnWriter(string columnName, string columnType, IReadOnlyList<ReadOnlyMemory<byte>> rows)
                : base(columnName, columnType)
            {
                _rows = rows;
            }

            protected override int GetByteCount(int rowIndex)
            {
                return _rows[rowIndex].Length;
            }

            protected override void WriteBytes(int rowIndex, Span<byte> writeTo)
            {
                _rows[rowIndex].Span.CopyTo(writeTo);
            }
        }

        private sealed class StringColumnWriter : StringColumnWriterBase
        {
            private readonly IReadOnlyList<string> _rows;
            private readonly Encoding _encoding;

            protected override int RowCount => _rows.Count;

            public StringColumnWriter(string columnName, string columnType, IReadOnlyList<string> rows, Encoding encoding)
                : base(columnName, columnType)
            {
                _rows = rows;
                _encoding = encoding;
            }

            protected override int GetByteCount(int rowIndex)
            {
                var str = _rows[rowIndex];
                if (string.IsNullOrEmpty(str))
                    return 0;

                return _encoding.GetByteCount(str);
            }

            protected override void WriteBytes(int rowIndex, Span<byte> writeTo)
            {
                _encoding.GetBytes(_rows[rowIndex], writeTo);
            }
        }

        private sealed class StringSpanColumnWriter : StringColumnWriterBase
        {
            private readonly IReadOnlyList<ReadOnlyMemory<char>> _rows;
            private readonly Encoding _encoding;

            protected override int RowCount => _rows.Count;

            public StringSpanColumnWriter(string columnName, string columnType, IReadOnlyList<ReadOnlyMemory<char>> rows, Encoding encoding)
                : base(columnName, columnType)
            {
                _rows = rows;
                _encoding = encoding;
            }

            protected override int GetByteCount(int rowIndex)
            {
                return _encoding.GetByteCount(_rows[rowIndex].Span);
            }

            protected override void WriteBytes(int rowIndex, Span<byte> writeTo)
            {
                _encoding.GetBytes(_rows[rowIndex].Span, writeTo);
            }
        }

        private abstract class StringColumnWriterBase : IClickHouseColumnWriter
        {
            public string ColumnName { get; }

            public string ColumnType { get; }

            protected abstract int RowCount { get; }

            private int _position;

            protected StringColumnWriterBase(string columnName, string columnType)
            {
                ColumnName = columnName;
                ColumnType = columnType;
            }

            public SequenceSize WriteNext(Span<byte> writeTo)
            {
                var rowCount = RowCount;
                if (_position == rowCount)
                    return new SequenceSize(0, 0);

                var result = new SequenceSize(0, 0);
                for (; _position < rowCount; _position++)
                {
                    var span = writeTo.Slice(result.Bytes);
                    if (span.IsEmpty)
                        break;

                    var byteCount = GetByteCount(_position);
                    if (byteCount == 0)
                    {
                        span[0] = 0;
                        result = new SequenceSize(result.Bytes + 1, result.Elements + 1);
                    }
                    else
                    {
                        var prefixLength = ClickHouseBinaryProtocolWriter.TryWrite7BitInteger(span, checked((ulong) byteCount));
                        if (prefixLength <= 0 || prefixLength + byteCount > span.Length)
                            return result;

                        WriteBytes(_position, span.Slice(prefixLength, byteCount));
                        result = new SequenceSize(result.Bytes + prefixLength + byteCount, result.Elements + 1);
                    }
                }

                return result;
            }

            protected abstract int GetByteCount(int rowIndex);

            protected abstract void WriteBytes(int rowIndex, Span<byte> writeTo);
        }
    }
}
