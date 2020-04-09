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
using System.Collections.Generic;
using System.Text;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;

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
            if (rows is IReadOnlyList<string> stringRows)
                return new StringColumnWriter(columnName, ComplexTypeName, stringRows, columnSettings?.StringEncoding ?? Encoding.UTF8);

            if (rows is IReadOnlyList<byte[]> byteRows)
                return new BinaryStringColumnWriter(columnName, ComplexTypeName, byteRows);

            throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
        }

        public override Type GetFieldType()
        {
            return typeof(string);
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

        private class BinaryStringColumnWriter : IClickHouseColumnWriter
        {
            private readonly IReadOnlyList<byte[]> _rows;

            public string ColumnName { get; }

            public string ColumnType { get; }

            private int _position;

            public BinaryStringColumnWriter(string columnName, string columnType, IReadOnlyList<byte[]> rows)
            {
                _rows = rows;
                ColumnName = columnName;
                ColumnType = columnType;
            }

            public SequenceSize WriteNext(Span<byte> writeTo)
            {
                if (_position == _rows.Count)
                    return new SequenceSize(0, 0);

                var result = new SequenceSize(0, 0);
                for (; _position < _rows.Count; _position++)
                {
                    var span = writeTo.Slice(result.Bytes);
                    if (span.IsEmpty)
                        break;

                    var bytes = _rows[_position];
                    if (bytes == null)
                    {
                        span[0] = 0;
                        result = new SequenceSize(result.Bytes + 1, result.Bytes + 1);
                    }
                    else
                    {
                        var prefixLength = ClickHouseBinaryProtocolWriter.TryWrite7BitInteger(span, (ulong) bytes.Length);
                        if (prefixLength <= 0 || prefixLength + bytes.Length > span.Length)
                            return result;

                        bytes.CopyTo(span.Slice(prefixLength, bytes.Length));
                        result = new SequenceSize(result.Bytes + prefixLength + bytes.Length, result.Elements + 1);
                    }
                }

                return result;
            }
        }

        private class StringColumnWriter : IClickHouseColumnWriter
        {
            private readonly IReadOnlyList<string> _rows;
            private readonly Encoding _encoding;

            public string ColumnName { get; }

            public string ColumnType { get; }

            private int _position;

            public StringColumnWriter(string columnName, string columnType, IReadOnlyList<string> rows, Encoding encoding)
            {
                _rows = rows;
                _encoding = encoding;
                ColumnName = columnName;
                ColumnType = columnType;
            }

            public SequenceSize WriteNext(Span<byte> writeTo)
            {
                if (_position == _rows.Count)
                    return new SequenceSize(0, 0);

                var result = new SequenceSize(0, 0);
                for (; _position < _rows.Count; _position++)
                {
                    var span = writeTo.Slice(result.Bytes);
                    if (span.IsEmpty)
                        break;
                    
                    var str = _rows[_position];
                    if (string.IsNullOrEmpty(str))
                    {
                        span[0] = 0;
                        result = new SequenceSize(result.Bytes + 1, result.Elements + 1);
                    }
                    else
                    {
                        var byteCount = _encoding.GetByteCount(str);
                        var prefixLength = ClickHouseBinaryProtocolWriter.TryWrite7BitInteger(span, (ulong) byteCount);
                        if (prefixLength <= 0 || prefixLength + byteCount > span.Length)
                            return result;

                        _encoding.GetBytes(str, span.Slice(prefixLength, byteCount));
                        result = new SequenceSize(result.Bytes + prefixLength + byteCount, result.Elements + 1);
                    }
                }

                return result;
            }
        }
    }
}
