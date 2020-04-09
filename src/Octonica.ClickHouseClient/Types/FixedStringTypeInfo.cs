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
using System.Globalization;
using System.Text;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class FixedStringTypeInfo : IClickHouseTypeInfo
    {
        private readonly int? _length;

        public string ComplexTypeName { get; }

        public string TypeName => "FixedString";

        public FixedStringTypeInfo()
        {
            ComplexTypeName = TypeName;
        }

        private FixedStringTypeInfo(int length)
        {
            _length = length;
            ComplexTypeName = string.Format(CultureInfo.InvariantCulture, "{0}({1})", TypeName, length);
        }

        public IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            if (_length == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, "The length of the fixed string is not specified.");

            return new FixedStringReader(rowCount, _length.Value);
        }

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (!(rows is IReadOnlyList<byte[]> byteRows))
            {
                if (rows is IReadOnlyList<string?> stringRows)
                {
                    var columnEncoding = columnSettings?.StringEncoding ?? Encoding.UTF8;
                    byteRows = new MappedReadOnlyList<string?, byte[]>(stringRows, str => str == null ? new byte[0] : columnEncoding.GetBytes(str));
                }
                else
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
            }

            if (_length == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, "The length of the fixed string is not specified.");

            return new FixedStringWriter(columnName, ComplexTypeName, byteRows, _length.Value);
        }

        public IClickHouseTypeInfo GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            if (_length != null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, "The type is already fully specified.");

            if (options.Count > 1)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"Too many arguments in the definition of \"{TypeName}\".");

            if (!int.TryParse(options[0].Span, NumberStyles.Integer, CultureInfo.InvariantCulture, out var length) || length <= 0)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The length of \"{TypeName}({options[0].ToString()})\" must be a positive number.");

            return new FixedStringTypeInfo(length);
        }

        public Type GetFieldType()
        {
            return typeof(byte[]);
        }

        private sealed class FixedStringReader : IClickHouseColumnReader
        {
            private readonly int _rowCount;
            private readonly int _rowSize;

            private readonly Memory<byte> _buffer;

            private int _position;

            public FixedStringReader(int rowCount, int rowSize)
            {
                _rowCount = rowCount;
                _rowSize = rowSize;
                if (rowCount > 0)
                    _buffer = new Memory<byte>(new byte[rowCount * rowSize]);
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                if (_position / _rowSize >= _rowCount)
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");

                if (sequence.Length < _rowSize)
                    return new SequenceSize(0, 0);

                var elementsCount = Math.Min((int) sequence.Length / _rowSize, _rowCount - _position / _rowSize);
                if (elementsCount == 0)
                    return new SequenceSize(0, 0);

                var bytesCount = _rowSize * elementsCount;
                sequence.Slice(0, bytesCount).CopyTo(_buffer.Span.Slice(_position));
                _position += bytesCount;
                return new SequenceSize(bytesCount, elementsCount);
            }

            public SequenceSize Skip(ReadOnlySequence<byte> sequence, int maxElementsCount, ref object? skipContext)
            {
                var elementsCount = Math.Min(maxElementsCount, (int) sequence.Length / _rowSize);
                return new SequenceSize(elementsCount * _rowSize, elementsCount);
            }

            public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
            {
                return new FixedStringTableColumn(_buffer, _rowSize, settings?.StringEncoding ?? Encoding.UTF8);
            }
        }

        private sealed class FixedStringWriter : IClickHouseColumnWriter
        {
            private readonly IReadOnlyList<byte[]> _rows;
            private readonly int _length;

            public string ColumnName { get; }

            public string ColumnType { get; }

            private int _position;

            public FixedStringWriter(string columnName, string columnType, IReadOnlyList<byte[]> rows, int length)
            {
                _rows = rows;
                _length = length;
                ColumnName = columnName ?? throw new ArgumentNullException(nameof(columnName));
                ColumnType = columnType;
            }

            public SequenceSize WriteNext(Span<byte> writeTo)
            {
                var size = Math.Min(_rows.Count - _position, writeTo.Length / _length);

                Span<byte> zeroSpan = new byte[_length];
                var span = writeTo;
                for (int i = 0; i < size; i++, span = span.Slice(_length))
                {
                    Span<byte> bytes = _rows[_position++];
                    if (bytes.Length > _length)
                        throw new InvalidCastException($"The length of the array ({bytes.Length}) is greater than the maximum length ({_length}).");

                    bytes.CopyTo(span);
                    zeroSpan.Slice(bytes.Length).CopyTo(span.Slice(bytes.Length));
                }

                return new SequenceSize(size * _length, size);
            }
        }
    }
}
