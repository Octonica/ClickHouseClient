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
using System.Globalization;
using System.Text;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class FixedStringTypeInfo : IClickHouseColumnTypeInfo
    {
        private readonly int? _length;

        public string ComplexTypeName { get; }

        public string TypeName => "FixedString";

        public int GenericArgumentsCount => 0;

        public int TypeArgumentsCount => _length == 0 ? 0 : 1;

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

        public IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            if (_length == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, "The length of the fixed string is not specified.");

            return new SimpleSkippingColumnReader(_length.Value, rowCount);
        }

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (_length == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, "The length of the fixed string is not specified.");

            var type = typeof(T);
            if (type == typeof(byte[]))
                return new FixedStringBytesColumnWriter(columnName, ComplexTypeName, (IReadOnlyList<byte[]?>)rows, _length.Value);
            if (type == typeof(string))
                return new FixedStringStringColumnWriter(columnName, ComplexTypeName, (IReadOnlyList<string?>)rows, _length.Value, columnSettings?.StringEncoding);
            if (type == typeof(ReadOnlyMemory<byte>))
                return new FixedStringBytesColumnWriter(columnName, ComplexTypeName, (IReadOnlyList<ReadOnlyMemory<byte>>)rows, _length.Value);
            if (type == typeof(Memory<byte>))
                return new FixedStringBytesColumnWriter(columnName, ComplexTypeName, (IReadOnlyList<Memory<byte>>)rows, _length.Value);
            if (type == typeof(ReadOnlyMemory<char>))
                return new FixedStringStringColumnWriter(columnName, ComplexTypeName, (IReadOnlyList<ReadOnlyMemory<char>>)rows, _length.Value, columnSettings?.StringEncoding);
            if (type == typeof(Memory<char>))
                return new FixedStringStringColumnWriter(columnName, ComplexTypeName, (IReadOnlyList<Memory<char>>)rows, _length.Value, columnSettings?.StringEncoding);

            throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
        }
        
        const string HexDigits = "0123456789ABCDEF";

        public void FormatValue(StringBuilder queryStringBuilder, object? value)
        {
            if (_length == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, "The length of the fixed string is not specified.");
            
            if (value == null || value is DBNull)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values");

            if (value is string stringValue)
                FormatCharString(stringValue);
            // else if (value is char[] charArrValue)
            //     FormatCharString(charArrValue);
            else if (value is ReadOnlyMemory<char> charRoMemValue)
                FormatCharString(charRoMemValue.Span);
            else if (value is Memory<char> charMemValue)
                FormatCharString(charMemValue.Span);
            else if (value is byte[] byteArrValue)
                FormatByteString(byteArrValue);
            else if (value is ReadOnlyMemory<byte> byteRoMemValue)
                FormatByteString(byteRoMemValue.Span);
            else if (value is Memory<byte> byteMemValue)
                FormatByteString(byteMemValue.Span);
            else
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{value.GetType()}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            void FormatCharString(ReadOnlySpan<char> data)
            {
                var length = _length.Value;
                var encoding = Encoding.UTF8;
                var bytesCount = encoding.GetByteCount(data);
                if (bytesCount > length)
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidQueryParameterConfiguration, $"The length of the string ({bytesCount}) is greater than the maximum length ({length}).");
                    
                queryStringBuilder.Append('\'');
                foreach (var charValue in data)
                {
                    switch (charValue)
                    {
                        case '\\':
                            queryStringBuilder.Append("\\\\");
                            break;
                        case '\'':
                            queryStringBuilder.Append("''");
                            break;
                        default:
                            queryStringBuilder.Append(charValue);
                            break;
                    }
                }
                queryStringBuilder.Append('\'');
            }
            
            void FormatByteString(ReadOnlySpan<byte> data)
            {
                var length = _length.Value;
                if (data.Length > length)
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidQueryParameterConfiguration, $"The length of the array ({data.Length}) is greater than the maximum length ({length}).");
                queryStringBuilder.Append('\'');
                foreach (var byteValue in data)
                {
                    queryStringBuilder.Append("\\x");
                    queryStringBuilder.Append(HexDigits[byteValue >> 4]);
                    queryStringBuilder.Append(HexDigits[byteValue & 0xF]);
                }
                queryStringBuilder.Append('\'');
            }
        }

        public IClickHouseColumnTypeInfo GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
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

        public ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.StringFixedLength;
        }

        public IClickHouseTypeInfo GetGenericArgument(int index)
        {
            throw new NotSupportedException($"The type \"{TypeName}\" doesn't have generic arguments.");
        }

        public object GetTypeArgument(int index)
        {
            if (_length == null)
                throw new NotSupportedException($"The type \"{TypeName}\" doesn't have arguments.");

            if (index == 0)
                return _length;

            throw new IndexOutOfRangeException();
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

            public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
            {
                return new FixedStringTableColumn(_buffer, _rowSize, settings?.StringEncoding ?? Encoding.UTF8);
            }
        }

        private sealed class FixedStringBytesColumnWriter : FixedStringColumnWriterBase
        {
            private readonly IReadOnlyList<ReadOnlyMemory<byte>> _rows;

            protected override int RowCount => _rows.Count;

            public FixedStringBytesColumnWriter(string columnName, string columnType, IReadOnlyList<byte[]?> rows, int length)
                : this(columnName, columnType, MappedReadOnlyList<byte[]?, ReadOnlyMemory<byte>>.Map(rows, b => b.AsMemory()), length)
            {
            }

            public FixedStringBytesColumnWriter(string columnName, string columnType, IReadOnlyList<Memory<byte>> rows, int length)
                : this(columnName, columnType, MappedReadOnlyList<Memory<byte>, ReadOnlyMemory<byte>>.Map(rows, m => m), length)
            {
            }

            public FixedStringBytesColumnWriter(string columnName, string columnType, IReadOnlyList<ReadOnlyMemory<byte>> rows, int length)
                : base(columnName, columnType, length)
            {
                _rows = rows;
            }

            protected override int GetBytes(int position, Span<byte> buffer)
            {
                var bytes = _rows[position];
                if (bytes.Length > buffer.Length)
                    throw new InvalidCastException($"The length of the array ({bytes.Length}) is greater than the maximum length ({buffer.Length}).");

                bytes.Span.CopyTo(buffer);
                return bytes.Length;
            }
        }

        private sealed class FixedStringStringColumnWriter : FixedStringColumnWriterBase
        {
            private readonly IReadOnlyList<ReadOnlyMemory<char>> _rows;
            private readonly Encoding _stringEncoding;

            protected override int RowCount => _rows.Count;

            public FixedStringStringColumnWriter(string columnName, string columnType, IReadOnlyList<Memory<char>> rows, int length, Encoding? stringEncoding)
                : this(columnName, columnType, MappedReadOnlyList<Memory<char>, ReadOnlyMemory<char>>.Map(rows, m => m), length, stringEncoding)
            {
            }

            public FixedStringStringColumnWriter(string columnName, string columnType, IReadOnlyList<string?> rows, int length, Encoding? stringEncoding)
                : this(columnName, columnType, MappedReadOnlyList<string?, ReadOnlyMemory<char>>.Map(rows, str => str.AsMemory()), length, stringEncoding)
            {
            }

            public FixedStringStringColumnWriter(string columnName, string columnType, IReadOnlyList<ReadOnlyMemory<char>> rows, int length, Encoding? stringEncoding)
                : base(columnName, columnType, length)
            {
                _rows = rows;
                _stringEncoding = stringEncoding ?? Encoding.UTF8;
            }

            protected override int GetBytes(int position, Span<byte> buffer)
            {
                var str = _rows[position].Span;
                var bytesCount = _stringEncoding.GetByteCount(str);
                if (bytesCount == 0)
                    return 0;

                if (bytesCount <= buffer.Length)
                {
                    _stringEncoding.GetBytes(str, buffer);
                    return bytesCount;
                }

                throw new InvalidCastException($"The length of the string ({bytesCount}) is greater than the maximum length ({buffer.Length}).");
            }
        }

        private abstract class FixedStringColumnWriterBase : IClickHouseColumnWriter
        {
            private readonly int _length;

            public string ColumnName { get; }

            public string ColumnType { get; }

            protected abstract int RowCount { get; }

            private int _position;

            public FixedStringColumnWriterBase(string columnName, string columnType, int length)
            {
                _length = length;
                ColumnName = columnName ?? throw new ArgumentNullException(nameof(columnName));
                ColumnType = columnType;
            }

            public SequenceSize WriteNext(Span<byte> writeTo)
            {
                var size = Math.Min(RowCount - _position, writeTo.Length / _length);

                ReadOnlySpan<byte> zeroSpan = new byte[_length];
                var span = writeTo;
                for (int i = 0; i < size; i++, span = span.Slice(_length))
                {
                    var bytesCount = GetBytes(_position++, span.Slice(0, _length));
                    zeroSpan.Slice(bytesCount).CopyTo(span.Slice(bytesCount));
                }

                return new SequenceSize(size * _length, size);
            }

            protected abstract int GetBytes(int position, Span<byte> buffer);
        }
    }
}
