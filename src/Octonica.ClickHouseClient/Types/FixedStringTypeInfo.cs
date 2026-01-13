#region License Apache 2.0
/* Copyright 2019-2021, 2023 Octonica
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
using System.Globalization;
using System.Text;

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
            return _length == null
                ? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, "The length of the fixed string is not specified.")
                : (IClickHouseColumnReader)new FixedStringReader(rowCount, _length.Value);
        }

        public IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            return _length == null
                ? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, "The length of the fixed string is not specified.")
                : (IClickHouseColumnReaderBase)new SimpleSkippingColumnReader(_length.Value, rowCount);
        }

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (_length == null)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, "The length of the fixed string is not specified.");
            }

            Type type = typeof(T);
            if (type == typeof(byte[]))
            {
                return new FixedStringBytesColumnWriter(columnName, ComplexTypeName, (IReadOnlyList<byte[]?>)rows, _length.Value);
            }

            if (type == typeof(string))
            {
                return new FixedStringStringColumnWriter(columnName, ComplexTypeName, (IReadOnlyList<string?>)rows, _length.Value, columnSettings?.StringEncoding);
            }

            if (type == typeof(ReadOnlyMemory<byte>))
            {
                return new FixedStringBytesColumnWriter(columnName, ComplexTypeName, (IReadOnlyList<ReadOnlyMemory<byte>>)rows, _length.Value);
            }

            return type == typeof(Memory<byte>)
                ? new FixedStringBytesColumnWriter(columnName, ComplexTypeName, (IReadOnlyList<Memory<byte>>)rows, _length.Value)
                : type == typeof(ReadOnlyMemory<char>)
                ? new FixedStringStringColumnWriter(columnName, ComplexTypeName, (IReadOnlyList<ReadOnlyMemory<char>>)rows, _length.Value, columnSettings?.StringEncoding)
                : type == typeof(Memory<char>)
                ? (IClickHouseColumnWriter)new FixedStringStringColumnWriter(columnName, ComplexTypeName, (IReadOnlyList<Memory<char>>)rows, _length.Value, columnSettings?.StringEncoding)
                : throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
        }

        public IClickHouseParameterWriter<T> CreateParameterWriter<T>()
        {
            if (_length == null)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, "The length of the fixed string is not specified.");
            }

            Type type = typeof(T);
            if (type == typeof(DBNull))
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values");
            }

            object writer;
            if (type == typeof(string))
            {
                writer = new FixedStringParameterWriter<string>(this, s => s.AsMemory());
            }
            else if (type == typeof(ReadOnlyMemory<char>))
            {
                writer = new FixedStringParameterWriter(this);
            }
            else if (type == typeof(Memory<char>))
            {
                writer = new FixedStringParameterWriter<Memory<char>>(this, mem => mem);
            }
            else
            {
                writer = type == typeof(byte[])
                    ? new FixedStringHexParameterWriter<byte[]>(this, a => a.AsMemory())
                    : type == typeof(ReadOnlyMemory<byte>)
                    ? new FixedStringHexParameterWriter(this)
                    : type == typeof(Memory<byte>)
                ? (object)new FixedStringHexParameterWriter<Memory<byte>>(this, mem => mem)
                : throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
            }

            return (IClickHouseParameterWriter<T>)writer;
        }

        public IClickHouseColumnTypeInfo GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            return _length != null
                ? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, "The type is already fully specified.")
                : options.Count > 1
                ? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"Too many arguments in the definition of \"{TypeName}\".")
                : !int.TryParse(options[0].Span, NumberStyles.Integer, CultureInfo.InvariantCulture, out int length) || length <= 0
                ? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The length of \"{TypeName}({options[0]})\" must be a positive number.")
                : (IClickHouseColumnTypeInfo)new FixedStringTypeInfo(length);
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
            return _length == null
                ? throw new NotSupportedException($"The type \"{TypeName}\" doesn't have arguments.")
                : index == 0 ? (object)_length : throw new IndexOutOfRangeException();
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
                {
                    _buffer = new Memory<byte>(new byte[rowCount * rowSize]);
                }
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                if (_position / _rowSize >= _rowCount)
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");
                }

                if (sequence.Length < _rowSize)
                {
                    return new SequenceSize(0, 0);
                }

                int elementsCount = Math.Min((int)sequence.Length / _rowSize, _rowCount - (_position / _rowSize));
                if (elementsCount == 0)
                {
                    return new SequenceSize(0, 0);
                }

                int bytesCount = _rowSize * elementsCount;
                sequence.Slice(0, bytesCount).CopyTo(_buffer.Span[_position..]);
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
                ReadOnlyMemory<byte> bytes = _rows[position];
                if (bytes.Length > buffer.Length)
                {
                    throw new InvalidCastException($"The length of the array ({bytes.Length}) is greater than the maximum length ({buffer.Length}).");
                }

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
                ReadOnlySpan<char> str = _rows[position].Span;
                int bytesCount = _stringEncoding.GetByteCount(str);
                if (bytesCount == 0)
                {
                    return 0;
                }

                if (bytesCount <= buffer.Length)
                {
                    _ = _stringEncoding.GetBytes(str, buffer);
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
                int size = Math.Min(RowCount - _position, writeTo.Length / _length);

                ReadOnlySpan<byte> zeroSpan = new byte[_length];
                Span<byte> span = writeTo;
                for (int i = 0; i < size; i++, span = span[_length..])
                {
                    int bytesCount = GetBytes(_position++, span[.._length]);
                    zeroSpan[bytesCount..].CopyTo(span[bytesCount..]);
                }

                return new SequenceSize(size * _length, size);
            }

            protected abstract int GetBytes(int position, Span<byte> buffer);
        }

        private class FixedStringParameterWriter : IClickHouseParameterWriter<ReadOnlyMemory<char>>
        {
            private readonly FixedStringTypeInfo _type;

            public FixedStringParameterWriter(FixedStringTypeInfo type)
            {
                _type = type;
            }

            public bool TryCreateParameterValueWriter(ReadOnlyMemory<char> value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter)
            {
                StringLiteralValueWriter writer = new(value, isNested);

                Debug.Assert(_type._length != null);
                if (writer.Length - 2 > _type._length.Value)
                {
                    ValidateLength(value); // Validate the length of the unescaped string without quota signs
                }

                valueWriter = writer;
                return true;
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, ReadOnlyMemory<char> value)
            {
                ValidateLength(value);
                return StringParameterWriter.Interpolate(queryBuilder, value.Span);
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseColumnTypeInfo, Func<StringBuilder, Func<StringBuilder, StringBuilder>, StringBuilder>, StringBuilder> writeValue)
            {
                return writeValue(queryBuilder, _type, FunctionHelper.Apply);
            }

            private void ValidateLength(ReadOnlyMemory<char> value)
            {
                Debug.Assert(_type._length != null);
                int length = _type._length.Value;
                Encoding encoding = Encoding.UTF8;
                int bytesCount = encoding.GetByteCount(value.Span);
                if (bytesCount > length)
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidQueryParameterConfiguration, $"The length of the string ({bytesCount}) is greater than the maximum length ({length}).");
                }
            }
        }

        private sealed class FixedStringParameterWriter<T> : FixedStringParameterWriter, IClickHouseParameterWriter<T>
        {
            private readonly Func<T, ReadOnlyMemory<char>> _convert;

            public FixedStringParameterWriter(FixedStringTypeInfo type, Func<T, ReadOnlyMemory<char>> convert)
                : base(type)
            {
                _convert = convert;
            }

            public bool TryCreateParameterValueWriter(T value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter)
            {
                return TryCreateParameterValueWriter(_convert(value), isNested, out valueWriter);
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, T value)
            {
                return Interpolate(queryBuilder, _convert(value));
            }
        }

        private class FixedStringHexParameterWriter : IClickHouseParameterWriter<ReadOnlyMemory<byte>>
        {
            private readonly FixedStringTypeInfo _type;

            public FixedStringHexParameterWriter(FixedStringTypeInfo type)
            {
                _type = type;
            }

            public bool TryCreateParameterValueWriter(ReadOnlyMemory<byte> value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter)
            {
                ValidateLength(value);
                valueWriter = new HexStringLiteralValueWriter(value, isNested);
                return true;
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, ReadOnlyMemory<byte> value)
            {
                ValidateLength(value);
                return HexStringParameterWriter.Interpolate(queryBuilder, value.Span);
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseColumnTypeInfo, Func<StringBuilder, Func<StringBuilder, StringBuilder>, StringBuilder>, StringBuilder> writeValue)
            {
                return writeValue(queryBuilder, _type, FunctionHelper.Apply);
            }

            private void ValidateLength(ReadOnlyMemory<byte> value)
            {
                Debug.Assert(_type._length != null);
                int length = _type._length.Value;
                if (value.Length > length)
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidQueryParameterConfiguration, $"The length of the array ({value.Length}) is greater than the maximum length ({length}).");
                }
            }
        }

        private sealed class FixedStringHexParameterWriter<T> : FixedStringHexParameterWriter, IClickHouseParameterWriter<T>
        {
            private readonly Func<T, ReadOnlyMemory<byte>> _convert;

            public FixedStringHexParameterWriter(FixedStringTypeInfo type, Func<T, ReadOnlyMemory<byte>> convert)
            : base(type)
            {
                _convert = convert;
            }

            public bool TryCreateParameterValueWriter(T value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter)
            {
                return TryCreateParameterValueWriter(_convert(value), isNested, out valueWriter);
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, T value)
            {
                return Interpolate(queryBuilder, _convert(value));
            }
        }
    }
}
