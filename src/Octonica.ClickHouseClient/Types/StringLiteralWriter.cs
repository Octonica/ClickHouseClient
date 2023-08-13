#region License Apache 2.0
/* Copyright 2023 Octonica
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

using Octonica.ClickHouseClient.Protocol;
using System;
using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class StringLiteralWriter : IClickHouseLiteralWriter<ReadOnlyMemory<char>>
    {
        private readonly IClickHouseTypeInfo _type;

        public StringLiteralWriter(IClickHouseTypeInfo type)
        {
            _type = type;
        }

        public StringBuilder Interpolate(StringBuilder queryBuilder, ReadOnlyMemory<char> value)
        {
            Interpolate(queryBuilder, value.Span);

            if (_type.ComplexTypeName != "String")
                queryBuilder.Append("::").Append(_type.ComplexTypeName);

            return queryBuilder;
        }

        public static StringBuilder Interpolate(StringBuilder queryBuilder, ReadOnlySpan<char> stringSpan)
        {
            queryBuilder.Append('\'');
            foreach (var charValue in stringSpan)
            {
                switch (charValue)
                {
                    case '\\':
                        queryBuilder.Append("\\\\");
                        break;
                    case '\'':
                        queryBuilder.Append("''");
                        break;
                    default:
                        queryBuilder.Append(charValue);
                        break;
                }
            }

            return queryBuilder.Append('\'');
        }

        public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseTypeInfo, StringBuilder> writeValue)
        {
            return writeValue(queryBuilder, _type);
        }

        public SequenceSize Write(Memory<byte> buffer, ReadOnlyMemory<char> value)
        {
            return Write(buffer, value.Span);
        }

        public static SequenceSize Write(Memory<byte> buffer, ReadOnlySpan<char> stringSpan)
        {
            var encoding = Encoding.UTF8;
            var length = encoding.GetByteCount(stringSpan);
            var offset = ClickHouseBinaryProtocolWriter.TryWrite7BitInteger(buffer.Span, (ulong)length);
            if (offset == 0)
                return SequenceSize.Empty;

            var span = buffer.Span.Slice(offset);
            if (span.Length < length)
                return SequenceSize.Empty;

            var count = Encoding.UTF8.GetBytes(stringSpan, span);
            Debug.Assert(count == length);
            return new SequenceSize(length + offset, 1);
        }

        public static StringLiteralWriter<T> Create<T>(IClickHouseTypeInfo typeInfo, string? format = null)
            where T : IFormattable
        {
            return new StringLiteralWriter<T>(typeInfo, value => value.ToString(format, CultureInfo.InvariantCulture).AsMemory());
        }
    }

    internal sealed class StringLiteralWriter<T> : IClickHouseLiteralWriter<T>
    {
        private readonly IClickHouseTypeInfo _type;
        private readonly Func<T, ReadOnlyMemory<char>> _toString;

        public StringLiteralWriter(IClickHouseTypeInfo type, Func<T, ReadOnlyMemory<char>> toString)
        {
            _type = type;
            _toString = toString;
        }

        public StringBuilder Interpolate(StringBuilder queryBuilder, T value)
        {
            var str = _toString(value);
            StringLiteralWriter.Interpolate(queryBuilder, str.Span);

            if (_type.ComplexTypeName != "String")
                queryBuilder.Append("::").Append(_type.ComplexTypeName);

            return queryBuilder;
        }

        public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseTypeInfo, StringBuilder> writeValue)
        {
            return writeValue(queryBuilder, _type);
        }

        public SequenceSize Write(Memory<byte> buffer, T value)
        {
            var str = _toString(value);
            return StringLiteralWriter.Write(buffer, str.Span);
        }
    }
}
