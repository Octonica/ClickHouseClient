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
using Octonica.ClickHouseClient.Utils;
using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class StringParameterWriter : IClickHouseParameterWriter<ReadOnlyMemory<char>>
    {
        private readonly IClickHouseColumnTypeInfo _type;

        public StringParameterWriter(IClickHouseColumnTypeInfo type)
        {
            _type = type;
        }

        public bool TryCreateParameterValueWriter(ReadOnlyMemory<char> value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter)
        {
            valueWriter = new StringLiteralValueWriter(value, isNested);
            return true;
        }

        public StringBuilder Interpolate(StringBuilder queryBuilder, ReadOnlyMemory<char> value)
        {
            _ = Interpolate(queryBuilder, value.Span);

            if (_type.ComplexTypeName != "String")
            {
                _ = queryBuilder.Append("::").Append(_type.ComplexTypeName);
            }

            return queryBuilder;
        }

        public static StringBuilder Interpolate(StringBuilder queryBuilder, ReadOnlySpan<char> stringSpan)
        {
            _ = queryBuilder.Append('\'');
            foreach (char charValue in stringSpan)
            {
                _ = charValue switch
                {
                    '\\' => queryBuilder.Append("\\\\"),
                    '\'' => queryBuilder.Append("''"),
                    _ => queryBuilder.Append(charValue),
                };
            }

            return queryBuilder.Append('\'');
        }

        public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseColumnTypeInfo, Func<StringBuilder, Func<StringBuilder, StringBuilder>, StringBuilder>, StringBuilder> writeValue)
        {
            return writeValue(queryBuilder, _type, FunctionHelper.Apply);
        }

        public static StringParameterWriter<T> Create<T>(IClickHouseColumnTypeInfo typeInfo, string? format = null)
            where T : IFormattable
        {
            return new StringParameterWriter<T>(typeInfo, value => value.ToString(format, CultureInfo.InvariantCulture).AsMemory());
        }
    }

    internal sealed class StringParameterWriter<T> : IClickHouseParameterWriter<T>
    {
        private readonly IClickHouseColumnTypeInfo _type;
        private readonly Func<T, ReadOnlyMemory<char>> _toString;

        public StringParameterWriter(IClickHouseColumnTypeInfo type, Func<T, ReadOnlyMemory<char>> toString)
        {
            _type = type;
            _toString = toString;
        }

        public bool TryCreateParameterValueWriter(T value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter)
        {
            valueWriter = new StringLiteralValueWriter(_toString(value), isNested);
            return true;
        }

        public StringBuilder Interpolate(StringBuilder queryBuilder, T value)
        {
            ReadOnlyMemory<char> str = _toString(value);
            _ = StringParameterWriter.Interpolate(queryBuilder, str.Span);

            if (_type.ComplexTypeName != "String")
            {
                _ = queryBuilder.Append("::").Append(_type.ComplexTypeName);
            }

            return queryBuilder;
        }

        public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseColumnTypeInfo, Func<StringBuilder, Func<StringBuilder, StringBuilder>, StringBuilder>, StringBuilder> writeValue)
        {
            return writeValue(queryBuilder, _type, FunctionHelper.Apply);
        }
    }
}
