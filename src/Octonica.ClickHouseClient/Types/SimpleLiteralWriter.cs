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
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class SimpleLiteralWriter<T> : IClickHouseLiteralWriter<T>
        where T : IFormattable
    {
        private readonly string? _valueType;
        private readonly IClickHouseTypeInfo _type;
        private readonly string? _format;
        private readonly bool _appendTypeCast;

        public SimpleLiteralWriter(IClickHouseTypeInfo type, string? format = null, bool appendTypeCast = false)
            : this(null, type, format, appendTypeCast)
        {
        }

        public SimpleLiteralWriter(string? valueType, IClickHouseTypeInfo type, string? format = null, bool appendTypeCast = false)
        {
            _valueType = valueType;
            _type = type;
            _format = format;
            _appendTypeCast = appendTypeCast;
        }

        public bool TryCreateParameterValueWriter(T value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter)
        {
            var strVal = value.ToString(_format, CultureInfo.InvariantCulture);
            valueWriter = new SimpleLiteralValueWriter(strVal.AsMemory());
            return true;
        }

        public StringBuilder Interpolate(StringBuilder queryBuilder, T value)
        {
            var strVal = value.ToString(_format, CultureInfo.InvariantCulture);
            queryBuilder.Append(strVal);
            if (_appendTypeCast)
                queryBuilder.Append("::").Append(_type.ComplexTypeName);

            return queryBuilder;
        }

        public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseTypeInfo, StringBuilder> writeValue)
        {
            if (_valueType != null)
            {
                var valueTypeInfo = typeInfoProvider.GetTypeInfo(_valueType);
                writeValue(queryBuilder, valueTypeInfo);
            }
            else
            {
                writeValue(queryBuilder, _type);
            }

            if (_appendTypeCast)
                queryBuilder.Append("::").Append(_type.ComplexTypeName);

            return queryBuilder;
        }
    }

    internal sealed class SimpleLiteralWriter<TIn, TOut> : IClickHouseLiteralWriter<TIn>
        where TOut : IFormattable
    {
        private readonly string? _valueType;
        private readonly IClickHouseTypeInfo _type;
        private readonly Func<TIn, TOut> _convert;
        private readonly string? _format;
        private readonly bool _appendTypeCast;

        public SimpleLiteralWriter(IClickHouseTypeInfo type, Func<TIn, TOut> convert)
            : this(null, type, null, false, convert)
        {
        }

        public SimpleLiteralWriter(IClickHouseTypeInfo type, bool appendTypeCast, Func<TIn, TOut> convert)
            : this(null, type, null, appendTypeCast, convert)
        {
        }

        public SimpleLiteralWriter(IClickHouseTypeInfo type, string? format, Func<TIn, TOut> convert)
            : this(null, type, format, false, convert)
        {
        }

        public SimpleLiteralWriter(string? valueType, IClickHouseTypeInfo type, string? format, bool appendTypeCast, Func<TIn, TOut> convert)
        {
            _valueType = valueType;
            _type = type;
            _format = format;
            _appendTypeCast = appendTypeCast;
            _convert = convert;
        }

        public bool TryCreateParameterValueWriter(TIn value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter)
        {
            var strVal = _convert(value).ToString(_format, CultureInfo.InvariantCulture);
            valueWriter = new SimpleLiteralValueWriter(strVal.AsMemory());
            return true;
        }

        public StringBuilder Interpolate(StringBuilder queryBuilder, TIn value)
        {
            var strVal = _convert(value).ToString(_format, CultureInfo.InvariantCulture);

            queryBuilder.Append(strVal);
            if (_valueType != null)
                queryBuilder.Append("::").Append(_valueType);

            if (_appendTypeCast)
            {
                if (_valueType == null || _valueType != _type.ComplexTypeName)
                    queryBuilder.Append("::").Append(_type.ComplexTypeName);
            }

            return queryBuilder;
        }

        public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseTypeInfo, StringBuilder> writeValue)
        {
            if (_valueType != null)
            {
                var valueTypeInfo = typeInfoProvider.GetTypeInfo(_valueType);
                writeValue(queryBuilder, valueTypeInfo);

                if (valueTypeInfo.ComplexTypeName != _type.ComplexTypeName)
                    queryBuilder.Append("::").Append(_type.ComplexTypeName);
            }
            else
            {
                writeValue(queryBuilder, _type);
            }

            return queryBuilder;
        }
    }
}
