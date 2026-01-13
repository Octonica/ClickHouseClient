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

using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;
using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class HexStringParameterWriter : IClickHouseParameterWriter<ReadOnlyMemory<byte>>
    {
        private const string HexDigits = HexStringLiteralValueWriter.HexDigits;

        private readonly IClickHouseColumnTypeInfo _typeInfo;

        public HexStringParameterWriter(IClickHouseColumnTypeInfo typeInfo)
        {
            _typeInfo = typeInfo;
        }

        public bool TryCreateParameterValueWriter(ReadOnlyMemory<byte> value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter)
        {
            valueWriter = new HexStringLiteralValueWriter(value, isNested);
            return true;
        }

        public StringBuilder Interpolate(StringBuilder queryBuilder, ReadOnlyMemory<byte> value)
        {
            return Interpolate(queryBuilder, value.Span);
        }

        public static StringBuilder Interpolate(StringBuilder queryBuilder, ReadOnlySpan<byte> value)
        {
            _ = queryBuilder.Append('\'');
            foreach (byte byteValue in value)
            {
                _ = queryBuilder.Append("\\x");
                _ = queryBuilder.Append(HexDigits[byteValue >> 4]);
                _ = queryBuilder.Append(HexDigits[byteValue & 0xF]);
            }
            return queryBuilder.Append('\'');
        }

        public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseColumnTypeInfo, Func<StringBuilder, Func<StringBuilder, StringBuilder>, StringBuilder>, StringBuilder> writeValue)
        {
            return writeValue(queryBuilder, _typeInfo, FunctionHelper.Apply);
        }

        public static HexStringParameterWriter<T> Create<T>(IClickHouseColumnTypeInfo typeInfo)
            where T : struct
        {
            T dummy = default;
            ReadOnlySpan<byte> dummyBytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan(ref dummy, 1));
            string binaryTypeName = $"FixedString({dummyBytes.Length.ToString(CultureInfo.InvariantCulture)})";
            return new HexStringParameterWriter<T>(typeInfo, HexStringLiteralWriterCastMode.Reinterpret, binaryTypeName, Convert);
        }

        public static HexStringParameterWriter<TIn> Create<TIn, TOut>(IClickHouseColumnTypeInfo typeInfo, Func<TIn, TOut> convert)
            where TOut : struct
        {
            TOut dummy = default;
            ReadOnlySpan<byte> dummyBytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan(ref dummy, 1));
            string binaryTypeName = $"FixedString({dummyBytes.Length.ToString(CultureInfo.InvariantCulture)})";
            return new HexStringParameterWriter<TIn>(typeInfo, HexStringLiteralWriterCastMode.Reinterpret, binaryTypeName, v => Convert(convert(v)));
        }

        private static ReadOnlyMemory<byte> Convert<T>(T value)
            where T : struct
        {
            ReadOnlySpan<byte> src = MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan(ref value, 1));
            byte[] dst = new byte[src.Length];
            src.CopyTo(dst);
            return dst;
        }
    }

    internal sealed class HexStringParameterWriter<T> : IClickHouseParameterWriter<T>
    {
        private readonly IClickHouseColumnTypeInfo _typeInfo;
        private readonly HexStringLiteralWriterCastMode _castMode;
        private readonly string? _valueType;
        private readonly Func<T, ReadOnlyMemory<byte>> _convert;

        public HexStringParameterWriter(IClickHouseColumnTypeInfo typeInfo, Func<T, ReadOnlyMemory<byte>> convert)
            : this(typeInfo, HexStringLiteralWriterCastMode.None, null, convert)
        {
        }

        public HexStringParameterWriter(IClickHouseColumnTypeInfo typeInfo, HexStringLiteralWriterCastMode castMode, string? valueType, Func<T, ReadOnlyMemory<byte>> convert)
        {
            _typeInfo = typeInfo;
            _castMode = castMode;
            _valueType = valueType;
            _convert = convert;
        }

        public bool TryCreateParameterValueWriter(T value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter)
        {
            ReadOnlyMemory<byte> bytes = _convert(value);
            valueWriter = new HexStringLiteralValueWriter(bytes, isNested);
            return true;
        }

        public StringBuilder Interpolate(StringBuilder queryBuilder, T value)
        {
            ReadOnlyMemory<byte> bytes = _convert(value);

            switch (_castMode)
            {
                case HexStringLiteralWriterCastMode.Cast:
                    _ = queryBuilder.Append("CAST(");
                    break;

                case HexStringLiteralWriterCastMode.Reinterpret:
                    _ = queryBuilder.Append("reinterpret(");
                    break;

                case HexStringLiteralWriterCastMode.None:
                    break;

                default:
                    throw new ClickHouseException(ClickHouseErrorCodes.InternalError, $"Internal error. Unknown value cast mode: \"{_castMode}\".");
            }

            if (_valueType != null)
            {
                _ = queryBuilder.Append("CAST(");
            }

            _ = HexStringParameterWriter.Interpolate(queryBuilder, bytes.Span);

            if (_valueType != null)
            {
                _ = queryBuilder.Append(" AS ").Append(_valueType).Append(')');
            }

            switch (_castMode)
            {
                case HexStringLiteralWriterCastMode.Cast:
                    _ = queryBuilder.Append(" AS ").Append(_typeInfo.ComplexTypeName).Append(')');
                    break;

                case HexStringLiteralWriterCastMode.Reinterpret:
                    _ = queryBuilder.Append(",'").Append(_typeInfo.ComplexTypeName.Replace("'", "''")).Append("')");
                    break;
            }

            return queryBuilder;
        }

        public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseColumnTypeInfo, Func<StringBuilder, Func<StringBuilder, StringBuilder>, StringBuilder>, StringBuilder> writeValue)
        {
            if (_valueType == null)
            {
                return writeValue(queryBuilder, _typeInfo, FunctionHelper.Apply);
            }

            IClickHouseColumnTypeInfo valueType = typeInfoProvider.GetTypeInfo(_valueType);

            HexStringLiteralWriterCastMode castMode = _castMode;
            if (_typeInfo.ComplexTypeName == valueType.ComplexTypeName)
            {
                castMode = HexStringLiteralWriterCastMode.None;
            }

            switch (castMode)
            {
                case HexStringLiteralWriterCastMode.Cast:
                    return writeValue(queryBuilder, valueType, (qb, realWrite) =>
                    {
                        _ = qb.Append('(');
                        _ = realWrite(qb);
                        return qb.Append("::").Append(_typeInfo.ComplexTypeName).Append(')');
                    });

                case HexStringLiteralWriterCastMode.Reinterpret:
                    return writeValue(queryBuilder, valueType, (qb, realWrite) =>
                    {
                        _ = qb.Append("reinterpret(");
                        _ = realWrite(qb);
                        return qb.Append(",'").Append(_typeInfo.ComplexTypeName.Replace("'", "''")).Append("')");
                    });

                default:
                    Debug.Assert(castMode == HexStringLiteralWriterCastMode.None);
                    return writeValue(queryBuilder, valueType, FunctionHelper.Apply);
            }
        }
    }
}
