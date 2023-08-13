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
using System;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class HexStringLiteralWriter : IClickHouseLiteralWriter<ReadOnlyMemory<byte>>
    {
        private const string HexDigits = "0123456789ABCDEF";

        private readonly IClickHouseTypeInfo _typeInfo;

        public HexStringLiteralWriter(IClickHouseTypeInfo typeInfo)
        {
            _typeInfo = typeInfo;
        }

        public StringBuilder Interpolate(StringBuilder queryBuilder, ReadOnlyMemory<byte> value)
        {
            return Interpolate(queryBuilder, value.Span);
        }

        public static StringBuilder Interpolate(StringBuilder queryBuilder, ReadOnlySpan<byte> value)
        {
            queryBuilder.Append('\'');
            foreach (var byteValue in value)
            {
                queryBuilder.Append("\\x");
                queryBuilder.Append(HexDigits[byteValue >> 4]);
                queryBuilder.Append(HexDigits[byteValue & 0xF]);
            }
            return queryBuilder.Append('\'');
        }

        public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseTypeInfo, StringBuilder> writeValue)
        {
            return writeValue(queryBuilder, _typeInfo);
        }

        public SequenceSize Write(Memory<byte> buffer, ReadOnlyMemory<byte> value)
        {
            return Write(buffer, value.Span);
        }

        public static SequenceSize Write(Memory<byte> buffer, ReadOnlySpan<byte> value)
        {
            throw new NotImplementedException();
        }

        public static HexStringLiteralWriter<T> Create<T>(IClickHouseTypeInfo typeInfo)
            where T : struct
        {
            var dummy = default(T);
            var dummyBytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan(ref dummy, 1));
            var binaryTypeName = $"FixedString({dummyBytes.Length.ToString(CultureInfo.InvariantCulture)})";
            return new HexStringLiteralWriter<T>(typeInfo, HexStringLiteralWriterCastMode.Reinterpret, binaryTypeName, Convert);
        }

        public static HexStringLiteralWriter<TIn> Create<TIn, TOut>(IClickHouseTypeInfo typeInfo, Func<TIn, TOut> convert)
            where TOut : struct
        {
            var dummy = default(TOut);
            var dummyBytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan(ref dummy, 1));
            var binaryTypeName = $"FixedString({dummyBytes.Length.ToString(CultureInfo.InvariantCulture)})";
            return new HexStringLiteralWriter<TIn>(typeInfo, HexStringLiteralWriterCastMode.Reinterpret, binaryTypeName, v => Convert(convert(v)));
        }

        private static ReadOnlyMemory<byte> Convert<T>(T value)
            where T : struct
        {
            var src = MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan(ref value, 1));
            var dst = new byte[src.Length];
            src.CopyTo(dst);
            return dst;
        }
    }

    internal sealed class HexStringLiteralWriter<T> : IClickHouseLiteralWriter<T>
    {
        private readonly IClickHouseTypeInfo _typeInfo;
        private readonly HexStringLiteralWriterCastMode _castMode;
        private readonly string? _valueType;
        private readonly Func<T, ReadOnlyMemory<byte>> _convert;

        public HexStringLiteralWriter(IClickHouseTypeInfo typeInfo, Func<T, ReadOnlyMemory<byte>> convert)
            : this(typeInfo, HexStringLiteralWriterCastMode.None, null, convert)
        {
        }

        public HexStringLiteralWriter(IClickHouseTypeInfo typeInfo, HexStringLiteralWriterCastMode castMode, string? valueType, Func<T, ReadOnlyMemory<byte>> convert)
        {
            _typeInfo = typeInfo;
            _castMode = castMode;
            _valueType = valueType;
            _convert = convert;
        }

        public StringBuilder Interpolate(StringBuilder queryBuilder, T value)
        {
            var bytes = _convert(value);

            switch (_castMode)
            {
                case HexStringLiteralWriterCastMode.Cast:
                    queryBuilder.Append("CAST(");
                    break;

                case HexStringLiteralWriterCastMode.Reinterpret:
                    queryBuilder.Append("reinterpret(");
                    break;

                case HexStringLiteralWriterCastMode.None:
                    break;

                default:
                    throw new ClickHouseException(ClickHouseErrorCodes.InternalError, $"Internal error. Unknown value cast mode: \"{_castMode}\".");
            }

            if (_valueType != null)
                queryBuilder.Append("CAST(");

            HexStringLiteralWriter.Interpolate(queryBuilder, bytes.Span);

            if (_valueType != null)
                queryBuilder.Append(" AS ").Append(_valueType).Append(')');

            switch (_castMode)
            {
                case HexStringLiteralWriterCastMode.Cast:
                    queryBuilder.Append(" AS ").Append(_typeInfo.ComplexTypeName).Append(')');
                    break;

                case HexStringLiteralWriterCastMode.Reinterpret:
                    queryBuilder.Append(",'").Append(_typeInfo.ComplexTypeName.Replace("'", "''")).Append("')");
                    break;
            }

            return queryBuilder;
        }

        public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseTypeInfo, StringBuilder> writeValue)
        {
            if (this._valueType == null)
                return writeValue(queryBuilder, _typeInfo);

            var valueType = typeInfoProvider.GetTypeInfo(this._valueType);
            return writeValue(queryBuilder, valueType);
        }

        public SequenceSize Write(Memory<byte> buffer, T value)
        {
            var bytes = _convert(value);
            return HexStringLiteralWriter.Write(buffer, bytes.Span);
        }
    }
}
