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

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient.Types
{
    internal abstract class DecimalTypeInfoBase : IClickHouseColumnTypeInfo
    {
        public const byte DefaultPrecision = 38, DefaultScale = 9;

        private readonly int? _precision;
        private readonly int? _scale;

        public string ComplexTypeName { get; }

        public string TypeName { get; }

        public int GenericArgumentsCount => 0;

        public virtual int TypeArgumentsCount => (_precision == null ? 0 : 1) + (_scale == null ? 0 : 1);

        protected DecimalTypeInfoBase(string typeName)
        {   
            TypeName = typeName;
            ComplexTypeName = typeName;
        }

        protected DecimalTypeInfoBase(string typeName, string complexTypeName, int precision, int scale)
        {
            if (precision < 1 || precision > 38)
                throw new ArgumentOutOfRangeException(nameof(precision), "The precision must be in the range [1:38].");
            if (scale < 0)
                throw new ArgumentOutOfRangeException(nameof(scale), "The scale must be a non-negative number.");
            if (scale > precision)
                throw new ArgumentOutOfRangeException(nameof(scale), "The scale must not be greater than the precision.");

            TypeName = typeName;
            ComplexTypeName = complexTypeName;
            _precision = precision;
            _scale = scale;
        }

        public IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            if (_precision == null || _scale == null)
            {
                if (_precision == null && _scale == null)
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"Both scale and precision are required for the type \"{TypeName}\".");

                if (_scale == null)
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"Scale is required for the type \"{TypeName}\".");

                // Currently there is no implementation which requires only the precision value
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"Precision is required for the type \"{TypeName}\".");
            }

            return new DecimalReader(_precision.Value, _scale.Value, rowCount);
        }

        public IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            if (_precision == null || _scale == null)
            {
                if (_precision == null && _scale == null)
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"Both scale and precision are required for the type \"{TypeName}\".");

                if (_scale == null)
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"Scale is required for the type \"{TypeName}\".");

                // Currently there is no implementation which requires only the precision value
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"Precision is required for the type \"{TypeName}\".");
            }

            return new SimpleSkippingColumnReader(GetElementSize(_precision.Value), rowCount);
        }

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (_precision == null && _scale == null)
            {
                var specifiedType = CloneWithOptions(string.Format(CultureInfo.InvariantCulture, "Decimal128({0})", DefaultScale), DefaultPrecision, DefaultScale);
                return specifiedType.CreateColumnWriter(columnName, rows, columnSettings);
            }

            if (_scale == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"Scale is required for the type \"{TypeName}\".");

            if (_precision == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"Precision is required for the type \"{TypeName}\".");

            var type = typeof(T);
            IReadOnlyList<decimal> decimalRows;
            if (type == typeof(decimal))
                decimalRows = (IReadOnlyList<decimal>)rows;
            else if (type == typeof(long))
                decimalRows = MappedReadOnlyList<long, decimal>.Map((IReadOnlyList<long>)rows, v => v);
            else if (type == typeof(ulong))
                decimalRows = MappedReadOnlyList<ulong, decimal>.Map((IReadOnlyList<ulong>)rows, v => v);
            else if (type == typeof(int))
                decimalRows = MappedReadOnlyList<int, decimal>.Map((IReadOnlyList<int>)rows, v => v);
            else if (type == typeof(uint))
                decimalRows = MappedReadOnlyList<uint, decimal>.Map((IReadOnlyList<uint>)rows, v => v);
            else if (type == typeof(short))
                decimalRows = MappedReadOnlyList<short, decimal>.Map((IReadOnlyList<short>)rows, v => v);
            else if (type == typeof(ushort))
                decimalRows = MappedReadOnlyList<ushort, decimal>.Map((IReadOnlyList<ushort>)rows, v => v);
            else if (type == typeof(sbyte))
                decimalRows = MappedReadOnlyList<sbyte, decimal>.Map((IReadOnlyList<sbyte>)rows, v => v);
            else if (type == typeof(byte))
                decimalRows = MappedReadOnlyList<byte, decimal>.Map((IReadOnlyList<byte>)rows, v => v);
            else
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return new DecimalWriter(columnName, ComplexTypeName, _precision.Value, _scale.Value, decimalRows);
        }

        public IClickHouseLiteralWriter<T> CreateLiteralWriter<T>()
        {
            if (_precision == null && _scale == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"Both scale and precision are required for the type \"{TypeName}\".");

            if (_scale == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"Scale is required for the type \"{TypeName}\".");

            if (_precision == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"Precision is required for the type \"{TypeName}\".");

            var type = typeof(T);
            if (type == typeof(DBNull))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values.");

            var binaryWriter = new DecimalWriter("Value", ComplexTypeName, _precision.Value, _scale.Value, Array.Empty<decimal>());
            object writer = default(T) switch
            {
                decimal _ => new DecimalLiteralWriter<decimal>(this, binaryWriter, v => v),
                long _ => new DecimalLiteralWriter<long>(this, binaryWriter, v => v),
                ulong _ => new DecimalLiteralWriter<ulong>(this, binaryWriter, v => v),
                int _ => new DecimalLiteralWriter<int>(this, binaryWriter, v => v),
                uint _ => new DecimalLiteralWriter<uint>(this, binaryWriter, v => v),
                short _ => new DecimalLiteralWriter<short>(this, binaryWriter, v => v),
                ushort _ => new DecimalLiteralWriter<ushort>(this, binaryWriter, v => v),
                sbyte _ => new DecimalLiteralWriter<sbyte>(this, binaryWriter, v => v),
                byte _ => new DecimalLiteralWriter<byte>(this, binaryWriter, v => v),
                _ => throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\"."),
            };

            return (IClickHouseLiteralWriter<T>)writer;
        }

        public IClickHouseColumnTypeInfo GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            int? precision = null;
            int scale;
            if (options.Count == 1)
            {
                if (!int.TryParse(options[0].Span, NumberStyles.Integer, CultureInfo.InvariantCulture, out scale) || scale < 0)
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The scale value for the type \"{TypeName}\" must be a non-negative number.");
            }
            else if (options.Count == 2)
            {
                if (!int.TryParse(options[0].Span, NumberStyles.Integer, CultureInfo.InvariantCulture, out var firstValue) || firstValue <= 0)
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The first parameter in options (precision) for the type \"{TypeName}\" must be a positive number.");

                precision = firstValue;

                if (!int.TryParse(options[1].Span, NumberStyles.Integer, CultureInfo.InvariantCulture, out scale) || scale < 0)
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The second parameter in options (scale) for the type \"{TypeName}\" must be a non-negative number.");
            }
            else
            {
                throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"Too many options for the type \"{TypeName}\".");
            }

            if (_precision != null && precision != null)
                throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The value of the precision can not be redefined for the type \"{TypeName}\".");

            var complexTypeName = TypeName + "(" + string.Join(", ", options) + ")";
            return CloneWithOptions(complexTypeName, precision, scale);
        }

        public Type GetFieldType()
        {
            return typeof(decimal);
        }

        public ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Decimal;
        }

        public IClickHouseTypeInfo GetGenericArgument(int index)
        {
            throw new NotSupportedException($"The type \"{TypeName}\" doesn't have generic arguments.");
        }

        public virtual object GetTypeArgument(int index)
        {
            if (_precision == null && _scale == null)
                throw new NotSupportedException($"The type \"{TypeName}\" doesn't have arguments.");

            switch (index)
            {
                case 0:
                    if (_precision == null)
                    {
                        Debug.Assert(_scale != null);
                        return _scale;
                    }
                    else
                    {
                        return _precision;
                    }

                case 1:
                    if (_scale == null || _precision == null)
                        goto default;

                    return _scale;

                default:
                    throw new IndexOutOfRangeException();
            }
        }

        protected abstract DecimalTypeInfoBase CloneWithOptions(string complexTypeName, int? precision, int scale);

        private static int GetElementSize(int precision)
        {
            if (precision <= 9)
                return 4;
            if (precision <= 18)
                return 8;

            Debug.Assert(precision <= 38);
            return 16;
        }

        private sealed class DecimalReader : IClickHouseColumnReader
        {
            private readonly int _rowCount;
            private readonly int _elementSize;
            private readonly byte _scale;

            private readonly uint[] _values;

            private int _position;

            public DecimalReader(int precision, int scale, int rowCount)
            {
                _rowCount = rowCount;
                _elementSize = GetElementSize(precision);
                _values = new uint[_elementSize / 4 * rowCount];
                _scale = (byte) scale;
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                var elementPosition = _position * sizeof(uint) / _elementSize;
                if (elementPosition >= _rowCount)
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");

                var byteLength = (int) Math.Min((_rowCount - elementPosition) * _elementSize, sequence.Length - sequence.Length % _elementSize);
                var uintLength = byteLength / sizeof(uint);

                var targetSpan = MemoryMarshal.AsBytes(new Span<uint>(_values, _position, uintLength));
                Debug.Assert(targetSpan.Length == byteLength);

                sequence.Slice(0, byteLength).CopyTo(targetSpan);

                _position += uintLength;
                return new SequenceSize(byteLength, byteLength / _elementSize);
            }

            public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
            {
                return EndReadInternal();
            }

            private DecimalTableColumn EndReadInternal()
            {
                var memory = new ReadOnlyMemory<uint>(_values, 0, _position);
                return new DecimalTableColumn(memory, _elementSize / 4, _scale);
            }
        }

        private sealed class DecimalWriter : StructureWriterBase<decimal>
        {
            private const byte MaxDecimalScale = 28;
            private static readonly uint[] Scales = { 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000 };

            private readonly byte _scale;

            public new int ElementSize { get => base.ElementSize; }

            public DecimalWriter(string columnName, string columnType, int precision, int scale, IReadOnlyList<decimal> rows)
                : base(columnName, columnType, GetElementSize(precision), rows)
            {
                _scale = (byte) scale;
            }

            public void WriteDecimal(Span<byte> writeTo, decimal value)
            {
                WriteElement(writeTo, value);
            }

            protected override void WriteElement(Span<byte> writeTo, in decimal value)
            {
                var rescaledValue = Math.Round(value, Math.Min(_scale, MaxDecimalScale), MidpointRounding.AwayFromZero);
                var bits = decimal.GetBits(rescaledValue);

                uint lowLow = unchecked((uint) bits[0]);
                uint lowHigh = unchecked((uint) bits[1]);
                uint highLow = unchecked((uint) bits[2]);
                uint highHigh = 0;

                bool isNegative = (bits[3] & int.MinValue) != 0;
                int scale = (bits[3] & ~int.MinValue) >> 16;

                var deltaScale = _scale - scale;
                if (deltaScale < 0)
                    throw new InvalidOperationException("Internal error: unexpected scale difference.");

                bool overflow = false;
                while (deltaScale > 0)
                {
                    var iterationScale = Math.Min(deltaScale, Scales.Length);
                    deltaScale -= iterationScale;
                    var multiplier = (ulong) Scales[iterationScale - 1];

                    ulong lowLowMul = lowLow * multiplier;
                    ulong lowHighMul = lowHigh * multiplier;
                    ulong highLowMul = highLow * multiplier;
                    ulong highHighMul = highHigh * multiplier;

                    lowLow = unchecked((uint) lowLowMul);
                    lowHigh = unchecked((uint) lowHighMul);
                    highLow = unchecked((uint) highLowMul);
                    highHigh = unchecked((uint) highHighMul);

                    var val = lowLowMul >> 32;
                    if (val != 0)
                    {
                        val += lowHigh;
                        lowHigh = unchecked((uint) val);

                        val >>= 32;
                        if (val != 0)
                        {
                            val += highLow;
                            highLow = unchecked((uint) val);

                            val >>= 32;
                            if (val != 0)
                            {
                                val += highHigh;
                                highHigh = unchecked((uint) val);

                                val >>= 32;
                                if (val != 0)
                                {
                                    overflow = true;
                                    break;
                                }
                            }
                        }
                    }

                    val = lowHighMul >> 32;
                    if (val != 0)
                    {
                        val += highLow;
                        highLow = unchecked((uint)val);

                        val >>= 32;
                        if (val != 0)
                        {
                            val += highHigh;
                            highHigh = unchecked((uint)val);

                            val >>= 32;
                            if (val != 0)
                            {
                                overflow = true;
                                break;
                            }
                        }
                    }

                    val = highLowMul >> 32;
                    if (val != 0)
                    {
                        val += highHigh;
                        highHigh = unchecked((uint)val);

                        val >>= 32;
                        if (val != 0)
                        {
                            overflow = true;
                            break;
                        }
                    }

                    val = highHighMul >> 32;
                    if (val != 0)
                    {
                        overflow = true;
                        break;
                    }
                }

                if (!overflow)
                {
                    if (isNegative)
                    {
                        lowLow = unchecked(0 - lowLow);
                        uint max = lowLow == 0 ? 0 : uint.MaxValue;

                        lowHigh = unchecked(max - lowHigh);
                        if (lowHigh != 0 && max == 0)
                            max = uint.MaxValue;

                        highLow = unchecked(max - highLow);
                        if (highLow != 0 && max == 0)
                            max = uint.MaxValue;

                        highHigh = unchecked(max - highHigh);

                        if (ElementSize == 4)
                            overflow = highHigh != uint.MaxValue || highLow != uint.MaxValue || lowHigh != uint.MaxValue || (lowLow & unchecked((uint) int.MinValue)) == 0;
                        else if (ElementSize == 8)
                            overflow = highHigh != uint.MaxValue || highLow != uint.MaxValue || (lowHigh & unchecked((uint) int.MinValue)) == 0;
                        else
                            overflow = (highHigh & unchecked((uint) int.MinValue)) == 0;

                        if (overflow && rescaledValue == 0)
                            overflow = false;
                    }
                    else
                    {
                        if (ElementSize == 4)
                            overflow = highHigh != 0 || highLow != 0 || lowHigh != 0 || (lowLow & unchecked((uint) int.MinValue)) != 0;
                        else if (ElementSize == 8)
                            overflow = highHigh != 0 || highLow != 0 || (lowHigh & unchecked((uint) int.MinValue)) != 0;
                        else
                            overflow = (highHigh & unchecked((uint) int.MinValue)) != 0;
                    }
                }

                if (overflow)
                    throw new OverflowException($"The decimal value is too big and can't be written to the column of type \"{ColumnType}\".");

                var success = BitConverter.TryWriteBytes(writeTo, lowLow);
                Debug.Assert(success);
                if (ElementSize == 4)
                    return;

                success = BitConverter.TryWriteBytes(writeTo.Slice(4), lowHigh);
                Debug.Assert(success);
                if (ElementSize == 8)
                    return;

                Debug.Assert(ElementSize == 16);
                success = BitConverter.TryWriteBytes(writeTo.Slice(8), highLow);
                Debug.Assert(success);
                success = BitConverter.TryWriteBytes(writeTo.Slice(12), highHigh);
                Debug.Assert(success);
            }
        }

        private sealed class DecimalLiteralWriter<T> : IClickHouseLiteralWriter<T>
        {
            private readonly DecimalTypeInfoBase _type;
            private readonly DecimalWriter _binaryWriter;
            private readonly Func<T, decimal> _convert;

            public DecimalLiteralWriter(DecimalTypeInfoBase type, DecimalWriter binaryWriter, Func<T, decimal> convert)
            {
                _type = type;
                _binaryWriter = binaryWriter;
                _convert = convert;
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, T value)
            {
                Span<byte> buffer = stackalloc byte[_binaryWriter.ElementSize];
                GetBytes(value, buffer);

                // Not all decimal values can be parsed:
                // > Real value ranges that can be stored in memory are a bit larger than specified above, which are checked only on conversion from a string.
                // But reinterpret_cast lets obtain any Decimal value
                queryBuilder.Append("reinterpret(");
                HexStringLiteralWriter.Interpolate(queryBuilder, buffer);

                queryBuilder.Append(", 'Decimal");
                switch (_binaryWriter.ElementSize)
                {
                    case 4:
                        queryBuilder.Append("32");
                        break;
                    case 8:
                        queryBuilder.Append("64");
                        break;
                    case 16:
                        queryBuilder.Append("128");
                        break;
                    default:
                        throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Invalid element size.");
                }

                if (_type._scale != null)
                {
                    queryBuilder.Append('(');
                    queryBuilder.Append(_type._scale.Value.ToString(CultureInfo.InvariantCulture));
                    queryBuilder.Append(")')");
                }

                return queryBuilder;
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseTypeInfo, StringBuilder> writeValue)
            {
                throw new NotImplementedException();
            }

            public SequenceSize Write(Memory<byte> buffer, T value)
            {
                throw new NotImplementedException();
            }

            private void GetBytes(T value, Span<byte> buffer)
            {
                var decValue = _convert(value);
                _binaryWriter.WriteDecimal(buffer, decValue);
            }
        }
    }
}
