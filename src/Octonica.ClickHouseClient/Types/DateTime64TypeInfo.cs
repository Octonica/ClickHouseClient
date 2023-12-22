#region License Apache 2.0
/* Copyright 2020-2023 Octonica
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
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class DateTime64TypeInfo : IClickHouseConfigurableTypeInfo
    {
        internal static readonly int[] DateTimeTicksScales;
        private static readonly long[] DateTimeTicksMaxValues;

        public const int DefaultPrecision = 3;

        private readonly int? _precision;
        private readonly string? _timeZoneCode;

        /// <summary>
        /// Indicates that the <see cref="_timeZoneCode"/> was acquired from the name of the type.
        /// </summary>
        private readonly bool _explicitTimeZoneCode;

        private TimeZoneInfo? _timeZone;

        public string ComplexTypeName { get; }

        public string TypeName => "DateTime64";

        public int GenericArgumentsCount => 0;

        public int TypeArgumentsCount => (_precision == null ? 0 : 1) + (_timeZoneCode == null || !_explicitTimeZoneCode ? 0 : 1);

        static DateTime64TypeInfo()
        {
            const int maxPrecision = 9;
            var scales = new int[maxPrecision + 1];

            for (int i = 0; i < scales.Length; i++)
                scales[i] = i == 0 ? 1 : checked(scales[i - 1] * 10);

            var maxValues = new long[scales.Length];
            var dateTimeTicksMax = (DateTime.MaxValue - DateTime.UnixEpoch).Ticks;
            for (int i = 0; i < scales.Length; i++)
            {
                long scale, rem;
                if (scales[i] <= TimeSpan.TicksPerSecond)
                    scale = Math.DivRem(TimeSpan.TicksPerSecond, scales[i], out rem);
                else
                    scale = -Math.DivRem(scales[i], TimeSpan.TicksPerSecond, out rem);

                if (rem != 0)
                    throw new InvalidOperationException($"Internal error. Expected that the value of {typeof(TimeSpan)}.{nameof(TimeSpan.TicksPerSecond)} is a power of 10.");

                scales[i] = checked((int) scale);

                if (scale < 0)
                    maxValues[i] = Math.Min(dateTimeTicksMax, checked((long) (ulong.MaxValue / (uint) -scale)));
                else
                    maxValues[i] = dateTimeTicksMax;
            }

            DateTimeTicksScales = scales;
            DateTimeTicksMaxValues = maxValues;
        }

        public DateTime64TypeInfo()
            : this(null, null, false)
        {
        }

        private DateTime64TypeInfo(int? precision, string? timeZoneCode, bool explicitTimeZoneCode)
        {
            if (precision != null)
            {
                if (precision.Value < 0)
                    throw new ArgumentOutOfRangeException(nameof(precision), "The precision must be a non-negative number.");
                if (precision.Value >= DateTimeTicksScales.Length)
                    throw new ArgumentOutOfRangeException(nameof(precision), $"The precision can't be greater than {DateTimeTicksScales.Length - 1}.");
            }

            _explicitTimeZoneCode = explicitTimeZoneCode;
            _precision = precision;
            _timeZoneCode = timeZoneCode;

            if (timeZoneCode != null && _explicitTimeZoneCode)
            {
                if (precision == null)
                    throw new ArgumentNullException(nameof(precision));

                ComplexTypeName = string.Format(CultureInfo.InvariantCulture, "{0}({1}, '{2}')", TypeName, precision.Value, timeZoneCode);
            }
            else if (precision != null)
            {
                ComplexTypeName = string.Format(CultureInfo.InvariantCulture, "{0}({1})", TypeName, precision.Value);
            }
            else
            {
                ComplexTypeName = TypeName;
            }
        }

        public Type GetFieldType()
        {
            return typeof(DateTimeOffset);
        }

        public ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.DateTime64;
        }

        public IClickHouseTypeInfo GetGenericArgument(int index)
        {
            throw new NotSupportedException($"The type \"{TypeName}\" doesn't have generic arguments.");
        }

        public object GetTypeArgument(int index)
        {
            if (_precision == null)
            {
                Debug.Assert(_timeZoneCode == null || !_explicitTimeZoneCode);
                throw new NotSupportedException($"The type \"{TypeName}\" doesn't have arguments.");
            }

            switch (index)
            {
                case 0:
                    return _precision;

                case 1:
                    if (_timeZoneCode != null && _explicitTimeZoneCode)
                        return _timeZoneCode;

                    goto default;

                default:
                    throw new IndexOutOfRangeException();
            }
        }

        public IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            var timeZone = GetTimeZone();
            return new DateTime64Reader(rowCount, _precision ?? DefaultPrecision, timeZone);
        }

        public IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            return new SimpleSkippingColumnReader(sizeof(ulong), rowCount);
        }

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (typeof(T) == typeof(DateTime))
            {
                var timeZone = GetTimeZone();
                return new DateTimeWriter(columnName, ComplexTypeName, _precision ?? DefaultPrecision, timeZone, (IReadOnlyList<DateTime>)rows);
            }

            if (typeof(T) == typeof(DateTimeOffset))
            {
                var timeZone = GetTimeZone();
                return new DateTimeOffsetWriter(columnName, ComplexTypeName, _precision ?? DefaultPrecision, timeZone, (IReadOnlyList<DateTimeOffset>)rows);
            }

            if (typeof(T) == typeof(ulong))
                return new UInt64TypeInfo.UInt64Writer(columnName, ComplexTypeName, (IReadOnlyList<ulong>)rows);

            throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
        }

        public IClickHouseLiteralWriter<T> CreateLiteralWriter<T>()
        {
            var type = typeof(T);
            if (type == typeof(DBNull))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values.");

            object? converter = default(T) switch
            {
                ulong _ => null,
                DateTime _ => new DateTimeWriter("dummy", ComplexTypeName, _precision ?? DefaultPrecision, GetTimeZone(), Array.Empty<DateTime>()),
                DateTimeOffset _ => new DateTimeOffsetWriter("dummy", ComplexTypeName, _precision ?? DefaultPrecision, GetTimeZone(), Array.Empty<DateTimeOffset>()),
                _ => throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".")
            };

            return new DateTime64LiteralWriter<T>(this, (IConverter<T, ulong>?)converter);
        }

        public IClickHouseColumnTypeInfo GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            if (options.Count > 2)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"Too many arguments in the definition of \"{TypeName}\".");

            if (!int.TryParse(options[0].Span, NumberStyles.Integer, CultureInfo.InvariantCulture, out var precision))
                throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The first argument (precision) of the type \"{TypeName}\" must be an integer.");
            else if (precision < 0)
                throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The precision value for the type \"{TypeName}\" must be a non-negative number.");

            if (options.Count == 2)
            {
                var tzCode = options[1].Trim('\'').ToString();
                return new DateTime64TypeInfo(precision, tzCode, true);
            }

            return new DateTime64TypeInfo(precision, _timeZoneCode, false);
        }

        public IClickHouseColumnTypeInfo Configure(ClickHouseServerInfo serverInfo)
        {
            return new DateTime64TypeInfo(null, serverInfo.Timezone, false);
        }

        private TimeZoneInfo GetTimeZone()
        {
            if (_timeZone != null)
                return _timeZone;

            if (_timeZoneCode == null)
                return TimeZoneInfo.Utc;

            _timeZone = TimeZoneHelper.GetTimeZoneInfo(_timeZoneCode);
            return _timeZone;
        }

        private sealed class DateTime64Reader : StructureReaderBase<ulong, DateTimeOffset>
        {
            private readonly int _precision;
            private readonly TimeZoneInfo _timeZone;

            protected override bool BitwiseCopyAllowed => true;

            public DateTime64Reader(int rowCount, int precision, TimeZoneInfo timeZone)
                : base(sizeof(ulong), rowCount)
            {
                _precision = precision;
                _timeZone = timeZone;
            }

            protected override ulong ReadElement(ReadOnlySpan<byte> source)
            {
                return BitConverter.ToUInt64(source);
            }

            protected override IClickHouseTableColumn<DateTimeOffset> EndRead(ClickHouseColumnSettings? settings, ReadOnlyMemory<ulong> buffer)
            {
                return new DateTime64TableColumn(buffer, _precision, _timeZone);
            }
        }

        private sealed class DateTimeWriter : StructureWriterBase<DateTime, ulong>, IConverter<DateTime, ulong>
        {
            private readonly int _ticksScale;
            private readonly long _ticksMaxValue;
            private readonly TimeZoneInfo _timeZone;

            public DateTimeWriter(string columnName, string columnType, int precision, TimeZoneInfo timeZone, IReadOnlyList<DateTime> rows)
                : base(columnName, columnType, sizeof(ulong), rows)
            {
                _ticksScale = DateTimeTicksScales[precision];
                _ticksMaxValue = DateTimeTicksMaxValues[precision];
                _timeZone = timeZone;
            }

            ulong IConverter<DateTime, ulong>.Convert(DateTime value) => Convert(value);

            protected override ulong Convert(DateTime value)
            {
                ulong ticks;
                if (value == default)
                {
                    ticks = 0;
                }
                else
                {
                    var dateTimeTicks = (value - DateTime.UnixEpoch - _timeZone.GetUtcOffset(value)).Ticks;
                    if (dateTimeTicks < 0 || dateTimeTicks > _ticksMaxValue)
                        throw new OverflowException($"The value must be in range [{DateTime.UnixEpoch:O}, {DateTime.UnixEpoch.AddTicks(_ticksMaxValue):O}].");

                    if (_ticksScale < 0)
                        ticks = checked((ulong) dateTimeTicks * (uint) -_ticksScale);
                    else
                        ticks = (ulong) dateTimeTicks / (uint) _ticksScale;
                }

                return ticks;
            }
        }

        private sealed class DateTimeOffsetWriter : StructureWriterBase<DateTimeOffset, ulong>, IConverter<DateTimeOffset, ulong>
        {
            private readonly int _ticksScale;
            private readonly long _ticksMaxValue;
            private readonly TimeZoneInfo _timeZone;

            public DateTimeOffsetWriter(string columnName, string columnType, int precision, TimeZoneInfo timeZone, IReadOnlyList<DateTimeOffset> rows)
                : base(columnName, columnType, sizeof(ulong), rows)
            {
                _ticksScale = DateTimeTicksScales[precision];
                _ticksMaxValue = DateTimeTicksMaxValues[precision];
                _timeZone = timeZone;
            }

            ulong IConverter<DateTimeOffset, ulong>.Convert(DateTimeOffset value) => Convert(value);

            protected override ulong Convert(DateTimeOffset value)
            {
                ulong ticks;
                if (value == default)
                {
                    ticks = 0;
                }
                else
                {
                    var offset = _timeZone.GetUtcOffset(value);
                    var valueWithOffset = value.ToOffset(offset);
                    var dateTimeTicks = (valueWithOffset - DateTimeOffset.UnixEpoch).Ticks;
                    if (dateTimeTicks < 0 || dateTimeTicks > _ticksMaxValue)
                        throw new OverflowException($"The value must be in range [{DateTimeOffset.UnixEpoch:O}, {DateTimeOffset.UnixEpoch.AddTicks(_ticksMaxValue):O}].");

                    if (_ticksScale < 0)
                        ticks = checked((ulong) dateTimeTicks * (uint) -_ticksScale);
                    else
                        ticks = (ulong) dateTimeTicks / (uint) _ticksScale;
                }

                return ticks;
            }
        }

        private sealed class DateTime64LiteralWriter<T> : IClickHouseLiteralWriter<T>
        {
            private readonly DateTime64TypeInfo _typeInfo;
            private readonly IConverter<T, ulong>? _converter;

            public DateTime64LiteralWriter(DateTime64TypeInfo typeInfo, IConverter<T, ulong>? converter)
            {
                Debug.Assert(converter != null || typeof(T) == typeof(ulong));

                _typeInfo = typeInfo;
                _converter = converter;
            }

            public bool TryCreateParameterValueWriter(T value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter)
            {
                var ticks = Convert(value);
                var str = ticks.ToString(CultureInfo.InvariantCulture);
                valueWriter = new SimpleLiteralValueWriter(str.AsMemory());
                return true;
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, T value)
            {
                var ticks = Convert(value);

                queryBuilder.Append("reinterpret(cast(");
                queryBuilder.Append(ticks.ToString(CultureInfo.InvariantCulture));
                queryBuilder.Append(",'Int64'),'");
                queryBuilder.Append(_typeInfo.ComplexTypeName.Replace("'", "''"));
                queryBuilder.Append("')");

                return queryBuilder;
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseTypeInfo, StringBuilder> writeValue)
            {
                var ticksType = typeInfoProvider.GetTypeInfo("Int64");

                queryBuilder.Append("reinterpret(");
                writeValue(queryBuilder, ticksType);
                queryBuilder.Append(",'");
                queryBuilder.Append(_typeInfo.ComplexTypeName.Replace("'", "''"));
                queryBuilder.Append("')");

                return queryBuilder;
            }

            private ulong Convert(T value)
            {
                if (_converter == null)
                {
                    Debug.Assert(typeof(T) == typeof(ulong));
                    Debug.Assert(value != null);
                    return (ulong)(object)value;
                }

                return _converter.Convert(value);
            }
        }
    }
}
