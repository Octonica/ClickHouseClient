#region License Apache 2.0
/* Copyright 2020 Octonica
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
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using TimeZoneConverter;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class DateTime64TypeInfo : IClickHouseConfigurableTypeInfo
    {
        internal static readonly int[] DateTimeTicksScales;
        private static readonly long[] DateTimeTicksMaxValues;

        public const int DefaultPrecision = 3;

        private readonly int? _precision;
        private readonly TimeZoneInfo _timeZone;

        public string ComplexTypeName { get; }

        public string TypeName => "DateTime64";

        public int GenericArgumentsCount => 0;

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
            : this(null, TimeZoneInfo.Utc, null)
        {
        }

        private DateTime64TypeInfo(int? precision, TimeZoneInfo timeZone, string? timeZoneCode)
        {
            if (precision != null)
            {
                if (precision.Value < 0)
                    throw new ArgumentOutOfRangeException(nameof(precision), "The precision must be a non-negative number.");
                if (precision.Value >= DateTimeTicksScales.Length)
                    throw new ArgumentOutOfRangeException(nameof(precision), $"The precision can't be greater than {DateTimeTicksScales.Length - 1}.");
            }

            _timeZone = timeZone;
            _precision = precision;

            if (timeZoneCode != null)
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

        public IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new DateTime64Reader(rowCount, _precision ?? DefaultPrecision, _timeZone);
        }

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (rows is IReadOnlyList<DateTime> dateTimeRows)
                return new DateTimeWriter(columnName, ComplexTypeName, _precision ?? DefaultPrecision, _timeZone, dateTimeRows);

            if (rows is IReadOnlyList<DateTimeOffset> dateTimeOffsetRows)
                return new DateTimeOffsetWriter(columnName, ComplexTypeName, _precision ?? DefaultPrecision, _timeZone, dateTimeOffsetRows);

            if (rows is IReadOnlyList<ulong> uint64Rows)
                return new UInt64TypeInfo.UInt64Writer(columnName, ComplexTypeName, uint64Rows);

            throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
        }

        public IClickHouseColumnTypeInfo GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            if (options.Count > 2)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"Too many arguments in the definition of \"{TypeName}\".");

            if (!int.TryParse(options[0].Span, NumberStyles.Integer, CultureInfo.InvariantCulture, out var precision) || precision < 0)
                throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The precision value for the type \"{TypeName}\" must be a non-negative number.");

            string? tzCode = null;
            TimeZoneInfo? timeZone = null;
            if (options.Count == 2)
            {
                tzCode = options[1].Trim('\'').ToString();
                timeZone = TZConvert.GetTimeZoneInfo(tzCode);
            }

            return new DateTime64TypeInfo(precision, timeZone ?? _timeZone, tzCode);
        }

        public IClickHouseColumnTypeInfo Configure(ClickHouseServerInfo serverInfo)
        {
            var timezone = TZConvert.GetTimeZoneInfo(serverInfo.Timezone);
            return new DateTime64TypeInfo(null, timezone, null);
        }

        private sealed class DateTime64Reader : IClickHouseColumnReader
        {
            private readonly int _precision;
            private readonly TimeZoneInfo _timeZone;
            private readonly Memory<ulong> _buffer;

            private int _position;

            public DateTime64Reader(int rowCount, int precision, TimeZoneInfo timeZone)
            {
                _buffer = new Memory<ulong>(new ulong[rowCount]);
                _precision = precision;
                _timeZone = timeZone;
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                if (_position >= _buffer.Length)
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");

                Span<byte> tmpSpan = stackalloc byte[sizeof(ulong)];
                int count = 0, maxElementsCount = _buffer.Length - _position;
                for (var slice = sequence; slice.Length >= sizeof(ulong) && count < maxElementsCount; slice = slice.Slice(sizeof(ulong)), count++)
                {
                    if (slice.FirstSpan.Length > sizeof(ulong))
                    {
                        _buffer.Span[_position++] = BitConverter.ToUInt64(slice.FirstSpan);
                    }
                    else
                    {
                        slice.Slice(0, sizeof(ulong)).CopyTo(tmpSpan);
                        _buffer.Span[_position++] = BitConverter.ToUInt64(tmpSpan);
                    }
                }

                return new SequenceSize(count * sizeof(ulong), count);
            }

            public SequenceSize Skip(ReadOnlySequence<byte> sequence, int maxElementsCount, ref object? skipContext)
            {
                var availableElementsCount = (int) sequence.Length / sizeof(uint);
                var elementsCount = Math.Min(availableElementsCount, maxElementsCount);
                return new SequenceSize(elementsCount * sizeof(uint), elementsCount);
            }

            public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
            {
                return new DateTime64TableColumn(_buffer.Slice(0, _position), _precision, _timeZone);
            }
        }

        private sealed class DateTimeWriter : StructureWriterBase<DateTime>
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

            protected override void WriteElement(Span<byte> writeTo, in DateTime value)
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

                var success = BitConverter.TryWriteBytes(writeTo, ticks);
                Debug.Assert(success);
            }
        }

        private sealed class DateTimeOffsetWriter : StructureWriterBase<DateTimeOffset>
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

            protected override void WriteElement(Span<byte> writeTo, in DateTimeOffset value)
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

                var success = BitConverter.TryWriteBytes(writeTo, ticks);
                Debug.Assert(success);
            }
        }
    }
}
