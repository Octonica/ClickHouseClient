#region License Apache 2.0
/* Copyright 2020-2021, 2024 Octonica
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

using NodaTime;
using System;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class DateTime64TableColumn : IClickHouseTableColumn<DateTimeOffset>
    {
        private static readonly (long min, long max)[] ClickHouseTicksRange;
        private readonly ReadOnlyMemory<long> _buffer;
        private readonly (long min, long max) _range;
        private readonly DateTimeZone _timeZone;
        public int RowCount { get; }
        public DateTimeOffset DefaultValue => DateTimeZone.Utc.AtStrictly(LocalDateTime.FromDateTime(DateTime.MinValue)).ToDateTimeOffset();

        static DateTime64TableColumn()
        {
            (long min, long max)[] ranges = new (long min, long max)[DateTime64TypeInfo.DateTimeTicksScales.Length];
            long dateTimeTicksMax = (DateTime.MaxValue - DateTime.UnixEpoch).Ticks;
            long dateTimeTicksMin = (DateTime.MinValue - DateTime.UnixEpoch).Ticks;
            for (int i = 0; i < ranges.Length; i++)
            {
                long min, max;
                int magnitude = DateTime64TypeInfo.DateTimeTicksScales[i];
                if (magnitude < 0)
                {
                    max = dateTimeTicksMax > long.MaxValue / -magnitude ? long.MaxValue : dateTimeTicksMax * -magnitude;

                    min = dateTimeTicksMin < long.MinValue / -magnitude ? long.MinValue : dateTimeTicksMin * -magnitude;
                }
                else
                {
                    max = dateTimeTicksMax / magnitude;
                    min = dateTimeTicksMin / magnitude;
                }

                ranges[i] = (min, max);
            }

            ClickHouseTicksRange = ranges;
        }

        public DateTime64TableColumn(ReadOnlyMemory<long> buffer, int precision, DateTimeZone timeZone)
        {
            _buffer = buffer;
            _range = ClickHouseTicksRange[precision];
            _timeZone = timeZone;
            RowCount = buffer.Length;
        }

        public bool IsNull(int index)
        {
            return false;
        }

        public DateTimeOffset GetValue(int index)
        {
            long ticks = _buffer.Span[index];
            if (ticks < _range.min)
            {
                throw new OverflowException(
                    $"The value 0x{ticks:X} is lesser than the minimal value of the type \"{typeof(DateTime)}\". " +
                    $"It is only possible to read this value as \"{typeof(long)}\".");
            }

            if (ticks > _range.max)
            {
                throw new OverflowException(
                    $"The value 0x{ticks:X} is greater than the maximal value of the type \"{typeof(DateTime)}\". " +
                    $"It is only possible to read this value as \"{typeof(long)}\".");
            }

            Instant instant = Instant.FromUnixTimeMilliseconds(ticks);
            ZonedDateTime zonedDateTime = instant.InZone(_timeZone);

            return zonedDateTime.ToDateTimeOffset();
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            if (typeof(T) == typeof(DateTime))
            {
                return (IClickHouseTableColumn<T>)(object)new ReinterpretedTableColumn<DateTimeOffset, DateTime>(this, dto => dto.DateTime);
            }

            if (typeof(T) == typeof(DateTime?))
            {
                return (IClickHouseTableColumn<T>)(object)new NullableStructTableColumn<DateTime>(null, new ReinterpretedTableColumn<DateTimeOffset, DateTime>(this, dto => dto.DateTime));
            }

            if (typeof(T) == typeof(DateTimeOffset?))
            {
                return (IClickHouseTableColumn<T>)(object)new NullableStructTableColumn<DateTimeOffset>(null, this);
            }

            StructureTableColumn<long> rawColumn = new(_buffer);
            IClickHouseTableColumn<T>? rawReinterpreted = rawColumn as IClickHouseTableColumn<T> ?? rawColumn.TryReinterpret<T>();
            return rawReinterpreted != null ? new ReinterpretedTableColumn<T>(this, rawReinterpreted) : (IClickHouseTableColumn<T>?)null;
        }

        bool IClickHouseTableColumn.TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, out T dispatchedValue)
        {
            dispatchedValue = dispatcher.Dispatch(this);
            return true;
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            return GetValue(index);
        }
    }
}
