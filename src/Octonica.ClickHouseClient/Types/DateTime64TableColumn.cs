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

using System;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class DateTime64TableColumn : IClickHouseTableColumn<DateTimeOffset>
    {
        private static readonly (long min, long max)[] ClickHouseTicksRange;

        private readonly ReadOnlyMemory<long> _buffer;
        private readonly int _ticksScale;
        private readonly (long min, long max) _range;
        private readonly TimeZoneInfo _timeZone;

        public int RowCount { get; }

        public DateTimeOffset DefaultValue => new DateTimeOffset(DateTime.UnixEpoch).ToOffset(_timeZone.GetUtcOffset(DateTime.UnixEpoch));

        static DateTime64TableColumn()
        {
            var ranges = new (long min, long max)[DateTime64TypeInfo.DateTimeTicksScales.Length];
            var dateTimeTicksMax = (DateTime.MaxValue - DateTime.UnixEpoch).Ticks;
            var dateTimeTicksMin = (DateTime.MinValue - DateTime.UnixEpoch).Ticks;
            for (int i = 0; i < ranges.Length; i++)
            {
                long min, max;
                var magnitude = DateTime64TypeInfo.DateTimeTicksScales[i];
                if (magnitude < 0)
                {
                    if (dateTimeTicksMax > long.MaxValue / -magnitude)
                        max = long.MaxValue;
                    else
                        max = checked(dateTimeTicksMax * -magnitude);

                    if (dateTimeTicksMin < long.MinValue / -magnitude)
                        min = long.MinValue;
                    else
                        min = checked(dateTimeTicksMin * -magnitude);
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

        public DateTime64TableColumn(ReadOnlyMemory<long> buffer, int precision, TimeZoneInfo timeZone)
        {
            _buffer = buffer;
            _ticksScale = DateTime64TypeInfo.DateTimeTicksScales[precision];
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
            var ticks = _buffer.Span[index];
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

            if (_ticksScale < 0)
                ticks /= -_ticksScale;
            else
                ticks = checked(ticks * _ticksScale);

            var dateTime = DateTime.UnixEpoch.AddTicks(ticks);
            var offset = _timeZone.GetUtcOffset(dateTime);
            return new DateTimeOffset(dateTime).ToOffset(offset);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            if (typeof(T) == typeof(DateTime))
                return (IClickHouseTableColumn<T>) (object) new ReinterpretedTableColumn<DateTimeOffset, DateTime>(this, dto => dto.DateTime);

            if (typeof(T) == typeof(DateTime?))
                return (IClickHouseTableColumn<T>) (object) new NullableStructTableColumn<DateTime>(null, new ReinterpretedTableColumn<DateTimeOffset, DateTime>(this, dto => dto.DateTime));

            if (typeof(T) == typeof(DateTimeOffset?))
                return (IClickHouseTableColumn<T>) (object) new NullableStructTableColumn<DateTimeOffset>(null, this);

            var rawColumn = new StructureTableColumn<long>(_buffer);
            var rawReinterpreted = rawColumn as IClickHouseTableColumn<T> ?? rawColumn.TryReinterpret<T>();
            if (rawReinterpreted != null)
                return new ReinterpretedTableColumn<T>(this, rawReinterpreted);

            return null;
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
