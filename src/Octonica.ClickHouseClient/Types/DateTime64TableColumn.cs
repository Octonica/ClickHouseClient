#region License Apache 2.0
/* Copyright 2020-2021 Octonica
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
        private static readonly ulong[] ClickHouseTicksMaxValue;

        private readonly ReadOnlyMemory<ulong> _buffer;
        private readonly int _ticksScale;
        private readonly ulong _maxValue;
        private readonly TimeZoneInfo _timeZone;

        public int RowCount { get; }

        static DateTime64TableColumn()
        {
            var maxValues = new ulong[DateTime64TypeInfo.DateTimeTicksScales.Length];
            var dateTimeTicksMax = (ulong) (DateTime.MaxValue - DateTime.UnixEpoch).Ticks;
            for (int i = 0; i < maxValues.Length; i++)
            {
                var magnitude = DateTime64TypeInfo.DateTimeTicksScales[i];
                if (magnitude < 0)
                {
                    if (dateTimeTicksMax > ulong.MaxValue / (uint) -magnitude)
                        maxValues[i] = ulong.MaxValue;
                    else
                        maxValues[i] = checked(dateTimeTicksMax * (uint) -magnitude);
                }
                else
                {
                    maxValues[i] = dateTimeTicksMax / (uint) magnitude;
                }
            }

            ClickHouseTicksMaxValue = maxValues;
        }

        public DateTime64TableColumn(ReadOnlyMemory<ulong> buffer, int precision, TimeZoneInfo timeZone)
        {
            _buffer = buffer;
            _ticksScale = DateTime64TypeInfo.DateTimeTicksScales[precision];
            _maxValue = ClickHouseTicksMaxValue[precision];
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
            if (ticks == 0)
                return default;

            if (ticks > _maxValue)
            {
                throw new OverflowException(
                    $"The value 0x{ticks:X} is greater than the maximal value of the type \"{typeof(DateTime)}\". " +
                    $"It is only possible to read this value as \"{typeof(ulong)}\".");
            }

            if (_ticksScale < 0)
                ticks /= (uint) -_ticksScale;
            else
                ticks = checked(ticks * (uint) _ticksScale);

            var dateTime = DateTime.UnixEpoch.AddTicks(checked((long) ticks));
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

            var rawColumn = new StructureTableColumn<ulong>(_buffer);
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
