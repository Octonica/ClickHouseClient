#region License Apache 2.0
/* Copyright 2019-2021, 2024 Octonica
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
    internal sealed class DateTimeTableColumn : IClickHouseTableColumn<DateTimeOffset>
    {
        private readonly ReadOnlyMemory<uint> _buffer;
        private readonly DateTimeZone _timeZone;

        public int RowCount { get; }

        public DateTimeOffset DefaultValue => default;

        public DateTimeTableColumn(ReadOnlyMemory<uint> buffer, DateTimeZone timeZone)
        {
            _buffer = buffer;
            _timeZone = timeZone;
            RowCount = buffer.Length;
        }

        public bool IsNull(int index)
        {
            return false;
        }

        public DateTimeOffset GetValue(int index)
        {
            uint seconds = _buffer.Span[index];
            return seconds == 0 ? default : Instant.FromUnixTimeSeconds(seconds).InZone(_timeZone).ToDateTimeOffset();
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            return typeof(T) == typeof(DateTime)
                ? (IClickHouseTableColumn<T>)(object)new ReinterpretedTableColumn<DateTimeOffset, DateTime>(this, dto => dto.DateTime)
                : typeof(T) == typeof(DateTime?)
                ? (IClickHouseTableColumn<T>)(object)new NullableStructTableColumn<DateTime>(null, new ReinterpretedTableColumn<DateTimeOffset, DateTime>(this, dto => dto.DateTime))
                : typeof(T) == typeof(DateTimeOffset?)
                ? (IClickHouseTableColumn<T>)(object)new NullableStructTableColumn<DateTimeOffset>(null, this)
                : null;
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
