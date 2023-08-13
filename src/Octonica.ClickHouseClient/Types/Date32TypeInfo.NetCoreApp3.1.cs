#region License Apache 2.0
/* Copyright 2021, 2023 Octonica
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

#if NETCOREAPP3_1_OR_GREATER && !NET6_0_OR_GREATER

using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using System;
using System.Collections.Generic;

namespace Octonica.ClickHouseClient.Types
{
    partial class Date32TypeInfo
    {
        public override Type GetFieldType()
        {
            return typeof(DateTime);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (typeof(T) != typeof(DateTime))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return new Date32Writer(columnName, ComplexTypeName, (IReadOnlyList<DateTime>)rows);
        }

        public override IClickHouseLiteralWriter<T> CreateLiteralWriter<T>()
        {
            var type = typeof(T);
            if (type == typeof(DBNull))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values.");

            if (type == typeof(DateTime))
                return (IClickHouseLiteralWriter<T>)(object)new SimpleLiteralWriter<DateTime, int>(this, DateTimeToDays);

            throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
        }

        private static int DateTimeToDays(DateTime value)
        {
            if (value == default)
                return MinValue;

            var days = (value - DateTime.UnixEpoch).TotalDays;
            if (days < MinValue || days > MaxValue)
                throw new OverflowException("The value must be in range [1925-01-01, 2283-11-11].");

            return (int)days;
        }

        partial class Date32Reader : StructureReaderBase<int, DateTime>
        {
            protected override IClickHouseTableColumn<DateTime> EndRead(ClickHouseColumnSettings? settings, ReadOnlyMemory<int> buffer)
            {
                return new Date32TableColumn(buffer);
            }
        }

        partial class Date32Writer : StructureWriterBase<DateTime, int>
        {
            public Date32Writer(string columnName, string columnType, IReadOnlyList<DateTime> rows)
                : base(columnName, columnType, sizeof(int), rows)
            {
            }

            protected override int Convert(DateTime value)
            {
                return DateTimeToDays(value);
            }
        }
    }
}

#endif