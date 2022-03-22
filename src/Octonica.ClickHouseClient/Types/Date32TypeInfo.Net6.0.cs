#region License Apache 2.0
/* Copyright 2021 Octonica
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

#if NET6_0_OR_GREATER

using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Octonica.ClickHouseClient.Types
{
    partial class Date32TypeInfo
    {
        private static readonly DateOnly UnixEpoch = DateOnly.FromDateTime(DateTime.UnixEpoch);

        public override Type GetFieldType()
        {
            return typeof(DateOnly);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            IReadOnlyList<DateOnly> dateOnlyRows;
            if (typeof(T) == typeof(DateOnly))
                dateOnlyRows = (IReadOnlyList<DateOnly>)rows;
            else if (typeof(T) == typeof(DateTime))
                dateOnlyRows = ((IReadOnlyList<DateTime>)rows).Map(DateOnly.FromDateTime);
            else
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return new Date32Writer(columnName, ComplexTypeName, dateOnlyRows);
        }

        public override void FormatValue(StringBuilder queryStringBuilder, object? value)
        {
            if (value == null || value is DBNull)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values");

            int days;

            if (value is DateOnly dateOnlyValue)
                days = dateOnlyValue == default ?
                    MinValue :
                    dateOnlyValue.DayNumber - UnixEpoch.DayNumber;
            else if (value is DateTime dateTimeValue)
                days = dateTimeValue == default ?
                    MinValue :
                    DateOnly.FromDateTime(dateTimeValue).DayNumber - UnixEpoch.DayNumber;
            else
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{value.GetType()}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
            
            if (days < MinValue || days > MaxValue)
                throw new OverflowException("The value must be in range [1925-01-01, 2283-11-11].");

            queryStringBuilder.Append(days.ToString(CultureInfo.InvariantCulture));
        }

        partial class Date32Reader : StructureReaderBase<int, DateOnly>
        {
            protected override IClickHouseTableColumn<DateOnly> EndRead(ClickHouseColumnSettings? settings, ReadOnlyMemory<int> buffer)
            {
                return new Date32TableColumn(buffer);
            }
        }

        partial class Date32Writer : StructureWriterBase<DateOnly, int>
        {
            public Date32Writer(string columnName, string columnType, IReadOnlyList<DateOnly> rows)
                : base(columnName, columnType, sizeof(int), rows)
            {
            }

            protected override int Convert(DateOnly value)
            {
                if (value == default)
                    return MinValue;

                var days = value.DayNumber - UnixEpoch.DayNumber;
                if (days < MinValue || days > MaxValue)
                    throw new OverflowException("The value must be in range [1925-01-01, 2283-11-11].");

                return days;
            }
        }
    }
}

#endif