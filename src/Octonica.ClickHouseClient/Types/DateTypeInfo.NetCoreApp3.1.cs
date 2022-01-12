#region License Apache 2.0
/* Copyright 2019-2021 Octonica
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
using System.Globalization;
using System.Text;

namespace Octonica.ClickHouseClient.Types
{
    partial class DateTypeInfo
    {
        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (typeof(T) != typeof(DateTime))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return new DateWriter(columnName, ComplexTypeName, (IReadOnlyList<DateTime>)rows);
        }

        public override void FormatValue(StringBuilder queryStringBuilder, object? value)
        {
            if (value == null || value is DBNull)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values");

            DateTime dateTimeValue = value switch
            {
                DateTime theValue => theValue,
                _ => throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{value.GetType()}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\"."),
            };
            
            var days = dateTimeValue == default ? 0 : (dateTimeValue - DateTime.UnixEpoch).TotalDays;
            
            if (days < 0 || days > ushort.MaxValue)
                throw new OverflowException("The value must be in range [1970-01-01, 2149-06-06].");

            queryStringBuilder.Append(days.ToString(CultureInfo.InvariantCulture));
        }

        public override Type GetFieldType()
        {
            return typeof(DateTime);
        }

        partial class DateReader : StructureReaderBase<DateTime>
        {
            public DateReader(int rowCount)
                : base(sizeof(ushort), rowCount)
            {
            }

            protected override DateTime ReadElement(ReadOnlySpan<byte> source)
            {
                var value = BitConverter.ToUInt16(source);
                if (value == 0)
                    return default;

                return DateTime.UnixEpoch.AddDays(value);
            }
        }

        partial class DateWriter : StructureWriterBase<DateTime, ushort>
        {
            public DateWriter(string columnName, string columnType, IReadOnlyList<DateTime> rows)
                : base(columnName, columnType, sizeof(ushort), rows)
            {
            }

            protected override ushort Convert(DateTime value)
            {
                if (value == default)
                    return 0;

                var days = (value - DateTime.UnixEpoch).TotalDays;
                if (days < 0 || days > ushort.MaxValue)
                    throw new OverflowException("The value must be in range [1970-01-01, 2149-06-06].");

                return (ushort)days;
            }
        }
    }
}

#endif