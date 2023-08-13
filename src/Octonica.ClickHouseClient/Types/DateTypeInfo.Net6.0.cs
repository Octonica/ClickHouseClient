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
    partial class DateTypeInfo
    {
        private static readonly DateOnly UnixEpoch = DateOnly.FromDateTime(DateTime.UnixEpoch);

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            IReadOnlyList<DateOnly> dateOnlyRows;
            if (typeof(T) == typeof(DateOnly))
                dateOnlyRows = (IReadOnlyList<DateOnly>)rows;
            else if (typeof(T) == typeof(DateTime))
                dateOnlyRows = ((IReadOnlyList<DateTime>)rows).Map(DateOnly.FromDateTime);
            else
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return new DateWriter(columnName, ComplexTypeName, dateOnlyRows);
        }

        public override IClickHouseLiteralWriter<T> CreateLiteralWriter<T>()
        {
            var type = typeof(T);
            if (type == typeof(DBNull))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values.");

            object writer = default(T) switch
            {
                DateOnly theValue => new SimpleLiteralWriter<DateOnly, ushort>(this, DateOnlyToDays),
                DateTime theValue => new SimpleLiteralWriter<DateTime, ushort>(this, dt => DateOnlyToDays(DateOnly.FromDateTime(dt))),
                _ => throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".")
            };

            return (IClickHouseLiteralWriter<T>)writer;
        }

        public override Type GetFieldType()
        {
            return typeof(DateOnly);
        }

        private static ushort DateOnlyToDays(DateOnly value)
        {
            if (value == default)
                return 0;

            var days = value.DayNumber - UnixEpoch.DayNumber;
            if (days < 0 || days > ushort.MaxValue)
                throw new OverflowException("The value must be in range [1970-01-01, 2149-06-06].");

            return (ushort)days;
        }

        partial class DateReader : StructureReaderBase<DateOnly>
        {
            public DateReader(int rowCount)
                : base(sizeof(ushort), rowCount)
            {
            }

            protected override DateOnly ReadElement(ReadOnlySpan<byte> source)
            {
                var value = BitConverter.ToUInt16(source);
                if (value == 0)
                    return default;

                return UnixEpoch.AddDays(value);
            }

            protected override IClickHouseTableColumn<DateOnly> EndRead(ClickHouseColumnSettings? settings, ReadOnlyMemory<DateOnly> buffer)
            {
                return new DateOnlyTableColumn(buffer);
            }
        }

        partial class DateWriter : StructureWriterBase<DateOnly, ushort>
        {
            public DateWriter(string columnName, string columnType, IReadOnlyList<DateOnly> rows)
                : base(columnName, columnType, sizeof(ushort), rows)
            {
            }

            protected override ushort Convert(DateOnly value)
            {
                return DateOnlyToDays(value);
            }
        }
    }
}

#endif