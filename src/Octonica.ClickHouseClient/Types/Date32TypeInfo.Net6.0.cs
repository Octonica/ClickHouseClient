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
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;

namespace Octonica.ClickHouseClient.Types
{
    internal partial class Date32TypeInfo
    {
        private static readonly DateOnly UnixEpoch = DateOnly.FromDateTime(DateTime.UnixEpoch);

        private static readonly DateOnly MinDateOnlyValue = UnixEpoch.AddDays(MinValue);
        private static readonly DateOnly MaxDateOnlyValue = UnixEpoch.AddDays(MaxValue);

        public override Type GetFieldType()
        {
            return typeof(DateOnly);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            IReadOnlyList<DateOnly> dateOnlyRows = typeof(T) == typeof(DateOnly)
                ? (IReadOnlyList<DateOnly>)rows
                : typeof(T) == typeof(DateTime)
                ? ((IReadOnlyList<DateTime>)rows).Map(DateOnly.FromDateTime)
                : throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
            return new Date32Writer(columnName, ComplexTypeName, dateOnlyRows);
        }

        public override IClickHouseParameterWriter<T> CreateParameterWriter<T>()
        {
            Type type = typeof(T);
            if (type == typeof(DBNull))
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values.");
            }

            object writer = default(T) switch
            {
                DateOnly => new DateOnlyParameterWriter(this),
                DateTime => new DateTimeParameterWriter(this),
                _ => throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".")
            };

            return (IClickHouseParameterWriter<T>)writer;
        }

        private static int DateOnlyToDays(DateOnly value)
        {
            if (value == default)
            {
                return MinValue;
            }

            int days = value.DayNumber - UnixEpoch.DayNumber;
            return days is < MinValue or > MaxValue
                ? throw new OverflowException("The value must be in range [1925-01-01, 2283-11-11].")
                : days;
        }

        private partial class Date32Reader : StructureReaderBase<int, DateOnly>
        {
            protected override IClickHouseTableColumn<DateOnly> EndRead(ClickHouseColumnSettings? settings, ReadOnlyMemory<int> buffer)
            {
                return new Date32TableColumn(buffer);
            }
        }

        private partial class Date32Writer : StructureWriterBase<DateOnly, int>
        {
            public Date32Writer(string columnName, string columnType, IReadOnlyList<DateOnly> rows)
                : base(columnName, columnType, sizeof(int), rows)
            {
            }

            protected override int Convert(DateOnly value)
            {
                return DateOnlyToDays(value);
            }
        }

        private sealed class DateOnlyParameterWriter : IClickHouseParameterWriter<DateOnly>
        {
            private readonly Date32TypeInfo _typeInfo;

            public DateOnlyParameterWriter(Date32TypeInfo typeInfo)
            {
                _typeInfo = typeInfo;
            }

            public bool TryCreateParameterValueWriter(DateOnly value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter)
            {
                string strVal = ValueToString(value);
                if (isNested)
                {
                    strVal = $"'{strVal}'";
                }

                valueWriter = new SimpleLiteralValueWriter(strVal.AsMemory());
                return true;
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, DateOnly value)
            {
                string strVal = ValueToString(value);
                return queryBuilder.Append('\'').Append(strVal).Append("'::").Append(_typeInfo.ComplexTypeName);
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseColumnTypeInfo, Func<StringBuilder, Func<StringBuilder, StringBuilder>, StringBuilder>, StringBuilder> writeValue)
            {
                return writeValue(queryBuilder, _typeInfo, FunctionHelper.Apply);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static string ValueToString(DateOnly value)
            {
                return value == default
                    ? DefaultValueStr
                    : value < MinDateOnlyValue || value > MaxDateOnlyValue
                    ? throw new OverflowException($"The value must be in range [{MinDateOnlyValue}, {MaxDateOnlyValue}].")
                    : value.ToString(FormatStr, CultureInfo.InvariantCulture);
            }
        }
    }
}

#endif