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

using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;
using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed partial class DateTypeInfo : SimpleTypeInfo
    {
        public const string FormatStr = "yyyy-MM-dd";

        private const string DefaultValueStr = "1970-01-01";

        private static readonly DateTime MaxDateTimeValue = DateTime.UnixEpoch.AddDays(ushort.MaxValue);

        public DateTypeInfo()
            : base("Date")
        {
        }

        public override IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new DateReader(rowCount);
        }

        public override IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            return new SimpleSkippingColumnReader(sizeof(ushort), rowCount);
        }

        public override ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Date;
        }

        private sealed partial class DateReader
        {
        }

        private sealed partial class DateWriter
        {
        }

        private sealed class DateTimeLiteralWriter : IClickHouseLiteralWriter<DateTime>
        {
            private readonly DateTypeInfo _typeInfo;

            public DateTimeLiteralWriter(DateTypeInfo typeInfo)
            {
                _typeInfo = typeInfo;
            }

            public bool TryCreateParameterValueWriter(DateTime value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter)
            {
                var strVal = ValueToString(value);
                if (isNested)
                    strVal = $"'{strVal}'";

                valueWriter = new SimpleLiteralValueWriter(strVal.AsMemory());
                return true;
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, DateTime value)
            {
                var strVal = ValueToString(value);
                return queryBuilder.Append('\'').Append(strVal).Append("'::").Append(_typeInfo.ComplexTypeName);
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseColumnTypeInfo, Func<StringBuilder, Func<StringBuilder, StringBuilder>, StringBuilder>, StringBuilder> writeValue)
            {
                return writeValue(queryBuilder, _typeInfo, FunctionHelper.Apply);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static string ValueToString(DateTime value)
            {
                if (value == default)
                    return DefaultValueStr;

                if (value < DateTime.UnixEpoch || value > MaxDateTimeValue)
                    throw new OverflowException($"The value must be in range [{DateTime.UnixEpoch}, {MaxDateTimeValue}].");

                return value.ToString(FormatStr, CultureInfo.InvariantCulture);
            }
        }
    }
}
