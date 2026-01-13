#region License Apache 2.0
/* Copyright 2021, 2023-2024 Octonica
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
    internal sealed partial class Date32TypeInfo : SimpleTypeInfo
    {
        public const int MinValue = -25567;
        public const int MaxValue = 120529;

        private const string FormatStr = DateTypeInfo.FormatStr;
        private const string DefaultValueStr = "1900-01-01";

        private static readonly DateTime MinDateTimeValue = DateTime.UnixEpoch.AddDays(MinValue);
        private static readonly DateTime MaxDateTimeValue = DateTime.UnixEpoch.AddDays(MaxValue + 1).Subtract(TimeSpan.FromTicks(1));

        public Date32TypeInfo()
            : base("Date32")
        {
        }

        public override IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new Date32Reader(rowCount);
        }

        public override IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            return new SimpleSkippingColumnReader(sizeof(uint), rowCount);
        }

        public override ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Date32;
        }

        private sealed partial class Date32Reader
        {
            protected override bool BitwiseCopyAllowed => true;

            public Date32Reader(int rowCount)
                : base(sizeof(int), rowCount)
            {
            }

            protected override int ReadElement(ReadOnlySpan<byte> source)
            {
                return BitConverter.ToInt32(source);
            }
        }

        private sealed partial class Date32Writer
        {
        }

        private sealed class DateTimeParameterWriter : IClickHouseParameterWriter<DateTime>
        {
            private readonly Date32TypeInfo _typeInfo;

            public DateTimeParameterWriter(Date32TypeInfo typeInfo)
            {
                _typeInfo = typeInfo;
            }

            public bool TryCreateParameterValueWriter(DateTime value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter)
            {
                string strVal = ValueToString(value);
                valueWriter = new SimpleLiteralValueWriter(strVal.AsMemory());
                return true;
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, DateTime value)
            {
                string strVal = ValueToString(value);
                return queryBuilder.Append('\'').Append(strVal).Append("\'::").Append(_typeInfo.ComplexTypeName);
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseColumnTypeInfo, Func<StringBuilder, Func<StringBuilder, StringBuilder>, StringBuilder>, StringBuilder> writeValue)
            {
                return writeValue(queryBuilder, _typeInfo, FunctionHelper.Apply);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static string ValueToString(DateTime value)
            {
                return value == default
                    ? DefaultValueStr
                    : value < MinDateTimeValue || value > MaxDateTimeValue
                    ? throw new OverflowException($"The value must be in range [{MinDateTimeValue}, {MaxDateTimeValue}].")
                    : value.ToString(FormatStr, CultureInfo.InvariantCulture);
            }
        }
    }
}
