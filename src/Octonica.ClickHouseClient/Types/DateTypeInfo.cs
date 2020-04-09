#region License Apache 2.0
/* Copyright 2019-2020 Octonica LLC
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
using System.Collections.Generic;
using System.Diagnostics;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class DateTypeInfo : SimpleTypeInfo
    {
        public DateTypeInfo()
            : base("Date")
        {
        }

        public override IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new DateReader(rowCount);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (!(rows is IReadOnlyList<DateTime> dateTimeRows))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return new DateWriter(columnName, ComplexTypeName, dateTimeRows);
        }

        public override Type GetFieldType()
        {
            return typeof(DateTime);
        }

        private sealed class DateReader : StructureReaderBase<DateTime>
        {
            public DateReader(int rowCount)
                : base(sizeof(ushort), rowCount)
            {
            }

            protected override DateTime ReadElement(ReadOnlySpan<byte> source)
            {
                var value = BitConverter.ToUInt16(source);
                return DateTime.UnixEpoch.AddDays(value);
            }
        }

        private sealed class DateWriter : StructureWriterBase<DateTime>
        {
            public DateWriter(string columnName, string columnType, IReadOnlyList<DateTime> rows)
                : base(columnName, columnType, sizeof(ushort), rows)
            {
            }

            protected override void WriteElement(Span<byte> writeTo, in DateTime value)
            {
                ushort days = value == default ? (ushort) 0 : (ushort) (value - DateTime.UnixEpoch).TotalDays;
                var success = BitConverter.TryWriteBytes(writeTo, days);
                Debug.Assert(success);
            }
        }
    }
}
