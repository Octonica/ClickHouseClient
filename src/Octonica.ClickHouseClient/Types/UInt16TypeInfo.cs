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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class UInt16TypeInfo : SimpleTypeInfo
    {
        public UInt16TypeInfo()
            : base("UInt16")
        {
        }

        public override IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new UInt16Reader(rowCount);
        }

        public override IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            return new SimpleSkippingColumnReader(sizeof(ushort), rowCount);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            var type = typeof(T);
            IReadOnlyList<ushort> ushortRows;
            if (type == typeof(ushort))
                ushortRows = (IReadOnlyList<ushort>)rows;
            else if (type == typeof(byte))
                ushortRows = MappedReadOnlyList<byte, ushort>.Map((IReadOnlyList<byte>)rows, v => v);
            else
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return new UInt16Writer(columnName, ComplexTypeName, ushortRows);
        }

        public override void FormatValue(StringBuilder queryStringBuilder, object? value)
        {
            if (value == null || value is DBNull)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values");


            ushort outputValue;
            
            if (value is ushort ushortValue)
                outputValue = ushortValue;
            else if (value is byte byteValue)
                outputValue = byteValue;
            else
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{value.GetType()}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
            
            queryStringBuilder.Append(outputValue.ToString(CultureInfo.InvariantCulture));
        }

        public override Type GetFieldType()
        {
            return typeof(ushort);
        }

        public override ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.UInt16;
        }

        private sealed class UInt16Reader : StructureReaderBase<ushort>
        {
            protected override bool BitwiseCopyAllowed => true;

            public UInt16Reader(int rowCount)
                : base(sizeof(ushort), rowCount)
            {
            }

            protected override ushort ReadElement(ReadOnlySpan<byte> source)
            {
                return BitConverter.ToUInt16(source);
            }

            protected override IClickHouseTableColumn<ushort> EndRead(ClickHouseColumnSettings? settings, ReadOnlyMemory<ushort> buffer)
            {
                return new UInt16TableColumn(buffer);
            }
        }

        private sealed class UInt16Writer : StructureWriterBase<ushort>
        {
            protected override bool BitwiseCopyAllowed => true;

            public UInt16Writer(string columnName, string columnType, IReadOnlyList<ushort> rows)
                : base(columnName, columnType, sizeof(ushort), rows)
            {
            }

            protected override void WriteElement(Span<byte> writeTo, in ushort value)
            {
                var success = BitConverter.TryWriteBytes(writeTo, value);
                Debug.Assert(success);
            }
        }
    }
}
