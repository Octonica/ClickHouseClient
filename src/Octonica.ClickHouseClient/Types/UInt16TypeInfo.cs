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

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if(!(rows is IReadOnlyList<ushort> ushortRows))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return new UInt16Writer(columnName, ComplexTypeName, ushortRows);
        }

        public override Type GetFieldType()
        {
            return typeof(ushort);
        }

        private sealed class UInt16Reader : StructureReaderBase<ushort>
        {
            public UInt16Reader(int rowCount)
                : base(sizeof(ushort), rowCount)
            {
            }

            protected override ushort ReadElement(ReadOnlySpan<byte> source)
            {
                return BitConverter.ToUInt16(source);
            }

            protected override IClickHouseTableColumn<ushort> EndRead(ReadOnlyMemory<ushort> buffer)
            {
                return new UInt16TableColumn(buffer);
            }
        }

        private sealed class UInt16Writer : StructureWriterBase<ushort>
        {
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
