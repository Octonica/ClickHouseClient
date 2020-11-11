#region License Apache 2.0
/* Copyright 2019-2020 Octonica
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
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class UInt32TypeInfo : SimpleTypeInfo
    {
        public UInt32TypeInfo()
            : base("UInt32")
        {
        }

        public override IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new UInt32Reader(rowCount);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (!(rows is IReadOnlyList<uint> uintRows))
            {
                if (rows is IReadOnlyList<ushort> ushortRows)
                    uintRows = new MappedReadOnlyList<ushort, uint>(ushortRows, v => v);
                else if (rows is IReadOnlyList<byte> byteRows)
                    uintRows = new MappedReadOnlyList<byte, uint>(byteRows, v => v);
                else
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
            }

            return new UInt32Writer(columnName, ComplexTypeName, uintRows);
        }

        public override Type GetFieldType()
        {
            return typeof(uint);
        }

        public override ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.UInt32;
        }

        private sealed class UInt32Reader : StructureReaderBase<uint>
        {
            public UInt32Reader(int rowCount)
                : base(sizeof(uint), rowCount)
            {
            }

            protected override uint ReadElement(ReadOnlySpan<byte> source)
            {
                return BitConverter.ToUInt32(source);
            }

            protected override IClickHouseTableColumn<uint> EndRead(ReadOnlyMemory<uint> buffer)
            {
                return new UInt32TableColumn(buffer);
            }
        }

        private sealed class UInt32Writer : StructureWriterBase<uint>
        {
            public UInt32Writer(string columnName, string columnType, IReadOnlyList<uint> rows)
                : base(columnName, columnType, sizeof(uint), rows)
            {
            }

            protected override void WriteElement(Span<byte> writeTo, in uint value)
            {
                var success = BitConverter.TryWriteBytes(writeTo, value);
                Debug.Assert(success);
            }
        }
    }
}
