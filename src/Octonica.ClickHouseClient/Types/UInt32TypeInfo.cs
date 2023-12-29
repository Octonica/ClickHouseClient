#region License Apache 2.0
/* Copyright 2019-2021, 2023 Octonica
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

        public override IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            return new SimpleSkippingColumnReader(sizeof(uint), rowCount);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            var type = typeof(T);
            IReadOnlyList<uint> uintRows;

            if (type == typeof(uint))
                uintRows = (IReadOnlyList<uint>)rows;
            else if (type == typeof(ushort))
                uintRows = MappedReadOnlyList<ushort, uint>.Map((IReadOnlyList<ushort>)rows, v => v);
            else if (type == typeof(byte))
                uintRows = MappedReadOnlyList<byte, uint>.Map((IReadOnlyList<byte>)rows, v => v);
            else
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
            
            return new UInt32Writer(columnName, ComplexTypeName, uintRows);
        }

        public override IClickHouseParameterWriter<T> CreateParameterWriter<T>()
        {
            var type = typeof(T);
            if (type == typeof(DBNull))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values.");

            object writer = default(T) switch
            {
                uint _ => new SimpleParameterWriter<uint>(this, appendTypeCast: true),
                ushort _ => new SimpleParameterWriter<ushort, uint>(this, appendTypeCast: true, v => v),
                byte _ => new SimpleParameterWriter<byte, uint>(this, appendTypeCast: true, v => v),
                _ => new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".")
            };

            return (IClickHouseParameterWriter<T>)writer;
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
            protected override bool BitwiseCopyAllowed => true;

            public UInt32Reader(int rowCount)
                : base(sizeof(uint), rowCount)
            {
            }

            protected override uint ReadElement(ReadOnlySpan<byte> source)
            {
                return BitConverter.ToUInt32(source);
            }

            protected override IClickHouseTableColumn<uint> EndRead(ClickHouseColumnSettings? settings, ReadOnlyMemory<uint> buffer)
            {
                return new UInt32TableColumn(buffer);
            }
        }

        private sealed class UInt32Writer : StructureWriterBase<uint>
        {
            protected override bool BitwiseCopyAllowed => true;

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
