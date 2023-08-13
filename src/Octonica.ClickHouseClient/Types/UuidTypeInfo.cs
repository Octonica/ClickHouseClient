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

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class UuidTypeInfo : SimpleTypeInfo
    {
        private const int UuidSize = 16;

        public UuidTypeInfo()
            : base("UUID")
        {
        }

        public override IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new UuidReader(rowCount);
        }

        public override IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            return new SimpleSkippingColumnReader(UuidSize, rowCount);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (typeof(T) != typeof(Guid))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return new UuidWriter(columnName, ComplexTypeName, (IReadOnlyList<Guid>)rows);
        }

        public override IClickHouseLiteralWriter<T> CreateLiteralWriter<T>()
        {
            var type = typeof(T);
            if (type == typeof(DBNull))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values.");

            if (type == typeof(Guid))
                return (IClickHouseLiteralWriter<T>)(object)new StringLiteralWriter<Guid>(this, uuidValue => uuidValue.ToString().AsMemory());

            throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
        }

        public override Type GetFieldType()
        {
            return typeof(Guid);
        }

        public override ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Guid;
        }

        private sealed class UuidReader : StructureReaderBase<Guid>
        {
            public UuidReader(int rowCount)
                : base(UuidSize, rowCount)
            {
            }

            protected override Guid ReadElement(ReadOnlySpan<byte> source)
            {
                ushort c = BitConverter.ToUInt16(source.Slice(0));
                ushort b = BitConverter.ToUInt16(source.Slice(2));
                uint a = BitConverter.ToUInt32(source.Slice(4));
                
                return new Guid(a, b, c, source[15], source[14], source[13], source[12], source[11], source[10], source[9], source[8]);
            }
        }

        private sealed class UuidWriter:StructureWriterBase<Guid>
        {
            public UuidWriter(string columnName, string columnType, IReadOnlyList<Guid> rows)
                : base(columnName, columnType, UuidSize, rows)
            {
            }

            protected override void WriteElement(Span<byte> writeTo, in Guid value)
            {
                var success = value.TryWriteBytes(writeTo);
                Debug.Assert(success);

                var tmp = writeTo[0];
                writeTo[0] = writeTo[6];
                writeTo[6] = writeTo[2];
                writeTo[2] = writeTo[4];
                writeTo[4] = tmp;

                tmp = writeTo[1];
                writeTo[1] = writeTo[7];
                writeTo[7] = writeTo[3];
                writeTo[3] = writeTo[5];
                writeTo[5] = tmp;

                for (int i = 0; i < 4; i++)
                {
                    tmp = writeTo[8 + i];
                    writeTo[8 + i] = writeTo[15 - i];
                    writeTo[15 - i] = tmp;
                }
            }
        }
    }
}
