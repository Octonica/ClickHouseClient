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
    internal sealed class UInt64TypeInfo : SimpleTypeInfo
    {
        public UInt64TypeInfo()
            : base("UInt64")
        {
        }

        public override IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new UInt64Reader(rowCount);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (!(rows is IReadOnlyList<ulong> ulongRows))
            {
                if (rows is IReadOnlyList<uint> uintRows)
                    ulongRows = new MappedReadOnlyList<uint, ulong>(uintRows, v => v);
                else if (rows is IReadOnlyList<ushort> ushortRows)
                    ulongRows = new MappedReadOnlyList<ushort, ulong>(ushortRows, v => v);
                else if (rows is IReadOnlyList<byte> byteRows)
                    ulongRows = new MappedReadOnlyList<byte, ulong>(byteRows, v => v);
                else
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
            }

            return new UInt64Writer(columnName, ComplexTypeName, ulongRows);
        }

        public override Type GetFieldType()
        {
            return typeof(ulong);
        }

        public override ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.UInt64;
        }

        private sealed class UInt64Reader : StructureReaderBase<ulong>
        {
            public UInt64Reader(int rowCount)
                : base(sizeof(ulong), rowCount)
            {
            }

            protected override ulong ReadElement(ReadOnlySpan<byte> source)
            {
                return BitConverter.ToUInt64(source);
            }
        }

        internal sealed class UInt64Writer : StructureWriterBase<ulong>
        {
            public UInt64Writer(string columnName, string columnType, IReadOnlyList<ulong> rows)
                : base(columnName, columnType, sizeof(ulong), rows)
            {
            }

            protected override void WriteElement(Span<byte> writeTo, in ulong value)
            {
                var success = BitConverter.TryWriteBytes(writeTo, value);
                Debug.Assert(success);
            }
        }
    }
}
