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
    internal sealed class Int64TypeInfo : SimpleTypeInfo
    {
        public Int64TypeInfo()
            : base("Int64")
        {
        }

        public override IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new Int64Reader(rowCount);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (!(rows is IReadOnlyList<long> longRows))
            {
                if (rows is IReadOnlyList<int> intRows)
                    longRows = new MappedReadOnlyList<int, long>(intRows, v => v);
                else if (rows is IReadOnlyList<uint> uintRows)
                    longRows = new MappedReadOnlyList<uint, long>(uintRows, v => v);
                else if (rows is IReadOnlyList<short> shortRows)
                    longRows = new MappedReadOnlyList<short, long>(shortRows, v => v);
                else if (rows is IReadOnlyList<ushort> ushortRows)
                    longRows = new MappedReadOnlyList<ushort, long>(ushortRows, v => v);
                else if (rows is IReadOnlyList<sbyte> sbyteRows)
                    longRows = new MappedReadOnlyList<sbyte, long>(sbyteRows, v => v);
                else if (rows is IReadOnlyList<byte> byteRows)
                    longRows = new MappedReadOnlyList<byte, long>(byteRows, v => v);
                else
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
            }

            return new Int64Writer(columnName, ComplexTypeName, longRows);
        }

        public override Type GetFieldType()
        {
            return typeof(long);
        }

        public override ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Int64;
        }

        private sealed class Int64Reader : StructureReaderBase<long>
        {
            public Int64Reader(int rowCount)
                : base(sizeof(long), rowCount)
            {
            }

            protected override long ReadElement(ReadOnlySpan<byte> source)
            {
                return BitConverter.ToInt64(source);
            }
        }

        private sealed class Int64Writer : StructureWriterBase<long>
        {
            public Int64Writer(string columnName, string columnType, IReadOnlyList<long> rows)
                : base(columnName, columnType, sizeof(long), rows)
            {
            }

            protected override void WriteElement(Span<byte> writeTo, in long value)
            {
                var success = BitConverter.TryWriteBytes(writeTo, value);
                Debug.Assert(success);
            }
        }
    }
}
