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
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class Int16TypeInfo : SimpleTypeInfo
    {
        public Int16TypeInfo()
            : base("Int16")
        {
        }

        public override IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new Int16Reader(rowCount);
        }

        public override IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            return new SimpleSkippingColumnReader(sizeof(short), rowCount);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            var type = typeof(T);
            IReadOnlyList<short> shortRows;
            if (type == typeof(short))
                shortRows = (IReadOnlyList<short>)rows;
            else if (type == typeof(sbyte))
                shortRows = MappedReadOnlyList<sbyte, short>.Map((IReadOnlyList<sbyte>)rows, v => v);
            else if (type == typeof(byte))
                shortRows = MappedReadOnlyList<byte, short>.Map((IReadOnlyList<byte>)rows, v => v);
            else
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
            
            return new Int16Writer(columnName, ComplexTypeName, shortRows);
        }

        public override Type GetFieldType()
        {
            return typeof(short);
        }

        public override ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Int16;
        }

        internal sealed class Int16Reader : StructureReaderBase<short>
        {
            protected override bool BitwiseCopyAllowed => true;

            public Int16Reader(int rowCount)
                : base(sizeof(short), rowCount)
            {
            }

            protected override short ReadElement(ReadOnlySpan<byte> source)
            {
                return BitConverter.ToInt16(source);
            }

            protected override IClickHouseTableColumn<short> EndRead(ClickHouseColumnSettings? settings, ReadOnlyMemory<short> buffer)
            {
                return new Int16TableColumn(buffer);
            }
        }

        internal sealed class Int16Writer : StructureWriterBase<short>
        {
            protected override bool BitwiseCopyAllowed => true;

            public Int16Writer(string columnName, string columnType, IReadOnlyList<short> rows)
                : base(columnName, columnType, sizeof(short), rows)
            {
            }

            protected override void WriteElement(Span<byte> writeTo, in short value)
            {
                var success = BitConverter.TryWriteBytes(writeTo, value);
                Debug.Assert(success);
            }
        }
    }
}
