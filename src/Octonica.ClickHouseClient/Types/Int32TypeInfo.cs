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
    internal sealed class Int32TypeInfo : SimpleTypeInfo
    {
        public Int32TypeInfo()
            : base("Int32")
        {
        }

        public override IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new Int32Reader(rowCount);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (!(rows is IReadOnlyList<int> intRows))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return new Int32Writer(columnName, ComplexTypeName, intRows);
        }

        public override Type GetFieldType()
        {
            return typeof(int);
        }

        private sealed class Int32Reader : StructureReaderBase<int>
        {
            public Int32Reader(int rowCount)
                : base(sizeof(int), rowCount)
            {
            }

            protected override int ReadElement(ReadOnlySpan<byte> source)
            {
                return BitConverter.ToInt32(source);
            }

            protected override IClickHouseTableColumn<int> EndRead(ReadOnlyMemory<int> buffer)
            {
                return new Int32TableColumn(buffer);
            }
        }

        private sealed class Int32Writer : StructureWriterBase<int>
        {
            public Int32Writer(string columnName, string columnType, IReadOnlyList<int> rows)
                : base(columnName, columnType, sizeof(int), rows)
            {
            }

            protected override void WriteElement(Span<byte> writeTo, in int value)
            {
                var success = BitConverter.TryWriteBytes(writeTo, value);
                Debug.Assert(success);
            }
        }
    }
}
