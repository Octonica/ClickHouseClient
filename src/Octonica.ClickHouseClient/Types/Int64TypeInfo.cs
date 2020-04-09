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
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return new Int64Writer(columnName, ComplexTypeName, longRows);
        }

        public override Type GetFieldType()
        {
            return typeof(long);
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
