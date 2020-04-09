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
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class Int8TypeInfo : SimpleTypeInfo
    {
        public Int8TypeInfo()
            : base("Int8")
        {
        }

        public override IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new Int8Reader(rowCount);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (!(rows is IReadOnlyList<sbyte> sbyteRows))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return new Int8Writer(columnName, ComplexTypeName, sbyteRows);
        }

        public override Type GetFieldType()
        {
            return typeof(sbyte);
        }

        private sealed class Int8Reader : StructureReaderBase<sbyte>
        {
            public Int8Reader(int rowCount)
                : base(sizeof(sbyte), rowCount)
            {
            }

            protected override sbyte ReadElement(ReadOnlySpan<byte> source)
            {
                return unchecked((sbyte) source[0]);
            }

            protected override IClickHouseTableColumn<sbyte> EndRead(ReadOnlyMemory<sbyte> buffer)
            {
                return new Int8TableColumn(buffer);
            }
        }

        private sealed class Int8Writer : IClickHouseColumnWriter
        {
            private readonly IReadOnlyList<sbyte> _rows;

            private int _position;

            public string ColumnName { get; }

            public string ColumnType { get; }

            public Int8Writer(string columnName, string columnType, IReadOnlyList<sbyte> rows)
            {
                _rows = rows;
                ColumnName = columnName;
                ColumnType = columnType;
            }

            public SequenceSize WriteNext(Span<byte> writeTo)
            {
                var size = Math.Min(writeTo.Length, _rows.Count - _position);

                for (int i = 0; i < size; i++, _position++)
                    writeTo[i] = unchecked((byte) _rows[_position]);

                return new SequenceSize(size, size);
            }
        }
    }
}
