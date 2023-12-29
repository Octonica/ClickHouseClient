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
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class UInt8TypeInfo : SimpleTypeInfo
    {
        public UInt8TypeInfo()
            : base("UInt8")
        {
        }

        public override IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new UInt8Reader(rowCount);
        }

        public override IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            return new SimpleSkippingColumnReader(sizeof(byte), rowCount);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            IReadOnlyList<byte> byteRows;
            if (typeof(T) == typeof(byte))
                byteRows = (IReadOnlyList<byte>) rows;
            else if (typeof(T) == typeof(bool))
                byteRows = MappedReadOnlyList<bool, byte>.Map((IReadOnlyList<bool>) rows, v => v ? (byte) 1 : (byte) 0);
            else
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return new UInt8Writer(columnName, ComplexTypeName, byteRows);
        }

        public override IClickHouseParameterWriter<T> CreateParameterWriter<T>()
        {
            var type = typeof(T);
            if (type == typeof(DBNull))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values.");

            object writer;
            if (type == typeof(byte))
                writer = new SimpleParameterWriter<byte>(this);
            else if (type == typeof(bool))
                writer = new SimpleParameterWriter<bool, byte>(this, b => b ? (byte)1 : (byte)0);
            else
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return (IClickHouseParameterWriter<T>)writer;
        }

        public override Type GetFieldType()
        {
            return typeof(byte);
        }

        public override ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Byte;
        }

        private sealed class UInt8Reader : StructureReaderBase<byte>
        {
            protected override bool BitwiseCopyAllowed => true;

            public UInt8Reader(int rowCount)
                : base(sizeof(byte), rowCount)
            {
            }

            protected override byte ReadElement(ReadOnlySpan<byte> source)
            {
                return source[0];
            }

            protected override IClickHouseTableColumn<byte> EndRead(ClickHouseColumnSettings? settings, ReadOnlyMemory<byte> buffer)
            {
                return new UInt8TableColumn(buffer);
            }
        }

        private sealed class UInt8Writer : IClickHouseColumnWriter
        {
            private readonly IReadOnlyList<byte> _rows;

            private int _position;

            public string ColumnName { get; }

            public string ColumnType { get; }

            public UInt8Writer(string columnName, string columnType, IReadOnlyList<byte> rows)
            {
                _rows = rows;
                ColumnName = columnName;
                ColumnType = columnType;
            }

            public SequenceSize WriteNext(Span<byte> writeTo)
            {
                var size = Math.Min(writeTo.Length, _rows.Count - _position);

                for (int i = 0; i < size; i++, _position++)
                    writeTo[i] = _rows[_position];

                return new SequenceSize(size, size);
            }
        }
    }
}
