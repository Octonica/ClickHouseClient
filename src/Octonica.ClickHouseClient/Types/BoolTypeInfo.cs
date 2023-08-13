#region License Apache 2.0
/* Copyright 2022-2023 Octonica
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

using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;
using System;
using System.Collections.Generic;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class BoolTypeInfo : SimpleTypeInfo
    {
        public BoolTypeInfo()
            : base("Bool")
        {
        }

        public override IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new BoolReader(rowCount);
        }

        public override IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            return new SimpleSkippingColumnReader(sizeof(byte), rowCount);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            IReadOnlyList<bool> typedList;

            if (typeof(T) == typeof(bool))
            {
                typedList = (IReadOnlyList<bool>)rows;
            }
            else if (typeof(T) == typeof(byte))
            {
                // Some kind of a compatibility mode. Write bytes as bools. Any non-zero value is treated as true.
                typedList = MappedReadOnlyList<byte, bool>.Map((IReadOnlyList<byte>)rows, b => b != 0);
            }
            else
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
            }

            return new BoolWriter(columnName, ComplexTypeName, typedList);
        }

        public override IClickHouseLiteralWriter<T> CreateLiteralWriter<T>()
        {
            var type = typeof(T);
            if (type == typeof(DBNull))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values.");

            object writer;
            if (type == typeof(bool))
                writer = new SimpleLiteralWriter<bool, byte>(this, appendTypeCast: true, boolValue => boolValue ? (byte)1 : (byte)0);
            else if (type == typeof(byte))
                writer = new SimpleLiteralWriter<byte>(this, appendTypeCast: true);
            else
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return (IClickHouseLiteralWriter<T>)writer;
        }

        public override ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Boolean;
        }

        public override Type GetFieldType()
        {
            return typeof(bool);
        }

        private sealed class BoolReader : StructureReaderBase<byte, bool>
        {
            protected override bool BitwiseCopyAllowed => true;

            public BoolReader(int rowCount)
                : base(sizeof(byte), rowCount)
            {
            }

            protected override byte ReadElement(ReadOnlySpan<byte> source)
            {
                return source[0];
            }

            protected override IClickHouseTableColumn<bool> EndRead(ClickHouseColumnSettings? settings, ReadOnlyMemory<byte> buffer)
            {
                return new BoolTableColumn(buffer);
            }
        }

        private sealed class BoolWriter : StructureWriterBase<bool, byte>
        {
            public BoolWriter(string columnName, string columnType, IReadOnlyList<bool> rows)
                : base(columnName, columnType, sizeof(byte), rows)
            {
            }

            protected override byte Convert(bool value)
            {
                return value ? (byte)1 : (byte)0;
            }
        }
    }
}
