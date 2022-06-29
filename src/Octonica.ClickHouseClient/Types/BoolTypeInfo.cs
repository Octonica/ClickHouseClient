#region License Apache 2.0
/* Copyright 2022 Octonica
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
using System;
using System.Collections.Generic;
using System.Text;

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
            if (typeof(T) != typeof(bool))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return new BoolWriter(columnName, ComplexTypeName, (IReadOnlyList<bool>)rows);
        }

        public override void FormatValue(StringBuilder queryStringBuilder, object? value)
        {
            if (value == null || value is DBNull)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values.");

            if (!(value is bool val))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{value.GetType()}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            queryStringBuilder.Append(val ? '1' : '0').Append("::Bool");
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
