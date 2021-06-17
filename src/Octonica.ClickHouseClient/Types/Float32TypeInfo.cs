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

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class Float32TypeInfo : SimpleTypeInfo
    {
        public Float32TypeInfo()
            : base("Float32")
        {
        }

        public override IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new Float32Reader(rowCount);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (typeof(T) != typeof(float))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return new Float32Writer(columnName, ComplexTypeName, (IReadOnlyList<float>)rows);
        }

        public override Type GetFieldType()
        {
            return typeof(float);
        }

        public override ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Single;
        }

        private sealed class Float32Reader : StructureReaderBase<float>
        {
            protected override bool BitwiseCopyAllowed => true;

            public Float32Reader(int rowCount)
                : base(sizeof(float), rowCount)
            {
            }

            protected override float ReadElement(ReadOnlySpan<byte> source)
            {
                return BitConverter.ToSingle(source);
            }

            protected override IClickHouseTableColumn<float> EndRead(ClickHouseColumnSettings? settings, ReadOnlyMemory<float> buffer)
            {
                return new Float32TableColumn(buffer);
            }
        }

        private sealed class Float32Writer : StructureWriterBase<float>
        {
            protected override bool BitwiseCopyAllowed => true;

            public Float32Writer(string columnName, string columnType, IReadOnlyList<float> rows)
                : base(columnName, columnType, sizeof(float), rows)
            {
            }

            protected override void WriteElement(Span<byte> writeTo, in float value)
            {
                var success = BitConverter.TryWriteBytes(writeTo, value);
                Debug.Assert(success);
            }
        }
    }
}
