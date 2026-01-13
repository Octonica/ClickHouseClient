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

using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class Float64TypeInfo : SimpleTypeInfo
    {
        public Float64TypeInfo()
            : base("Float64")
        {
        }

        public override IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new Float64Reader(rowCount);
        }

        public override IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            return new SimpleSkippingColumnReader(sizeof(double), rowCount);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            IReadOnlyList<double> doubleRows = typeof(T) == typeof(double)
                ? (IReadOnlyList<double>)rows
                : typeof(T) == typeof(float)
                ? (IReadOnlyList<double>)MappedReadOnlyList<float, double>.Map((IReadOnlyList<float>)rows, v => v)
                : throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
            return new Float64Writer(columnName, ComplexTypeName, doubleRows);
        }

        public override IClickHouseParameterWriter<T> CreateParameterWriter<T>()
        {
            Type type = typeof(T);
            if (type == typeof(DBNull))
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values");
            }

            object writer = default(T) switch
            {
                double _ => HexStringParameterWriter.Create<double>(this),
                float _ => HexStringParameterWriter.Create<float, double>(this, v => v),
                _ => throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\"."),
            };

            return (IClickHouseParameterWriter<T>)writer;
        }

        public override Type GetFieldType()
        {
            return typeof(double);
        }

        public override ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Double;
        }

        private sealed class Float64Reader : StructureReaderBase<double>
        {
            protected override bool BitwiseCopyAllowed => true;

            public Float64Reader(int rowCount)
                : base(sizeof(double), rowCount)
            {
            }

            protected override double ReadElement(ReadOnlySpan<byte> source)
            {
                return BitConverter.ToDouble(source);
            }
        }

        private sealed class Float64Writer : StructureWriterBase<double>
        {
            protected override bool BitwiseCopyAllowed => true;

            public Float64Writer(string columnName, string columnType, IReadOnlyList<double> rows)
                : base(columnName, columnType, sizeof(double), rows)
            {
            }

            protected override void WriteElement(Span<byte> writeTo, in double value)
            {
                bool success = BitConverter.TryWriteBytes(writeTo, value);
                Debug.Assert(success);
            }
        }
    }
}
