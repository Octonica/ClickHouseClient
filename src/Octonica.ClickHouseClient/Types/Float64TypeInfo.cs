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

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            IReadOnlyList<double> doubleRows;
            if (typeof(T) == typeof(double))
                doubleRows = (IReadOnlyList<double>)rows;
            else if (typeof(T) == typeof(float))
                doubleRows = new MappedReadOnlyList<float, double>((IReadOnlyList<float>)rows, v => v);
            else
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return new Float64Writer(columnName, ComplexTypeName, doubleRows);
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
            public Float64Writer(string columnName, string columnType, IReadOnlyList<double> rows)
                : base(columnName, columnType, sizeof(double), rows)
            {
            }

            protected override void WriteElement(Span<byte> writeTo, in double value)
            {
                var success = BitConverter.TryWriteBytes(writeTo, value);
                Debug.Assert(success);
            }
        }
    }
}
