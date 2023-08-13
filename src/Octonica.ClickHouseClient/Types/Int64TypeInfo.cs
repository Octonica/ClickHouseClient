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
using System.Diagnostics;
using System.Globalization;
using System.Text;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;

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

        public override IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            return new SimpleSkippingColumnReader(sizeof(long), rowCount);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            var type = typeof(T);
            IReadOnlyList<long> longRows;
            if (type == typeof(long))
                longRows = (IReadOnlyList<long>)rows;
            else if (type == typeof(int))
                longRows = MappedReadOnlyList<int, long>.Map((IReadOnlyList<int>)rows, v => v);
            else if (type == typeof(uint))
                longRows = MappedReadOnlyList<uint, long>.Map((IReadOnlyList<uint>)rows, v => v);
            else if (type == typeof(short))
                longRows = MappedReadOnlyList<short, long>.Map((IReadOnlyList<short>)rows, v => v);
            else if (type == typeof(ushort))
                longRows = MappedReadOnlyList<ushort, long>.Map((IReadOnlyList<ushort>)rows, v => v);
            else if (type == typeof(sbyte))
                longRows = MappedReadOnlyList<sbyte, long>.Map((IReadOnlyList<sbyte>)rows, v => v);
            else if (type == typeof(byte))
                longRows = MappedReadOnlyList<byte, long>.Map((IReadOnlyList<byte>)rows, v => v);
            else
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
            
            return new Int64Writer(columnName, ComplexTypeName, longRows);
        }

        public override IClickHouseLiteralWriter<T> CreateLiteralWriter<T>()
        {
            var type = typeof(T);
            if (type == typeof(DBNull))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values.");

            object writer = default(T) switch
            {
                long _ => new SimpleLiteralWriter<long>(this),
                int _ => new SimpleLiteralWriter<int>(this),
                short _ => new SimpleLiteralWriter<short>(this),
                ushort _ => new SimpleLiteralWriter<ushort>(this),
                sbyte _ => new SimpleLiteralWriter<sbyte>(this),
                byte _ => new SimpleLiteralWriter<byte>(this),
                _ => throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\"."),
            };

            return (IClickHouseLiteralWriter<T>)writer;
        }

        public override Type GetFieldType()
        {
            return typeof(long);
        }

        public override ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Int64;
        }

        private sealed class Int64Reader : StructureReaderBase<long>
        {
            protected override bool BitwiseCopyAllowed => true;

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
            protected override bool BitwiseCopyAllowed => true;

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
