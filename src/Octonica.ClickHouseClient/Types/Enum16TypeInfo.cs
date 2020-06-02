#region License Apache 2.0
/* Copyright 2020 Octonica
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
    internal sealed class Enum16TypeInfo : EnumTypeInfoBase<short>
    {
        public Enum16TypeInfo()
            : base("Enum16")
        {
        }

        private Enum16TypeInfo(string typeName, string complexTypeName, IEnumerable<KeyValuePair<string, short>> values)
            : base(typeName, complexTypeName, values)
        {
        }

        protected override IClickHouseColumnTypeInfo CreateDetailedTypeInfo(string complexTypeName, IEnumerable<KeyValuePair<string, short>> values)
        {
            return new Enum16TypeInfo(TypeName, complexTypeName, values);
        }

        protected override StructureReaderBase<short> CreateInternalColumnReader(int rowCount)
        {
            return new Int16TypeInfo.Int16Reader(rowCount);
        }

        protected override IClickHouseColumnWriter CreateInternalColumnWriter<T>(string columnName, IReadOnlyList<T> rows)
        {
            if (!(rows is IReadOnlyList<short> shortRows))
            {
                if (rows is IReadOnlyList<byte> byteRows)
                    shortRows = new MappedReadOnlyList<byte, short>(byteRows, v => v);
                else if (rows is IReadOnlyList<sbyte> sbyteRows)
                    shortRows = new MappedReadOnlyList<sbyte, short>(sbyteRows, v => v);
                else
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{TypeName}\".");
            }

            return new Int16TypeInfo.Int16Writer(columnName, ComplexTypeName, shortRows);
        }

        protected override bool TryParse(ReadOnlySpan<char> text, out short value)
        {
            return short.TryParse(text, out value);
        }
    }
}
