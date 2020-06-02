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

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class Enum8TypeInfo : EnumTypeInfoBase<sbyte>
    {
        public Enum8TypeInfo()
            : base("Enum8")
        {
        }

        private Enum8TypeInfo(string typeName, string complexTypeName, IEnumerable<KeyValuePair<string, sbyte>> values)
            : base(typeName, complexTypeName, values)
        {
        }

        protected override IClickHouseColumnTypeInfo CreateDetailedTypeInfo(string complexTypeName, IEnumerable<KeyValuePair<string, sbyte>> values)
        {
            return new Enum8TypeInfo(TypeName, complexTypeName, values);
        }

        protected override StructureReaderBase<sbyte> CreateInternalColumnReader(int rowCount)
        {
            return new Int8TypeInfo.Int8Reader(rowCount);
        }

        protected override IClickHouseColumnWriter CreateInternalColumnWriter<T>(string columnName, IReadOnlyList<T> rows)
        {
            if (!(rows is IReadOnlyList<sbyte> sbyteRows))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{TypeName}\".");

            return new Int8TypeInfo.Int8Writer(columnName, ComplexTypeName, sbyteRows);
        }

        protected override bool TryParse(ReadOnlySpan<char> text, out sbyte value)
        {
            return sbyte.TryParse(text, out value);
        }
    }
}
