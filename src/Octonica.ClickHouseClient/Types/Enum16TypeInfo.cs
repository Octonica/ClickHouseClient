#region License Apache 2.0
/* Copyright 2020-2021, 2023 Octonica
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

        protected override EnumColumnReaderBase CreateColumnReader(StructureReaderBase<short> internalReader, IReadOnlyDictionary<short, string> reversedEnumMap)
        {
            return new EnumColumnReader(internalReader, reversedEnumMap);
        }

        protected override SimpleSkippingColumnReader CreateInternalSkippingColumnReader(int rowCount)
        {
            return new SimpleSkippingColumnReader(sizeof(short), rowCount);
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
            var type = typeof(T);
            IReadOnlyList<short> shortRows;
            if (type == typeof(short))
                shortRows = (IReadOnlyList<short>)rows;
            else if (type == typeof(byte))
                shortRows = MappedReadOnlyList<byte, short>.Map((IReadOnlyList<byte>)rows, v => v);
            else if (type == typeof(sbyte))
                shortRows = MappedReadOnlyList<sbyte, short>.Map((IReadOnlyList<sbyte>)rows, v => v);
            else
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{TypeName}\".");

            return new Int16TypeInfo.Int16Writer(columnName, ComplexTypeName, shortRows);
        }

        public override IClickHouseLiteralWriter<T> CreateLiteralWriter<T>()
        {
            // TODO: ClickHouseDbType.Enum is not supported in DefaultTypeInfoProvider.GetTypeInfo
            if (_enumMap == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, "The list of items is not specified.");

            var type = typeof(T);
            if (type == typeof(DBNull))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values.");

            object writer;
            if (type == typeof(string))
            {
                writer = new EnumLiteralWriter(this);
            }
            else
            {
                writer = default(T) switch
                {
                    short _ => new SimpleLiteralWriter<short>(this),
                    byte _ => new SimpleLiteralWriter<byte>(this),
                    sbyte _ => new SimpleLiteralWriter<sbyte>(this),
                    _ => throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\"."),
                };
            }

            return (IClickHouseLiteralWriter<T>)writer;
        }

        protected override bool TryParse(ReadOnlySpan<char> text, out short value)
        {
            return short.TryParse(text, out value);
        }

        private sealed class EnumColumnReader : EnumColumnReaderBase
        {
            public EnumColumnReader(StructureReaderBase<short> internalReader, IReadOnlyDictionary<short, string> reversedEnumMap)
                : base(internalReader, reversedEnumMap)
            {
            }

            protected override EnumTableColumnDispatcherBase CreateColumnDispatcher(IClickHouseTableColumn<short> column, IReadOnlyDictionary<short, string> reversedEnumMap)
            {
                return new EnumTableColumnDispatcher(column, reversedEnumMap);
            }
        }

        private sealed class EnumTableColumnDispatcher : EnumTableColumnDispatcherBase
        {
            public EnumTableColumnDispatcher(IClickHouseTableColumn<short> column, IReadOnlyDictionary<short, string> reversedEnumMap)
                : base(column, reversedEnumMap)
            {
            }

            protected override bool TryMap<TEnum>(IClickHouseEnumConverter<TEnum> enumConverter, short value, string stringValue, out TEnum enumValue)
            {
                return enumConverter.TryMap(value, stringValue, out enumValue);
            }
        }
    }
}
