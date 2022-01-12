#region License Apache 2.0
/* Copyright 2020-2021 Octonica
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
using System.Globalization;
using System.Text;
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

        protected override EnumColumnReaderBase CreateColumnReader(StructureReaderBase<sbyte> internalReader, IReadOnlyDictionary<sbyte, string> reversedEnumMap)
        {
            return new EnumColumnReader(internalReader, reversedEnumMap);
        }

        protected override IClickHouseColumnTypeInfo CreateDetailedTypeInfo(string complexTypeName, IEnumerable<KeyValuePair<string, sbyte>> values)
        {
            return new Enum8TypeInfo(TypeName, complexTypeName, values);
        }

        protected override StructureReaderBase<sbyte> CreateInternalColumnReader(int rowCount)
        {
            return new Int8TypeInfo.Int8Reader(rowCount);
        }

        protected override SimpleSkippingColumnReader CreateInternalSkippingColumnReader(int rowCount)
        {
            return new SimpleSkippingColumnReader(sizeof(byte), rowCount);
        }

        protected override IClickHouseColumnWriter CreateInternalColumnWriter<T>(string columnName, IReadOnlyList<T> rows)
        {
            if (typeof(T) != typeof(sbyte))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{TypeName}\".");

            return new Int8TypeInfo.Int8Writer(columnName, ComplexTypeName, (IReadOnlyList<sbyte>)rows);
        }

        public override void FormatValue(StringBuilder queryStringBuilder, object? value)
        {
            // TODO: ClickHouseDbType.Enum is not supported in DefaultTypeInfoProvider.GetTypeInfo
            
            if (_enumMap == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, "The list of items is not specified.");
            
            if (value == null || value is DBNull)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values");

            sbyte outputValue = value switch
            {
                string theValue => _enumMap.TryGetValue(theValue, out var tmp) ? tmp :
                    throw new InvalidCastException($"The value \"{theValue}\" can't be converted to {ComplexTypeName}."),
                sbyte theValue => theValue,
                _ => throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{value.GetType()}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\"."),
            };

            queryStringBuilder.Append(outputValue.ToString(CultureInfo.InvariantCulture));
        }

        protected override bool TryParse(ReadOnlySpan<char> text, out sbyte value)
        {
            return sbyte.TryParse(text, out value);
        }

        private sealed class EnumColumnReader : EnumColumnReaderBase
        {
            public EnumColumnReader(StructureReaderBase<sbyte> internalReader, IReadOnlyDictionary<sbyte, string> reversedEnumMap)
                : base(internalReader, reversedEnumMap)
            {
            }

            protected override EnumTableColumnDispatcherBase CreateColumnDispatcher(IClickHouseTableColumn<sbyte> column, IReadOnlyDictionary<sbyte, string> reversedEnumMap)
            {
                return new EnumTableColumnDispatcher(column, reversedEnumMap);
            }
        }

        private sealed class EnumTableColumnDispatcher : EnumTableColumnDispatcherBase
        {
            public EnumTableColumnDispatcher(IClickHouseTableColumn<sbyte> column, IReadOnlyDictionary<sbyte, string> reversedEnumMap)
                : base(column, reversedEnumMap)
            {
            }

            protected override bool TryMap<TEnum>(IClickHouseEnumConverter<TEnum> enumConverter, sbyte value, string stringValue, out TEnum enumValue)
            {
                return enumConverter.TryMap(value, stringValue, out enumValue);
            }
        }
    }
}
