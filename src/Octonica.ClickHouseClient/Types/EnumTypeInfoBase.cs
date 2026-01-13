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

using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace Octonica.ClickHouseClient.Types
{
    internal abstract class EnumTypeInfoBase<TValue> : IClickHouseColumnTypeInfo
        where TValue : struct, IFormattable
    {
        protected readonly Dictionary<string, TValue>? _enumMap;
        private readonly Dictionary<TValue, string>? _reversedEnumMap;
        private readonly List<string>? _mapOrder;

        public string ComplexTypeName { get; }

        public string TypeName { get; }

        public int GenericArgumentsCount => 0;

        public int TypeArgumentsCount => _mapOrder?.Count ?? 0;

        protected EnumTypeInfoBase(string typeName)
        {
            TypeName = typeName;
            ComplexTypeName = typeName;
        }

        protected EnumTypeInfoBase(string typeName, string complexTypeName, IEnumerable<KeyValuePair<string, TValue>> values)
        {
            TypeName = typeName;
            ComplexTypeName = complexTypeName;

            _enumMap = new Dictionary<string, TValue>(StringComparer.Ordinal);
            _reversedEnumMap = [];
            _mapOrder = [];

            foreach (KeyValuePair<string, TValue> pair in values)
            {
                _enumMap.Add(pair.Key, pair.Value);
                _reversedEnumMap.Add(pair.Value, pair.Key);
                _mapOrder.Add(pair.Key);
            }
        }

        public Type GetFieldType()
        {
            return typeof(string);
        }

        public ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Enum;
        }

        public IClickHouseTypeInfo GetGenericArgument(int index)
        {
            throw new NotSupportedException($"The type \"{TypeName}\" doesn't have generic arguments.");
        }

        public object GetTypeArgument(int index)
        {
            if (_mapOrder == null || _enumMap == null)
            {
                throw new NotSupportedException($"The type \"{TypeName}\" doesn't have arguments.");
            }

            string key = _mapOrder[index];
            TValue value = _enumMap[key];

            return new KeyValuePair<string, TValue>(key, value);
        }

        public IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            if (_enumMap == null || _reversedEnumMap == null)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, "The list of items is not specified.");
            }

            StructureReaderBase<TValue> internalReader = CreateInternalColumnReader(rowCount);
            return CreateColumnReader(internalReader, _reversedEnumMap);
        }

        public IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            return _enumMap == null || _reversedEnumMap == null
                ? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, "The list of items is not specified.")
                : (IClickHouseColumnReaderBase)CreateInternalSkippingColumnReader(rowCount);
        }

        protected abstract EnumColumnReaderBase CreateColumnReader(StructureReaderBase<TValue> internalReader, IReadOnlyDictionary<TValue, string> reversedEnumMap);

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (_enumMap == null)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, "The list of items is not specified.");
            }

            if (typeof(T) == typeof(string))
            {
                IReadOnlyListExt<TValue> list = MappedReadOnlyList<string, TValue>.Map(
                    (IReadOnlyList<string>)rows,
                    key => key == null ? default : _enumMap.TryGetValue(key, out TValue value) ? value : throw new InvalidCastException($"The value \"{key}\" can't be converted to {ComplexTypeName}."));
                return CreateInternalColumnWriter(columnName, list);
            }

            return CreateInternalColumnWriter(columnName, rows);
        }

        public abstract IClickHouseParameterWriter<T> CreateParameterWriter<T>();

        public IClickHouseColumnTypeInfo GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            List<KeyValuePair<string, TValue>> parsedOptions = new(options.Count);
            StringBuilder complexNameBuilder = new StringBuilder(TypeName).Append('(');
            bool isFirst = true;
            foreach (ReadOnlyMemory<char> option in options)
            {
                if (isFirst)
                {
                    isFirst = false;
                }
                else
                {
                    _ = complexNameBuilder.Append(", ");
                }

                int keyStrLen = ClickHouseSyntaxHelper.GetSingleQuoteStringLength(option.Span);
                if (keyStrLen < 0)
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The fragment \"{option}\" is not recognized as an item of the enum.");
                }

                string key = ClickHouseSyntaxHelper.GetSingleQuoteString(option[..keyStrLen].Span);
                ReadOnlyMemory<char> valuePart = option[keyStrLen..];
                int eqSignIdx = valuePart.Span.IndexOf('=');
                if (eqSignIdx < 0)
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The fragment \"{option}\" is not recognized as an item of the enum.");
                }

                valuePart = valuePart[(eqSignIdx + 1)..].Trim();
                if (!TryParse(valuePart.Span, out TValue value))
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The value {valuePart} is not a valid value of {TypeName}.");
                }

                _ = complexNameBuilder.Append(option);
                parsedOptions.Add(new KeyValuePair<string, TValue>(key, value));
            }

            string complexName = complexNameBuilder.Append(')').ToString();
            return CreateDetailedTypeInfo(complexName, parsedOptions);
        }

        protected abstract IClickHouseColumnTypeInfo CreateDetailedTypeInfo(string complexTypeName, IEnumerable<KeyValuePair<string, TValue>> values);

        protected abstract StructureReaderBase<TValue> CreateInternalColumnReader(int rowCount);

        protected abstract SimpleSkippingColumnReader CreateInternalSkippingColumnReader(int rowCount);

        protected abstract IClickHouseColumnWriter CreateInternalColumnWriter<T>(string columnName, IReadOnlyList<T> rows);

        protected abstract bool TryParse(ReadOnlySpan<char> text, out TValue value);

        protected abstract class EnumColumnReaderBase : IClickHouseColumnReader
        {
            private readonly StructureReaderBase<TValue> _internalReader;
            private readonly IReadOnlyDictionary<TValue, string> _reversedEnumMap;

            public EnumColumnReaderBase(StructureReaderBase<TValue> internalReader, IReadOnlyDictionary<TValue, string> reversedEnumMap)
            {
                _internalReader = internalReader;
                _reversedEnumMap = reversedEnumMap;
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                return _internalReader.ReadNext(sequence);
            }

            public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
            {
                IClickHouseTableColumn<TValue> column = _internalReader.EndRead(null);
                IClickHouseEnumConverter? enumConverter = settings?.EnumConverter;
                if (enumConverter != null)
                {
                    EnumTableColumnDispatcherBase dispatcher = CreateColumnDispatcher(column, _reversedEnumMap);
                    return enumConverter.Dispatch(dispatcher);
                }

                return new EnumTableColumn<TValue>(column, _reversedEnumMap);
            }

            protected abstract EnumTableColumnDispatcherBase CreateColumnDispatcher(IClickHouseTableColumn<TValue> column, IReadOnlyDictionary<TValue, string> reversedEnumMap);
        }

        protected abstract class EnumTableColumnDispatcherBase : IClickHouseEnumConverterDispatcher<IClickHouseTableColumn>
        {
            private readonly IClickHouseTableColumn<TValue> _column;
            private readonly IReadOnlyDictionary<TValue, string> _reversedEnumMap;

            public EnumTableColumnDispatcherBase(IClickHouseTableColumn<TValue> column, IReadOnlyDictionary<TValue, string> reversedEnumMap)
            {
                _column = column;
                _reversedEnumMap = reversedEnumMap;
            }

            public IClickHouseTableColumn Dispatch<TEnum>(IClickHouseEnumConverter<TEnum> enumConverter)
                where TEnum : Enum
            {
                Dictionary<TValue, TEnum> map = new(_reversedEnumMap.Count);
                foreach (KeyValuePair<TValue, string> pair in _reversedEnumMap)
                {
                    if (TryMap(enumConverter, pair.Key, pair.Value, out TEnum? enumValue))
                    {
                        map.Add(pair.Key, enumValue);
                    }
                }

                return new EnumTableColumn<TValue, TEnum>(_column, map, _reversedEnumMap);
            }

            protected abstract bool TryMap<TEnum>(IClickHouseEnumConverter<TEnum> enumConverter, TValue value, string stringValue, out TEnum enumValue)
                where TEnum : Enum;
        }

        protected sealed class EnumParameterWriter : IClickHouseParameterWriter<string>
        {
            private readonly EnumTypeInfoBase<TValue> _type;
            private readonly SimpleParameterWriter<TValue> _writer;

            public EnumParameterWriter(EnumTypeInfoBase<TValue> type)
            {
                _type = type;
                _writer = new SimpleParameterWriter<TValue>(_type);
            }

            public bool TryCreateParameterValueWriter(string value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter)
            {
                TValue enumValue = Convert(value);
                return _writer.TryCreateParameterValueWriter(enumValue, isNested, out valueWriter);
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, string value)
            {
                TValue enumValue = Convert(value);
                return _writer.Interpolate(queryBuilder, enumValue);
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseColumnTypeInfo, Func<StringBuilder, Func<StringBuilder, StringBuilder>, StringBuilder>, StringBuilder> writeValue)
            {
                return _writer.Interpolate(queryBuilder, typeInfoProvider, writeValue);
            }

            private TValue Convert(string value)
            {
                Dictionary<string, TValue>? enumMap = _type._enumMap;
                Debug.Assert(enumMap != null);

                return enumMap.TryGetValue(value, out TValue enumValue)
                    ? enumValue
                    : throw new InvalidCastException($"The value \"{value}\" can't be converted to the ClickHouse type \"{_type.ComplexTypeName}\".");
            }
        }
    }
}
