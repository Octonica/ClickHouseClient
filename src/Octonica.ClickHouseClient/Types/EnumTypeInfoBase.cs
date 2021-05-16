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
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient.Types
{
    internal abstract class EnumTypeInfoBase<TValue> : IClickHouseColumnTypeInfo
        where TValue : struct
    {
        private readonly Dictionary<string, TValue>? _enumMap;
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
            _reversedEnumMap = new Dictionary<TValue, string>();
            _mapOrder = new List<string>();

            foreach (var pair in values)
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
                throw new NotSupportedException($"The type \"{TypeName}\" doesn't have arguments.");

            var key = _mapOrder[index];
            var value = _enumMap[key];

            return new KeyValuePair<string, TValue>(key, value);
        }

        public IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            if (_enumMap == null || _reversedEnumMap == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, "The list of items is not specified.");

            var internalReader = CreateInternalColumnReader(rowCount);
            return CreateColumnReader(internalReader, _reversedEnumMap);
        }

        protected abstract EnumColumnReaderBase CreateColumnReader(StructureReaderBase<TValue> internalReader, IReadOnlyDictionary<TValue, string> reversedEnumMap);

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (_enumMap == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, "The list of items is not specified.");

            if (typeof(T) == typeof(string))
            {
                var list = new MappedReadOnlyList<string, TValue>(
                    (IReadOnlyList<string>)rows,
                    key => _enumMap.TryGetValue(key, out var value) ? value : throw new InvalidCastException($"The value \"{key}\" can't be converted to {ComplexTypeName}."));
                return CreateInternalColumnWriter(columnName, list);
            }

            return CreateInternalColumnWriter(columnName, rows);
        }

        public IClickHouseColumnTypeInfo GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            var parsedOptions = new List<KeyValuePair<string, TValue>>(options.Count);
            var complexNameBuilder = new StringBuilder(TypeName).Append('(');
            bool isFirst = true;
            foreach (var option in options)
            {
                if (isFirst)
                    isFirst = false;
                else
                    complexNameBuilder.Append(", ");

                var keyStrLen = ClickHouseSyntaxHelper.GetSingleQuoteStringLength(option.Span);
                if (keyStrLen < 0)
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The fragment \"{option}\" is not recognized as an item of the enum.");

                var key = ClickHouseSyntaxHelper.GetSingleQuoteString(option.Slice(0, keyStrLen).Span);
                var valuePart = option.Slice(keyStrLen);
                var eqSignIdx = valuePart.Span.IndexOf('=');
                if (eqSignIdx < 0)
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The fragment \"{option}\" is not recognized as an item of the enum.");

                valuePart = valuePart.Slice(eqSignIdx + 1).Trim();                      
                if (!TryParse(valuePart.Span, out var value))
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The value {valuePart} is not a valid value of {TypeName}.");

                complexNameBuilder.Append(option);
                parsedOptions.Add(new KeyValuePair<string, TValue>(key, value));
            }

            var complexName = complexNameBuilder.Append(')').ToString();
            return CreateDetailedTypeInfo(complexName, parsedOptions);
        }

        protected abstract IClickHouseColumnTypeInfo CreateDetailedTypeInfo(string complexTypeName, IEnumerable<KeyValuePair<string, TValue>> values);

        protected abstract StructureReaderBase<TValue> CreateInternalColumnReader(int rowCount);

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

            public SequenceSize Skip(ReadOnlySequence<byte> sequence, int maxElementsCount, ref object? skipContext)
            {
                return _internalReader.Skip(sequence, maxElementsCount, ref skipContext);
            }

            public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
            {
                var column = _internalReader.EndRead();
                var enumConverter = settings?.EnumConverter;
                if (enumConverter != null)
                {
                    var dispatcher = CreateColumnDispatcher(column, _reversedEnumMap);
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
                var map = new Dictionary<TValue, TEnum>(_reversedEnumMap.Count);
                foreach (var pair in _reversedEnumMap)
                {
                    if (TryMap(enumConverter, pair.Key, pair.Value, out var enumValue))
                        map.Add(pair.Key, enumValue);
                }

                return new EnumTableColumn<TValue, TEnum>(_column, map, _reversedEnumMap);
            }

            protected abstract bool TryMap<TEnum>(IClickHouseEnumConverter<TEnum> enumConverter, TValue value, string stringValue, out TEnum enumValue)
                where TEnum : Enum;
        }
    }
}
