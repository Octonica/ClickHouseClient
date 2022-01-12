#region License Apache 2.0
/* Copyright 2021 Octonica
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
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class MapTypeInfo : IClickHouseColumnTypeInfo
    {
        // Map(key, value) is the alias for Array(Tuple(key, value))
        private KeyValuePair<IClickHouseColumnTypeInfo, IClickHouseColumnTypeInfo>? _typeArgs;        
        private readonly IClickHouseColumnTypeInfo? _underlyingType;

        public string ComplexTypeName { get; }

        public string TypeName => "Map";

        public int GenericArgumentsCount => _typeArgs == null ? 0 : 2;

        public MapTypeInfo()
        {
            ComplexTypeName = TypeName;
        }

        private MapTypeInfo(IClickHouseColumnTypeInfo keyType, IClickHouseColumnTypeInfo valueType, IClickHouseColumnTypeInfo underlyingType)
        {
            _typeArgs = new KeyValuePair<IClickHouseColumnTypeInfo, IClickHouseColumnTypeInfo>(keyType, valueType);
            ComplexTypeName = $"{TypeName}({keyType.ComplexTypeName}, {valueType.ComplexTypeName})";
            _underlyingType = underlyingType;
        }

        public IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            if (_underlyingType == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            var fieldType = GetFieldType();
            return new MapReader(_underlyingType.CreateColumnReader(rowCount), fieldType);
        }

        public IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            if (_underlyingType == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            return _underlyingType.CreateSkippingColumnReader(rowCount);
        }

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (_underlyingType == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            var elementType = typeof(T);
            Type? dictionaryItf = null;
            foreach (var itf in elementType.GetInterfaces())
            {
                if (itf.IsGenericType && itf.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>))
                {
                    if (dictionaryItf != null)
                    {
                        dictionaryItf = null;
                        break;
                    }

                    dictionaryItf = itf;
                }
            }

            IClickHouseColumnWriter underlyingWriter;
            if (dictionaryItf != null)
            {
                var dispatcherType = typeof(DictionaryDispatcher<,>).MakeGenericType(dictionaryItf.GetGenericArguments());
                var dispatcher = (IDictionaryDispatcher)Activator.CreateInstance(dispatcherType)!;
                underlyingWriter = dispatcher.Dispatch(_underlyingType, columnName, rows, columnSettings);
            }
            else
            {
                underlyingWriter = _underlyingType.CreateColumnWriter(columnName, rows, columnSettings);
            }

            return new MapColumnWriter(underlyingWriter);
        }

        public void FormatValue(StringBuilder queryStringBuilder, object? value)
        {
            // TODO: ClickHouseDbType.Map is not supported in DefaultTypeInfoProvider.GetTypeInfo
            
            if (_underlyingType == null || _typeArgs == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");
            
            if (value == null || value is DBNull)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values");

            Type? dictionaryItf = null;
            foreach (var itf in value.GetType().GetInterfaces())
            {
                if (itf.IsGenericType && itf.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>))
                {
                    if (dictionaryItf != null)
                    {
                        dictionaryItf = null;
                        break;
                    }

                    dictionaryItf = itf;
                }
            }
            if (dictionaryItf == null)
            {
                _underlyingType.FormatValue(queryStringBuilder, value);
            }
            else
            {
                if (!(_underlyingType is ArrayTypeInfo arrayTypeInfo) ||
                    !(arrayTypeInfo.GetGenericArgument(0) is TupleTypeInfo tupleTypeInfo) ||
                    tupleTypeInfo.GenericArgumentsCount != 2)
                {
                    
                }
                var mapFormatter = (MapFormatterBase) Activator.CreateInstance(typeof(MapFormatter<,>).MakeGenericType(dictionaryItf.GetGenericArguments()))!;
                queryStringBuilder.Append("{");
                var keyType = _typeArgs.Value.Key;
                var valueType = _typeArgs.Value.Value;
                mapFormatter.Format(keyType, valueType, queryStringBuilder, value);
                queryStringBuilder.Append("}");
            }
        }

        abstract class MapFormatterBase
        {
            public abstract void Format(IClickHouseColumnTypeInfo keyType, IClickHouseColumnTypeInfo valueType, StringBuilder queryStringBuilder, object map);
        }

        class MapFormatter<TKey, TValue> : MapFormatterBase where TKey : notnull
        {
            public override void Format(IClickHouseColumnTypeInfo keyType, IClickHouseColumnTypeInfo valueType, StringBuilder queryStringBuilder, object untypedMap)
            {
                var map = (IReadOnlyDictionary<TKey, TValue>) untypedMap;
                queryStringBuilder.Append("tuple([");
                {
                    var needComma = false;
                    foreach (var (key, _) in map)
                    {
                        if (needComma)
                            queryStringBuilder.Append(',');
                        else
                            needComma = true;
                        keyType.FormatValue(queryStringBuilder, key);
                    }
                }
                queryStringBuilder.Append("],[");
                {
                    var needComma = false;
                    foreach (var (_, value) in map)
                    {
                        if (needComma)
                            queryStringBuilder.Append(',');
                        else
                            needComma = true;
                        keyType.FormatValue(queryStringBuilder, value);
                    }
                }
                queryStringBuilder.Append("])");
            }
        }

        public ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Map;
        }

        public IClickHouseColumnTypeInfo GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            if (_typeArgs != null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, "The type is already fully specified.");

            if (options.Count < 2)
                throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The type \"{TypeName}\" requires two type arguments: key and value.");

            if (options.Count > 2)
                throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"Too many options for the type \"{TypeName}\".");

            var keyType = typeInfoProvider.GetTypeInfo(options[0]);
            var valueType = typeInfoProvider.GetTypeInfo(options[1]);

            var underlyingTypeName = $"Array(Tuple({keyType.ComplexTypeName}, {valueType.ComplexTypeName}))";
            var undelyingType = typeInfoProvider.GetTypeInfo(underlyingTypeName);

            return new MapTypeInfo(keyType, valueType, undelyingType);
        }

        public Type GetFieldType()
        {
            if (_typeArgs == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            var keyType = _typeArgs.Value.Key.GetFieldType();
            var valueType = _typeArgs.Value.Value.GetFieldType();

            var fieldType = typeof(KeyValuePair<,>).MakeGenericType(keyType, valueType).MakeArrayType();
            return fieldType;
        }

        public IClickHouseTypeInfo GetGenericArgument(int index)
        {
            if (_typeArgs == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            switch (index)
            {
                case 0:
                    return _typeArgs.Value.Key;
                case 1:
                    return _typeArgs.Value.Value;
                default:
                    throw new IndexOutOfRangeException();
            }
        }

        private sealed class MapReader : IClickHouseColumnReader
        {
            private readonly IClickHouseColumnReader _underlyingReader;
            private readonly Type _fieldType;

            public MapReader(IClickHouseColumnReader underlyingReader, Type fieldType)
            {
                _underlyingReader = underlyingReader;
                _fieldType = fieldType;
            }            

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                return _underlyingReader.ReadNext(sequence);
            }

            public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
            {
                var column = _underlyingReader.EndRead(settings);
                var dispatcher = new MapColumnDispatcher(column);
                return TypeDispatcher.Dispatch(_fieldType, dispatcher);                
            }
        }

        private sealed class MapColumnDispatcher : ITypeDispatcher<IClickHouseTableColumn>
        {
            private readonly IClickHouseTableColumn _column;

            public MapColumnDispatcher(IClickHouseTableColumn column)
            {
                _column = column;
            }

            public IClickHouseTableColumn Dispatch<T>()
            {
                var column = _column.TryReinterpret<T>();
                if (column == null)
                    throw new ClickHouseException(ClickHouseErrorCodes.InternalError, $"Internal error. Column was not converted to the type \"{typeof(T)}\" as expected.");

                return column;
            }
        }

        private sealed class MapColumnWriter : IClickHouseColumnWriter
        {
            private readonly IClickHouseColumnWriter _underlyingWriter;

            public string ColumnName => _underlyingWriter.ColumnName;

            public string ColumnType { get; }

            public MapColumnWriter(IClickHouseColumnWriter underlyingWriter)
            {
                const string typeNameStart = "Array(Tuple", typeNameEnd = ")";
                var typeName = underlyingWriter.ColumnType;
                if (!typeName.StartsWith(typeNameStart) || !typeName.EndsWith(typeNameEnd))
                    throw new ClickHouseException(ClickHouseErrorCodes.InternalError, $"Internal error. The name of the type \"{typeName}\" doesn't match to the expected pattern \"{Regex.Escape(typeNameStart)}.*{Regex.Escape(typeNameEnd)}\".");

                ColumnType = "Map" + typeName[typeNameStart.Length..^typeNameEnd.Length];
                _underlyingWriter = underlyingWriter;
            }

            public SequenceSize WriteNext(Span<byte> writeTo)
            {
                return _underlyingWriter.WriteNext(writeTo);
            }
        }

        private interface IDictionaryDispatcher
        {
            IClickHouseColumnWriter Dispatch(IClickHouseColumnTypeInfo underlyingType, string columnName, object rows, ClickHouseColumnSettings? columnSettings);
        }

        private sealed class DictionaryDispatcher<TKey, TValue> : IDictionaryDispatcher
            where TKey : notnull
        {
            public IClickHouseColumnWriter Dispatch(IClickHouseColumnTypeInfo underlyingType, string columnName, object rows, ClickHouseColumnSettings? columnSettings)
            {
                var dictionaryList = (IReadOnlyList<IReadOnlyDictionary<TKey, TValue>>)rows;
                var listOfLists = new KeyValuePair<TKey, TValue>[dictionaryList.Count][];
                for (int i = 0; i < listOfLists.Length; i++)
                    listOfLists[i] = ((IEnumerable<KeyValuePair<TKey, TValue>>)dictionaryList[i]).ToArray();
                    
                return underlyingType.CreateColumnWriter(columnName, listOfLists, columnSettings);
            }
        }
    }
}
