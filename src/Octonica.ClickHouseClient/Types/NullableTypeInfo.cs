#region License Apache 2.0
/* Copyright 2019-2020 Octonica
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
using System.Collections;
using System.Collections.Generic;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class NullableTypeInfo : IClickHouseColumnTypeInfo
    {
        public string ComplexTypeName { get; }

        public string TypeName => "Nullable";

        public int GenericArgumentsCount => UnderlyingType == null ? 0 : 1;

        public IClickHouseColumnTypeInfo? UnderlyingType { get; }

        public NullableTypeInfo()
        {
            ComplexTypeName = TypeName;
        }

        public NullableTypeInfo(IClickHouseColumnTypeInfo underlyingType)
        {
            if (underlyingType is NullableTypeInfo)
                throw new ArgumentException("The underlying type can't be nullable.", nameof(underlyingType));

            UnderlyingType = underlyingType ?? throw new ArgumentNullException(nameof(underlyingType));
            ComplexTypeName = $"{TypeName}({UnderlyingType.ComplexTypeName})";
        }

        public IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            if (UnderlyingType == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            return new NullableColumnReader(rowCount, UnderlyingType);
        }

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (UnderlyingType == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            return new NullableColumnWriter<T>(columnName, ComplexTypeName, rows, columnSettings, UnderlyingType);
        }

        public IClickHouseColumnTypeInfo GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            if (UnderlyingType != null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, "The type is already fully specified.");

            if (options.Count > 1)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"Too many arguments in the definition of \"{TypeName}\".");

            var underlyingType = typeInfoProvider.GetTypeInfo(options[0]);
            return new NullableTypeInfo(underlyingType);
        }

        public Type GetFieldType()
        {
            if (UnderlyingType == null)
                return typeof(DBNull);

            var underlyingFieldType = UnderlyingType.GetFieldType();
            if (underlyingFieldType.IsValueType)
                return typeof(Nullable<>).MakeGenericType(underlyingFieldType);

            return underlyingFieldType;
        }

        public ClickHouseDbType GetDbType()
        {
            if (UnderlyingType == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            return UnderlyingType.GetDbType();
        }

        public IClickHouseTypeInfo GetGenericArgument(int index)
        {
            if (UnderlyingType == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"The type \"{ComplexTypeName}\" is not fully specified.");

            if (index != 0)
                throw new IndexOutOfRangeException();

            return UnderlyingType;
        }

        private sealed class NullableColumnReader : IClickHouseColumnReader
        {
            private readonly int _rowCount;
            private readonly IClickHouseColumnTypeInfo _underlyingType;

            private BitArray? _nullFlags;
            private IClickHouseColumnReader? _baseColumnReader;

            private int _nullFlagPosition;

            public NullableColumnReader(int rowCount, IClickHouseColumnTypeInfo underlyingType)
            {
                _rowCount = rowCount;
                _underlyingType = underlyingType;
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                int bytesCount = 0;
                if (_baseColumnReader == null)
                {
                    foreach (var mem in sequence)
                    {
                        if (_nullFlagPosition == _rowCount)
                            break;

                        foreach (var byteVal in mem.Span)
                        {
                            if (byteVal != 0)
                            {
                                if (_nullFlags == null)
                                    _nullFlags = new BitArray(_rowCount, false);

                                _nullFlags[_nullFlagPosition] = true;
                            }

                            bytesCount++;
                            if (_rowCount == ++_nullFlagPosition)
                                break;
                        }
                    }

                    if (_nullFlagPosition < _rowCount)
                        return new SequenceSize(bytesCount, 0);

                    _baseColumnReader = _underlyingType.CreateColumnReader(_rowCount);
                }

                var baseResult = _baseColumnReader.ReadNext(sequence.Slice(bytesCount));

                return new SequenceSize(bytesCount + baseResult.Bytes, baseResult.Elements);
            }

            public SequenceSize Skip(ReadOnlySequence<byte> sequence, int maxElementsCount, ref object? skipContext)
            {
                int position = 0;
                if (skipContext != null)
                {
                    (IClickHouseColumnReader? baseReader, object? originalSkipContext, int count) = (Tuple<IClickHouseColumnReader?, object?, int>) skipContext;
                    position = count;

                    if (baseReader != null)
                    {
                        var baseSkipContext = originalSkipContext;
                        var result = baseReader.Skip(sequence, maxElementsCount, ref baseSkipContext);

                        if (!ReferenceEquals(baseSkipContext, originalSkipContext))
                            skipContext = new Tuple<IClickHouseColumnReader?, object?, int>(baseReader, baseSkipContext, count);

                        return result;
                    }
                }

                var prefixBytesCount = maxElementsCount - position;
                if (prefixBytesCount < 0)
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");

                if (sequence.Length <= prefixBytesCount)
                {
                    skipContext = new Tuple<IClickHouseColumnReader?, object?, int>(null, null, (int) (position + sequence.Length));
                    return new SequenceSize((int) sequence.Length, 0);
                }

                var baseColumnReader = _underlyingType.CreateColumnReader(0);
                object? baseColumnReaderSkipContext = null;

                var baseSize = baseColumnReader.Skip(sequence.Slice(prefixBytesCount), maxElementsCount, ref baseColumnReaderSkipContext);
                skipContext = new Tuple<IClickHouseColumnReader?, object?, int>(baseColumnReader, baseColumnReaderSkipContext, position + baseSize.Elements);

                return new SequenceSize(baseSize.Bytes + prefixBytesCount, baseSize.Elements);
            }

            public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
            {
                var baseReader = _baseColumnReader ?? _underlyingType.CreateColumnReader(0);
                return NullableTableColumn.MakeNullableColumn(_nullFlags, baseReader.EndRead(settings));
            }
        }

        private sealed class NullableColumnWriter<T> : IClickHouseColumnWriter
        {
            private readonly IReadOnlyList<T> _rows;
            private readonly IClickHouseColumnWriter _internalColumnWriter;

            public string ColumnName { get; }

            public string ColumnType { get; }

            private int _position;

            public NullableColumnWriter(string columnName, string columnType, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings, IClickHouseColumnTypeInfo underlyingTypeInfo)
            {
                if (underlyingTypeInfo == null)
                    throw new ArgumentNullException(nameof(underlyingTypeInfo));
                _rows = rows ?? throw new ArgumentNullException(nameof(rows));
                ColumnName = columnName ?? throw new ArgumentNullException(nameof(columnName));
                ColumnType = columnType ?? throw new ArgumentNullException(nameof(columnType));

                if (typeof(T).IsValueType && typeof(T).IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    var valueType = typeof(T).GetGenericArguments()[0];
                    var dispatcherType = typeof(ValueOrDefaultListDispatcher<>).MakeGenericType(valueType);
                    var columnDispatcher = (IValueOrDefaultListDispatcherBase) Activator.CreateInstance(dispatcherType)!;
                    _internalColumnWriter = columnDispatcher.Dispatch(columnName, rows, columnSettings, underlyingTypeInfo);
                }
                else
                {
                    _internalColumnWriter = underlyingTypeInfo.CreateColumnWriter(columnName, rows, columnSettings);
                }
            }

            public SequenceSize WriteNext(Span<byte> writeTo)
            {
                if (_position == _rows.Count)
                    return _internalColumnWriter.WriteNext(writeTo);

                var len = Math.Min(_rows.Count - _position, writeTo.Length);
                for (int i = 0; i < len; i++)
                {
                    writeTo[i] = _rows[_position + i] == null ? (byte) 1 : (byte) 0;
                }

                _position += len;

                if (_position < _rows.Count)
                    return new SequenceSize(len, 0);

                var size = _internalColumnWriter.WriteNext(writeTo.Slice(len));
                return new SequenceSize(size.Bytes + len, size.Elements);
            }
        }

        private interface IValueOrDefaultListDispatcherBase
        {
            IClickHouseColumnWriter Dispatch(string columnName, object rows, ClickHouseColumnSettings? columnSettings, IClickHouseColumnTypeInfo underlyingTypeInfo);
        }

        private sealed class ValueOrDefaultListDispatcher<TValue> : IValueOrDefaultListDispatcherBase
            where TValue : struct
        {
            public IClickHouseColumnWriter Dispatch(string columnName, object rows, ClickHouseColumnSettings? columnSettings, IClickHouseColumnTypeInfo underlyingTypeInfo)
            {
                var genericList = (IReadOnlyList<TValue?>) rows;
                var listWrapper = new MappedReadOnlyList<TValue?, TValue>(genericList, item => item ?? default);
                return underlyingTypeInfo.CreateColumnWriter(columnName, listWrapper, columnSettings);
            }
        }
    }
}
