#region License Apache 2.0
/* Copyright 2019-2021, 2023-2024 Octonica
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
using System.Diagnostics.CodeAnalysis;
using System.Text;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class NothingTypeInfo : IClickHouseColumnTypeInfo
    {
        public string ComplexTypeName => TypeName;

        public string TypeName => "Nothing";

        public int GenericArgumentsCount => 0;

        public IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new NothingColumnReader(rowCount);
        }

        IClickHouseColumnReader IClickHouseColumnTypeInfo.CreateColumnReader(int rowCount, ClickHouseColumnSerializationMode serializationMode)
        {
            if (serializationMode == ClickHouseColumnSerializationMode.Default)
                return CreateColumnReader(rowCount);

            throw new NotSupportedException($"Custom serialization for {TypeName} type is not supported by ClickHouseClient.");
        }

        public IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            return new NothingColumnReader(rowCount);
        }

        IClickHouseColumnReaderBase IClickHouseColumnTypeInfo.CreateSkippingColumnReader(int rowCount, ClickHouseColumnSerializationMode serializationMode)
        {
            if (serializationMode == ClickHouseColumnSerializationMode.Default)
                return CreateSkippingColumnReader(rowCount);

            throw new NotSupportedException($"Custom serialization for {TypeName} type is not supported by ClickHouseClient.");
        }

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            return new NothingColumnWriter(columnName, ComplexTypeName, rows.Count);
        }

        public IClickHouseParameterWriter<T> CreateParameterWriter<T>()
        {
            var type = typeof(T);
            if (type == typeof(DBNull))
                return (IClickHouseParameterWriter<T>)(object)NothingParameterWriter.Instance;

            throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
        }

        IClickHouseColumnTypeInfo IClickHouseColumnTypeInfo.GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{TypeName}\" does not support arguments.");
        }

        public Type GetFieldType()
        {
            return typeof(DBNull);
        }

        public ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Nothing;
        }

        public IClickHouseTypeInfo GetGenericArgument(int index)
        {
            throw new NotSupportedException($"The type \"{TypeName}\" doesn't have generic arguments.");
        }

        private sealed class NothingColumnReader : IClickHouseColumnReader
        {
            private readonly int _rowCount;

            private int _position;

            public NothingColumnReader(int rowCount)
            {
                _rowCount = rowCount;
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                if (_position >= _rowCount)
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");

                var size = (int)Math.Min(sequence.Length, _rowCount - _position);
                _position += size;
                return new SequenceSize(size, size);
            }

            public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
            {
                return new NothingTableColumn(_position);
            }
        }

        private sealed class NothingColumnWriter : IClickHouseColumnWriter
        {
            private readonly int _count;

            public string ColumnName { get; }

            public string ColumnType { get; }

            private int _position;

            public NothingColumnWriter(string columnName, string columnType, int count)
            {
                ColumnName = columnName;
                ColumnType = columnType;
                _count = count;
            }

            public SequenceSize WriteNext(Span<byte> writeTo)
            {
                var size = Math.Min(_count - _position, writeTo.Length);
                for (int i = 0; i < size; i++)
                    writeTo[i] = 48; // 48 == NOTHING

                _position += size;
                return new SequenceSize(size, size);
            }
        }

        internal sealed class NothingParameterWriter : IClickHouseParameterWriter<DBNull>
        {
            public static readonly NothingParameterWriter Instance = new NothingParameterWriter();

            private NothingParameterWriter()
            {
            }

            public bool TryCreateParameterValueWriter(DBNull value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter)
            {
                valueWriter = EmptyParameterValueWriter.Instance;
                return true;
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, DBNull value)
            {
                return queryBuilder.Append("null");
            }

            public StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseColumnTypeInfo, Func<StringBuilder, Func<StringBuilder, StringBuilder>, StringBuilder>, StringBuilder> writeValue)
            {
                var nothingType = typeInfoProvider.GetTypeInfo("Nothing");
                return writeValue(queryBuilder, nothingType, (qb, _) => qb.Append("null"));
            }
        }
    }
}
