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

using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.Diagnostics;
using System.Numerics;

namespace Octonica.ClickHouseClient.Types
{
    internal abstract class BigIntegerTypeInfoBase : SimpleTypeInfo
    {
        int _elementByteSize;
        bool _isUnsigned;

        protected BigIntegerTypeInfoBase(string typeName, int elementByteSize, bool isUnsigned)
            : base(typeName)
        {
            _elementByteSize = elementByteSize;
            _isUnsigned = isUnsigned;
        }

        public sealed override IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new BigIntegerColumnReader(rowCount, _elementByteSize, _isUnsigned);
        }

        public sealed override IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            return new SimpleSkippingColumnReader(_elementByteSize, rowCount);
        }

        public sealed override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            var type = typeof(T);
            IReadOnlyList<BigInteger>? bigIntegerRows = null;
            if (type == typeof(BigInteger))
            {
                bigIntegerRows = (IReadOnlyList<BigInteger>)rows;
            }
            else if (type == typeof(ulong))
            {
                bigIntegerRows = MappedReadOnlyList<ulong, BigInteger>.Map((IReadOnlyList<ulong>)rows, v => v);
            }
            else if (type == typeof(uint))
            {
                bigIntegerRows = MappedReadOnlyList<uint, BigInteger>.Map((IReadOnlyList<uint>)rows, v => v);
            }
            else if (type == typeof(ushort))
            {
                bigIntegerRows = MappedReadOnlyList<ushort, BigInteger>.Map((IReadOnlyList<ushort>)rows, v => v);
            }
            else if (type == typeof(byte))
            {
                bigIntegerRows = MappedReadOnlyList<byte, BigInteger>.Map((IReadOnlyList<byte>)rows, v => v);
            }
            else if (!_isUnsigned)
            {
                if (type == typeof(long))
                    bigIntegerRows = MappedReadOnlyList<long, BigInteger>.Map((IReadOnlyList<long>)rows, v => v);
                else if (type == typeof(int))
                    bigIntegerRows = MappedReadOnlyList<int, BigInteger>.Map((IReadOnlyList<int>)rows, v => v);
                else if (type == typeof(short))
                    bigIntegerRows = MappedReadOnlyList<short, BigInteger>.Map((IReadOnlyList<short>)rows, v => v);
                else if (type == typeof(sbyte))
                    bigIntegerRows = MappedReadOnlyList<sbyte, BigInteger>.Map((IReadOnlyList<sbyte>)rows, v => v);
            }

            if (bigIntegerRows == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return new BigIntegerColumnWriter(columnName, ComplexTypeName, _elementByteSize, bigIntegerRows, _isUnsigned);
        }

        public override IClickHouseParameterWriter<T> CreateParameterWriter<T>()
        {
            var type = typeof(T);
            if (type == typeof(DBNull))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow null values.");

            object? writer = null;
            if (type == typeof(BigInteger))
            {
                if (_isUnsigned)
                {
                    writer = new StringParameterWriter<BigInteger>(
                        this,
                        bigIntegerValue =>
                        {
                            if (bigIntegerValue.Sign < 0)
                                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The ClickHouse type \"{ComplexTypeName}\" does not allow negative BigInteger values.");

                            return bigIntegerValue.ToString(CultureInfo.InvariantCulture).AsMemory();
                        });
                }
                else
                {
                    writer = StringParameterWriter.Create<BigInteger>(this);
                }
            }
            else if (type == typeof(ulong))
            {
                writer = StringParameterWriter.Create<ulong>(this);
            }
            else if (type == typeof(uint))
            {
                writer = StringParameterWriter.Create<uint>(this);
            }
            else if (type == typeof(ushort))
            {
                writer = StringParameterWriter.Create<ushort>(this);
            }
            else if (type == typeof(byte))
            {
                writer = StringParameterWriter.Create<byte>(this);
            }
            else if (!_isUnsigned)
            {
                if (type == typeof(long))
                    writer = StringParameterWriter.Create<long>(this);
                if (type == typeof(int))
                    writer = StringParameterWriter.Create<int>(this);
                if (type == typeof(short))
                    writer = StringParameterWriter.Create<short>(this);
                else if (type == typeof(sbyte))
                    writer = StringParameterWriter.Create<sbyte>(this);
            }

            if (writer == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{type}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return (IClickHouseParameterWriter<T>)writer;
        }

        public sealed override Type GetFieldType()
        {
            return typeof(BigInteger);
        }

        private sealed class BigIntegerColumnReader : IClickHouseColumnReader
        {
            private readonly byte[] _buffer;
            private readonly int _elementByteSize;
            private readonly bool _isUnsigned;

            private int _position;

            public BigIntegerColumnReader(int rowCount, int elementByteSize, bool isUnsigned)
            {
                if (rowCount == 0)
                    _buffer = Array.Empty<byte>();
                else
                    _buffer = new byte[rowCount * elementByteSize];

                _elementByteSize = elementByteSize;
                _isUnsigned = isUnsigned;
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                var rowCount = _buffer.Length / _elementByteSize;
                if (_position >= rowCount)
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");

                var elementCount = (int)Math.Min(rowCount - _position, sequence.Length / _elementByteSize);
                var byteCount = elementCount * _elementByteSize;
                sequence.Slice(0, byteCount).CopyTo(((Span<byte>)_buffer).Slice(_position * _elementByteSize, byteCount));

                _position += elementCount;
                return new SequenceSize(byteCount, elementCount);
            }

            public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
            {
                return new BigIntegerTableColumn(_buffer, _position, _elementByteSize, _isUnsigned);
            }
        }

        private sealed class BigIntegerColumnWriter : StructureWriterBase<BigInteger>
        {
            private readonly bool _isUnsigned;

            public BigIntegerColumnWriter(string columnName, string columnType, int elementSize, IReadOnlyList<BigInteger> rows, bool isUnsigned) 
                : base(columnName, columnType, elementSize, rows)
            {
                _isUnsigned = isUnsigned;
            }

            protected override void WriteElement(Span<byte> writeTo, in BigInteger value)
            {
                if (_isUnsigned && value.Sign < 0)
                    throw new OverflowException($"A negative value can't be written to the column \"{ColumnName}\" of type \"{ColumnType}\".");

                var byteCount = value.GetByteCount(_isUnsigned);
                if (byteCount > ElementSize)
                    throw new OverflowException($"A value can't be written to the column \"{ColumnName}\" of type \"{ColumnType}\" because it's outside of supported range of values.");

                var success = value.TryWriteBytes(writeTo.Slice(0, byteCount), out var bytesWritten, _isUnsigned);
                Debug.Assert(success);
                Debug.Assert(byteCount == bytesWritten);

                if (byteCount < ElementSize)
                    writeTo.Slice(byteCount, ElementSize - byteCount).Fill(_isUnsigned || value.Sign >= 0 ? (byte)0 : (byte)0xff);                
            }
        }
    }
}
