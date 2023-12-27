#region License Apache 2.0
/* Copyright 2023 Octonica
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

using Octonica.ClickHouseClient.Types;
using Octonica.ClickHouseClient.Utils;
using System;
using System.Text;

namespace Octonica.ClickHouseClient.Protocol
{
    internal abstract class ClickHouseParameterWriter : IClickHouseParameterValueWriter
    {
        public abstract int Length { get; }

        public abstract int Write(Memory<byte> buffer);

        public abstract StringBuilder Interpolate(StringBuilder queryBuilder);

        public abstract StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeProvider, Func<StringBuilder, IClickHouseTypeInfo, StringBuilder> writeValue);

        public static ClickHouseParameterWriter Dispatch(IClickHouseColumnTypeInfo typeInfo, object? value)
        {
            var dispatcher = new Dispatcher(typeInfo, value);
            return dispatcher.Dispatch();
        }

        private sealed class Dispatcher : ITypeDispatcher<ClickHouseParameterWriter>
        {
            private readonly IClickHouseColumnTypeInfo _typeInfo;
            private readonly object _value;

            public Dispatcher(IClickHouseColumnTypeInfo typeInfo, object? value)
            {
                _typeInfo = typeInfo;
                _value = value ?? DBNull.Value;
            }

            public ClickHouseParameterWriter Dispatch<T>()
            {
                var value = (T)_value;
                var writer = _typeInfo.CreateLiteralWriter<T>();
                if (!writer.TryCreateParameterValueWriter(value, isNested: false, out var valueWriter))
                    valueWriter = null;

                return new ClickHouseParameterWriter<T>(writer, value, valueWriter);
            }

            public ClickHouseParameterWriter Dispatch()
            {
                return TypeDispatcher.Dispatch(_value.GetType(), this);
            }
        }
    }

    internal sealed class ClickHouseParameterWriter<T> : ClickHouseParameterWriter
    {
        private readonly IClickHouseLiteralWriter<T> _writer;
        private readonly T _value;
        private readonly IClickHouseParameterValueWriter? _valueWriter;

        public override int Length => _valueWriter?.Length ?? 0;

        public ClickHouseParameterWriter(IClickHouseLiteralWriter<T> writer, T value, IClickHouseParameterValueWriter? valueWriter)
        {
            _writer = writer;
            _value = value;
            _valueWriter = valueWriter;
        }

        public override StringBuilder Interpolate(StringBuilder queryBuilder)
        {
            return _writer.Interpolate(queryBuilder, _value);
        }

        public override StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeProvider, Func<StringBuilder, IClickHouseTypeInfo, StringBuilder> writerValue)
        {
            if (_valueWriter == null)
            {
                // Here we do not have a real value writer, so we can't pass the parameter to the query. We have to interpolate the value directly into the query.
                return Interpolate(queryBuilder);
            }

            return _writer.Interpolate(queryBuilder, typeProvider, (qb, t, w) => w(qb, b => writerValue(b, t)));
        }

        public override int Write(Memory<byte> buffer)
        {
            return _valueWriter?.Write(buffer) ?? 0;
        }
    }
}
