#region License Apache 2.0
/* Copyright 2019-2022 Octonica
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
    internal sealed class DateTimeTypeInfo : IClickHouseConfigurableTypeInfo
    {
        private readonly string? _timeZoneCode;

        /// <summary>
        /// Indicates that the <see cref="_timeZoneCode"/> was acquired from the name of the type.
        /// </summary>
        private readonly bool _explicitTimeZoneCode;

        private TimeZoneInfo? _timeZone;

        public string ComplexTypeName { get; }

        public string TypeName => "DateTime";

        public int GenericArgumentsCount => 0;

        public int TypeArgumentsCount => _timeZoneCode == null || !_explicitTimeZoneCode ? 0 : 1;

        public DateTimeTypeInfo()
            : this(null, false)
        {
        }

        private DateTimeTypeInfo(string? timeZoneCode, bool explicitTimeZoneCode)
        {
            _timeZoneCode = timeZoneCode;
            _explicitTimeZoneCode = explicitTimeZoneCode;
            ComplexTypeName = timeZoneCode == null || !_explicitTimeZoneCode ? TypeName : $"{TypeName}('{timeZoneCode}')";
        }

        public IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            var timeZone = GetTimeZone();
            return new DateTimeReader(rowCount, timeZone);
        }

        public IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            return new SimpleSkippingColumnReader(sizeof(uint), rowCount);
        }

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (typeof(T) == typeof(DateTime))
            {
                var timeZone = GetTimeZone();
                return new DateTimeWriter(columnName, ComplexTypeName, timeZone, (IReadOnlyList<DateTime>)rows);
            }

            if (typeof(T) == typeof(DateTimeOffset))
            {
                var timeZone = GetTimeZone();
                return new DateTimeOffsetWriter(columnName, ComplexTypeName, timeZone, (IReadOnlyList<DateTimeOffset>)rows);
            }

            throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
        }

        public IClickHouseColumnTypeInfo GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            if (options.Count > 1)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"Too many arguments in the definition of \"{TypeName}\".");

            var tzCode = options[0].Trim('\'').ToString();
            return new DateTimeTypeInfo(tzCode, true);
        }

        public Type GetFieldType()
        {
            return typeof(DateTimeOffset);
        }

        public ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.DateTimeOffset;
        }

        public IClickHouseTypeInfo GetGenericArgument(int index)
        {
            throw new NotSupportedException($"The type \"{TypeName}\" doesn't have generic arguments.");
        }

        public object GetTypeArgument(int index)
        {
            if (_timeZoneCode == null || !_explicitTimeZoneCode)
                throw new NotSupportedException($"The type \"{TypeName}\" doesn't have arguments.");

            if (index != 0)
                throw new IndexOutOfRangeException();

            return _timeZoneCode;
        }

        public IClickHouseColumnTypeInfo Configure(ClickHouseServerInfo serverInfo)
        {
            return new DateTimeTypeInfo(serverInfo.Timezone, false);
        }

        private TimeZoneInfo GetTimeZone()
        {
            if (_timeZone != null)
                return _timeZone;

            if (_timeZoneCode == null)
                return TimeZoneInfo.Utc;

            _timeZone = TimeZoneHelper.GetTimeZoneInfo(_timeZoneCode);
            return _timeZone;
        }

        private sealed class DateTimeReader : StructureReaderBase<uint, DateTimeOffset>
        {
            private readonly TimeZoneInfo _timeZone;            

            protected override bool BitwiseCopyAllowed => true;

            public DateTimeReader(int rowCount, TimeZoneInfo timeZone)
                : base(sizeof(uint), rowCount)
            {
                _timeZone = timeZone;
            }

            protected override uint ReadElement(ReadOnlySpan<byte> source)
            {
                return BitConverter.ToUInt32(source);
            }

            protected override IClickHouseTableColumn<DateTimeOffset> EndRead(ClickHouseColumnSettings? settings, ReadOnlyMemory<uint> buffer)
            {
                return new DateTimeTableColumn(buffer, _timeZone);
            }
        }

        private sealed class DateTimeWriter : StructureWriterBase<DateTime, uint>
        {
            private readonly TimeZoneInfo _timeZone;

            public DateTimeWriter(string columnName, string columnType, TimeZoneInfo timeZone, IReadOnlyList<DateTime> rows)
                : base(columnName, columnType, sizeof(uint), rows)
            {
                _timeZone = timeZone;
            }

            protected override uint Convert(DateTime value)
            {
                uint seconds;
                if (value == default)
                {
                    seconds = 0;
                }
                else
                {
                    var doubleSeconds = (value - DateTime.UnixEpoch).TotalSeconds - _timeZone.GetUtcOffset(value).TotalSeconds;
                    if (doubleSeconds < 0 || doubleSeconds > uint.MaxValue)
                        throw new OverflowException("The value must be in range [1970-01-01 00:00:00, 2105-12-31 23:59:59].");

                    seconds = (uint) doubleSeconds;
                }

                return seconds;
            }
        }

        private sealed class DateTimeOffsetWriter : StructureWriterBase<DateTimeOffset, uint>
        {
            private readonly TimeZoneInfo _timeZone;

            public DateTimeOffsetWriter(string columnName, string columnType, TimeZoneInfo timeZone, IReadOnlyList<DateTimeOffset> rows)
                : base(columnName, columnType, sizeof(uint), rows)
            {
                _timeZone = timeZone;
            }

            protected override uint Convert(DateTimeOffset value)
            {
                uint seconds;
                if (value == default)
                {
                    seconds = 0;
                }
                else
                {
                    var offset = _timeZone.GetUtcOffset(value);
                    var valueWithOffset = value.ToOffset(offset);
                    var doubleSeconds = (valueWithOffset - DateTimeOffset.UnixEpoch).TotalSeconds;
                    if (doubleSeconds < 0 || doubleSeconds > uint.MaxValue)
                        throw new OverflowException("The value must be in range [1970-01-01 00:00:00, 2105-12-31 23:59:59].");

                    seconds = (uint)doubleSeconds;
                }

                return seconds;
            }
        }
    }
}
