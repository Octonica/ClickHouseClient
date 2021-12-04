#region License Apache 2.0
/* Copyright 2019-2021 Octonica
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
using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Types;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// Provides a way of reading a forward-only stream of rows from a ClickHouse database. This class cannot be inherited.
    /// </summary>
    public sealed class ClickHouseDataReader : ClickHouseDataReaderBase
    {
        private readonly ClickHouseTcpClient.Session _session;
        private readonly ClickHouseDataReaderRowLimit _rowLimit;
        private readonly bool _ignoreProfileEvents;
        private ulong _recordsAffected;

        private int _rowIndex = -1;

        private IServerMessage? _nextResultMessage;
        private ClickHouseTable _currentTable;

        private IClickHouseTableColumn[] _reinterpretedColumnsCache;
        private ClickHouseColumnSettings?[]? _columnSettings;

        /// <summary>
        /// Gets the current state of the reader.
        /// </summary>
        public ClickHouseDataReaderState State { get; private set; }

        /// <summary>
        /// Gets the number of affected rows.
        /// </summary>
        /// <returns>A non-negative number of rows affected by the query.</returns>
        /// <exception cref="OverflowException">The number of affected rows is greater than <see cref="int.MaxValue"/>.</exception>
        /// <remarks>Use the property <see cref="RecordsAffectedLong"/> if the query can affect more than <see cref="int.MaxValue"/> rows .</remarks>
        public override int RecordsAffected
        {
            get
            {
                if (_recordsAffected <= int.MaxValue)
                    return (int) _recordsAffected;

                throw new OverflowException($"The number of affected records is too large. Use the property \"{nameof(RecordsAffectedLong)}\" to get this number.");
            }
        }

        /// <summary>
        /// Gets the number of affected rows.
        /// </summary>
        /// <returns>A number of rows affected by the query.</returns>
        public ulong RecordsAffectedLong => _recordsAffected;

        /// <summary>
        /// Gets a value that indicates whether the reader contains one or more rows.
        /// </summary>
        /// /// <returns><see langword="true"/> if the reader contains one or more rows; otherwise <see langword="false"/>.</returns>
        public override bool HasRows => _rowIndex < 0 || _recordsAffected > 0;

        /// <summary>
        /// Gets the value indicating whether the reader is closed.
        /// </summary>
        /// <returns><see langword="true"/> if the reader is closed; otherwise <see langword="false"/>.</returns>
        public override bool IsClosed => State == ClickHouseDataReaderState.Closed || State == ClickHouseDataReaderState.Broken;

        /// <summary>
        /// Gets the number of columns.
        /// </summary>
        public override int FieldCount => _currentTable.Header.Columns.Count;

        /// <summary>
        /// Gets a value that indicates the depth of nesting for the current row.
        /// </summary>
        /// <returns>Always returns 0.</returns>
        public override int Depth => 0;

        internal ClickHouseDataReader(ClickHouseTable table, ClickHouseTcpClient.Session session, ClickHouseDataReaderRowLimit rowLimit, bool ignoreProfileEvents)
        {
            _currentTable = table.Header == null || table.Columns == null ? throw new ArgumentNullException(nameof(table)) : table;
            _session = session ?? throw new ArgumentNullException(nameof(session));
            _rowLimit = rowLimit;
            _ignoreProfileEvents = ignoreProfileEvents;
            _reinterpretedColumnsCache = new IClickHouseTableColumn[_currentTable.Columns.Count];
            _recordsAffected = checked((ulong) _currentTable.Header.RowCount);
            State = _rowLimit == ClickHouseDataReaderRowLimit.Zero ? ClickHouseDataReaderState.ClosePending : ClickHouseDataReaderState.Data;
        }

        // Note that this xml comment is inherited by ClickHouseColumnWriter.ConfigureColumn
        /// <summary>
        /// Applies the settings to the specified column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <param name="columnSettings">The settings.</param>
        public void ConfigureColumn(string name, ClickHouseColumnSettings columnSettings)
        {
            var index = GetOrdinal(name);
            if (index < 0)
                throw new ArgumentException($"A column with the name \"{name}\" not found.", nameof(name));

            ConfigureColumn(index, columnSettings);
        }

        // Note that this xml comment is inherited by ClickHouseColumnWriter.ConfigureColumn
        /// <summary>
        /// Applies the settings to the specified column.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <param name="columnSettings">The settings.</param>
        public void ConfigureColumn(int ordinal, ClickHouseColumnSettings columnSettings)
        {
            if (_rowIndex >= 0)
                throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "The column can't be reconfigured during reading.");

            if (State == ClickHouseDataReaderState.ProfileEvents)
                throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "The column can't be configured when reading profile events.");

            if (_columnSettings == null)
                _columnSettings = new ClickHouseColumnSettings?[_currentTable.Columns.Count];

            _columnSettings[ordinal] = columnSettings;
        }

        // Note that this xml comment is inherited by ClickHouseColumnWriter.ConfigureColumnWriter
        /// <summary>
        /// Applies the settings to all columns. All previously applied settings are discarded.
        /// </summary>
        /// <param name="columnSettings">The settings.</param>
        public void ConfigureDataReader(ClickHouseColumnSettings columnSettings)
        {
            if (_rowIndex >= 0)
                throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "The reader can't be reconfigured during reading.");

            if (_columnSettings == null)
                _columnSettings = new ClickHouseColumnSettings?[_currentTable.Columns.Count];

            for (int i = 0; i < _currentTable.Columns.Count; i++)
                _columnSettings[i] = columnSettings;
        }

        // Note that this xml comment is inherited by ClickHouseColumnWriter.GetFieldTypeInfo
        /// <summary>
        /// Gets the <see cref="IClickHouseTypeInfo"/> which represents information about the type of the column.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The <see cref="IClickHouseTypeInfo"/> which represents information about the type of the column.</returns>
        public IClickHouseTypeInfo GetFieldTypeInfo(int ordinal)
        {
            return _currentTable.Header.Columns[ordinal].TypeInfo;
        }

        // Note that this xml comment is inherited by ClickHouseColumnWriter.GetName
        /// <summary>
        /// Gets the name of the specified column.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The name of the specified column.</returns>
        public sealed override string GetName(int ordinal)
        {
            return _currentTable.Header.Columns[ordinal].Name;
        }

        // Note that this xml comment is inherited by ClickHouseColumnWriter.GetDataTypeName
        /// <summary>
        /// Gets the name of the data type of the specified column.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The string representing the data type of the specified column.</returns>
        public sealed override string GetDataTypeName(int ordinal)
        {
            return _currentTable.Header.Columns[ordinal].TypeInfo.ComplexTypeName;
        }

        // Note that this xml comment is inherited by ClickHouseColumnWriter.GetFieldType
        /// <summary>
        /// Gets the <see cref="Type"/> that is the data type of the object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The <see cref="Type"/> that is the data type of the object.</returns>
        public override Type GetFieldType(int ordinal)
        {
            // This method must return the type of a value returned by GetValue.
            // GetValue should always return DBNull.Value instead of null.
            // So an actual field type should be unboxed from Nullable<T>.

            var type = _columnSettings?[ordinal]?.ColumnType;
            type ??= _currentTable.Header.Columns[ordinal].TypeInfo.GetFieldType();
            return Nullable.GetUnderlyingType(type) ?? type;
        }

        // Note that this xml comment is inherited by ClickHouseColumnWriter.GetOrdinal
        /// <summary>
        /// Gets the column ordinal, given the name of the column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>The zero-based column ordinal.</returns>
        public sealed override int GetOrdinal(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            return CommonUtils.GetColumnIndex(_currentTable.Header.Columns, name);
        }

        /// <summary>
        /// Reads the value as an array of <see cref="byte"/> and copies values from it to the buffer.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <param name="dataOffset">The index within the field from which to begin the read operation.</param>
        /// <param name="buffer">The buffer into which to copy bytes.</param>
        /// <param name="bufferOffset">The index within the <paramref name="buffer"/> where the write operation is to start.</param>
        /// <param name="length">The maximum length to copy into the buffer.</param>
        /// <returns>The actual number of bytes copied.</returns>
        public sealed override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            var arrayColumn = _reinterpretedColumnsCache[ordinal] as IClickHouseArrayTableColumn<byte>;
            if (arrayColumn == null)
            {
                arrayColumn = _currentTable.Columns[ordinal].TryReinterpretAsArray<byte>();
                if (arrayColumn != null)
                    _reinterpretedColumnsCache[ordinal] = arrayColumn;
            }

            if (arrayColumn != null)
            {
                CheckRowIndex();
                return arrayColumn.CopyTo(_rowIndex, new Span<byte>(buffer, bufferOffset, Math.Min(buffer?.Length ?? 0, length)), checked((int)dataOffset));
            }

            var value = GetFieldValue<byte[]>(ordinal, null);
            if (value == null)
            {
                if (dataOffset == 0)
                    return 0;

                throw new ArgumentOutOfRangeException(nameof(dataOffset));
            }

            ReadOnlySpan<byte> source = value;
            Span<byte> target = buffer;

            int resultLength = Math.Min(value.Length - (int) dataOffset, length);

            source = source.Slice((int) dataOffset, resultLength);
            target = target.Slice(bufferOffset, resultLength);

            source.CopyTo(target);
            return resultLength;
        }

        /// <summary>
        /// Reads the value as a <see cref="string"/> and copies characters from it to the buffer.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <param name="dataOffset">The index within the field from which to begin the read operation.</param>
        /// <param name="buffer">The buffer into which to copy characters of the string.</param>
        /// <param name="bufferOffset">The index within the <paramref name="buffer"/> where the write operation is to start.</param>
        /// <param name="length">The maximum length to copy into the buffer.</param>
        /// <returns>The actual number of characters copied.</returns>
        public sealed override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            var arrayColumn = _reinterpretedColumnsCache[ordinal] as IClickHouseArrayTableColumn<char>;
            if (arrayColumn == null)
            {
                arrayColumn = _currentTable.Columns[ordinal].TryReinterpretAsArray<char>();
                if (arrayColumn != null)
                    _reinterpretedColumnsCache[ordinal] = arrayColumn;
            }

            if (arrayColumn != null)
            {
                CheckRowIndex();
                return arrayColumn.CopyTo(_rowIndex, new Span<char>(buffer, bufferOffset, Math.Min(buffer?.Length ?? 0, length)), checked((int)dataOffset));
            }

            var value = GetFieldValue<string>(ordinal, null);
            if (value == null)
            {
                if (dataOffset == 0)
                    return 0;

                throw new ArgumentOutOfRangeException(nameof(dataOffset));
            }

            ReadOnlySpan<char> source = value;
            Span<char> target = buffer;

            int resultLength = Math.Min(value.Length - (int) dataOffset, length);

            source = source.Slice((int) dataOffset, resultLength);
            target = target.Slice(bufferOffset, resultLength);

            source.CopyTo(target);
            return resultLength;
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="BigInteger"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public BigInteger GetBigInteger(int ordinal)
        {
            return GetFieldValue<BigInteger>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="bool"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override bool GetBoolean(int ordinal)
        {
            return GetFieldValue<bool>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="byte"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override byte GetByte(int ordinal)
        {
            return GetFieldValue<byte>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="sbyte"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sbyte GetSByte(int ordinal)
        {
            return GetFieldValue<sbyte>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="char"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override char GetChar(int ordinal)
        {
            return GetFieldValue<char>(ordinal);
        }

#if NET6_0_OR_GREATER
        /// <summary>
        /// Gets the value of the specified column as a <see cref="DateOnly"/> object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public DateOnly GetDate(int ordinal)
        {
            return GetFieldValue<DateOnly>(ordinal);
        }
#endif

        /// <summary>
        /// Gets the value of the specified column as a <see cref="DateTime"/> object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override DateTime GetDateTime(int ordinal)
        {
            return GetFieldValue<DateTime>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="decimal"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override decimal GetDecimal(int ordinal)
        {
            return GetFieldValue<decimal>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="double"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override double GetDouble(int ordinal)
        {
            return GetFieldValue<double>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="float"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override float GetFloat(int ordinal)
        {
            return GetFieldValue<float>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a globally unique identifier (<see cref="Guid"/>).
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override Guid GetGuid(int ordinal)
        {
            return GetFieldValue<Guid>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="short"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override short GetInt16(int ordinal)
        {
            return GetFieldValue<short>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="ushort"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public ushort GetUInt16(int ordinal)
        {
            return GetFieldValue<ushort>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as an <see cref="int"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override int GetInt32(int ordinal)
        {
            return GetFieldValue<int>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as an <see cref="uint"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public uint GetUInt32(int ordinal)
        {
            return GetFieldValue<uint>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="long"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override long GetInt64(int ordinal)
        {
            return GetFieldValue<long>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="ulong"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public ulong GetUInt64(int ordinal)
        {
            return GetFieldValue<ulong>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of <see cref="IPAddress"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public IPAddress GetIPAddress(int ordinal)
        {
            return GetFieldValue<IPAddress>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of <see cref="string"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override string GetString(int ordinal)
        {
            return GetFieldValue<string>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of <see cref="string"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <param name="nullValue">The default value which should be returned if the value of the specified column is <see cref="DBNull.Value"/>.</param>
        /// <returns>The value of the specified column or <paramref name="nullValue"/> if the value of the column is <see cref="DBNull.Value"/>.</returns>
        [return: NotNullIfNotNull("nullValue")]
        public string? GetString(int ordinal, string? nullValue)
        {
            return GetFieldValue(ordinal, nullValue);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="DateTimeOffset"/> object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public DateTimeOffset GetDateTimeOffset(int ordinal)
        {
            return GetFieldValue<DateTimeOffset>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as an object of the type <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The expected type of the column's value.</typeparam>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <param name="nullValue">The default value which should be returned if the value of the specified column is <see cref="DBNull.Value"/>.</param>
        /// <returns>The value of the specified column or <paramref name="nullValue"/> if the value of the column is <see cref="DBNull.Value"/>.</returns>
        [return: NotNullIfNotNull("nullValue")]
        public T? GetFieldValue<T>(int ordinal, T? nullValue)
            where T : class
        {
            CheckRowIndex();
            var column = _currentTable.Columns[ordinal];
            if (column is IClickHouseTableColumn<T> typedColumn)
                return typedColumn.GetValue(_rowIndex) ?? nullValue;

            if (_reinterpretedColumnsCache[ordinal] is IClickHouseTableColumn<T> reinterpretedColumn)
                return reinterpretedColumn.GetValue(_rowIndex) ?? nullValue;

            var rc = column.TryReinterpret<T>();
            if (rc != null)
            {
                _reinterpretedColumnsCache[ordinal] = rc;
                return rc.GetValue(_rowIndex) ?? nullValue;
            }

            if (column.IsNull(_rowIndex))
                return nullValue;

            var value = column.GetValue(_rowIndex);
            return (T) value;
        }

        /// <inheritdoc cref="GetFieldValue{T}(int, T)"/>
        [return: NotNullIfNotNull("nullValue")]
        public T? GetFieldValue<T>(int ordinal, T? nullValue)
            where T : struct
        {
            CheckRowIndex();
            var column = _currentTable.Columns[ordinal];
            if (column is IClickHouseTableColumn<T?> nullableTypedColumn)
                return nullableTypedColumn.GetValue(_rowIndex) ?? nullValue;

            var rc = _reinterpretedColumnsCache[ordinal] as IClickHouseTableColumn<T?>;
            if (rc != null)
                return rc.GetValue(_rowIndex) ?? nullValue;

            rc = column.TryReinterpret<T?>();
            if (rc == null)
            {
                var structRc = column.TryReinterpret<T>();
                if (structRc != null)
                    rc = new NullableStructTableColumn<T>(null, structRc);
            }

            if (rc == null && column is IClickHouseTableColumn<T> typedColumn)
                rc = new NullableStructTableColumn<T>(null, typedColumn);

            if (rc != null)
            {
                _reinterpretedColumnsCache[ordinal] = rc;
                return rc.GetValue(_rowIndex) ?? nullValue;
            }

            if (column.IsNull(_rowIndex))
                return nullValue;

            var value = column.GetValue(_rowIndex);
            return (T) value;
        }

        /// <summary>
        /// Gets the value of the specified column as an object of the type <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The expected type of the column's value.</typeparam>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column.</returns>
        public sealed override T GetFieldValue<T>(int ordinal)
        {
            CheckRowIndex();
            var column = _currentTable.Columns[ordinal];
            if (column.IsNull(_rowIndex))
                throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "The value is null.");

            if (column is IClickHouseTableColumn<T> typedColumn)
                return typedColumn.GetValue(_rowIndex);

            if (_reinterpretedColumnsCache[ordinal] is IClickHouseTableColumn<T> reinterpretedColumn)
                return reinterpretedColumn.GetValue(_rowIndex);

            var rc = column.TryReinterpret<T>();
            if (rc != null)
            {
                _reinterpretedColumnsCache[ordinal] = rc;
                return rc.GetValue(_rowIndex);
            }

            var value = column.GetValue(_rowIndex);
            return (T) value;
        }

        /// <summary>
        /// Gets the value of the specified column as an <see cref="object"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column or <see cref="DBNull.Value"/>.</returns>
        public sealed override object GetValue(int ordinal)
        {
            CheckRowIndex();
            var column = _currentTable.Columns[ordinal];
            return column.IsNull(_rowIndex) ? DBNull.Value : column.GetValue(_rowIndex);
        }

        /// <summary>
        /// Populates an array of objects with the column values of the current row.
        /// </summary>
        /// <param name="values">An array of <see cref="object"/> into which to copy the attribute columns.</param>
        /// <returns>The number of instances of <see cref="object"/> in the array.</returns>
        public sealed override int GetValues(object[] values)
        {
            CheckRowIndex();
            var count = Math.Min(_currentTable.Columns.Count, values.Length);
            for (int i = 0; i < count; i++)
            {
                var column = _currentTable.Columns[i];
                if (column.IsNull(_rowIndex))
                    values[i] = DBNull.Value;
                else
                    values[i] = column.GetValue(_rowIndex);
            }

            return count;
        }

        /// <summary>
        /// Gets a value that indicates whether the column contains non-existent or missing values.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns><see langword="true"/> if the specified column is equivalent to <see cref="DBNull.Value"/>. Otherwise <see langword="false"/>.</returns>
        public sealed override bool IsDBNull(int ordinal)
        {
            CheckRowIndex();
            var column = _currentTable.Columns[ordinal];
            return column.IsNull(_rowIndex);
        }

        /// <inheritdoc cref="GetValue(int)"/>
        public sealed override object this[int ordinal] => GetValue(ordinal);

        /// <summary>
        /// Gets the value of the specified column as an <see cref="object"/>.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>The value of the specified column or <see cref="DBNull.Value"/>.</returns>
        public sealed override object this[string name]
        {
            get
            {
                var ordinal = GetOrdinal(name);
                if (ordinal < 0)
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, $"Column \"{name}\" not found.");

                return GetValue(ordinal);
            }
        }

        /// <summary>
        /// Advances the reader to the next record in a result set.
        /// </summary>
        /// <returns><see langword="true"/> if there are more rows or <see langword="false"/> if there aren't.</returns>
        public sealed override bool Read()
        {
            return TaskHelper.WaitNonAsyncTask(Read(false, CancellationToken.None));
        }

        /// <summary>
        /// Asyncronously advances the reader to the next record in a result set.
        /// </summary>
        /// <returns>
        /// A <see cref="ValueTask{TResult}"/> whose <see cref="ValueTask{TResult}.Result"/> is
        /// <see langword="true"/> if there are more rows or <see langword="false"/> if there aren't.
        /// </returns>
        public new ValueTask<bool> ReadAsync()
        {
            return Read(true, CancellationToken.None);
        }

        /// <summary>
        /// Asyncronously advances the reader to the next record in a result set.
        /// </summary>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>
        /// A <see cref="ValueTask{TResult}"/> whose <see cref="ValueTask{TResult}.Result"/> is
        /// <see langword="true"/> if there are more rows or <see langword="false"/> if there aren't.
        /// </returns>
        public new ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
        {
            return Read(true, cancellationToken);
        }

        /// <inheritdoc cref="ClickHouseDataReaderBase.ReadAsync(CancellationToken)"/>
        protected sealed override async Task<bool> ReadAsyncInternal(CancellationToken cancellationToken)
        {
            return await Read(true, cancellationToken);
        }

        private async ValueTask<bool> Read(bool async, CancellationToken cancellationToken)
        {
            if (State == ClickHouseDataReaderState.Closed || State == ClickHouseDataReaderState.Broken || State == ClickHouseDataReaderState.ClosePending)
                return false;

            bool result;
            if (++_rowIndex >= _currentTable.Header.RowCount)
            {
                _rowIndex = _currentTable.Header.RowCount;

                if (State == ClickHouseDataReaderState.Data || State == ClickHouseDataReaderState.Totals || State == ClickHouseDataReaderState.Extremes)
                {
                    result = await Read(false, async, cancellationToken);

                    if (!result && _rowLimit == ClickHouseDataReaderRowLimit.OneResult && State == ClickHouseDataReaderState.NextResultPending)
                        await Cancel(false, async);
                }
                else
                {
                    result = false;
                }
            }
            else
            {                
                result = true;
            }

            if (_rowLimit == ClickHouseDataReaderRowLimit.OneRow && result)
                await Cancel(false, async);

            return result;
        }

        private async ValueTask<bool> Read(bool nextResult, bool async, CancellationToken cancellationToken)
        {
            while (true)
            {
                ClickHouseTable nextTable;

                try
                {
                    var message = _nextResultMessage;
                    if (message == null)
                        message = await _session.ReadMessage(async, cancellationToken);
                    else
                        _nextResultMessage = null;

                    switch (message.MessageCode)
                    {
                        case ServerMessageCode.Data:
                            switch (State)
                            {
                                case ClickHouseDataReaderState.NextResultPending:
                                    State = ClickHouseDataReaderState.Data;
                                    goto case ClickHouseDataReaderState.Data;

                                case ClickHouseDataReaderState.Data:
                                {
                                    var dataMessage = (ServerDataMessage) message;
                                    nextTable = await _session.ReadTable(dataMessage, _columnSettings, async, cancellationToken);
                                    break;
                                }

                                case ClickHouseDataReaderState.Totals:
                                case ClickHouseDataReaderState.Extremes:
                                {
                                    var dataMessage = (ServerDataMessage)message;
                                    var table = await _session.SkipTable(dataMessage, async, cancellationToken);
                                    if (table.RowCount != 0 || table.Columns.Count != 0)
                                        throw new ClickHouseException(ClickHouseErrorCodes.ProtocolUnexpectedResponse, "Unexpected data block after totals or extremes.");

                                    continue;
                                }

                                case ClickHouseDataReaderState.ProfileEvents:
                                    _nextResultMessage = message;
                                    State = ClickHouseDataReaderState.NextResultPending;
                                    return false;

                                default:
                                    goto UNEXPECTED_RESPONSE;
                            }

                            break;

                        case ServerMessageCode.Error:
                            State = ClickHouseDataReaderState.Closed;
                            await _session.Dispose(async);
                            throw ((ServerErrorMessage) message).Exception;

                        case ServerMessageCode.Progress:
                            var progressMessage = (ServerProgressMessage) message;
                            _recordsAffected = progressMessage.Rows;
                            continue;

                        case ServerMessageCode.EndOfStream:
                            State = ClickHouseDataReaderState.Closed;
                            await _session.Dispose(async);
                            return false;

                        case ServerMessageCode.ProfileInfo:
                            continue;

                        case ServerMessageCode.Totals:
                            switch (State)
                            {
                                case ClickHouseDataReaderState.NextResultPending:
                                    State = ClickHouseDataReaderState.Totals;
                                    goto case ClickHouseDataReaderState.Totals;

                                case ClickHouseDataReaderState.Totals:
                                    var totalsMessage = (ServerDataMessage) message;
                                    nextTable = await _session.ReadTable(totalsMessage, _columnSettings, async, cancellationToken);
                                    break;

                                case ClickHouseDataReaderState.Data:
                                case ClickHouseDataReaderState.Extremes:
                                case ClickHouseDataReaderState.ProfileEvents:
                                    _nextResultMessage = message;
                                    State = ClickHouseDataReaderState.NextResultPending;
                                    return false;

                                default:
                                    goto UNEXPECTED_RESPONSE;
                            }

                            break;

                        case ServerMessageCode.Extremes:
                            switch (State)
                            {
                                case ClickHouseDataReaderState.NextResultPending:
                                    State = ClickHouseDataReaderState.Extremes;
                                    goto case ClickHouseDataReaderState.Extremes;

                                case ClickHouseDataReaderState.Extremes:
                                    var extremesMessage = (ServerDataMessage) message;
                                    nextTable = await _session.ReadTable(extremesMessage, _columnSettings, async, cancellationToken);
                                    break;

                                case ClickHouseDataReaderState.Data:
                                case ClickHouseDataReaderState.Totals:
                                case ClickHouseDataReaderState.ProfileEvents:
                                    _nextResultMessage = message;
                                    State = ClickHouseDataReaderState.NextResultPending;
                                    return false;

                                default:
                                    goto UNEXPECTED_RESPONSE;
                            }

                            break;

                        case ServerMessageCode.ProfileEvents:
                            if (_ignoreProfileEvents)
                            {
                                await _session.SkipTable((ServerDataMessage)message, async, cancellationToken);
                                continue;
                            }

                            switch (State)
                            {
                                case ClickHouseDataReaderState.NextResultPending:
                                    State = ClickHouseDataReaderState.ProfileEvents;
                                    goto case ClickHouseDataReaderState.ProfileEvents;

                                case ClickHouseDataReaderState.ProfileEvents:
                                    var profileEventsMessage = (ServerDataMessage) message;
                                    nextTable = await _session.ReadTable(profileEventsMessage, null, async, cancellationToken);
                                    break;

                                case ClickHouseDataReaderState.Data:
                                case ClickHouseDataReaderState.Extremes:
                                case ClickHouseDataReaderState.Totals:
                                    _nextResultMessage = message;
                                    State = ClickHouseDataReaderState.NextResultPending;
                                    return false;

                                default:
                                    goto UNEXPECTED_RESPONSE;
                            }

                            break;

                        case ServerMessageCode.Pong:
                        case ServerMessageCode.Hello:
                        case ServerMessageCode.Log:
                            UNEXPECTED_RESPONSE:
                            throw new ClickHouseException(ClickHouseErrorCodes.ProtocolUnexpectedResponse, $"Unexpected server message: \"{message.MessageCode}\".");

                        default:
                            throw new NotSupportedException($"Internal error. Message code \"{message.MessageCode}\" not supported.");
                    }
                }
                catch (ClickHouseHandledException)
                {
                    throw;
                }
                catch (ClickHouseServerException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    State = ClickHouseDataReaderState.Broken;
                    var aggrEx = await _session.SetFailed(ex, true, async);
                    if (aggrEx != null)
                        throw aggrEx;

                    throw;
                }

                if (nextTable.Header.RowCount == 0)
                    continue;

                _currentTable = nextTable;
                _reinterpretedColumnsCache = new IClickHouseTableColumn[_currentTable.Columns.Count];
                _recordsAffected = checked(_recordsAffected + (ulong) nextTable.Header.RowCount);
                _rowIndex = nextResult ? -1 : 0;
                return true;
            }
        }

        /// <summary>
        /// Asyncronously advances the reader to the next result set. The ClickHouse server can send totals or extremes as additional result sets.
        /// </summary>
        /// <returns>A <see cref="Task{T}"/> representing asyncronous operation. The result (<see cref="Task{TResult}.Result"/>) is
        /// <see langword="true"/> if there are more result sets; otherwise <see langword="false"/>
        /// </returns>
        public override async Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            return await NextResult(true, cancellationToken);
        }

        /// <summary>
        /// Advances the reader to the next result set. The ClickHouse server can send totals or extremes as additional result sets.
        /// </summary>
        /// <returns><see langword="true"/> if there are more result sets; otherwise <see langword="false"/></returns>
        public override bool NextResult()
        {
            return TaskHelper.WaitNonAsyncTask(NextResult(false, CancellationToken.None));
        }

        private async ValueTask<bool> NextResult(bool async, CancellationToken cancellationToken)
        {
            if (State == ClickHouseDataReaderState.Data || State == ClickHouseDataReaderState.Totals || State == ClickHouseDataReaderState.Extremes || State == ClickHouseDataReaderState.ProfileEvents)
            {
                bool canReadNext;
                do
                {
                    _rowIndex = Math.Max(_rowIndex, _currentTable.Header.RowCount - 1);
                    // TODO: skip without actual reading
                    canReadNext = await Read(false, async, cancellationToken);
                } while (canReadNext);
            }

            if (State != ClickHouseDataReaderState.NextResultPending)
                return false;

            return await Read(true, async, cancellationToken);
        }

        private async ValueTask Cancel(bool disposing, bool async)
        {
            if (State == ClickHouseDataReaderState.Closed || State == ClickHouseDataReaderState.Broken || State == ClickHouseDataReaderState.ClosePending)
                return;

            try
            {
                await _session.SendCancel(async);
                State = ClickHouseDataReaderState.ClosePending;
            }
            catch (Exception ex)
            {
                State = ClickHouseDataReaderState.Broken;
                await _session.SetFailed(ex, false, async);

                if (!disposing)
                    throw;
            }
        }

        /// <summary>
        /// Closes the reader.
        /// </summary>
        public override void Close()
        {
            TaskHelper.WaitNonAsyncTask(Close(false, false));
        }

        /// <summary>
        /// Asyncronously closes the reader.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the asyncronous operation.</returns>
        public override async Task CloseAsync()
        {
            await Close(false, true);
        }

        private async ValueTask Close(bool disposing, bool async)
        {
            if (_session.IsDisposed || _session.IsFailed)
                return;

            if (!(State == ClickHouseDataReaderState.Closed || State == ClickHouseDataReaderState.Broken))
            {
                await Cancel(disposing, async);

                try
                {
                    while (true)
                    {

                        var message = _nextResultMessage ?? await _session.ReadMessage(async, CancellationToken.None);
                        _nextResultMessage = null;

                        switch (message.MessageCode)
                        {
                            case ServerMessageCode.Data:
                            case ServerMessageCode.Totals:
                            case ServerMessageCode.Extremes:
                            case ServerMessageCode.ProfileEvents:
                                var dataMessage = (ServerDataMessage) message;
                                await _session.SkipTable(dataMessage, async, CancellationToken.None);
                                continue;

                            case ServerMessageCode.Error:
                                State = ClickHouseDataReaderState.Closed;
                                if (disposing)
                                    break;

                                throw ((ServerErrorMessage) message).Exception;

                            case ServerMessageCode.Progress:
                                var progressMessage = (ServerProgressMessage) message;
                                _recordsAffected = progressMessage.Rows;
                                continue;

                            case ServerMessageCode.EndOfStream:
                                State = ClickHouseDataReaderState.Closed;
                                break;

                            case ServerMessageCode.ProfileInfo:
                                continue;

                            case ServerMessageCode.Pong:
                            case ServerMessageCode.Hello:
                            case ServerMessageCode.Log:
                                throw new ClickHouseException(ClickHouseErrorCodes.ProtocolUnexpectedResponse, $"Unexpected server message: \"{message.MessageCode}\".");

                            default:
                                throw new NotSupportedException($"Internal error. Message code \"{message.MessageCode}\" not supported.");
                        }

                        break;
                    }
                }
                catch (ClickHouseHandledException ex)
                {
                    if (!disposing)
                        throw;

                    State = ClickHouseDataReaderState.Broken;
                    await _session.SetFailed(ex.InnerException, false, async);
                    return;
                }
                catch (Exception ex)
                {
                    State = ClickHouseDataReaderState.Broken;
                    var aggrEx = await _session.SetFailed(ex, false, async);

                    if (disposing)
                        return;

                    if (aggrEx != null)
                        throw aggrEx;

                    throw;
                }
            }

            await _session.Dispose(async);
        }

        /// <inheritdoc/>
        protected override void Dispose(bool disposing)
        {
            if (!disposing)
                return;

            TaskHelper.WaitNonAsyncTask(Close(true, false));
        }

        /// <inheritdoc/>
        public override ValueTask DisposeAsync()
        {
            return Close(true, true);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckRowIndex()
        {
            if (_rowIndex >= _currentTable.Header.RowCount)
                throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "The reader is reached the end of the table.");
            if (_rowIndex < 0)
                throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, $"There are no rows to read. The call of the method {nameof(Read)} is required.");
        }

        /// <summary>
        /// Not supported. An enumerator iterating through the rows of the reader is not implemented.
        /// </summary>
        /// <exception cref="NotImplementedException">Always throws <see cref="NotImplementedException"/>.</exception>
        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }
    }
}
