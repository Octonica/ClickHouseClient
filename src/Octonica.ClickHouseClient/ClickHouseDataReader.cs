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
using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Types;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient
{
    public class ClickHouseDataReader : ClickHouseDataReaderBase
    {
        private readonly BlockHeader _blockHeader;
        private readonly ClickHouseTcpClient.Session _session;
        private readonly CancellationTokenSource? _sessionTokenSource;

        private int _recordsAffected;
        private bool _isClosed;

        private int _rowIndex = -1;

        private ClickHouseTable _currentTable;
        
        private IClickHouseTableColumn[] _reinterpretedColumnsCache;
        private ClickHouseColumnSettings?[]? _columnSettings;

        public override int RecordsAffected => _recordsAffected;

        public override bool HasRows => _rowIndex < 0 || _recordsAffected > 0;

        public override bool IsClosed => _isClosed;

        public override int FieldCount => _blockHeader.Columns.Count;

        public override int Depth => 0;

        internal ClickHouseDataReader(ClickHouseTable table, ClickHouseTcpClient.Session session, CancellationTokenSource? sessionTokenSource)
        {
            _currentTable = table ?? throw new ArgumentNullException(nameof(table));
            _session = session ?? throw new ArgumentNullException(nameof(session));
            _sessionTokenSource = sessionTokenSource;
            _reinterpretedColumnsCache = new IClickHouseTableColumn[_currentTable.Columns.Count];
            _recordsAffected = _currentTable.Header.RowCount;
            _blockHeader = _currentTable.Header;
        }

        public void ConfigureColumn(string name, ClickHouseColumnSettings columnSettings)
        {
            var index = GetOrdinal(name);
            if (index < 0)
                throw new ArgumentException($"A column with the name \"{name}\" not found.", nameof(name));

            ConfigureColumn(index, columnSettings);
        }

        public void ConfigureColumn(int ordinal, ClickHouseColumnSettings columnSettings)
        {
            if (_rowIndex >= 0)
                throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "The column can't be reconfigured during reading.");

            if (_columnSettings == null)
                _columnSettings = new ClickHouseColumnSettings?[_currentTable.Columns.Count];

            _columnSettings[ordinal] = columnSettings;
        }

        public void ConfigureDataReader(ClickHouseColumnSettings columnSettings)
        {
            if (_rowIndex >= 0)
                throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "The reader can't be reconfigured during reading.");

            if (_columnSettings == null)
                _columnSettings = new ClickHouseColumnSettings?[_currentTable.Columns.Count];

            for (int i = 0; i < _currentTable.Columns.Count; i++)
                _columnSettings[i] = columnSettings;
        }

        public sealed override string GetName(int ordinal)
        {
            return _blockHeader.Columns[ordinal].Name;
        }

        public sealed override string GetDataTypeName(int ordinal)
        {
            return _blockHeader.Columns[ordinal].TypeInfo.ComplexTypeName;
        }

        public override Type GetFieldType(int ordinal)
        {
            return _blockHeader.Columns[ordinal].TypeInfo.GetFieldType();
        }

        public sealed override int GetOrdinal(string name)
        {
            var comparer = StringComparer.Ordinal;
            for (int i = 0; i < _blockHeader.Columns.Count; i++)
            {
                if (comparer.Equals(_blockHeader.Columns[i].Name, name))
                    return i;
            }

            return -1;
        }

        public sealed override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
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

        public sealed override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
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

        public sealed override bool GetBoolean(int ordinal)
        {
            return GetFieldValue<bool>(ordinal);
        }

        public sealed override byte GetByte(int ordinal)
        {
            return GetFieldValue<byte>(ordinal);
        }

        public sealed override char GetChar(int ordinal)
        {
            return GetFieldValue<char>(ordinal);
        }

        public sealed override DateTime GetDateTime(int ordinal)
        {
            return GetFieldValue<DateTime>(ordinal);
        }

        public sealed override decimal GetDecimal(int ordinal)
        {
            return GetFieldValue<decimal>(ordinal);
        }

        public sealed override double GetDouble(int ordinal)
        {
            return GetFieldValue<double>(ordinal);
        }

        public sealed override float GetFloat(int ordinal)
        {
            return GetFieldValue<float>(ordinal);
        }

        public sealed override Guid GetGuid(int ordinal)
        {
            return GetFieldValue<Guid>(ordinal);
        }

        public sealed override short GetInt16(int ordinal)
        {
            return GetFieldValue<short>(ordinal);
        }

        public sealed override int GetInt32(int ordinal)
        {
            return GetFieldValue<int>(ordinal);
        }

        public sealed override long GetInt64(int ordinal)
        {
            return GetFieldValue<long>(ordinal);
        }

        public sealed override string GetString(int ordinal)
        {
            return GetFieldValue<string>(ordinal);
        }

        [return: NotNullIfNotNull("nullValue")]
        public string? GetString(int ordinal, string? nullValue)
        {
            return GetFieldValue(ordinal, nullValue);
        }

        public DateTimeOffset GetDateTimeOffset(int ordinal)
        {
            return GetFieldValue<DateTimeOffset>(ordinal);
        }

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

        public sealed override object GetValue(int ordinal)
        {
            CheckRowIndex();
            var column = _currentTable.Columns[ordinal];
            return column.IsNull(_rowIndex) ? DBNull.Value : column.GetValue(_rowIndex);
        }

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

        public sealed override bool IsDBNull(int ordinal)
        {
            CheckRowIndex();
            var column = _currentTable.Columns[ordinal];
            return column.IsNull(_rowIndex);
        }

        public sealed override object this[int ordinal] => GetValue(ordinal);

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

        public sealed override bool Read()
        {
            return TaskHelper.WaitNonAsyncTask(Read(false, CancellationToken.None));
        }

        public new ValueTask<bool> ReadAsync()
        {
            return Read(true, CancellationToken.None);
        }

        public new ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
        {
            return Read(true, cancellationToken);
        }

        protected sealed override async Task<bool> ReadAsyncInternal(CancellationToken cancellationToken)
        {
            return await Read(true, cancellationToken);
        }

        private async ValueTask<bool> Read(bool async, CancellationToken cancellationToken)
        {
            if (_rowIndex == _currentTable.Header.RowCount)
                return false;

            if (++_rowIndex < _currentTable.Header.RowCount)
                return true;

            while (true)
            {
                ClickHouseTable nextTable;

                try
                {
                    var message = await _session.ReadMessage(async, cancellationToken);
                    switch (message.MessageCode)
                    {
                        case ServerMessageCode.Data:
                            var dataMessage = (ServerDataMessage) message;
                            nextTable = await _session.ReadTable(dataMessage, _columnSettings, async, cancellationToken);
                            break;

                        case ServerMessageCode.Error:
                            _isClosed = true;
                            _session.Dispose();
                            throw ((ServerErrorMessage) message).Exception;

                        case ServerMessageCode.Progress:
                            var progressMessage = (ServerProgressMessage) message;
                            _recordsAffected = progressMessage.Rows;
                            continue;

                        case ServerMessageCode.EndOfStream:
                            _isClosed = true;
                            _session.Dispose();
                            return false;

                        case ServerMessageCode.ProfileInfo:
                            continue;

                        case ServerMessageCode.Totals:
                        case ServerMessageCode.Extremes:
                            var totalsMessage = (ServerDataMessage) message;
                            await _session.SkipTable(totalsMessage, async, cancellationToken);
                            continue;

                        case ServerMessageCode.Pong:
                        case ServerMessageCode.Hello:
                        case ServerMessageCode.Log:
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
                    _isClosed = true;
                    var aggrEx = await _session.SetFailed(ex, true, async);
                    if (aggrEx != null)
                        throw aggrEx;

                    throw;
                }

                if (nextTable.Header.RowCount == 0)
                    continue;

                _currentTable = nextTable;
                _reinterpretedColumnsCache = new IClickHouseTableColumn[_currentTable.Columns.Count];
                _recordsAffected += nextTable.Header.RowCount;
                _rowIndex = 0;
                return true;
            }
        }

        public override bool NextResult()
        {
            throw new NotImplementedException();
        }

        public override void Close()
        {
            TaskHelper.WaitNonAsyncTask(Close(false, false));
        }

        public override async Task CloseAsync()
        {
            await Close(false, true);
        }

        private async ValueTask Close(bool disposing, bool async)
        {
            if (_session.IsDisposed || _session.IsFailed)
                return;

            if (!_isClosed)
            {
                await _session.SendCancel(async);

                while (true)
                {
                    try
                    {
                        var message = await _session.ReadMessage(async, CancellationToken.None);
                        switch (message.MessageCode)
                        {
                            case ServerMessageCode.Data:
                            case ServerMessageCode.Totals:
                            case ServerMessageCode.Extremes:
                                var dataMessage = (ServerDataMessage) message;
                                await _session.SkipTable(dataMessage, async, CancellationToken.None);
                                continue;

                            case ServerMessageCode.Error:
                                _isClosed = true;
                                if (disposing)
                                    break;

                                throw ((ServerErrorMessage) message).Exception;

                            case ServerMessageCode.Progress:
                                var progressMessage = (ServerProgressMessage) message;
                                _recordsAffected = progressMessage.Rows;
                                continue;

                            case ServerMessageCode.EndOfStream:
                                _isClosed = true;
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
                    }
                    catch (ClickHouseHandledException ex)
                    {
                        if (!disposing)
                            throw;

                        _isClosed = true;
                        await _session.SetFailed(ex.InnerException, false, async);
                        return;
                    }
                    catch (Exception ex)
                    {
                        _isClosed = true;
                        var aggrEx = await _session.SetFailed(ex, false, async);

                        if (disposing)
                            return;

                        if (aggrEx != null)
                            throw aggrEx;

                        throw;
                    }

                    break;
                }
            }

            _session.Dispose();
            _sessionTokenSource?.Dispose();
        }

        protected override void Dispose(bool disposing)
        {
            if (!disposing)
                return;

            TaskHelper.WaitNonAsyncTask(Close(true, false));
        }

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

        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }
    }
}
