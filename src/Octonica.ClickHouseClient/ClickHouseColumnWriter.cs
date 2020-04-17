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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient
{
    public class ClickHouseColumnWriter : IDisposable, IAsyncDisposable
    {
        private readonly ClickHouseTcpClient.Session _session;

        private readonly ReadOnlyCollection<ColumnInfo> _columns;

        private ClickHouseColumnSettings?[]? _columnSettings;

        public int FieldCount => _columns.Count;

        public bool IsClosed => _session.IsDisposed || _session.IsFailed;

        internal ClickHouseColumnWriter(ClickHouseTcpClient.Session session, ReadOnlyCollection<ColumnInfo> columns)
        {
            _session = session ?? throw new ArgumentNullException(nameof(session));
            _columns = columns;
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
            if (_columnSettings == null)
                _columnSettings = new ClickHouseColumnSettings?[_columns.Count];

            _columnSettings[ordinal] = columnSettings;
        }

        public void ConfigureColumnWriter(ClickHouseColumnSettings columnSettings)
        {
            if (_columnSettings == null)
                _columnSettings = new ClickHouseColumnSettings?[_columns.Count];

            for (int i = 0; i < _columns.Count; i++)
                _columnSettings[i] = columnSettings;
        }

        public string GetName(int ordinal)
        {
            return _columns[ordinal].Name;
        }

        public string GetDataTypeName(int ordinal)
        {
            return _columns[ordinal].TypeInfo.ComplexTypeName;
        }

        public Type GetFieldType(int ordinal)
        {
            return _columns[ordinal].TypeInfo.GetFieldType();
        }

        public int GetOrdinal(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            return CommonUtils.GetColumnIndex(_columns, name);
        }

        public void WriteRow(params object?[] values)
        {
            TaskHelper.WaitNonAsyncTask(WriteRow(values, false, CancellationToken.None));
        }

        public void WriteRow(IReadOnlyCollection<object?> values)
        {
            TaskHelper.WaitNonAsyncTask(WriteRow(values, false, CancellationToken.None));
        }

        public async Task WriteRowAsync(IReadOnlyCollection<object?> values)
        {
            await WriteRow(values, true, CancellationToken.None);
        }

        public async Task WriteRowAsync(IReadOnlyCollection<object?> values, CancellationToken cancellationToken)
        {
            await WriteRow(values, true, cancellationToken);
        }

        private async ValueTask WriteRow(IReadOnlyCollection<object?> values, bool async, CancellationToken cancellationToken)
        {
            if (values == null)
                throw new ArgumentNullException(nameof(values));

            if (values.Count != _columns.Count)
                throw new ArgumentException("The number of values must be equal to the number of columns.");

            var columnWriters = new List<IClickHouseColumnWriter>(_columns.Count);
            foreach (var value in values)
            {
                int i = columnWriters.Count;

                var columnInfo = _columns[i];
                SingleRowColumnWriterDispatcher dispatcher;
                Type valueType;
                if (value != null && !(value is DBNull))
                {
                    dispatcher = new SingleRowColumnWriterDispatcher(value, columnInfo, _columnSettings?[i]);
                    valueType = value.GetType();
                }
                else if (columnInfo.TypeInfo.TypeName != "Nullable")
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.ColumnMismatch, $"The column \"{columnInfo.Name}\" at the position {i} doesn't support nulls.");
                }
                else
                {
                    dispatcher = new SingleRowColumnWriterDispatcher(null, columnInfo, _columnSettings?[i]);
                    valueType = columnInfo.TypeInfo.GetFieldType();
                }

                IClickHouseColumnWriter columnWriter;
                try
                {
                    columnWriter = TypeDispatcher.Dispatch(valueType, dispatcher);
                }
                catch (ClickHouseException ex)
                {
                    throw new ClickHouseException(ex.ErrorCode, $"Column \"{columnInfo.Name}\" (position {i}): {ex.Message}", ex);
                }

                columnWriters.Add(columnWriter);
            }

            var table = new ClickHouseTableWriter(string.Empty, 1, columnWriters);
            await SendTable(table, async, cancellationToken);
        }

        public void WriteTable(IReadOnlyDictionary<string, object?> columns, int rowCount)
        {
            TaskHelper.WaitNonAsyncTask(WriteTable(columns, rowCount, false, CancellationToken.None));
        }

        public void WriteTable(IReadOnlyList<object?> columns, int rowCount)
        {
            TaskHelper.WaitNonAsyncTask(WriteTable(columns, rowCount, false, CancellationToken.None));
        }

        public async Task WriteTableAsync(IReadOnlyDictionary<string, object?> columns, int rowCount, CancellationToken cancellationToken)
        {
            await WriteTable(columns, rowCount, true, cancellationToken);
        }

        public async Task WriteTableAsync(IReadOnlyList<object?> columns, int rowCount, CancellationToken cancellationToken)
        {
            await WriteTable(columns, rowCount, true, cancellationToken);
        }

        private async ValueTask WriteTable(IReadOnlyDictionary<string, object?> columns, int rowCount, bool async, CancellationToken cancellationToken)
        {
            if (columns == null)
                throw new ArgumentNullException(nameof(columns));

            var list = new List<object?>(_columns.Count);
            foreach (var columnInfo in _columns)
            {
                if (columns.TryGetValue(columnInfo.Name, out var column))
                    list.Add(column);
                else
                    list.Add(null);
            }

            await WriteTable(list, rowCount, async, cancellationToken);
        }

        private async ValueTask WriteTable(IReadOnlyList<object?> columns, int rowCount, bool async, CancellationToken cancellationToken)
        {
            if (columns == null)
                throw new ArgumentNullException(nameof(columns));
            if (columns.Count != _columns.Count)
                throw new ArgumentException("The number of columns for writing must be equal to the number of columns in the table.", nameof(columns));
            if (rowCount < 0)
                throw new ArgumentOutOfRangeException(nameof(rowCount));
            if (rowCount == 0)
                throw new ArgumentException("The number of rows must be grater than zero.", nameof(rowCount));

            if (IsClosed)
                throw new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, "The writer is closed.");

            var writers = new List<IClickHouseColumnWriter>(_columns.Count);
            for (int i = 0; i < _columns.Count; i++)
            {
                var column = columns[i];
                var columnInfo = _columns[i];

                if (column == null)
                {
                    if (!columnInfo.TypeInfo.TypeName.StartsWith("Nullable"))
                        throw new ClickHouseException(ClickHouseErrorCodes.ColumnMismatch, $"The column \"{columnInfo.Name}\" at the position {i} doesn't support nulls.");

                    var constColumn = TypeDispatcher.Dispatch(columnInfo.TypeInfo.GetFieldType(), new NullColumnWriterDispatcher(columnInfo, _columnSettings?[i], rowCount));
                    writers.Add(constColumn);
                    continue;
                }

                var columnType = column.GetType();

                bool isEnumerable = false;
                Type? enumerable = null, asyncEnumerable = null, readOnlyList = null, list = null;
                foreach (var ifs in columnType.GetInterfaces())
                {
                    if (ifs == typeof(IEnumerable))
                        isEnumerable = true;
                    else if (ifs.IsGenericType)
                    {
                        var ifsDefinition = ifs.GetGenericTypeDefinition();
                        if (ifsDefinition == typeof(IEnumerable<>))
                            enumerable ??= ifs;
                        else if (ifsDefinition == typeof(IAsyncEnumerable<>))
                            asyncEnumerable ??= ifs;
                        else if (ifsDefinition == typeof(IReadOnlyList<>))
                            readOnlyList ??= ifs;
                        else if (ifsDefinition == typeof(IList<>))
                            list ??= ifs;
                    }
                }

                Type dispatchedElementType;
                if (readOnlyList != null)
                {
                    dispatchedElementType = readOnlyList.GetGenericArguments()[0];
                }
                else if (list != null)
                {
                    dispatchedElementType = list.GetGenericArguments()[0];
                }
                else
                {
                    if (asyncEnumerable != null)
                    {
                        if (async)
                        {
                            var genericArg = asyncEnumerable.GetGenericArguments()[0];
                            var asyncDispatcher = new AsyncColumnWriterDispatcher(column, columnInfo, _columnSettings?[i], rowCount, i, cancellationToken);
                            var asyncColumn = await TypeDispatcher.Dispatch(genericArg, asyncDispatcher);
                            writers.Add(asyncColumn);
                            continue;
                        }

                        if (!isEnumerable && enumerable == null)
                        {
                            throw new ClickHouseException(
                                ClickHouseErrorCodes.ColumnMismatch,
                                $"The column \"{columnInfo.Name}\" at the position {i} implements interface \"{asyncEnumerable}\". Call async method \"{nameof(WriteTableAsync)}\".");
                        }
                    }

                    if (enumerable != null)
                    {
                        dispatchedElementType = enumerable.GetGenericArguments()[0];
                    }
                    else if (isEnumerable)
                    {
                        dispatchedElementType = columnInfo.TypeInfo.GetFieldType();
                    }
                    else
                    {
                        throw new ClickHouseException(ClickHouseErrorCodes.ColumnMismatch, $"The column \"{columnInfo.Name}\" at the position {i} is not a collection.");
                    }
                }

                var dispatcher = new ColumnWriterDispatcher(column, columnInfo, _columnSettings?[i], rowCount, i);
                var columnWriter = TypeDispatcher.Dispatch(dispatchedElementType, dispatcher);
                writers.Add(columnWriter);
            }

            var table = new ClickHouseTableWriter(string.Empty, rowCount, writers);
            await SendTable(table, async, cancellationToken);
        }

        private async ValueTask SendTable(ClickHouseTableWriter table, bool async, CancellationToken cancellationToken)
        {
            try
            {
                await _session.SendTable(table, async, cancellationToken);
            }
            catch (ClickHouseHandledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                var aggrEx = await _session.SetFailed(ex, false, async);
                if (aggrEx != null)
                    throw aggrEx;

                throw;
            }
        }

        public void EndWrite()
        {
            TaskHelper.WaitNonAsyncTask(EndWrite(false, false, CancellationToken.None));
        }

        public async Task EndWriteAsync(CancellationToken cancellationToken)
        {
            await EndWrite(false, true, cancellationToken);
        }

        private async ValueTask EndWrite(bool disposing, bool async, CancellationToken cancellationToken)
        {
            if (IsClosed)
                return;

            try
            {
                if (disposing)
                    await _session.SendCancel(async);
                else
                    await _session.SendTable(ClickHouseEmptyTableWriter.Instance, async, cancellationToken);

                var message = await _session.ReadMessage(async, CancellationToken.None);
                switch (message.MessageCode)
                {
                    case ServerMessageCode.EndOfStream:
                        _session.Dispose();
                        break;

                    case ServerMessageCode.Error:
                        // An error is also indicates the end of the stream.
                        _session.Dispose();
                        if (disposing)
                            break;

                        var exception = ((ServerErrorMessage) message).Exception;
                        throw exception;

                    default:
                        throw new ClickHouseException(ClickHouseErrorCodes.ProtocolUnexpectedResponse, $"Unexpected server message: \"{message.MessageCode}\".");
                }
            }
            catch (ClickHouseServerException)
            {
                throw;
            }
            catch (ClickHouseHandledException ex)
            {
                if (!disposing)
                    throw;

                // Connection state can't be restored
                await _session.SetFailed(ex.InnerException, false, async);
            }
            catch (Exception ex)
            {
                var aggrEx = await _session.SetFailed(ex, false, async);
                if (aggrEx != null)
                    throw aggrEx;

                throw;
            }
        }

        public void Dispose()
        {
            TaskHelper.WaitNonAsyncTask(Dispose(false));
        }

        public ValueTask DisposeAsync()
        {
            return Dispose(true);
        }

        private async ValueTask Dispose(bool async)
        {
            await EndWrite(true, async, CancellationToken.None);
        }

        private class NullColumnWriterDispatcher : ITypeDispatcher<IClickHouseColumnWriter>
        {
            private readonly ColumnInfo _columnInfo;
            private readonly ClickHouseColumnSettings? _columnSettings;
            private readonly int _rowCount;

            public NullColumnWriterDispatcher(ColumnInfo columnInfo, ClickHouseColumnSettings? columnSettings, int rowCount)
            {
                _columnInfo = columnInfo;
                _columnSettings = columnSettings;
                _rowCount = rowCount;
            }

            public IClickHouseColumnWriter Dispatch<T>()
            {
                var rows = new ConstantReadOnlyList<T>(default, _rowCount);
                return _columnInfo.TypeInfo.CreateColumnWriter(_columnInfo.Name, rows, _columnSettings);
            }
        }

        private class AsyncColumnWriterDispatcher : ITypeDispatcher<Task<IClickHouseColumnWriter>>
        {
            private readonly object _asyncEnumerable;
            private readonly ColumnInfo _columnInfo;
            private readonly ClickHouseColumnSettings? _columnSettings;
            private readonly int _rowCount;
            private readonly int _columnIndex;
            private readonly CancellationToken _cancellationToken;

            public AsyncColumnWriterDispatcher(object asyncEnumerable, ColumnInfo columnInfo, ClickHouseColumnSettings? columnSettings, int rowCount, int columnIndex, CancellationToken cancellationToken)
            {
                _asyncEnumerable = asyncEnumerable;
                _columnInfo = columnInfo;
                _columnSettings = columnSettings;
                _rowCount = rowCount;
                _columnIndex = columnIndex;
                _cancellationToken = cancellationToken;
            }

            public async Task<IClickHouseColumnWriter> Dispatch<T>()
            {
                var rows = new List<T>(_rowCount);
                if (_rowCount == 0)
                    return _columnInfo.TypeInfo.CreateColumnWriter(_columnInfo.Name, rows, _columnSettings);

                await foreach (var rowValue in ((IAsyncEnumerable<T>) _asyncEnumerable).WithCancellation(_cancellationToken))
                {
                    rows.Add(rowValue);

                    if (rows.Count == _rowCount)
                        break;
                }

                if (rows.Count < _rowCount)
                {
                    throw new ClickHouseException(
                        ClickHouseErrorCodes.ColumnMismatch,
                        $"The column \"{_columnInfo.Name}\" at the position {_columnIndex} has only {rows.Count} row(s), but the required number of rows is {_rowCount}.");
                }

                return _columnInfo.TypeInfo.CreateColumnWriter(_columnInfo.Name, rows, _columnSettings);
            }
        }

        private class ColumnWriterDispatcher : ITypeDispatcher<IClickHouseColumnWriter>
        {
            private readonly object _collection;
            private readonly ColumnInfo _columnInfo;
            private readonly ClickHouseColumnSettings? _columnSettings;
            private readonly int _rowCount;
            private readonly int _columnIndex;

            public ColumnWriterDispatcher(object collection, ColumnInfo columnInfo, ClickHouseColumnSettings? columnSettings, int rowCount, int columnIndex)
            {
                _collection = collection;
                _columnInfo = columnInfo;
                _columnSettings = columnSettings;
                _rowCount = rowCount;
                _columnIndex = columnIndex;
            }

            public IClickHouseColumnWriter Dispatch<T>()
            {
                if (_collection is IReadOnlyList<T> readOnlyList)
                {
                    if (readOnlyList.Count < _rowCount)
                    {
                        throw new ClickHouseException(
                            ClickHouseErrorCodes.ColumnMismatch,
                            $"The column \"{_columnInfo.Name}\" at the position {_columnIndex} has only {readOnlyList.Count} row(s), but the required number of rows is {_rowCount}.");
                    }

                    if (readOnlyList.Count > _rowCount)
                        readOnlyList = new ReadOnlyListSpan<T>(readOnlyList, 0, _rowCount);

                    return _columnInfo.TypeInfo.CreateColumnWriter(_columnInfo.Name, readOnlyList, _columnSettings);
                }

                if (_collection is IList<T> list)
                {
                    if (list.Count < _rowCount)
                    {
                        throw new ClickHouseException(
                            ClickHouseErrorCodes.ColumnMismatch,
                            $"The column \"{_columnInfo.Name}\" at the position {_columnIndex} has only {list.Count} row(s), but the required number of rows is {_rowCount}.");
                    }

                    var listSpan = new ListSpan<T>(list, 0, _rowCount);
                    return _columnInfo.TypeInfo.CreateColumnWriter(_columnInfo.Name, listSpan, _columnSettings);
                }

                List<T> rows = new List<T>(_rowCount);
                if (_collection is IEnumerable<T> genericEnumerable)
                {
                    rows.AddRange(genericEnumerable.Take(_rowCount));
                }
                else
                {
                    foreach (object? item in (IEnumerable) _collection)
                    {
                        // T may be nullable but there is no way to declare T?
                        if (item == DBNull.Value)
                        {
                            rows.Add((T) (default(T) is null ? null! : item));
                        }
                        else
                        {
                            rows.Add((T) item!);
                        }
                    }
                }

                if (rows.Count < _rowCount)
                {
                    throw new ClickHouseException(
                        ClickHouseErrorCodes.ColumnMismatch,
                        $"The column \"{_columnInfo.Name}\" at the position {_columnIndex} has only {rows.Count} row(s), but the required number of rows is {_rowCount}.");
                }

                return _columnInfo.TypeInfo.CreateColumnWriter(_columnInfo.Name, rows, _columnSettings);
            }
        }

        private class SingleRowColumnWriterDispatcher : ITypeDispatcher<IClickHouseColumnWriter>
        {
            private readonly object? _value;
            private readonly ColumnInfo _columnInfo;
            private readonly ClickHouseColumnSettings? _columnSettings;

            public SingleRowColumnWriterDispatcher(object? value, ColumnInfo columnInfo, ClickHouseColumnSettings? columnSettings)
            {
                _value = value;
                _columnInfo = columnInfo;
                _columnSettings = columnSettings;
            }

            public IClickHouseColumnWriter Dispatch<T>()
            {
                var rows = new ConstantReadOnlyList<T>((T) _value, 1);
                return _columnInfo.TypeInfo.CreateColumnWriter(_columnInfo.Name, rows, _columnSettings);
            }
        }
    }
}
