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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Types;
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

        public IClickHouseTypeInfo GetFieldTypeInfo(int ordinal)
        {
            return _columns[ordinal].TypeInfo;
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
            // This method should implement the same logic as ClickHouseDataReader.GetFieldType

            var type = _columnSettings?[ordinal]?.ColumnType;
            type ??= _columns[ordinal].TypeInfo.GetFieldType();
            return Nullable.GetUnderlyingType(type) ?? type;
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
                var settings = _columnSettings?[i];

                if (settings?.ColumnType == typeof(object))
                {
                    throw new ClickHouseException(
                        ClickHouseErrorCodes.InvalidColumnSettings,
                        $"Type \"{settings.ColumnType}\" should not be used as a type of a column. This type is defined in column settings of the column \"{columnInfo.Name}\" (position {i}).");
                }

                ITypeDispatcher? typeDispatcher;
                SingleRowColumnWriterDispatcher dispatcher;
                if (value != null && !(value is DBNull))
                {
                    dispatcher = new SingleRowColumnWriterDispatcher(value, columnInfo, _columnSettings?[i]);
                    var valueType = value.GetType();

                    if (settings?.ColumnType != null)
                    {
                        if (!settings.ColumnType.IsAssignableFrom(valueType))
                        {
                            throw new ClickHouseException(
                                ClickHouseErrorCodes.ColumnTypeMismatch,
                                $"The value of the row at the position {i} (column \"{columnInfo.Name}\") can't be converted to the type \"{settings.ColumnType}\". This type is defined in column settings.");
                        }

                        typeDispatcher = settings.GetColumnTypeDispatcher();
                        Debug.Assert(typeDispatcher != null);
                    }
                    else
                    {
                        typeDispatcher = TypeDispatcher.Create(valueType);
                    }
                }
                else if (columnInfo.TypeInfo.TypeName != "Nullable")
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.ColumnTypeMismatch, $"The column \"{columnInfo.Name}\" at the position {i} doesn't support nulls.");
                }
                else
                {
                    dispatcher = new SingleRowColumnWriterDispatcher(null, columnInfo, _columnSettings?[i]);
                    if (settings?.ColumnType != null)
                    {
                        if (settings.ColumnType.IsValueType && Nullable.GetUnderlyingType(settings.ColumnType) == null)
                        {
                            throw new ClickHouseException(
                                ClickHouseErrorCodes.ColumnTypeMismatch,
                                $"The value of the row at the position {i} (column \"{columnInfo.Name}\") is null. But the type of this column defined in the settings (\"{settings.ColumnType}\") doesn't allow nulls.");
                        }

                        typeDispatcher = settings.GetColumnTypeDispatcher();
                        Debug.Assert(typeDispatcher != null);
                    }
                    else
                    {
                        var fieldType = columnInfo.TypeInfo.GetFieldType();
                        typeDispatcher = TypeDispatcher.Create(fieldType);
                    }
                }

                IClickHouseColumnWriter columnWriter;
                try
                {
                    columnWriter = typeDispatcher.Dispatch(dispatcher);
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
                var settings = _columnSettings?[i];

                if (settings?.ColumnType == typeof(object))
                {
                    throw new ClickHouseException(
                        ClickHouseErrorCodes.InvalidColumnSettings,
                        $"Type \"{settings.ColumnType}\" should not be used as a type of a column. This type is defined in column settings of the column \"{columnInfo.Name}\" (position {i}).");
                }

                if (column == null)
                {
                    if (!columnInfo.TypeInfo.TypeName.StartsWith("Nullable"))
                        throw new ClickHouseException(ClickHouseErrorCodes.ColumnTypeMismatch, $"The column \"{columnInfo.Name}\" at the position {i} doesn't support nulls.");

                    ITypeDispatcher? typeDispatcher;
                    if (settings?.ColumnType != null)
                    {
                        if (settings.ColumnType.IsValueType && Nullable.GetUnderlyingType(settings.ColumnType) == null)
                        {
                            throw new ClickHouseException(
                                ClickHouseErrorCodes.ColumnTypeMismatch,
                                $"The column \"{columnInfo.Name}\" (position {i}) contains null value. But the type of this column defined in the settings (\"{settings.ColumnType}\") doesn't allow nulls.");
                        }

                        typeDispatcher = settings.GetColumnTypeDispatcher();
                        Debug.Assert(typeDispatcher != null);
                    }
                    else
                    {
                        typeDispatcher = TypeDispatcher.Create(columnInfo.TypeInfo.GetFieldType());
                    }

                    var constColumn = typeDispatcher.Dispatch(new NullColumnWriterDispatcher(columnInfo, settings, rowCount));
                    writers.Add(constColumn);
                    continue;
                }

                var columnType = column.GetType();
                Type? enumerable = null;
                Type? altEnumerable = null;
                Type? asyncEnumerable = null;
                Type? altAsyncEnumerable = null;
                Type? readOnlyList = null;
                Type? altReadOnlyList = null;
                Type? list = null;
                Type? altList = null;
                foreach (var ifs in columnType.GetInterfaces())
                {
                    if (!ifs.IsGenericType)
                        continue;

                    var ifsDefinition = ifs.GetGenericTypeDefinition();
                    if (ifsDefinition == typeof(IEnumerable<>) && ifs.GetGenericArguments()[0] != typeof(object))
                    {
                        altEnumerable ??= enumerable;
                        enumerable = ifs;
                    }
                    else if (ifsDefinition == typeof(IAsyncEnumerable<>) && ifs.GetGenericArguments()[0] != typeof(object))
                    {
                        altAsyncEnumerable = asyncEnumerable;
                        asyncEnumerable = ifs;
                    }
                    else if (ifsDefinition == typeof(IReadOnlyList<>) && ifs.GetGenericArguments()[0] != typeof(object))
                    {
                        altReadOnlyList = readOnlyList;
                        readOnlyList = ifs;
                    }
                    else if (ifsDefinition == typeof(IList<>) && ifs.GetGenericArguments()[0] != typeof(object))
                    {
                        altList = list;
                        list = ifs;
                    }
                }

                /*
                 * All supported interfaces (sorted by priority):
                 * 1. IReadOnlyList<T>
                 * 2. IList<T>
                 * 3. IAsyncEnumerable<T> (supported only in ascynronuous mode, i.e. async == true)
                 * 4. IEnumerable<T>
                 * 5. IEnumerable
                 */

                var explicitTypeDispatcher = settings?.GetColumnTypeDispatcher();
                IClickHouseColumnWriter? columnWriter;
                if (explicitTypeDispatcher != null)
                {
                    Debug.Assert(settings?.ColumnType != null);
                    
                    // The type is explicitly specified in the column settings. Either cast the column to a collection
                    // of this type or throw an exception.
                    columnWriter = explicitTypeDispatcher.Dispatch(new ColumnWriterDispatcher(column, columnInfo, settings, rowCount, i, async));
                    if (columnWriter != null)
                    {
                        writers.Add(columnWriter);
                        continue;
                    }

                    if (async && typeof(IAsyncEnumerable<>).MakeGenericType(settings.ColumnType).IsAssignableFrom(columnType))
                    {
                        columnWriter = await explicitTypeDispatcher.Dispatch(new AsyncColumnWriterDispatcher(column, columnInfo, settings, rowCount, i, cancellationToken));
                        writers.Add(columnWriter);
                        continue;
                    }

                    // There is almost no chance that IEnumerable's IEnumerator returns the value of expected type if at least one of interfaces is implemented by the column's type.
                    bool ignoreNonGenericEnumerable = readOnlyList != null || list != null || asyncEnumerable != null || enumerable != null;
                    columnWriter = explicitTypeDispatcher.Dispatch(new ColumnWriterObjectCollectionDispatcher(column, columnInfo, settings, rowCount, i, async, ignoreNonGenericEnumerable));
                    if (columnWriter != null)
                    {
                        writers.Add(columnWriter);
                        continue;
                    }

                    if (async && column is IAsyncEnumerable<object?> aeCol)
                    {
                        columnWriter = await explicitTypeDispatcher.Dispatch(new AsyncObjectColumnWriterDispatcher(aeCol, columnInfo, settings, rowCount, i, cancellationToken));
                        writers.Add(columnWriter);
                        continue;
                    }

                    if (!async)
                    {
                        Type? aeInterface = typeof(IAsyncEnumerable<>).MakeGenericType(settings.ColumnType);
                        if (!aeInterface.IsAssignableFrom(settings.ColumnType))
                        {
                            aeInterface = column is IAsyncEnumerable<object?> ? typeof(IAsyncEnumerable<object?>) : null;
                        }

                        if (aeInterface != null)
                        {
                            throw new ClickHouseException(
                                ClickHouseErrorCodes.NotSupportedInSyncronousMode,
                                $"The column \"{columnInfo.Name}\" at the position {i} implements interface \"{aeInterface}\". Call async method \"{nameof(WriteTableAsync)}\".");
                        }
                    }

                    throw new ClickHouseException(
                        ClickHouseErrorCodes.ColumnTypeMismatch,
                        $"The column \"{columnInfo.Name}\" at the position {i} is not a collection of type \"{settings.ColumnType}\". This type is defined in the column's settings.");
                }

                // Trying to extract an actual type of column's items from an interface implemented by this column.
                Type dispatchedElementType;
                if (readOnlyList != null)
                {
                    if (altReadOnlyList != null)
                        throw CreateInterfaceAmbiguousException(readOnlyList, altReadOnlyList, columnInfo.Name, i);

                    dispatchedElementType = readOnlyList.GetGenericArguments()[0];
                }
                else if (list != null)
                {
                    if (altList != null)
                        throw CreateInterfaceAmbiguousException(list, altList, columnInfo.Name, i);

                    dispatchedElementType = list.GetGenericArguments()[0];
                }
                else
                {
                    if (async && asyncEnumerable != null)
                    {
                        if (altAsyncEnumerable != null)
                            throw CreateInterfaceAmbiguousException(asyncEnumerable, altAsyncEnumerable, columnInfo.Name, i);

                        var genericArg = asyncEnumerable.GetGenericArguments()[0];
                        var asyncDispatcher = new AsyncColumnWriterDispatcher(column, columnInfo, settings, rowCount, i, cancellationToken);
                        var asyncColumn = await TypeDispatcher.Dispatch(genericArg, asyncDispatcher);
                        writers.Add(asyncColumn);
                        continue;
                    }

                    if (enumerable != null)
                    {
                        if (altEnumerable != null)
                            throw CreateInterfaceAmbiguousException(enumerable, altEnumerable, columnInfo.Name, i);

                        dispatchedElementType = enumerable.GetGenericArguments()[0];
                    }
                    else
                    {
                        // There is still hope that the column implements one of suported interfaces with typeof(T) == typeof(object).
                        // In this case assume that the type of the table's field is equal to the type of the column.

                        dispatchedElementType = columnInfo.TypeInfo.GetFieldType();
                        var typeDispatcher = TypeDispatcher.Create(dispatchedElementType);
                        var objDispatcher = new ColumnWriterObjectCollectionDispatcher(column, columnInfo, settings, rowCount, i, async);
                        var objColumnWriter = typeDispatcher.Dispatch(objDispatcher);
                        if (async && objColumnWriter == null && column is IAsyncEnumerable<object?> aeCol)
                        {
                            var asyncDispatcher = new AsyncObjectColumnWriterDispatcher(aeCol, columnInfo, settings, rowCount, i, cancellationToken);
                            objColumnWriter = await typeDispatcher.Dispatch(asyncDispatcher);
                        }

                        if (objColumnWriter == null)
                        {
                            if (!async && (asyncEnumerable != null || column is IAsyncEnumerable<object?>))
                            {
                                var aeInterface = asyncEnumerable ?? typeof(IAsyncEnumerable<object?>);
                                throw new ClickHouseException(
                                    ClickHouseErrorCodes.NotSupportedInSyncronousMode,
                                    $"The column \"{columnInfo.Name}\" at the position {i} implements interface \"{aeInterface}\". Call async method \"{nameof(WriteTableAsync)}\".");
                            }

                            throw new ClickHouseException(ClickHouseErrorCodes.ColumnTypeMismatch, $"The column \"{columnInfo.Name}\" at the position {i} is not a collection.");
                        }

                        writers.Add(objColumnWriter);
                        continue;
                    }
                }

                var dispatcher = new ColumnWriterDispatcher(column, columnInfo, settings, rowCount, i, false);
                columnWriter = TypeDispatcher.Dispatch(dispatchedElementType, dispatcher);

                if (columnWriter == null)
                    throw new ClickHouseException(ClickHouseErrorCodes.ColumnTypeMismatch, $"The column \"{columnInfo.Name}\" at the position {i} is not a collection.");

                writers.Add(columnWriter);
            }

            var table = new ClickHouseTableWriter(string.Empty, rowCount, writers);
            await SendTable(table, async, cancellationToken);

            static ClickHouseException CreateInterfaceAmbiguousException(Type itf, Type altItf, string columnName, int columnIndex)
            {
                return new ClickHouseException(ClickHouseErrorCodes.ColumnTypeMismatch,
                    $"A type of the column \"{columnName}\" at the position {columnIndex} is ambiguous. The column implements interfaces \"{itf}\" and \"{altItf}\".");
            }
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
                        await _session.Dispose(async);
                        break;

                    case ServerMessageCode.Error:
                        // An error is also indicates the end of the stream.
                        await _session.Dispose(async);
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

            public AsyncColumnWriterDispatcher(
                object asyncEnumerable,
                ColumnInfo columnInfo,
                ClickHouseColumnSettings? columnSettings,
                int rowCount,
                int columnIndex,
                CancellationToken cancellationToken)
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
                        ClickHouseErrorCodes.InvalidRowCount,
                        $"The column \"{_columnInfo.Name}\" at the position {_columnIndex} has only {rows.Count} row(s), but the required number of rows is {_rowCount}.");
                }

                return _columnInfo.TypeInfo.CreateColumnWriter(_columnInfo.Name, rows, _columnSettings);
            }
        }

        private class AsyncObjectColumnWriterDispatcher : ITypeDispatcher<Task<IClickHouseColumnWriter>>
        {
            private readonly IAsyncEnumerable<object?> _asyncEnumerable;
            private readonly ColumnInfo _columnInfo;
            private readonly ClickHouseColumnSettings? _columnSettings;
            private readonly int _rowCount;
            private readonly int _columnIndex;
            private readonly CancellationToken _cancellationToken;

            public AsyncObjectColumnWriterDispatcher(
                IAsyncEnumerable<object?> asyncEnumerable,
                ColumnInfo columnInfo,
                ClickHouseColumnSettings? columnSettings,
                int rowCount,
                int columnIndex,
                CancellationToken cancellationToken)
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

                await foreach (var rowValue in _asyncEnumerable.WithCancellation(_cancellationToken))
                {
                    var val = ColumnWriterObjectCollectionDispatcher.CastTo<T>(rowValue);
                    rows.Add(val);

                    if (rows.Count == _rowCount)
                        break;
                }

                if (rows.Count < _rowCount)
                {
                    throw new ClickHouseException(
                        ClickHouseErrorCodes.InvalidRowCount,
                        $"The column \"{_columnInfo.Name}\" at the position {_columnIndex} has only {rows.Count} row(s), but the required number of rows is {_rowCount}.");
                }

                return _columnInfo.TypeInfo.CreateColumnWriter(_columnInfo.Name, rows, _columnSettings);
            }
        }

        private class ColumnWriterDispatcher : ITypeDispatcher<IClickHouseColumnWriter?>
        {
            private readonly object _collection;
            private readonly ColumnInfo _columnInfo;
            private readonly ClickHouseColumnSettings? _columnSettings;
            private readonly int _rowCount;
            private readonly int _columnIndex;
            private readonly bool _checkAsyncEnumerable;

            public ColumnWriterDispatcher(
                object collection,
                ColumnInfo columnInfo,
                ClickHouseColumnSettings? columnSettings,
                int rowCount,
                int columnIndex,
                bool checkAsyncEnumerable)
            {
                _collection = collection;
                _columnInfo = columnInfo;
                _columnSettings = columnSettings;
                _rowCount = rowCount;
                _columnIndex = columnIndex;
                _checkAsyncEnumerable = checkAsyncEnumerable;
            }

            public IClickHouseColumnWriter? Dispatch<T>()
            {
                if (_collection is IReadOnlyList<T> readOnlyList)
                {
                    if (readOnlyList.Count < _rowCount)
                    {
                        throw new ClickHouseException(
                            ClickHouseErrorCodes.InvalidRowCount,
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
                            ClickHouseErrorCodes.InvalidRowCount,
                            $"The column \"{_columnInfo.Name}\" at the position {_columnIndex} has only {list.Count} row(s), but the required number of rows is {_rowCount}.");
                    }

                    var listSpan = new ListSpan<T>(list, 0, _rowCount);
                    return _columnInfo.TypeInfo.CreateColumnWriter(_columnInfo.Name, listSpan, _columnSettings);
                }

                if (_checkAsyncEnumerable && _collection is IAsyncEnumerable<T>)
                {
                    // Should be handled in async mode
                    return null;
                }

                if (_collection is IEnumerable<T> genericEnumerable)
                {
                    List<T> rows = new List<T>(_rowCount);
                    rows.AddRange(genericEnumerable.Take(_rowCount));
                    return _columnInfo.TypeInfo.CreateColumnWriter(_columnInfo.Name, rows, _columnSettings);
                }

                return null;
            }
        }

        private class ColumnWriterObjectCollectionDispatcher : ITypeDispatcher<IClickHouseColumnWriter?>
        {
            private readonly object _collection;
            private readonly ColumnInfo _columnInfo;
            private readonly ClickHouseColumnSettings? _columnSettings;
            private readonly int _rowCount;
            private readonly int _columnIndex;
            private readonly bool _checkAsyncEnumerable;
            private readonly bool _ignoreNonGenericEnumerable;

            public ColumnWriterObjectCollectionDispatcher(
                object collection,
                ColumnInfo columnInfo,
                ClickHouseColumnSettings? columnSettings,
                int rowCount,
                int columnIndex,
                bool checkAsyncEnumerable,
                bool ignoreNonGenericEnumerable = false)
            {
                _collection = collection;
                _columnInfo = columnInfo;
                _columnSettings = columnSettings;
                _rowCount = rowCount;
                _columnIndex = columnIndex;
                _checkAsyncEnumerable = checkAsyncEnumerable;
                _ignoreNonGenericEnumerable = ignoreNonGenericEnumerable;
            }

            public IClickHouseColumnWriter? Dispatch<T>()
            {
                if (_collection is IReadOnlyList<object?> readOnlyList)
                {
                    if (readOnlyList.Count < _rowCount)
                    {
                        throw new ClickHouseException(
                            ClickHouseErrorCodes.InvalidRowCount,
                            $"The column \"{_columnInfo.Name}\" at the position {_columnIndex} has only {readOnlyList.Count} row(s), but the required number of rows is {_rowCount}.");
                    }

                    if (readOnlyList.Count > _rowCount)
                        readOnlyList = new ReadOnlyListSpan<object?>(readOnlyList, 0, _rowCount);
                }
                else if (_collection is IList<object?> list)
                {
                    if (list.Count < _rowCount)
                    {
                        throw new ClickHouseException(
                            ClickHouseErrorCodes.InvalidRowCount,
                            $"The column \"{_columnInfo.Name}\" at the position {_columnIndex} has only {list.Count} row(s), but the required number of rows is {_rowCount}.");
                    }

                    readOnlyList = new ListSpan<object?>(list, 0, _rowCount);
                }
                else if (_checkAsyncEnumerable && _collection is IAsyncEnumerable<object?>)
                {
                    // Should be handled in async mode
                    return null;
                }
                else if (_collection is IEnumerable<object?> genericEnumerable)
                {
                    List<object?> rows = new List<object?>(_rowCount);
                    rows.AddRange(genericEnumerable.Take(_rowCount));
                    readOnlyList = rows;
                }
                else if (!_ignoreNonGenericEnumerable && _collection is IEnumerable enumerable)
                {
                    List<T> rows = new List<T>(_rowCount);

                    int count = 0;
                    foreach (object? item in enumerable)
                    {
                        rows.Add(CastTo<T>(item));

                        if (++count == _rowCount)
                            break;
                    }

                    if (rows.Count < _rowCount)
                    {
                        throw new ClickHouseException(
                            ClickHouseErrorCodes.ColumnTypeMismatch,
                            $"The column \"{_columnInfo.Name}\" at the position {_columnIndex} has only {rows.Count} row(s), but the required number of rows is {_rowCount}.");
                    }

                    return _columnInfo.TypeInfo.CreateColumnWriter(_columnInfo.Name, rows, _columnSettings);
                }
                else
                {
                    // An object is not a collection
                    return null;
                }

                var mappedList = new MappedReadOnlyList<object?, T>(readOnlyList, CastTo<T>);
                return _columnInfo.TypeInfo.CreateColumnWriter(_columnInfo.Name, mappedList, _columnSettings);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static T CastTo<T>(object? value)
            {
                // T may be nullable but there is no way to declare T?
                if (value == DBNull.Value)
                    return (T)(default(T) is null ? null! : value);

                return (T)value!;
            }
        }

        private class SingleRowColumnWriterDispatcher : ITypeDispatcher<IClickHouseColumnWriter>
        {
            [AllowNull] private readonly object _value;
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
