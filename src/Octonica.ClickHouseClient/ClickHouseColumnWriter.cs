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
    /// <summary>
    /// Provides a way of writing set of columns to a ClickHouse database.
    /// </summary>
    public class ClickHouseColumnWriter : IDisposable, IAsyncDisposable
    {
        private readonly ClickHouseTcpClient.Session _session;

        private readonly ReadOnlyCollection<ColumnInfo> _columns;

        private ClickHouseColumnSettings?[]? _columnSettings;

        private int? _rowsPerBlock;

        /// <summary>
        /// Gets the number of fields (columns) in the table.
        /// </summary>
        public int FieldCount => _columns.Count;

        /// <summary>
        /// Gets the value indicating whether the writer is closed.
        /// </summary>
        /// <returns><see langword="true"/> if the reader is closed; otherwise <see langword="false"/>.</returns>
        public bool IsClosed => _session.IsDisposed || _session.IsFailed;

        /// <summary>
        /// Gets or sets the maximal number of rows in a single block of data.
        /// </summary>
        /// <returns>The maximal number of rows in a single block of data. <see langword="null"/> if the size of the block is not limited.</returns>
        public int? MaxBlockSize
        {
            get => _rowsPerBlock;
            set
            {
                if (value <= 0)
                    throw new ArgumentException("A number of rows in a block must be greater than zero.");

                _rowsPerBlock = value;
            }
        }

        internal ClickHouseColumnWriter(ClickHouseTcpClient.Session session, ReadOnlyCollection<ColumnInfo> columns)
        {
            _session = session ?? throw new ArgumentNullException(nameof(session));
            _columns = columns;

            if (columns.Count <= 100)
                MaxBlockSize = 8000;
            else if (columns.Count >= 1000)
                MaxBlockSize = 800;
            else
                MaxBlockSize = 8800 - 8 * columns.Count;
        }

        /// <inheritdoc cref="ClickHouseDataReader.ConfigureColumn(string, ClickHouseColumnSettings)"/>
        public void ConfigureColumn(string name, ClickHouseColumnSettings columnSettings)
        {
            var index = GetOrdinal(name);
            if (index < 0)
                throw new ArgumentException($"A column with the name \"{name}\" not found.", nameof(name));

            ConfigureColumn(index, columnSettings);
        }

        /// <inheritdoc cref="ClickHouseDataReader.ConfigureColumn(int, ClickHouseColumnSettings)"/>
        public void ConfigureColumn(int ordinal, ClickHouseColumnSettings columnSettings)
        {
            if (_columnSettings == null)
                _columnSettings = new ClickHouseColumnSettings?[_columns.Count];

            _columnSettings[ordinal] = columnSettings;
        }

        /// <inheritdoc cref="ClickHouseDataReader.ConfigureDataReader(ClickHouseColumnSettings)"/>
        public void ConfigureColumnWriter(ClickHouseColumnSettings columnSettings)
        {
            if (_columnSettings == null)
                _columnSettings = new ClickHouseColumnSettings?[_columns.Count];

            for (int i = 0; i < _columns.Count; i++)
                _columnSettings[i] = columnSettings;
        }

        /// <inheritdoc cref="ClickHouseDataReader.GetFieldTypeInfo(int)"/>
        public IClickHouseTypeInfo GetFieldTypeInfo(int ordinal)
        {
            return _columns[ordinal].TypeInfo;
        }

        /// <inheritdoc cref="ClickHouseDataReader.GetName(int)"/>
        public string GetName(int ordinal)
        {
            return _columns[ordinal].Name;
        }

        /// <inheritdoc cref="ClickHouseDataReader.GetDataTypeName(int)"/>
        public string GetDataTypeName(int ordinal)
        {
            return _columns[ordinal].TypeInfo.ComplexTypeName;
        }

        /// <inheritdoc cref="ClickHouseDataReader.GetFieldType(int)"/>
        public Type GetFieldType(int ordinal)
        {
            // This method should implement the same logic as ClickHouseDataReader.GetFieldType

            var type = _columnSettings?[ordinal]?.ColumnType;
            type ??= _columns[ordinal].TypeInfo.GetFieldType();
            return Nullable.GetUnderlyingType(type) ?? type;
        }

        /// <inheritdoc cref="ClickHouseDataReader.GetOrdinal(string)"/>
        public int GetOrdinal(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            return CommonUtils.GetColumnIndex(_columns, name);
        }

        /// <summary>
        /// Writes a single row to the table.
        /// </summary>
        /// <param name="values">The list of column values.</param> 
        public void WriteRow(params object?[] values)
        {
            TaskHelper.WaitNonAsyncTask(WriteRow(values, false, CancellationToken.None));
        }

        /// <summary>
        /// Writes a single row to the table.
        /// </summary>
        /// <param name="values">The list of column values.</param>        
        public void WriteRow(IReadOnlyCollection<object?> values)
        {
            TaskHelper.WaitNonAsyncTask(WriteRow(values, false, CancellationToken.None));
        }

        /// <summary>
        /// Asyncronously writes a single row to the table.
        /// </summary>
        /// <param name="values">The list of column values.</param>
        /// <returns>A <see cref="Task"/> representing asyncronous operation.</returns>
        public async Task WriteRowAsync(IReadOnlyCollection<object?> values)
        {
            await WriteRow(values, true, CancellationToken.None);
        }

        /// <summary>
        /// Asyncronously writes a single row to the table.
        /// </summary>
        /// <param name="values">The list of column values.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A <see cref="Task"/> representing asyncronous operation.</returns>
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

        /// <summary>
        /// Writes the specified columns to the table.
        /// <br/>
        /// Each column must be an object implementing one of the interfaces:
        /// <see cref="IReadOnlyList{T}"/>,
        /// <see cref="IList{T}"/>,
        /// <see cref="IEnumerable{T}"/> or
        /// <see cref="IEnumerable"/>.
        /// </summary>
        /// <param name="columns">The <see cref="IReadOnlyDictionary{TKey, TValue}"/> object that provides access to columns by their names.</param>
        /// <param name="rowCount">The number of rows in columns.</param>
        public void WriteTable(IReadOnlyDictionary<string, object?> columns, int rowCount)
        {
            TaskHelper.WaitNonAsyncTask(WriteTable(columns, rowCount, false, CancellationToken.None));
        }

        /// <summary>
        /// Writes the specified columns to the table.
        /// <br/>
        /// Each column must be an object implementing one of the interfaces:
        /// <see cref="IReadOnlyList{T}"/>,
        /// <see cref="IList{T}"/>,
        /// <see cref="IEnumerable{T}"/> or
        /// <see cref="IEnumerable"/>.
        /// </summary>
        /// <param name="columns">The list of columns.</param>
        /// <param name="rowCount">The number of rows in columns.</param>
        public void WriteTable(IReadOnlyList<object?> columns, int rowCount)
        {
            TaskHelper.WaitNonAsyncTask(WriteTable(columns, rowCount, false, CancellationToken.None));
        }

        /// <summary>
        /// Asyncronously writes the specified columns to the table.
        /// <br/>
        /// Each column must be an object implementing one of the interfaces:
        /// <see cref="IReadOnlyList{T}"/>,
        /// <see cref="IList{T}"/>,
        /// <see cref="IAsyncEnumerable{T}"/>,
        /// <see cref="IEnumerable{T}"/> or
        /// <see cref="IEnumerable"/>.
        /// </summary>
        /// <param name="columns">The <see cref="IReadOnlyDictionary{TKey, TValue}"/> object that provides access to columns by their names.</param>
        /// <param name="rowCount">The number of rows in columns.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A <see cref="Task"/> representing asyncronous operation.</returns>
        public async Task WriteTableAsync(IReadOnlyDictionary<string, object?> columns, int rowCount, CancellationToken cancellationToken)
        {
            await WriteTable(columns, rowCount, true, cancellationToken);
        }

        /// <summary>
        /// Asyncronously writes the specified columns to the table.
        /// <br/>
        /// Each column must be an object implementing one of the interfaces:
        /// <see cref="IReadOnlyList{T}"/>,
        /// <see cref="IList{T}"/>,
        /// <see cref="IAsyncEnumerable{T}"/>,
        /// <see cref="IEnumerable{T}"/> or
        /// <see cref="IEnumerable"/>.
        /// </summary>
        /// <param name="columns">The list of columns.</param>
        /// <param name="rowCount">The number of rows in columns.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A <see cref="Task"/> representing asyncronous operation.</returns>
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

            var writerFactories = new List<IClickHouseColumnWriterFactory>(_columns.Count);
            for (int i = 0; i < _columns.Count; i++)
            {
                var factory = await CreateColumnWriterFactory(_columns[i], columns[i], i, rowCount, _columnSettings?[i], async, cancellationToken);
                writerFactories.Add(factory);
            }

            int offset;
            var blockSize = MaxBlockSize ?? rowCount;
            for (offset = 0; offset + blockSize < rowCount; offset += blockSize)
            {
                var table = new ClickHouseTableWriter(string.Empty, blockSize, writerFactories.Select(w => w.Create(offset, blockSize)));
                await SendTable(table, async, cancellationToken);
            }

            var finalBlockSize = rowCount - offset;
            var finalTable = new ClickHouseTableWriter(string.Empty, finalBlockSize, writerFactories.Select(w => w.Create(offset, finalBlockSize)));
            await SendTable(finalTable, async, cancellationToken);
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

        /// <summary>
        /// Closes the writer and releases all resources associated with it.
        /// </summary>
        public void EndWrite()
        {
            TaskHelper.WaitNonAsyncTask(EndWrite(false, false, CancellationToken.None));
        }

        /// <summary>
        /// Asyncronously closes the writer and releases all resources associated with it.
        /// </summary>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A <see cref="Task"/> representing asyncronous operation.</returns>
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

        /// <summary>
        /// Closes the writer and releases all resources associated with it.
        /// </summary>
        public void Dispose()
        {
            TaskHelper.WaitNonAsyncTask(Dispose(false));
        }

        /// <summary>
        /// Asyncronously closes the writer and releases all resources associated with it.
        /// </summary>
        /// <returns>A <see cref="ValueTask"/> representing asyncronous operation.</returns>
        public ValueTask DisposeAsync()
        {
            return Dispose(true);
        }

        private async ValueTask Dispose(bool async)
        {
            await EndWrite(true, async, CancellationToken.None);
        }

        internal static async ValueTask<IClickHouseColumnWriterFactory> CreateColumnWriterFactory(ColumnInfo columnInfo, object? column, int columnIndex, int rowCount, ClickHouseColumnSettings? settings, bool async, CancellationToken cancellationToken)
        {
            if (settings?.ColumnType == typeof(object))
            {
                throw new ClickHouseException(
                    ClickHouseErrorCodes.InvalidColumnSettings,
                    $"Type \"{settings.ColumnType}\" should not be used as a type of a column. This type is defined in column settings of the column \"{columnInfo.Name}\" (position {columnIndex}).");
            }

            if (column == null)
            {
                if (!columnInfo.TypeInfo.TypeName.StartsWith("Nullable"))
                    throw new ClickHouseException(ClickHouseErrorCodes.ColumnTypeMismatch, $"The column \"{columnInfo.Name}\" at the position {columnIndex} doesn't support nulls.");

                ITypeDispatcher? typeDispatcher;
                if (settings?.ColumnType != null)
                {
                    if (settings.ColumnType.IsValueType && Nullable.GetUnderlyingType(settings.ColumnType) == null)
                    {
                        throw new ClickHouseException(
                            ClickHouseErrorCodes.ColumnTypeMismatch,
                            $"The column \"{columnInfo.Name}\" (position {columnIndex}) contains null value. But the type of this column defined in the settings (\"{settings.ColumnType}\") doesn't allow nulls.");
                    }

                    typeDispatcher = settings.GetColumnTypeDispatcher();
                    Debug.Assert(typeDispatcher != null);
                }
                else
                {
                    typeDispatcher = TypeDispatcher.Create(columnInfo.TypeInfo.GetFieldType());
                }

                var constColumn = typeDispatcher.Dispatch(new NullColumnWriterDispatcher(columnInfo, settings, rowCount));
                return constColumn;
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
            IClickHouseColumnWriterFactory? columnWriter;
            if (explicitTypeDispatcher != null)
            {
                Debug.Assert(settings?.ColumnType != null);

                // The type is explicitly specified in the column settings. Either cast the column to a collection
                // of this type or throw an exception.
                columnWriter = explicitTypeDispatcher.Dispatch(new ColumnWriterDispatcher(column, columnInfo, settings, rowCount, columnIndex, async));
                if (columnWriter != null)
                    return columnWriter;

                if (async && typeof(IAsyncEnumerable<>).MakeGenericType(settings.ColumnType).IsAssignableFrom(columnType))
                {
                    columnWriter = await explicitTypeDispatcher.Dispatch(new AsyncColumnWriterDispatcher(column, columnInfo, settings, rowCount, columnIndex, cancellationToken));
                    return columnWriter;
                }

                // There is almost no chance that IEnumerable's IEnumerator returns the value of expected type if at least one of interfaces is implemented by the column's type.
                bool ignoreNonGenericEnumerable = readOnlyList != null || list != null || asyncEnumerable != null || enumerable != null;
                columnWriter = explicitTypeDispatcher.Dispatch(new ColumnWriterObjectCollectionDispatcher(column, columnInfo, settings, rowCount, columnIndex, async, ignoreNonGenericEnumerable));
                if (columnWriter != null)
                    return columnWriter;

                if (async && column is IAsyncEnumerable<object?> aeCol)
                {
                    columnWriter = await explicitTypeDispatcher.Dispatch(new AsyncObjectColumnWriterDispatcher(aeCol, columnInfo, settings, rowCount, columnIndex, cancellationToken));
                    return columnWriter;
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
                            $"The column \"{columnInfo.Name}\" at the position {columnIndex} implements interface \"{aeInterface}\". Call async method \"{nameof(WriteTableAsync)}\".");
                    }
                }

                throw new ClickHouseException(
                    ClickHouseErrorCodes.ColumnTypeMismatch,
                    $"The column \"{columnInfo.Name}\" at the position {columnIndex} is not a collection of type \"{settings.ColumnType}\". This type is defined in the column's settings.");
            }

            // Trying to extract an actual type of column's items from an interface implemented by this column.
            Type dispatchedElementType;
            if (readOnlyList != null)
            {
                if (altReadOnlyList != null)
                    throw CreateInterfaceAmbiguousException(readOnlyList, altReadOnlyList, columnInfo.Name, columnIndex);

                dispatchedElementType = readOnlyList.GetGenericArguments()[0];
            }
            else if (list != null)
            {
                if (altList != null)
                    throw CreateInterfaceAmbiguousException(list, altList, columnInfo.Name, columnIndex);

                dispatchedElementType = list.GetGenericArguments()[0];
            }
            else
            {
                if (async && asyncEnumerable != null)
                {
                    if (altAsyncEnumerable != null)
                        throw CreateInterfaceAmbiguousException(asyncEnumerable, altAsyncEnumerable, columnInfo.Name, columnIndex);

                    var genericArg = asyncEnumerable.GetGenericArguments()[0];
                    var asyncDispatcher = new AsyncColumnWriterDispatcher(column, columnInfo, settings, rowCount, columnIndex, cancellationToken);
                    var asyncColumn = await TypeDispatcher.Dispatch(genericArg, asyncDispatcher);
                    return asyncColumn;
                }

                if (enumerable != null)
                {
                    if (altEnumerable != null)
                        throw CreateInterfaceAmbiguousException(enumerable, altEnumerable, columnInfo.Name, columnIndex);

                    dispatchedElementType = enumerable.GetGenericArguments()[0];
                }
                else
                {
                    // There is still hope that the column implements one of suported interfaces with typeof(T) == typeof(object).
                    // In this case assume that the type of the table's field is equal to the type of the column.

                    dispatchedElementType = columnInfo.TypeInfo.GetFieldType();
                    var typeDispatcher = TypeDispatcher.Create(dispatchedElementType);
                    var objDispatcher = new ColumnWriterObjectCollectionDispatcher(column, columnInfo, settings, rowCount, columnIndex, async);
                    var objColumnWriter = typeDispatcher.Dispatch(objDispatcher);
                    if (async && objColumnWriter == null && column is IAsyncEnumerable<object?> aeCol)
                    {
                        var asyncDispatcher = new AsyncObjectColumnWriterDispatcher(aeCol, columnInfo, settings, rowCount, columnIndex, cancellationToken);
                        objColumnWriter = await typeDispatcher.Dispatch(asyncDispatcher);
                    }

                    if (objColumnWriter == null)
                    {
                        if (!async && (asyncEnumerable != null || column is IAsyncEnumerable<object?>))
                        {
                            var aeInterface = asyncEnumerable ?? typeof(IAsyncEnumerable<object?>);
                            throw new ClickHouseException(
                                ClickHouseErrorCodes.NotSupportedInSyncronousMode,
                                $"The column \"{columnInfo.Name}\" at the position {columnIndex} implements interface \"{aeInterface}\". Call async method \"{nameof(WriteTableAsync)}\".");
                        }

                        throw new ClickHouseException(ClickHouseErrorCodes.ColumnTypeMismatch, $"The column \"{columnInfo.Name}\" at the position {columnIndex} is not a collection.");
                    }

                    return objColumnWriter;
                }
            }

            var dispatcher = new ColumnWriterDispatcher(column, columnInfo, settings, rowCount, columnIndex, false);
            columnWriter = TypeDispatcher.Dispatch(dispatchedElementType, dispatcher);

            if (columnWriter == null)
                throw new ClickHouseException(ClickHouseErrorCodes.ColumnTypeMismatch, $"The column \"{columnInfo.Name}\" at the position {columnIndex} is not a collection.");

            return columnWriter;
        }

        private static ClickHouseException CreateInterfaceAmbiguousException(Type itf, Type altItf, string columnName, int columnIndex)
        {
            return new ClickHouseException(ClickHouseErrorCodes.ColumnTypeMismatch,
                $"A type of the column \"{columnName}\" at the position {columnIndex} is ambiguous. The column implements interfaces \"{itf}\" and \"{altItf}\".");
        }

        private class ColumnWriterFactory<T> : IClickHouseColumnWriterFactory
        {
            private readonly ColumnInfo _columnInfo;
            private readonly IReadOnlyList<T> _rows;
            private readonly ClickHouseColumnSettings? _columnSettings;

            public ColumnWriterFactory(ColumnInfo columnInfo, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
            {
                _columnInfo = columnInfo;
                _rows = rows;
                _columnSettings = columnSettings;
            }

            public IClickHouseColumnWriter Create(int offset, int length)
            {
                var slice = _rows.Slice(offset, length);
                return _columnInfo.TypeInfo.CreateColumnWriter(_columnInfo.Name, slice, _columnSettings);
            }
        }

        private class NullColumnWriterDispatcher : ITypeDispatcher<IClickHouseColumnWriterFactory>
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

            public IClickHouseColumnWriterFactory Dispatch<T>()
            {
                var rows = new ConstantReadOnlyList<T>(default, _rowCount);
                return new ColumnWriterFactory<T>(_columnInfo, rows, _columnSettings);
            }
        }

        private class AsyncColumnWriterDispatcher : ITypeDispatcher<Task<IClickHouseColumnWriterFactory>>
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

            public async Task<IClickHouseColumnWriterFactory> Dispatch<T>()
            {
                if (_rowCount == 0)
                    return new ColumnWriterFactory<T>(_columnInfo, Array.Empty<T>(), _columnSettings);

                ConfiguredCancelableAsyncEnumerable<T>.Enumerator enumerator = default;
                bool disposeEnumerator = false;
                try
                {
                    enumerator = ((IAsyncEnumerable<T>)_asyncEnumerable).WithCancellation(_cancellationToken).GetAsyncEnumerator();
                    disposeEnumerator = true;

                    var rows = new T[_rowCount];
                    for (int i = 0; i < _rowCount; i++)
                    {
                        if (!await enumerator.MoveNextAsync())
                        {
                            throw new ClickHouseException(
                                ClickHouseErrorCodes.InvalidRowCount,
                                $"The column \"{_columnInfo.Name}\" at the position {_columnIndex} has only {i} row(s), but the required number of rows is {_rowCount}.");
                        }

                        rows[i] = enumerator.Current;
                    }

                    return new ColumnWriterFactory<T>(_columnInfo, rows, _columnSettings);
                }
                finally
                {
                    if (disposeEnumerator)
                        await enumerator.DisposeAsync();
                }
            }
        }

        private class AsyncObjectColumnWriterDispatcher : ITypeDispatcher<Task<IClickHouseColumnWriterFactory>>
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

            public async Task<IClickHouseColumnWriterFactory> Dispatch<T>()
            {
                if (_rowCount == 0)
                    return new ColumnWriterFactory<T>(_columnInfo, Array.Empty<T>(), _columnSettings);

                ConfiguredCancelableAsyncEnumerable<object?>.Enumerator enumerator = default;
                bool disposeEnumerator = false;
                try
                {
                    enumerator = _asyncEnumerable.WithCancellation(_cancellationToken).GetAsyncEnumerator();
                    disposeEnumerator = true;

                    var rows = new T[_rowCount];
                    for (int i = 0; i < _rowCount; i++)
                    {
                        if (!await enumerator.MoveNextAsync())
                        {
                            throw new ClickHouseException(
                                ClickHouseErrorCodes.InvalidRowCount,
                                $"The column \"{_columnInfo.Name}\" at the position {_columnIndex} has only {i} row(s), but the required number of rows is {_rowCount}.");
                        }

                        rows[i] = ColumnWriterObjectCollectionDispatcher.CastTo<T>(enumerator.Current);
                    }

                    return new ColumnWriterFactory<T>(_columnInfo, rows, _columnSettings);
                }
                finally
                {
                    if (disposeEnumerator)
                        await enumerator.DisposeAsync();
                }
            }
        }

        private class ColumnWriterDispatcher : ITypeDispatcher<IClickHouseColumnWriterFactory?>
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

            public IClickHouseColumnWriterFactory? Dispatch<T>()
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
                        readOnlyList = readOnlyList.Slice(0, _rowCount);

                    return new ColumnWriterFactory<T>(_columnInfo, readOnlyList, _columnSettings);
                }

                if (_collection is IList<T> list)
                {
                    if (list.Count < _rowCount)
                    {
                        throw new ClickHouseException(
                            ClickHouseErrorCodes.InvalidRowCount,
                            $"The column \"{_columnInfo.Name}\" at the position {_columnIndex} has only {list.Count} row(s), but the required number of rows is {_rowCount}.");
                    }

                    var listSpan = list.Slice(0, _rowCount);
                    return new ColumnWriterFactory<T>(_columnInfo, listSpan, _columnSettings);
                }

                if (_checkAsyncEnumerable && _collection is IAsyncEnumerable<T>)
                {
                    // Should be handled in async mode
                    return null;
                }

                if (_collection is IEnumerable<T> genericEnumerable)
                {
                    using var enumerator = genericEnumerable.GetEnumerator();

                    T[] rows = new T[_rowCount];
                    for (int i = 0; i < _rowCount; i++)
                    {
                        if (!enumerator.MoveNext())
                        {
                            throw new ClickHouseException(
                                ClickHouseErrorCodes.InvalidRowCount,
                                $"The column \"{_columnInfo.Name}\" at the position {_columnIndex} has only {i} row(s), but the required number of rows is {_rowCount}.");
                        }

                        rows[i] = enumerator.Current;
                    }

                    return new ColumnWriterFactory<T>(_columnInfo, rows, _columnSettings);
                }

                return null;
            }
        }

        private class ColumnWriterObjectCollectionDispatcher : ITypeDispatcher<IClickHouseColumnWriterFactory?>
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

            public IClickHouseColumnWriterFactory? Dispatch<T>()
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
                        readOnlyList = readOnlyList.Slice(0, _rowCount);
                }
                else if (_collection is IList<object?> list)
                {
                    if (list.Count < _rowCount)
                    {
                        throw new ClickHouseException(
                            ClickHouseErrorCodes.InvalidRowCount,
                            $"The column \"{_columnInfo.Name}\" at the position {_columnIndex} has only {list.Count} row(s), but the required number of rows is {_rowCount}.");
                    }

                    readOnlyList = list.Slice(0, _rowCount);
                }
                else if (_checkAsyncEnumerable && _collection is IAsyncEnumerable<object?>)
                {
                    // Should be handled in async mode
                    return null;
                }
                else if (_collection is IEnumerable<object?> genericEnumerable)
                {
                    using var enumerator = genericEnumerable.GetEnumerator();

                    T[] rows = new T[_rowCount];
                    for (int i = 0; i < _rowCount; i++)
                    {
                        if (!enumerator.MoveNext())
                        {
                            throw new ClickHouseException(
                                ClickHouseErrorCodes.InvalidRowCount,
                                $"The column \"{_columnInfo.Name}\" at the position {_columnIndex} has only {i} row(s), but the required number of rows is {_rowCount}.");
                        }

                        rows[i] = CastTo<T>(enumerator.Current);
                    }

                    return new ColumnWriterFactory<T>(_columnInfo, rows, _columnSettings);
                }
                else if (!_ignoreNonGenericEnumerable && _collection is IEnumerable enumerable)
                {
                    IEnumerator? enumerator = null;
                    try
                    {
                        enumerator = enumerable.GetEnumerator();
                        T[] rows = new T[_rowCount];
                        for (int i = 0; i < _rowCount; i++)
                        {
                            if (!enumerator.MoveNext())
                            {
                                throw new ClickHouseException(
                                    ClickHouseErrorCodes.InvalidRowCount,
                                    $"The column \"{_columnInfo.Name}\" at the position {_columnIndex} has only {i} row(s), but the required number of rows is {_rowCount}.");
                            }

                            rows[i] = CastTo<T>(enumerator.Current);
                        }

                        return new ColumnWriterFactory<T>(_columnInfo, rows, _columnSettings);
                    }
                    finally
                    {
                        (enumerator as IDisposable)?.Dispose();
                    }
                }
                else
                {
                    // An object is not a collection
                    return null;
                }

                var mappedList = readOnlyList.Map(CastTo<T>);
                return new ColumnWriterFactory<T>(_columnInfo, mappedList, _columnSettings);
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
