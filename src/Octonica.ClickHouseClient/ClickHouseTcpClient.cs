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

using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Types;
using Octonica.ClickHouseClient.Utils;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Octonica.ClickHouseClient
{
    internal sealed class ClickHouseTcpClient : IDisposable
    {
        private readonly SemaphoreSlim _semaphore = new(1, 1);

        private readonly TcpClient _client;
        private readonly ClickHouseConnectionSettings _settings;
        private IClickHouseTypeInfoProvider _typeInfoProvider;
        private readonly ClickHouseBinaryProtocolReader _reader;
        private readonly ClickHouseBinaryProtocolWriter _writer;
        private readonly SslStream? _sslStream;

        private Exception? _unhandledException;
        private int _state;

        public ClickHouseTcpClientState State => (ClickHouseTcpClientState)_state;

        public ClickHouseServerInfo ServerInfo { get; private set; }

        public ClickHouseTcpClient(
            TcpClient client,
            ClickHouseBinaryProtocolReader reader,
            ClickHouseBinaryProtocolWriter writer,
            ClickHouseConnectionSettings settings,
            ClickHouseServerInfo serverInfo,
            IClickHouseTypeInfoProvider typeInfoProvider,
            SslStream? sslStream)
        {
            _reader = reader ?? throw new ArgumentNullException(nameof(reader));
            _writer = writer ?? throw new ArgumentNullException(nameof(writer));
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _settings = settings ?? throw new ArgumentNullException(nameof(settings));
            ServerInfo = serverInfo;
            _typeInfoProvider = typeInfoProvider;
            _sslStream = sslStream;
        }

        public async ValueTask<Session> OpenSession(bool async, IClickHouseSessionExternalResources? externalResources, CancellationToken sessionCancellationToken, CancellationToken cancellationToken)
        {
            ClickHouseTcpClientState state = (ClickHouseTcpClientState)_state;
            if (state != ClickHouseTcpClientState.Failed)
            {
                try
                {
                    if (async)
                    {
                        await _semaphore.WaitAsync(cancellationToken);
                    }
                    else
                    {
                        _semaphore.Wait(cancellationToken);
                    }

                    ClickHouseTcpClientState previousState = (ClickHouseTcpClientState)Interlocked.CompareExchange(ref _state, (int)ClickHouseTcpClientState.Active, (int)ClickHouseTcpClientState.Ready);
                    Debug.Assert(previousState != ClickHouseTcpClientState.Active);
                    state = previousState == ClickHouseTcpClientState.Ready ? ClickHouseTcpClientState.Active : previousState;
                }
                catch (ObjectDisposedException)
                {
                    // Reading an actual state without modification of field _state
                    state = (ClickHouseTcpClientState)Interlocked.CompareExchange(ref _state, (int)state, (int)state);
                    if (state != ClickHouseTcpClientState.Failed)
                    {
                        throw;
                    }
                }
            }

            if (state == ClickHouseTcpClientState.Failed)
            {
                if (_unhandledException != null)
                {
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, "Connection is broken.", _unhandledException);
                }

                throw new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, "Connection is broken.");
            }

            try
            {
                return new Session(this, externalResources, sessionCancellationToken);
            }
            catch
            {
                _ = _semaphore.Release();
                throw;
            }
        }

        private bool TryInterceptMessage(IServerMessage message)
        {
            switch (message.MessageCode)
            {
                case ServerMessageCode.TimezoneUpdate:
                    string timezone = ((ServerTimeZoneUpdateMessage)message).Timezone;
                    if (string.Equals(timezone, ServerInfo.Timezone, StringComparison.Ordinal))
                    {
                        return true;
                    }

                    ClickHouseServerInfo updServerInfo = ServerInfo.WithTimezone(timezone);
                    _typeInfoProvider = _typeInfoProvider.Configure(updServerInfo);
                    ServerInfo = updServerInfo;
                    return true;

                default:
                    return false;
            }
        }

        private void SetFailed(Exception? unhandledException)
        {
            Dispose(false);
            _unhandledException = unhandledException;
            // 'Failed' is the terminal state. Plain assignment should work just as well as interlocked operations.
            _state = (int)ClickHouseTcpClientState.Failed;
        }

        public void Dispose()
        {
            Dispose(true);
        }

        private void Dispose(bool disposing)
        {
            _semaphore.Dispose();
            _reader.Dispose();
            _writer.Dispose();
            _sslStream?.Dispose();

            // The disposed TcpClient returns null for Client
            _client.Client?.Close(disposing ? 1 : 0);
            _client.Dispose();
        }

        public sealed class Session : IDisposable, IAsyncDisposable
        {
            private readonly ClickHouseTcpClient _client;
            private readonly IClickHouseSessionExternalResources? _externalResources;
            private readonly CancellationToken _sessionCancellationToken;

            public IClickHouseTypeInfoProvider TypeInfoProvider => _client._typeInfoProvider;

            public ClickHouseServerInfo ServerInfo => _client.ServerInfo;

            public bool IsDisposed { get; private set; }

            public bool IsFailed => _client.State == ClickHouseTcpClientState.Failed;

            public Session(ClickHouseTcpClient client, IClickHouseSessionExternalResources? externalResources, CancellationToken sessionCancellationToken)
            {
                _client = client;
                _externalResources = externalResources;
                _sessionCancellationToken = sessionCancellationToken;
            }

            public async ValueTask<ClientQueryMessage> SendQuery(
                ClientQueryMessage.Builder messageBuilder,
                IReadOnlyCollection<IClickHouseTableWriter>? tables,
                bool async,
                CancellationToken cancellationToken)
            {
                CheckDisposed();

                ClickHouseBinaryProtocolWriter writer = _client._writer;
                ClientQueryMessage queryMessage;
                try
                {
                    ClickHouseConnectionSettings settings = _client._settings;

                    messageBuilder.ClientName = settings.ClientName;
                    messageBuilder.ClientVersion = settings.ClientVersion;
                    messageBuilder.Host = settings.Host;
                    messageBuilder.RemoteAddress = ((IPEndPoint?)_client._client.Client.RemoteEndPoint)?.ToString();
                    messageBuilder.ProtocolRevision = _client.ServerInfo.Revision;
                    messageBuilder.CompressionEnabled = _client._settings.Compress;

                    queryMessage = messageBuilder.Build();
                    if (queryMessage.Settings != null)
                    {
                        if (_client.ServerInfo.Revision < ClickHouseProtocolRevisions.MinRevisionWithSettingsSerializedAsStrings)
                        {
                            throw new ClickHouseException(
                                ClickHouseErrorCodes.ProtocolRevisionNotSupported,
                                $"Query settings are not supported. Current protocol revision is {_client.ServerInfo.Revision}. Minimal required protocol revision is {ClickHouseProtocolRevisions.MinRevisionWithSettingsSerializedAsStrings}.");
                        }
                    }

                    queryMessage.Write(writer);

                    if (tables != null)
                    {
                        foreach (IClickHouseTableWriter table in tables)
                        {
                            WriteTable(table);
                        }
                    }

                    WriteTable(ClickHouseEmptyTableWriter.Instance);
                }
                catch (Exception ex)
                {
                    writer.Discard();
                    throw ClickHouseHandledException.Wrap(ex);
                }

                await WithCancellationToken(cancellationToken, ct => writer.Flush(async, ct));

                return queryMessage;
            }

            public async ValueTask SendQuery(ClientQueryMessage queryMessage, bool async, CancellationToken cancellationToken)
            {
                CheckDisposed();

                ClickHouseBinaryProtocolWriter writer = _client._writer;
                try
                {
                    queryMessage.Write(writer);
                    WriteTable(ClickHouseEmptyTableWriter.Instance);
                }
                catch (Exception ex)
                {
                    writer.Discard();
                    throw ClickHouseHandledException.Wrap(ex);
                }

                await WithCancellationToken(cancellationToken, ct => writer.Flush(async, ct));
            }

            public async ValueTask SendCancel(bool async)
            {
                CheckDisposed();

                ClickHouseBinaryProtocolWriter writer = _client._writer;
                writer.Write7BitInt32((int)ClientMessageCode.Cancel);

                await writer.Flush(async, CancellationToken.None);
            }

            public async ValueTask SendPing(bool async, CancellationToken cancellationToken)
            {
                CheckDisposed();

                ClickHouseBinaryProtocolWriter writer = _client._writer;
                writer.Write7BitInt32((int)ClientMessageCode.Ping);

                await WithCancellationToken(cancellationToken, ct => writer.Flush(async, ct));
            }

            public async ValueTask SendTable(IClickHouseTableWriter table, bool async, CancellationToken cancellationToken)
            {
                CheckDisposed();

                try
                {
                    WriteTable(table);
                }
                catch (Exception ex)
                {
                    _client._writer.Discard();
                    throw ClickHouseHandledException.Wrap(ex);
                }

                await WithCancellationToken(cancellationToken, ct => _client._writer.Flush(async, ct));
            }

            private void WriteTable(IClickHouseTableWriter table)
            {
                ClickHouseBinaryProtocolWriter writer = _client._writer;

                writer.Write7BitInt32((int)ClientMessageCode.Data);
                writer.WriteString(table.TableName);

                CompressionAlgorithm compression = _client._settings.Compress ? CompressionAlgorithm.Lz4 : CompressionAlgorithm.None;
                writer.BeginCompress(compression, _client._settings.CompressionBlockSize);

                writer.WriteByte(BlockFieldCodes.IsOverflows);
                writer.WriteBool(false); // is overflow
                writer.WriteByte(BlockFieldCodes.BucketNum);
                writer.WriteInt32(-1); // data size in block. -1 for null
                writer.WriteByte(BlockFieldCodes.End);

                writer.Write7BitInt32(table.Columns.Count);
                writer.Write7BitInt32(table.RowCount);

                foreach (IClickHouseColumnWriter column in table.Columns)
                {
                    writer.WriteString(column.ColumnName);
                    writer.WriteString(column.ColumnType);

                    if (_client.ServerInfo.Revision >= ClickHouseProtocolRevisions.MinRevisionWithCustomSerialization)
                    {
                        writer.WriteBool(false); // has_custom
                    }

                    if (table.RowCount == 0)
                    {
                        continue;
                    }

                    while (true)
                    {
                        SequenceSize size = writer.WriteRaw(mem => column.WritePrefix(mem.Span));
                        if (size.Elements == 1)
                        {
                            break;
                        }

                        if (size.Elements != 0)
                        {
                            throw new ClickHouseException(ClickHouseErrorCodes.InternalError, $"Internal error. A column writer returned an unexpected number of prefixes: {size.Elements}.");
                        }
                    }

                    int rowCount = table.RowCount;
                    while (rowCount > 0)
                    {
                        SequenceSize size = writer.WriteRaw(mem => column.WriteNext(mem.Span));
                        rowCount -= size.Elements;
                    }
                }

                writer.EndCompress();
            }

            public async ValueTask<IServerMessage> ReadMessage(bool async, CancellationToken cancellationToken)
            {
                return await WithCancellationToken(cancellationToken, ct => ReadMessageInternal(async, ct));
            }

            private async ValueTask<IServerMessage> ReadMessageInternal(bool async, CancellationToken cancellationToken)
            {
                IServerMessage message;
                do
                {
                    message = await _client._reader.ReadMessage(_client.ServerInfo.Revision, true, async, cancellationToken);
                } while (_client.TryInterceptMessage(message));

                return message;
            }

            public async ValueTask<ClickHouseTable> ReadTable(ServerDataMessage dataMessage, IReadOnlyList<ClickHouseReaderColumnSettings>? columnSettings, bool async, CancellationToken cancellationToken)
            {
                bool withDecompression = dataMessage.MessageCode != ServerMessageCode.ProfileEvents;

                (List<ColumnInfo> columnInfos, List<IClickHouseTableColumn> columns, int rowCount) result = await WithCancellationToken(
                    cancellationToken,
                    ct =>
                        ReadTable(
                            withDecompression,
                            (typeInfo, rowCount, mode) => typeInfo.CreateColumnReader(rowCount, mode),
                            (columnInfo, reader, index) => ReadTableColumn(columnInfo, reader, columnSettings == null || columnSettings.Count <= index ? default : columnSettings[index]),
                            async,
                            ct)
                );

                Debug.Assert(result.columns != null);
                BlockHeader blockHeader = new(dataMessage.TempTableName, result.columnInfos.AsReadOnly(), result.rowCount);
                return new ClickHouseTable(blockHeader, result.columns.AsReadOnly());
            }

            public async ValueTask<BlockHeader> SkipTable(ServerDataMessage dataMessage, bool async, CancellationToken cancellationToken)
            {
                bool withDecompression = dataMessage.MessageCode != ServerMessageCode.ProfileEvents;
                (List<ColumnInfo> columnInfos, List<IClickHouseTableColumn> columns, int rowCount) result = await WithCancellationToken(cancellationToken, ct => ReadTable(withDecompression, (typeInfo, rowCount, mode) => typeInfo.CreateSkippingColumnReader(rowCount, mode), null, async, ct));
                return new BlockHeader(dataMessage.TempTableName, result.columnInfos.AsReadOnly(), result.rowCount);
            }

            private async ValueTask<(List<ColumnInfo> columnInfos, List<IClickHouseTableColumn>? columns, int rowCount)> ReadTable<TReader>(
                bool withDecompression,
                Func<IClickHouseColumnTypeInfo, int, ClickHouseColumnSerializationMode, TReader> createColumnReader,
                Func<ColumnInfo, TReader, int, IClickHouseTableColumn>? readTableColumn,
                bool async,
                CancellationToken cancellationToken)
                where TReader : IClickHouseColumnReaderBase
            {
                CheckDisposed();

                CompressionAlgorithm compression = withDecompression && _client._settings.Compress ? CompressionAlgorithm.Lz4 : CompressionAlgorithm.None;
                ClickHouseBinaryProtocolReader reader = _client._reader;
                reader.BeginDecompress(compression);

                int blockFieldCode;
                bool isOverflows = false;
                // It seems that this value is used only for internal purposes and does not affect the format of the block
                int bucketNum = -1;
                do
                {
                    blockFieldCode = await reader.Read7BitInt32(async, cancellationToken);
                    switch (blockFieldCode)
                    {
                        case BlockFieldCodes.IsOverflows:
                            isOverflows = await reader.ReadBool(async, cancellationToken);
                            break;
                        case BlockFieldCodes.BucketNum:
                            bucketNum = await reader.ReadInt32(async, cancellationToken);
                            break;
                        case BlockFieldCodes.End:
                            break;
                        default:
                            throw new ClickHouseException(ClickHouseErrorCodes.ProtocolUnexpectedResponse, $"Internal error. Unexpected block field code (0x{blockFieldCode:X}) received from the server.");
                    }
                } while (blockFieldCode != BlockFieldCodes.End);

                int columnCount = await reader.Read7BitInt32(async, cancellationToken);
                int rowCount = await reader.Read7BitInt32(async, cancellationToken);

                if (isOverflows)
                {
                    throw new NotImplementedException("TODO: implement support for is_overflows.");
                }

                List<ColumnInfo> columnInfos = new(columnCount);
                List<IClickHouseTableColumn>? columns = readTableColumn == null ? null : new List<IClickHouseTableColumn>(columnCount);
                for (int i = 0; i < columnCount; i++)
                {
                    string columnName = await reader.ReadString(async, cancellationToken);
                    string columnTypeName = await reader.ReadString(async, cancellationToken);

                    ClickHouseColumnSerializationMode serializationMode = ClickHouseColumnSerializationMode.Default;
                    if (_client.ServerInfo.Revision >= ClickHouseProtocolRevisions.MinRevisionWithCustomSerialization)
                    {
                        bool hasCustom = await reader.ReadBool(async, cancellationToken);
                        if (hasCustom)
                        {
                            serializationMode = ClickHouseColumnSerializationMode.Custom;
                        }
                    }

                    IClickHouseColumnTypeInfo columnType = _client._typeInfoProvider.GetTypeInfo(columnTypeName);
                    ColumnInfo columnInfo = new(columnName, columnType);
                    columnInfos.Add(columnInfo);

                    TReader columnReader = createColumnReader(columnType, rowCount, serializationMode);
                    if (rowCount > 0)
                    {
                        while (true)
                        {
                            SequenceSize sequenceSize = await reader.ReadRaw(columnReader.ReadPrefix, async, cancellationToken);
                            if (sequenceSize.Elements == 1)
                            {
                                break;
                            }

                            if (sequenceSize.Elements != 0)
                            {
                                throw new ClickHouseException(ClickHouseErrorCodes.InternalError, $"Internal error. Received an unexpected number of column prefixes: {sequenceSize.Elements}.");
                            }
                        }
                    }

                    int columnRowCount = rowCount;
                    while (columnRowCount > 0)
                    {
                        SequenceSize sequenceSize = await reader.ReadRaw(columnReader.ReadNext, async, cancellationToken);

                        if (sequenceSize.Elements < 0)
                        {
                            throw new InvalidOperationException("The number of elements must be greater than zero.");
                        }

                        if (sequenceSize.Elements > columnRowCount)
                        {
                            throw new InvalidOperationException($"The number of rows in the column \"{columnName}\" is greater than the number of rows in the table.");
                        }

                        if (sequenceSize.Elements < columnRowCount)
                        {
                            await reader.Advance(async, cancellationToken);
                        }

                        columnRowCount -= sequenceSize.Elements;
                    }

                    if (columns != null)
                    {
                        Debug.Assert(readTableColumn != null);
                        IClickHouseTableColumn column = readTableColumn(columnInfo, columnReader, i);
                        columns.Add(column);
                    }
                }

                reader.EndDecompress();
                return (columnInfos, columns, rowCount);
            }

            private static IClickHouseTableColumn ReadTableColumn(ColumnInfo columnInfo, IClickHouseColumnReader columnReader, ClickHouseReaderColumnSettings settings)
            {
                IClickHouseTableColumn? column = columnReader.EndRead(settings.Column) ??
                    throw new ClickHouseException(ClickHouseErrorCodes.InternalError, $"Internal error. Column reader returned null for column \"{columnInfo.Name}\".");

                IClickHouseColumnReinterpreter? reinterpreter = settings.Reinterpreter;
                if (reinterpreter != null)
                {
                    IClickHouseTableColumn? reinterpretedColumn = reinterpreter.TryReinterpret(column);
                    return reinterpretedColumn ?? new ReinterpretedTableColumn(column, CreateCastFunc(columnInfo, reinterpreter.BuiltInConvertToType, reinterpreter.ExternalConvertToType));
                }

                return column;
            }

            private static Func<object, object> CreateCastFunc(ColumnInfo columnInfo, Type? builtInConvertToType, Type? externalConvertToType)
            {
                if ((externalConvertToType ?? builtInConvertToType) == typeof(object))
                {
                    return value => value; // This is fine, everything is an object
                }

                return CastFailed;

                object CastFailed(object value)
                {
                    if (value == DBNull.Value)
                    {
                        return value;
                    }

                    if (builtInConvertToType != null)
                    {
                        if (externalConvertToType == null)
                        {
                            throw new ClickHouseException(
                                ClickHouseErrorCodes.ColumnTypeMismatch,
                                $"A value from the column \"{columnInfo.Name}\" of type \"{columnInfo.TypeInfo.GetFieldType()}\" can't be converted to type \"{builtInConvertToType}\". "
                                + "This type is defined in the column settings.");
                        }

                        throw new ClickHouseException(
                            ClickHouseErrorCodes.ColumnTypeMismatch,
                            $"A value from the column \"{columnInfo.Name}\" of type \"{columnInfo.TypeInfo.GetFieldType()}\" can't be converted to type \"{builtInConvertToType}\". "
                            + $"This type is implicitly defined by the callback function for a value conversion. The callback function returns a value of type \"{externalConvertToType}\".");
                    }

                    throw new ClickHouseException(
                        ClickHouseErrorCodes.InternalError,
                        $"ClickHouseClient didn't find a suitable type conversion for the column \"{columnInfo.Name}\" of type \"{columnInfo.TypeInfo.GetFieldType()}\". "
                        + "This is an internal error. Please, report a bug if you see this message.");
                }
            }

            private async ValueTask<T> WithCancellationToken<T>(CancellationToken token, Func<CancellationToken, ValueTask<T>> execute)
            {
                if (_sessionCancellationToken == CancellationToken.None)
                {
                    return await execute(token);
                }

                if (token == CancellationToken.None)
                {
                    return await execute(_sessionCancellationToken);
                }

                using CancellationTokenSource linkedTs = CancellationTokenSource.CreateLinkedTokenSource(token, _sessionCancellationToken);
                try
                {
                    return await execute(linkedTs.Token);
                }
                catch (TaskCanceledException taskCanceledEx)
                {
                    if (token.IsCancellationRequested)
                    {
                        throw new TaskCanceledException(taskCanceledEx.Message, taskCanceledEx, token);
                    }

                    throw;
                }
                catch (OperationCanceledException operationCanceledEx)
                {
                    if (token.IsCancellationRequested)
                    {
                        throw new OperationCanceledException(operationCanceledEx.Message, operationCanceledEx, token);
                    }

                    throw;
                }
            }

            private async ValueTask WithCancellationToken(CancellationToken token, Func<CancellationToken, ValueTask> execute)
            {
                if (_sessionCancellationToken == CancellationToken.None)
                {
                    await execute(token);
                    return;
                }

                if (token == CancellationToken.None)
                {
                    await execute(_sessionCancellationToken);
                    return;
                }

                using CancellationTokenSource linkedTs = CancellationTokenSource.CreateLinkedTokenSource(token, _sessionCancellationToken);
                try
                {
                    await execute(linkedTs.Token);
                }
                catch (TaskCanceledException taskCanceledEx)
                {
                    if (token.IsCancellationRequested)
                    {
                        throw new TaskCanceledException(taskCanceledEx.Message, taskCanceledEx, token);
                    }

                    throw;
                }
                catch (OperationCanceledException operationCanceledEx)
                {
                    if (token.IsCancellationRequested)
                    {
                        throw new OperationCanceledException(operationCanceledEx.Message, operationCanceledEx, token);
                    }

                    throw;
                }
            }

            private void CheckDisposed()
            {
                if (IsDisposed)
                {
                    throw new ObjectDisposedException("Internal error. This object was disposed and no more has an exclusive access to the network stream.");
                }

                if (_client.State == ClickHouseTcpClientState.Failed)
                {
                    if (_client._unhandledException != null)
                    {
                        throw new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, "Connection is broken.", _client._unhandledException);
                    }

                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, "Connection is broken.");
                }
            }

            public async ValueTask<Exception?> SetFailed(Exception? unhandledException, bool sendCancel, bool async)
            {
                if (IsDisposed)
                {
                    return null;
                }

                Exception? networkException = null;
                if (sendCancel)
                {
                    try
                    {
                        await SendCancel(async);
                    }
                    catch (Exception ex)
                    {
                        networkException = new ClickHouseException(ClickHouseErrorCodes.NetworkError, "Network error. Operation was not canceled properly.", ex);
                    }
                }

                _client.SetFailed(unhandledException);

                Exception? externalException = null;
                if (_externalResources != null)
                {
                    externalException = await _externalResources.ReleaseOnFailure(unhandledException, async);

                    if (ReferenceEquals(unhandledException, externalException))
                    {
                        externalException = null;
                    }
                }

                List<Exception> exceptions = new(3);
                if (unhandledException != null)
                {
                    exceptions.Add(unhandledException);
                }

                if (networkException != null)
                {
                    exceptions.Add(networkException);
                }

                if (externalException != null)
                {
                    exceptions.Add(externalException);
                }

                return exceptions.Count switch
                {
                    0 => null,
                    1 => exceptions[0],
                    _ => new AggregateException(exceptions),
                };
            }

            public void Dispose()
            {
                TaskHelper.WaitNonAsyncTask(Dispose(false));
            }

            public ValueTask DisposeAsync()
            {
                return Dispose(true);
            }

            public ValueTask Dispose(bool async)
            {
                if (IsDisposed || IsFailed)
                {
                    return default;
                }

                _ = Interlocked.CompareExchange(ref _client._state, (int)ClickHouseTcpClientState.Ready, (int)ClickHouseTcpClientState.Active);
                _ = _client._semaphore.Release();
                IsDisposed = true;

                return _externalResources?.Release(async) ?? default;
            }
        }
    }
}
