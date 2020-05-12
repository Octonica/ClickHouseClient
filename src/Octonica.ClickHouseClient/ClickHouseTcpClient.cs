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
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Types;

namespace Octonica.ClickHouseClient
{
    internal sealed class ClickHouseTcpClient : IDisposable
    {
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

        private readonly TcpClient _client;
        private readonly ClickHouseConnectionSettings _settings;
        private readonly ClickHouseBinaryProtocolReader _reader;
        private readonly ClickHouseBinaryProtocolWriter _writer;

        private Exception? _unhandledException;
        private volatile bool _isFailed;

        public event EventHandler<Exception?>? OnFailed;

        public ClickHouseServerInfo ServerInfo { get; }

        public IClickHouseTypeInfoProvider TypeInfoProvider { get; }

        public ClickHouseTcpClient(
            TcpClient client,
            ClickHouseBinaryProtocolReader reader,
            ClickHouseBinaryProtocolWriter writer,
            ClickHouseConnectionSettings settings,
            ClickHouseServerInfo serverInfo,
            IClickHouseTypeInfoProvider typeInfoProvider)
        {
            _reader = reader ?? throw new ArgumentNullException(nameof(reader));
            _writer = writer ?? throw new ArgumentNullException(nameof(writer));
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _settings = settings ?? throw new ArgumentNullException(nameof(settings));
            ServerInfo = serverInfo;
            TypeInfoProvider = typeInfoProvider;
        }

        public async ValueTask<Session> OpenSession(bool async, CancellationToken sessionCancellationToken, CancellationToken cancellationToken)
        {
            if (!_isFailed)
            {
                try
                {
                    if (async)
                        await _semaphore.WaitAsync(cancellationToken);
                    else
                        _semaphore.Wait(cancellationToken);
                }
                catch (ObjectDisposedException)
                {
                    if (!_isFailed)
                        throw;
                }
            }

            if (_isFailed)
            {
                if (_unhandledException != null)
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, "Connection is broken.", _unhandledException);

                throw new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, "Connection is broken.");
            }

            try
            {
                return new Session(this, sessionCancellationToken);
            }
            catch
            {
                _semaphore.Release();
                throw;
            }
        }

        private void SetFailed(Exception? unhandledException)
        {
            Dispose();
            _unhandledException = unhandledException;
            _isFailed = true;

            OnFailed?.Invoke(this, _unhandledException);
        }

        public void Dispose()
        {
            _semaphore?.Dispose();
            _reader?.Dispose();
            _writer.Dispose();
            _client?.Dispose();
        }

        public class Session : IDisposable
        {
            private readonly ClickHouseTcpClient _client;
            private readonly CancellationToken _sessionCancellationToken;

            public bool IsDisposed { get; private set; }

            public bool IsFailed => _client._isFailed;

            public Session(ClickHouseTcpClient client, CancellationToken sessionCancellationToken)
            {
                _client = client;
                _sessionCancellationToken = sessionCancellationToken;
            }

            public async ValueTask SendQuery(
                string query,
                IReadOnlyCollection<IClickHouseTableWriter>? tables,
                bool async,
                CancellationToken cancellationToken)
            {
                CheckDisposed();

                var writer = _client._writer;
                try
                {
                    var settings = _client._settings;

                    var queryMessage = new ClientQueryMessage.Builder
                    {
                        ClientName = settings.ClientName,
                        ClientVersion = settings.ClientVersion,
                        Host = settings.Host,
                        RemoteAddress = ((IPEndPoint)_client._client.Client.RemoteEndPoint).ToString(),
                        ProtocolRevision = Revisions.CurrentRevision,
                        QueryKind = QueryKind.InitialQuery,
                        Query = query,
                        CompressionEnabled = _client._settings.Compress
                    }.Build();

                    queryMessage.Write(writer);

                    if (tables != null)
                    {
                        foreach (var table in tables)
                            WriteTable(table);
                    }

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

                var writer = _client._writer;
                writer.Write7BitInt32((int) ClientMessageCode.Cancel);

                await writer.Flush(async, CancellationToken.None);
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
                var writer = _client._writer;

                writer.Write7BitInt32((int) ClientMessageCode.Data);
                writer.WriteString(table.TableName);

                var compression = _client._settings.Compress ? CompressionAlgorithm.Lz4 : CompressionAlgorithm.None;
                writer.BeginCompress(compression, _client._settings.CompressionBlockSize);

                writer.WriteByte(BlockFieldCodes.Overflow);
                writer.WriteBool(false); // is overflow
                writer.WriteByte(BlockFieldCodes.Size);
                writer.WriteInt32(-1); // data size in block. -1 for null
                writer.WriteByte(BlockFieldCodes.End);

                writer.Write7BitInt32(table.Columns.Count);
                writer.Write7BitInt32(table.RowCount);

                foreach (var column in table.Columns)
                {
                    writer.WriteString(column.ColumnName);
                    writer.WriteString(column.ColumnType);

                    int rowCount = table.RowCount;
                    while (rowCount > 0)
                    {
                        var size = writer.WriteRaw(mem => column.WriteNext(mem.Span));
                        rowCount -= size.Elements;
                    }
                }

                writer.EndCompress();
            }

            public async ValueTask<IServerMessage> ReadMessage(bool async, CancellationToken cancellationToken)
            {
                return await WithCancellationToken(cancellationToken, ct => _client._reader.ReadMessage(async, ct));
            }

            public async ValueTask<ClickHouseTable> ReadTable(ServerDataMessage dataMessage, IReadOnlyList<ClickHouseColumnSettings?>? columnSettings, bool async, CancellationToken cancellationToken)
            {
                return await WithCancellationToken(cancellationToken, ct => ReadTable(dataMessage, columnSettings, false, async, ct));
            }

            public async ValueTask<BlockHeader> SkipTable(ServerDataMessage dataMessage, bool async, CancellationToken cancellationToken)
            {
                var table = await WithCancellationToken(cancellationToken, ct => ReadTable(dataMessage, null, true, async, ct));
                return table.Header;
            }

            private async ValueTask<ClickHouseTable> ReadTable(ServerDataMessage dataMessage, IReadOnlyList<ClickHouseColumnSettings?>? columnSettings, bool skip, bool async, CancellationToken cancellationToken)
            {
                CheckDisposed();

                var compression = _client._settings.Compress ? CompressionAlgorithm.Lz4 : CompressionAlgorithm.None;
                var reader = _client._reader;
                reader.BeginDecompress(compression);

                int blockFieldCode;
                bool overflow = false;
                int size = -1;
                do
                {
                    blockFieldCode = await reader.Read7BitInt32(async, cancellationToken);
                    switch (blockFieldCode)
                    {
                        case BlockFieldCodes.Overflow:
                            overflow = await reader.ReadBool(async, cancellationToken);
                            break;
                        case BlockFieldCodes.Size:
                            size = await reader.ReadInt32(async, cancellationToken);
                            break;
                        case BlockFieldCodes.End:
                            break;
                        default:
                            throw new ClickHouseException(ClickHouseErrorCodes.ProtocolUnexpectedResponse, $"Internal error. Unexpected block field code (0x{blockFieldCode:X}) received from the server.");
                    }
                } while (blockFieldCode != BlockFieldCodes.End);

                var columnCount = await reader.Read7BitInt32(async, cancellationToken);
                var rowCount = await reader.Read7BitInt32(async, cancellationToken);

                if (overflow || size >= 0)
                    throw new NotImplementedException("TODO: implement support for overflow.");

                var columnInfos = new List<ColumnInfo>(columnCount);
                var columns = new List<IClickHouseTableColumn>(columnCount);
                for (int i = 0; i < columnCount; i++)
                {
                    var columnName = await reader.ReadString(async, cancellationToken);
                    var columnTypeName = await reader.ReadString(async, cancellationToken);
                    var columnType = _client.TypeInfoProvider.GetTypeInfo(columnTypeName);
                    columnInfos.Add(new ColumnInfo(columnName, columnType));

                    var columnRowCount = rowCount;
                    var columnReader = columnType.CreateColumnReader(skip ? 0 : columnRowCount);
                    object? skipContext = null;
                    while (columnRowCount > 0)
                    {
                        SequenceSize sequenceSize;
                        if (skip)
                            sequenceSize = await reader.ReadRaw(seq => columnReader.Skip(seq, columnRowCount, ref skipContext), async, cancellationToken);
                        else
                            sequenceSize = await reader.ReadRaw(columnReader.ReadNext, async, cancellationToken);

                        if (sequenceSize.Elements < 0)
                            throw new InvalidOperationException("The number of elements must be greater than zero.");
                        if (sequenceSize.Elements > columnRowCount)
                            throw new InvalidOperationException($"The number of rows in the column \"{columnName}\" is greater than the number of rows in the table.");
                        if (sequenceSize.Elements < columnRowCount)
                            await reader.Advance(async, cancellationToken);

                        columnRowCount -= sequenceSize.Elements;
                    }

                    var settings = columnSettings == null || columnSettings.Count <= i ? null : columnSettings[i];
                    var column = columnReader.EndRead(settings);
                    columns.Add(column);
                }

                reader.EndDecompress();
                var blockHeader = new BlockHeader(dataMessage.TempTableName, columnInfos.AsReadOnly(), rowCount);
                return new ClickHouseTable(blockHeader, columns.AsReadOnly());
            }

            private async ValueTask<T> WithCancellationToken<T>(CancellationToken token, Func<CancellationToken, ValueTask<T>> execute)
            {
                if (_sessionCancellationToken == CancellationToken.None)
                    return await execute(token);

                if (token == CancellationToken.None)
                    return await execute(_sessionCancellationToken);

                using var linkedTs = CancellationTokenSource.CreateLinkedTokenSource(token, _sessionCancellationToken);
                try
                {
                    return await execute(linkedTs.Token);
                }
                catch (TaskCanceledException taskCanceledEx)
                {
                    if (token.IsCancellationRequested)
                        throw new TaskCanceledException(taskCanceledEx.Message, taskCanceledEx, token);

                    throw;
                }
                catch (OperationCanceledException operationCanceledEx)
                {
                    if (token.IsCancellationRequested)
                        throw new OperationCanceledException(operationCanceledEx.Message, operationCanceledEx, token);

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

                using var linkedTs = CancellationTokenSource.CreateLinkedTokenSource(token, _sessionCancellationToken);
                try
                {
                    await execute(linkedTs.Token);
                }
                catch (TaskCanceledException taskCanceledEx)
                {
                    if (token.IsCancellationRequested)
                        throw new TaskCanceledException(taskCanceledEx.Message, taskCanceledEx, token);

                    throw;
                }
                catch (OperationCanceledException operationCanceledEx)
                {
                    if (token.IsCancellationRequested)
                        throw new OperationCanceledException(operationCanceledEx.Message, operationCanceledEx, token);

                    throw;
                }
            }

            private void CheckDisposed()
            {
                if (IsDisposed)
                    throw new ObjectDisposedException("Internal error. This object was disposed and no more has an exclusive access to the network stream.");

                if (_client._isFailed)
                {
                    if (_client._unhandledException != null)
                        throw new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, "Connection is broken.", _client._unhandledException);

                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, "Connection is broken.");
                }
            }

            public async ValueTask<Exception?> SetFailed(Exception? unhandledException, bool sendCancel, bool async)
            {
                if (IsDisposed)
                    return null;

                Exception? processedException = null;
                if (sendCancel)
                {
                    try
                    {
                        await SendCancel(async);
                    }
                    catch (Exception ex)
                    {
                        var networkException = new ClickHouseException(ClickHouseErrorCodes.NetworkError, "Network error. Operation was not canceled properly.", ex);
                        processedException = unhandledException != null ? (Exception) new AggregateException(unhandledException, networkException) : networkException;
                    }
                }

                _client.SetFailed(unhandledException);
                return processedException;
            }

            public void Dispose()
            {
                if (IsDisposed || IsFailed)
                    return;

                _client._semaphore.Release();
                IsDisposed = true;
            }
        }
    }
}
