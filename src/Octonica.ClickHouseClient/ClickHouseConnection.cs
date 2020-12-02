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
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Types;
using Octonica.ClickHouseClient.Utils;
using TimeZoneConverter;

namespace Octonica.ClickHouseClient
{
    public class ClickHouseConnection : DbConnection
    {
        private const int MinBufferSize = 32;

        private readonly ClickHouseConnectionSettings _connectionSettings;
        private readonly IClickHouseTypeInfoProvider _typeInfoProvider;
        
        private ClickHouseTcpClient? _tcpClient;

        private volatile ConnectionState _connectionState;

        public override string ConnectionString
        {
            get => new ClickHouseConnectionStringBuilder(_connectionSettings).ConnectionString;
            set => throw new NotSupportedException();
        }

        public override int ConnectionTimeout => Timeout.Infinite;

        public override string? Database => _connectionSettings.Database;

        public override string DataSource => _connectionSettings.Host + (_connectionSettings.Port != ClickHouseConnectionStringBuilder.DefaultPort ? ":" + _connectionSettings.Port : string.Empty);

        public override string? ServerVersion => _tcpClient?.ServerInfo.Version.ToString();

        public override ConnectionState State => _connectionState;

        public ClickHouseConnection(string connectionString, IClickHouseTypeInfoProvider? typeInfoProvider = null)
            : this(new ClickHouseConnectionStringBuilder(connectionString), typeInfoProvider)
        {
        }

        public ClickHouseConnection(ClickHouseConnectionStringBuilder stringBuilder, IClickHouseTypeInfoProvider? typeInfoProvider = null)
        {
            if (stringBuilder == null)
                throw new ArgumentNullException(nameof(stringBuilder));

            _connectionSettings = stringBuilder.BuildSettings();
            _typeInfoProvider = typeInfoProvider ?? DefaultTypeInfoProvider.Instance;
        }

        public ClickHouseConnection(ClickHouseConnectionSettings connectionSettings, IClickHouseTypeInfoProvider? typeInfoProvider = null)
        {
            _connectionSettings = connectionSettings ?? throw new ArgumentNullException(nameof(connectionSettings));
            _typeInfoProvider = typeInfoProvider ?? DefaultTypeInfoProvider.Instance;
        }

        public override void ChangeDatabase(string databaseName)
        {
            throw new NotSupportedException();
        }

        public override Task ChangeDatabaseAsync(string databaseName, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        public override void Close()
        {
            TaskHelper.WaitNonAsyncTask(Close(false));
        }

        public override async Task CloseAsync()
        {
            await Close(true);
        }

        public override void EnlistTransaction(Transaction transaction)
        {
            throw new NotSupportedException();
        }

        public override DataTable GetSchema()
        {
            throw new NotSupportedException();
        }

        public override DataTable GetSchema(string collectionName)
        {
            throw new NotSupportedException();
        }

        public override DataTable GetSchema(string collectionName, string[] restrictionValues)
        {
            throw new NotSupportedException();
        }

        public override void Open()
        {
            TaskHelper.WaitNonAsyncTask(Open(false, CancellationToken.None));
        }

        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            await Open(true, cancellationToken);
        }

        protected override DbTransaction BeginDbTransaction(System.Data.IsolationLevel isolationLevel)
        {
            throw new NotSupportedException();
        }

        protected override ValueTask<DbTransaction> BeginDbTransactionAsync(System.Data.IsolationLevel isolationLevel, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public new ClickHouseCommand CreateCommand()
        {
            if (_tcpClient == null)
            {
                Debug.Assert(_connectionState != ConnectionState.Open);
                throw new ClickHouseException(ClickHouseErrorCodes.ConnectionClosed, "The connection is closed.");
            }

            return new ClickHouseCommand(this, _tcpClient) {CommandTimeout = _connectionSettings.CommandTimeout};
        }

        public ClickHouseCommand CreateCommand(string commandText)
        {
            if (_tcpClient == null)
            {
                Debug.Assert(_connectionState != ConnectionState.Open);
                throw new ClickHouseException(ClickHouseErrorCodes.ConnectionClosed, "The connection is closed.");
            }

            return new ClickHouseCommand(this, _tcpClient) {CommandText = commandText, CommandTimeout = _connectionSettings.CommandTimeout};
        }

        protected override DbCommand CreateDbCommand()
        {
            return CreateCommand();
        }

        public ClickHouseColumnWriter CreateColumnWriter(string insertFormatCommand)
        {
            return TaskHelper.WaitNonAsyncTask(CreateColumnWriter(insertFormatCommand, false, CancellationToken.None));
        }

        public async Task<ClickHouseColumnWriter> CreateColumnWriterAsync(string insertFormatCommand, CancellationToken cancellationToken)
        {
            return await CreateColumnWriter(insertFormatCommand, true, cancellationToken);
        }

        private async ValueTask<ClickHouseColumnWriter> CreateColumnWriter(string insertFormatCommand, bool async, CancellationToken cancellationToken)
        {
            if (_tcpClient == null)
            {
                Debug.Assert(_connectionState != ConnectionState.Open);
                throw new ClickHouseException(ClickHouseErrorCodes.ConnectionClosed, "The connection is closed.");
            }

            ClickHouseTcpClient.Session? session = null;
            bool cancelOnFailure = false;
            try
            {
                session = await _tcpClient.OpenSession(async, CancellationToken.None, cancellationToken);

                var messageBuilder = new ClientQueryMessage.Builder {QueryKind = QueryKind.InitialQuery, Query = insertFormatCommand};
                await session.SendQuery(messageBuilder, null, async, cancellationToken);

                cancelOnFailure = true;
                var msg = await session.ReadMessage(async, cancellationToken);
                switch (msg.MessageCode)
                {
                    case ServerMessageCode.Error:
                        throw ((ServerErrorMessage) msg).Exception.CopyWithQuery(insertFormatCommand);

                    case ServerMessageCode.TableColumns:
                        break;

                    default:
                        throw new ClickHouseException(ClickHouseErrorCodes.ProtocolUnexpectedResponse, $"Unexpected server message. Received the message of type {msg.MessageCode}.");
                }

                msg = await session.ReadMessage(async, cancellationToken);
                ClickHouseTable data;
                switch (msg.MessageCode)
                {
                    case ServerMessageCode.Error:
                        throw ((ServerErrorMessage) msg).Exception.CopyWithQuery(insertFormatCommand);

                    case ServerMessageCode.Data:
                        data = await session.ReadTable((ServerDataMessage) msg, null, async, cancellationToken);
                        break;

                    default:
                        throw new ClickHouseException(ClickHouseErrorCodes.ProtocolUnexpectedResponse, $"Unexpected server message. Received the message of type {msg.MessageCode}.");
                }

                return new ClickHouseColumnWriter(session, data.Header.Columns);
            }
            catch (ClickHouseServerException)
            {
                session?.Dispose();
                throw;
            }
            catch(ClickHouseHandledException)
            {
                session?.Dispose();
                throw;
            }
            catch(Exception ex)
            {
                if (session != null)
                {
                    var aggrEx = await session.SetFailed(ex, cancelOnFailure, async);
                    if (aggrEx != null)
                        throw aggrEx;
                }

                throw;
            }
        }

        public TimeZoneInfo GetServerTimeZone()
        {
            var serverInfo = _tcpClient?.ServerInfo;
            if (serverInfo == null || _connectionState != ConnectionState.Open)
                throw new ClickHouseException(ClickHouseErrorCodes.ConnectionClosed, "The connection is closed.");

            return TZConvert.GetTimeZoneInfo(serverInfo.Timezone);
        }

        public override ValueTask DisposeAsync()
        {
            _tcpClient?.Dispose();
            return default;
        }

        protected override void Dispose(bool disposing)
        {
            _tcpClient?.Dispose();
        }

        private async ValueTask Open(bool async, CancellationToken cancellationToken)
        {
            switch (_connectionState)
            {
                case ConnectionState.Closed:
                    break;
                case ConnectionState.Open:
                    return; // Re-entrance is allowed
                case ConnectionState.Connecting:
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, "The connection is already opening.");
                case ConnectionState.Broken:
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, "The connection is broken.");
                default:
                    throw new NotSupportedException($"Internal error. The state {_connectionState} is not supported.");
            }

            const int defaultHttpPort = 8123;
            TcpClient? client = null;
            ClickHouseBinaryProtocolWriter? writer = null;
            ClickHouseBinaryProtocolReader? reader = null;
            
            SetConnectionState(ConnectionState.Connecting);
            try
            {
                try
                {
                    client = new TcpClient {SendTimeout = _connectionSettings.ReadWriteTimeout, ReceiveTimeout = _connectionSettings.ReadWriteTimeout};

                    if (async)
                        await client.ConnectAsync(_connectionSettings.Host, _connectionSettings.Port);
                    else
                        client.Connect(_connectionSettings.Host, _connectionSettings.Port);
                }
                catch
                {
                    client?.Dispose();
                    throw;
                }

                writer = new ClickHouseBinaryProtocolWriter(client.GetStream(), Math.Max(_connectionSettings.BufferSize, MinBufferSize));

                var clientHello = new ClientHelloMessage.Builder
                {
                    ClientName = _connectionSettings.ClientName,
                    ClientVersion = _connectionSettings.ClientVersion,
                    User = _connectionSettings.User,
                    Database = _connectionSettings.Database,
                    Password = _connectionSettings.Password,
                    ProtocolRevision = Revisions.CurrentRevision
                }.Build();

                clientHello.Write(writer);

                await writer.Flush(async, cancellationToken);

                reader = new ClickHouseBinaryProtocolReader(client.GetStream(), Math.Max(_connectionSettings.BufferSize, MinBufferSize));
                var message = await reader.ReadMessage(false, async, cancellationToken);
                switch (message.MessageCode)
                {
                    case ServerMessageCode.Hello:
                        var helloMessage = (ServerHelloMessage) message;

                        bool hasExtraByte = reader.TryPeekByte(out var extraByte);
                        if (!hasExtraByte && client.Available > 0)
                        {
                            hasExtraByte = true;
                            extraByte = await reader.ReadByte(async, cancellationToken);
                        }

                        if (hasExtraByte)
                        {
                            throw new ClickHouseException(ClickHouseErrorCodes.ProtocolUnexpectedResponse, $"Expected the end of the data. Unexpected byte (0x{extraByte:X}) received from the server.");
                        }

                        var serverInfo = helloMessage.ServerInfo;
                        var configuredTypeInfoProvider = _typeInfoProvider.Configure(serverInfo);
                        _tcpClient = new ClickHouseTcpClient(client, reader, writer, _connectionSettings, serverInfo, configuredTypeInfoProvider);
                        break;

                    case ServerMessageCode.Error:
                        throw ((ServerErrorMessage) message).Exception;

                    default:
                        if ((int) message.MessageCode == 'H')
                        {
                            // It looks like HTTP
                            string httpDetectedMessage;
                            if (_connectionSettings.Port == defaultHttpPort)
                            {
                                // It's definitely HTTP
                                httpDetectedMessage = $"Detected an attempt to connect by HTTP protocol with the default port {defaultHttpPort}. ";
                            }
                            else
                            {
                                httpDetectedMessage =
                                    $"Internal error. Unexpected message code (0x{message.MessageCode}) received from the server." +
                                    "This error may by caused by an attempt to connect with HTTP protocol. ";
                            }

                            httpDetectedMessage +=
                                $"{ClickHouseConnectionStringBuilder.DefaultClientName} supports only ClickHouse native protocol. " +
                                $"The default port for the native protocol is {ClickHouseConnectionStringBuilder.DefaultPort}.";

                            throw new ClickHouseException(ClickHouseErrorCodes.ProtocolUnexpectedResponse, httpDetectedMessage);
                        }

                        throw new ClickHouseException(ClickHouseErrorCodes.ProtocolUnexpectedResponse, $"Internal error. Unexpected message code (0x{message.MessageCode}) received from the server.");
                }
            }
            catch (Exception ex)
            {
                reader?.Dispose();
                writer?.Dispose();
                client?.Dispose();
                SetConnectionState(ConnectionState.Closed);

                if (_connectionSettings.Port == defaultHttpPort && ex is IOException)
                {
                    var extraMessage =
                        $"{ex.Message} This error may be caused by an attempt to connect to the default HTTP port ({defaultHttpPort}). " +
                        $"{ClickHouseConnectionStringBuilder.DefaultClientName} supports only ClickHouse native protocol. " +
                        $"The default port for the native protocol is {ClickHouseConnectionStringBuilder.DefaultPort}.";

                    throw new IOException(extraMessage, ex);
                }

                throw;
            }

            _tcpClient.OnFailed += OnClientFailed;
            SetConnectionState(ConnectionState.Open);
        }

        private void OnClientFailed(object? sender, Exception? e)
        {
            SetConnectionState(ConnectionState.Broken);
            Debug.Assert(_tcpClient != null);

            ((ClickHouseTcpClient) sender!).OnFailed -= OnClientFailed;
            _tcpClient = null;
        }

        private async ValueTask Close(bool async)
        {
            switch (_connectionState)
            {
                case ConnectionState.Closed:
                    break; // Re-entrance is allowed

                case ConnectionState.Open:
                    if (_tcpClient != null)
                    {
                        // Acquire session for preventing access to the communication object
                        try
                        {
                            await _tcpClient.OpenSession(async, CancellationToken.None, CancellationToken.None);
                        }
                        catch (ObjectDisposedException)
                        {
                            if (_connectionState != ConnectionState.Open)
                            {
                                await Close(async);
                            }
                            else
                            {
                                _tcpClient = null;
                                SetConnectionState(ConnectionState.Closed);
                            }

                            return;
                        }

                        _tcpClient.Dispose();
                        _tcpClient = null;
                    }

                    SetConnectionState(ConnectionState.Closed);
                    return;

                case ConnectionState.Broken:
                    _tcpClient?.Dispose();
                    _tcpClient = null;
                    SetConnectionState(ConnectionState.Closed);
                    break;

                case ConnectionState.Connecting:
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, "The connection is opening. It can't be closed.");

                default:
                    throw new NotSupportedException($"Internal error. The state {_connectionState} is not supported.");
            }
        }

        private void SetConnectionState(ConnectionState state)
        {
            var originalState = _connectionState;
            if (state == originalState)
                return;

            _connectionState = state;
            OnStateChange(new StateChangeEventArgs(originalState, state));
        }
    }
}
