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
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
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

        private readonly IClickHouseTypeInfoProvider? _typeInfoProvider;

        private ClickHouseConnectionState _connectionState;

        [AllowNull]
        public override string ConnectionString
        {
            get
            {
                var state = _connectionState;
                if (state.Settings == null)
                    return string.Empty;

                return new ClickHouseConnectionStringBuilder(state.Settings).ConnectionString;
            }
            set
            {
                var newSettings = value == null ? null : new ClickHouseConnectionStringBuilder(value).BuildSettings();
                var state = _connectionState;
                while (true)
                {
                    if (ReferenceEquals(state.Settings, newSettings))
                        break;

                    if (state.State != ConnectionState.Closed && state.State != ConnectionState.Broken)
                        throw new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, "The connection string can not be modified because the connection is active.");

                    var newState = new ClickHouseConnectionState(state.State, state.TcpClient, newSettings, unchecked(state.Counter + 1));
                    if (TryChangeConnectionState(state, newState, out state))
                        break;
                }
            }
        }

        public override int ConnectionTimeout => Timeout.Infinite;

        public override string Database => _connectionState.Settings?.Database ?? string.Empty;

        public override string DataSource
        {
            get
            {
                var state = _connectionState;
                if (state.Settings == null)
                    return string.Empty;

                return state.Settings.Host + (state.Settings.Port != ClickHouseConnectionStringBuilder.DefaultPort ? ":" + state.Settings.Port : string.Empty);
            }
        }

        public override string ServerVersion => _connectionState.TcpClient?.ServerInfo.Version.ToString() ?? string.Empty;

        public override ConnectionState State => _connectionState.State;

        internal TimeSpan? CommandTimeSpan
        {
            get
            {
                var commandTimeout = _connectionState.Settings?.CommandTimeout;
                if (commandTimeout == null)
                    return null;

                return TimeSpan.FromSeconds(commandTimeout.Value);
            }
        }

        public ClickHouseConnection()
        {
            _connectionState = new ClickHouseConnectionState();
        }

        public ClickHouseConnection(string connectionString, IClickHouseTypeInfoProvider? typeInfoProvider = null)
            : this(new ClickHouseConnectionStringBuilder(connectionString), typeInfoProvider)
        {
        }

        public ClickHouseConnection(ClickHouseConnectionStringBuilder stringBuilder, IClickHouseTypeInfoProvider? typeInfoProvider = null)
        {
            if (stringBuilder == null)
                throw new ArgumentNullException(nameof(stringBuilder));

            var connectionSettings = stringBuilder.BuildSettings();

            _connectionState = new ClickHouseConnectionState(ConnectionState.Closed, null, connectionSettings, 0);
            _typeInfoProvider = typeInfoProvider;
        }

        public ClickHouseConnection(ClickHouseConnectionSettings connectionSettings, IClickHouseTypeInfoProvider? typeInfoProvider = null)
        {
            if (connectionSettings == null)
                throw new ArgumentNullException(nameof(connectionSettings));

            _connectionState = new ClickHouseConnectionState(ConnectionState.Closed, null, connectionSettings, 0);
            _typeInfoProvider = typeInfoProvider;
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

        public override void EnlistTransaction(Transaction? transaction)
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

        public override DataTable GetSchema(string collectionName, string?[] restrictionValues)
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
            return new ClickHouseCommand(this);
        }

        public ClickHouseCommand CreateCommand(string commandText)
        {
            return new ClickHouseCommand(this) {CommandText = commandText};
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
            var connectionState = _connectionState;
            if (connectionState.TcpClient == null)
            {
                Debug.Assert(connectionState.State != ConnectionState.Open);
                throw new ClickHouseException(ClickHouseErrorCodes.ConnectionClosed, "The connection is closed.");
            }

            ClickHouseTcpClient.Session? session = null;
            bool cancelOnFailure = false;
            try
            {
                session = await connectionState.TcpClient.OpenSession(async, null, CancellationToken.None, cancellationToken);

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
                if (session != null)
                    await session.Dispose(async);
                
                throw;
            }
            catch(ClickHouseHandledException)
            {
                if (session != null)
                    await session.Dispose(async);

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
            var connectionState = _connectionState;
            var serverInfo = connectionState.TcpClient?.ServerInfo;
            if (serverInfo == null || connectionState.State != ConnectionState.Open)
                throw new ClickHouseException(ClickHouseErrorCodes.ConnectionClosed, "The connection is closed.");

            return TZConvert.GetTimeZoneInfo(serverInfo.Timezone);
        }

        protected override void Dispose(bool disposing)
        {
            var connectionState = _connectionState;
            var counter = connectionState.Counter;
            while (connectionState.Counter == counter)
            {
                var targetState = connectionState.State == ConnectionState.Closed ? ConnectionState.Closed : ConnectionState.Broken;
                if (connectionState.State == targetState && connectionState.TcpClient == null)
                    break;

                var tcpClient = connectionState.TcpClient;
                if (!TryChangeConnectionState(connectionState, targetState, null, out connectionState, out _))
                    continue;

                tcpClient?.Dispose();
                break;
            }
            
            base.Dispose(disposing);
        }

        private async ValueTask Open(bool async, CancellationToken cancellationToken)
        {
            var connectionState = _connectionState;
            switch (connectionState.State)
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

            if (!TryChangeConnectionState(connectionState, ConnectionState.Connecting, out connectionState, out var onStateChanged))
                throw new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, "The state of the connection was modified.");

            var stateChangeEx = onStateChanged(this);
            var connectionSettings = connectionState.Settings;
            if (stateChangeEx != null || connectionSettings == null)
            {
                var initialEx = stateChangeEx ?? new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, "The connection is not initialized.");
                if (!TryChangeConnectionState(connectionState, ConnectionState.Closed, out _, out onStateChanged))
                    throw new AggregateException(initialEx, new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, "The state of the connection was modified."));

                var stateChangeEx2 = onStateChanged(this);
                if (stateChangeEx2 != null)
                    throw new AggregateException(initialEx, stateChangeEx2);

                if (stateChangeEx != null)
                    throw new ClickHouseException(ClickHouseErrorCodes.CallbackError, "External callback error. See the inner exception for details.", stateChangeEx);

                throw initialEx;
            }

            const int defaultHttpPort = 8123;
            TcpClient? client = null;
            ClickHouseBinaryProtocolWriter? writer = null;
            ClickHouseBinaryProtocolReader? reader = null;

            try
            {
                try
                {
                    client = new TcpClient {SendTimeout = connectionSettings.ReadWriteTimeout, ReceiveTimeout = connectionSettings.ReadWriteTimeout};

                    if (async)
                        await client.ConnectAsync(connectionSettings.Host, connectionSettings.Port);
                    else
                        client.Connect(connectionSettings.Host, connectionSettings.Port);
                }
                catch
                {
                    client?.Client?.Close(0);
                    client?.Dispose();
                    client = null;
                    throw;
                }

                writer = new ClickHouseBinaryProtocolWriter(client.GetStream(), Math.Max(connectionSettings.BufferSize, MinBufferSize));

                var clientHello = new ClientHelloMessage.Builder
                {
                    ClientName = connectionSettings.ClientName,
                    ClientVersion = connectionSettings.ClientVersion,
                    User = connectionSettings.User,
                    Database = connectionSettings.Database,
                    Password = connectionSettings.Password,
                    ProtocolRevision = Revisions.CurrentRevision
                }.Build();

                clientHello.Write(writer);

                await writer.Flush(async, cancellationToken);

                reader = new ClickHouseBinaryProtocolReader(client.GetStream(), Math.Max(connectionSettings.BufferSize, MinBufferSize));
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
                        var configuredTypeInfoProvider = (_typeInfoProvider ?? DefaultTypeInfoProvider.Instance).Configure(serverInfo);
                        var tcpClient = new ClickHouseTcpClient(client, reader, writer, connectionSettings, serverInfo, configuredTypeInfoProvider);
                        
                        if (!TryChangeConnectionState(connectionState, ConnectionState.Open, tcpClient, out _, out onStateChanged))
                            throw new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, "The state of the connection was modified.");

                        break;

                    case ServerMessageCode.Error:
                        throw ((ServerErrorMessage) message).Exception;

                    default:
                        if ((int) message.MessageCode == 'H')
                        {
                            // It looks like HTTP
                            string httpDetectedMessage;
                            if (connectionSettings.Port == defaultHttpPort)
                            {
                                // It's definitely HTTP
                                httpDetectedMessage = $"Detected an attempt to connect by HTTP protocol with the default port {defaultHttpPort}. ";
                            }
                            else
                            {
                                httpDetectedMessage =
                                    $"Internal error. Unexpected message code (0x{message.MessageCode:X}) received from the server." +
                                    "This error may by caused by an attempt to connect with HTTP protocol. ";
                            }

                            httpDetectedMessage +=
                                $"{ClickHouseConnectionStringBuilder.DefaultClientName} supports only ClickHouse native protocol. " +
                                $"The default port for the native protocol is {ClickHouseConnectionStringBuilder.DefaultPort}.";

                            throw new ClickHouseException(ClickHouseErrorCodes.ProtocolUnexpectedResponse, httpDetectedMessage);
                        }

                        throw new ClickHouseException(ClickHouseErrorCodes.ProtocolUnexpectedResponse, $"Internal error. Unexpected message code (0x{message.MessageCode:X}) received from the server.");
                }
            }
            catch (Exception ex)
            {
                reader?.Dispose();
                writer?.Dispose();
                client?.Client?.Close(0);
                client?.Dispose();

                if (TryChangeConnectionState(connectionState, ConnectionState.Closed, out _, out onStateChanged))
                    stateChangeEx = onStateChanged(this);

                if (connectionSettings.Port == defaultHttpPort && ex is IOException)
                {
                    var extraMessage =
                        $"{ex.Message} This error may be caused by an attempt to connect to the default HTTP port ({defaultHttpPort}). " +
                        $"{ClickHouseConnectionStringBuilder.DefaultClientName} supports only ClickHouse native protocol. " +
                        $"The default port for the native protocol is {ClickHouseConnectionStringBuilder.DefaultPort}.";

                    var extraEx = new IOException(extraMessage, ex);
                    if (stateChangeEx != null)
                        throw new AggregateException(extraEx, stateChangeEx);

                    throw extraEx;
                }

                if (stateChangeEx != null)
                    throw new AggregateException(ex, stateChangeEx);

                throw;
            }

            stateChangeEx = onStateChanged.Invoke(this);
            if (stateChangeEx != null)
                throw new ClickHouseException(ClickHouseErrorCodes.CallbackError, "External callback error. See the inner exception for details.", stateChangeEx);
        }

        /// <summary>
        /// Send ping to the server and wait for response.
        /// </summary>
        /// <returns>
        /// Returns <b>true</b> if ping was successful.
        /// Returns <b>false</b> if the connection is busy with a command execution.
        /// </returns>
        public bool TryPing()
        {
            return TaskHelper.WaitNonAsyncTask(TryPing(false, CancellationToken.None));
        }

        /// <inheritdoc cref="TryPing()"/>
        public Task<bool> TryPingAsync()
        {
            return TryPingAsync(CancellationToken.None);
        }

        /// <inheritdoc cref="TryPing()"/>
        public async Task<bool> TryPingAsync(CancellationToken cancellationToken)
        {
            return await TryPing(true, cancellationToken);
        }

        private async ValueTask<bool> TryPing(bool async, CancellationToken cancellationToken)
        {
            if (_connectionState.TcpClient?.State == ClickHouseTcpClientState.Active)
                return false;

            ClickHouseTcpClient.Session? session = null;
            try
            {
                using (var ts = new CancellationTokenSource(TimeSpan.FromMilliseconds(5)))
                {
                    try
                    {
                        session = await OpenSession(async, null, CancellationToken.None, ts.Token);
                    }
                    catch (OperationCanceledException ex)
                    {
                        if (ex.CancellationToken == ts.Token)
                            return false;

                        throw;
                    }
                }

                await session.SendPing(async, cancellationToken);
                var responseMsg = await session.ReadMessage(async, cancellationToken);

                switch (responseMsg.MessageCode)
                {
                    case ServerMessageCode.Pong:
                        return true;

                    case ServerMessageCode.Error:
                        // Something else caused this error. Keep it in InnerException for debug.
                        throw new ClickHouseException(
                            ClickHouseErrorCodes.ProtocolUnexpectedResponse,
                            $"Internal error. Unexpected message code (0x{responseMsg.MessageCode:X}) received from the server as a response to ping.",
                            ((ServerErrorMessage)responseMsg).Exception);

                    default:
                        throw new ClickHouseException(
                            ClickHouseErrorCodes.ProtocolUnexpectedResponse,
                            $"Internal error. Unexpected message code (0x{responseMsg.MessageCode:X}) received from the server as a response to ping.");
                }
            }
            catch (ClickHouseHandledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                if (session != null)
                {
                    await session.SetFailed(ex, false, async);
                    session = null;
                }

                throw;
            }
            finally
            {
                if (session != null)
                {
                    if (async)
                        await session.DisposeAsync();
                    else
                        session.Dispose();
                }
            }
        }

        internal ValueTask<ClickHouseTcpClient.Session> OpenSession(bool async, IClickHouseSessionExternalResources? externalResources, CancellationToken sessionCancellationToken, CancellationToken cancellationToken)
        {
            var connectionSession = new ConnectionSession(this, externalResources);
            return connectionSession.OpenSession(async, sessionCancellationToken, cancellationToken);
        }

        internal async ValueTask Close(bool async)
        {
            var connectionState = _connectionState;
            var counter = connectionState.Counter;
            while (connectionState.Counter == counter)
            {
                var tcpClient = connectionState.TcpClient;
                Func<ClickHouseConnection, Exception?>? onStateChanged;
                Exception? stateChangedEx;
                switch (connectionState.State)
                {
                    case ConnectionState.Closed:
                        return; // Re-entrance is allowed

                    case ConnectionState.Open:
                        ClickHouseTcpClient.Session? session = null;
                        try
                        {
                            // Acquire session for preventing access to the communication object
                            var sessionTask = tcpClient?.OpenSession(async, null, CancellationToken.None, CancellationToken.None);
                            if (sessionTask != null)
                                session = await sessionTask.Value;
                        }
                        catch (ObjectDisposedException)
                        {
                            if (!TryChangeConnectionState(connectionState, ConnectionState.Closed, null, out connectionState, out onStateChanged))
                                continue;

                            stateChangedEx = onStateChanged(this);
                            if (stateChangedEx != null)
                                throw new ClickHouseException(ClickHouseErrorCodes.CallbackError, "External callback error. See the inner exception for details.", stateChangedEx);

                            return;
                        }
                        catch
                        {
                            if (session != null)
                                await session.Dispose(async);

                            throw;
                        }

                        if (!TryChangeConnectionState(connectionState, ConnectionState.Closed, null, out connectionState, out onStateChanged))
                        {
                            if (session != null)
                                await session.Dispose(async);

                            continue;
                        }

                        tcpClient?.Dispose();

                        stateChangedEx = onStateChanged(this);
                        if (stateChangedEx != null)
                            throw new ClickHouseException(ClickHouseErrorCodes.CallbackError, "External callback error. See the inner exception for details.", stateChangedEx);

                        return;

                    case ConnectionState.Broken:
                        if (!TryChangeConnectionState(connectionState, ConnectionState.Closed, null, out connectionState, out onStateChanged))
                            continue;

                        tcpClient?.Dispose();

                        stateChangedEx = onStateChanged(this);
                        if (stateChangedEx != null)
                            throw new ClickHouseException(ClickHouseErrorCodes.CallbackError, "External callback error. See the inner exception for details.", stateChangedEx);

                        break;

                    case ConnectionState.Connecting:
                        throw new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, "The connection is opening. It can't be closed.");

                    default:
                        throw new NotSupportedException($"Internal error. The state {_connectionState} is not supported.");
                }
            }
        }

        private bool TryChangeConnectionState(ClickHouseConnectionState from, ClickHouseConnectionState to, out ClickHouseConnectionState actualState)
        {
            actualState = Interlocked.CompareExchange(ref _connectionState, to, from);
            if (ReferenceEquals(actualState, from))
            {
                actualState = to;
                return true;
            }

            return false;
        }

        private bool TryChangeConnectionState(
            ClickHouseConnectionState state,
            ConnectionState newState,
            ClickHouseTcpClient? client,
            out ClickHouseConnectionState actualState,
            [NotNullWhen(true)] out Func<ClickHouseConnection, Exception?>? onStateChanged)
        {
            var counter = state.State != ConnectionState.Connecting && newState == ConnectionState.Connecting ? unchecked(state.Counter + 1) : state.Counter;
            var nextState = new ClickHouseConnectionState(newState, client, state.Settings, counter);
            if (TryChangeConnectionState(state, nextState, out actualState))
            {
                onStateChanged = CreateConnectionStateChangedCallback(state.State, actualState.State);
                return true;
            }

            onStateChanged = null;
            return false;
        }

        private bool TryChangeConnectionState(
            ClickHouseConnectionState state,
            ConnectionState newState,
            out ClickHouseConnectionState actualState,
            [NotNullWhen(true)] out Func<ClickHouseConnection, Exception?>? onStateChanged)
        {
            return TryChangeConnectionState(state, newState, state.TcpClient, out actualState, out onStateChanged);
        }

        private static Func<ClickHouseConnection, Exception?> CreateConnectionStateChangedCallback(ConnectionState originalState, ConnectionState currentState)
        {
            if (originalState == currentState)
                return _ => null;

            return FireEvent;

            Exception? FireEvent(ClickHouseConnection connection)
            {
                try
                {
                    connection.OnStateChange(new StateChangeEventArgs(originalState, currentState));
                }
                catch (Exception ex)
                {
                    return ex;
                }

                return null;
            }
        }

        private class ConnectionSession : IClickHouseSessionExternalResources
        {
            private readonly ClickHouseConnection _connection;
            private readonly ClickHouseConnectionState _state;
            private readonly IClickHouseSessionExternalResources? _externalResources;

            public ConnectionSession(ClickHouseConnection connection, IClickHouseSessionExternalResources? externalResources)
            {
                _connection = connection;
                _state = _connection._connectionState;
                _externalResources = externalResources;

                var tcpClient = _state.TcpClient;
                if (tcpClient == null)
                {
                    Debug.Assert(_state.State != ConnectionState.Open);
                    throw new ClickHouseException(ClickHouseErrorCodes.ConnectionClosed, "The connection is closed.");
                }

                if (_state.State != ConnectionState.Open)
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, "The connection is closed.");                
            }

            public ValueTask<ClickHouseTcpClient.Session> OpenSession(bool async, CancellationToken sessionCancellationToken, CancellationToken cancellationToken)
            {
                Debug.Assert(_state.TcpClient != null);
                return _state.TcpClient.OpenSession(async, this, sessionCancellationToken, cancellationToken);
            }

            public ValueTask Release(bool async)
            {
                return _externalResources?.Release(async) ?? default;
            }

            public async ValueTask<Exception?> ReleaseOnFailure(Exception? exception, bool async)
            {
                Exception? ex = null;
                if (_connection.TryChangeConnectionState(_state, ConnectionState.Broken, null, out _, out var onStateChanged))
                    ex = onStateChanged(_connection);

                Exception? externalEx = null;
                if (_externalResources != null)
                    externalEx = await _externalResources.ReleaseOnFailure(exception, async);

                if (ex != null && externalEx != null)
                    return new AggregateException(ex, externalEx);

                return externalEx ?? ex;
            }
        }
    }
}
