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
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Types;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// Represents an SQL statement to execute against a ClickHouse database. This class cannot be inherited.
    /// </summary>
    public sealed class ClickHouseCommand : DbCommand
    {
        private string? _commandText;
        private TimeSpan? _commandTimeout;

        /// <summary>
        /// Gets or sets the SQL statement to exeucute at the data source.
        /// </summary>
        [AllowNull]
        public override string CommandText
        {
            get => _commandText ?? string.Empty;
            set => _commandText = value;
        }

        /// <summary>
        /// Gets or sets the wait time (in seconds) before terminating the attempt to execute a command and generating an error.
        /// </summary>
        public override int CommandTimeout
        {
            get => (int)CommandTimeoutSpan.TotalSeconds;
            set => CommandTimeoutSpan = TimeSpan.FromSeconds(value);
        }

        /// <summary>
        /// Gets or sets the wait time before terminating the attempt to execute a command and generating an error.
        /// </summary>
        public TimeSpan CommandTimeoutSpan
        {
            get => GetCommandTimeout(Connection);
            set => _commandTimeout = value;
        }

        /// <summary>
        /// Gets the sets type of the command. The only supported type is <see cref="CommandType.Text"/>.
        /// </summary>
        /// <returns>The value <see cref="CommandType.Text"/>.</returns>
        /// <exception cref="NotSupportedException">The type set is not <see cref="CommandType.Text"/>.</exception>
        public override CommandType CommandType
        {
            get => CommandType.Text;
            set
            {
                if (value != CommandType.Text)
                    throw new NotSupportedException($"The type of the command \"{value}\" is not supported.");                
            }
        }

        /// <summary>
        /// Gets or sets how command results are applied to the <see cref="DataRow"/> when used by the Update method of the <see cref="DbDataAdapter"/>.
        /// The value of this property is ignored by the command and therefore doesn't affect it's behavior.
        /// </summary>
        /// <returns>One of enumeration values that indicates how command results are applied. The default value is <see cref="UpdateRowSource.None"/>.</returns>
        public override UpdateRowSource UpdatedRowSource { get; set; }

        /// <summary>
        /// Gets or sets the <see cref="ClickHouseConnection"/> used by this command.
        /// </summary>
        public new ClickHouseConnection? Connection { get; set; }

        /// <inheritdoc cref="Connection"/>    
        protected override DbConnection? DbConnection
        {
            get => Connection;
            set => Connection = (ClickHouseConnection?) value;
        }

        /// <summary>
        /// Gets the <see cref="ClickHouseParameterCollection"/>.
        /// </summary>
        /// <returns>The parameters of the SQL statement. The default is an empty collection.</returns>
        public new ClickHouseParameterCollection Parameters { get; } = new ClickHouseParameterCollection();

        /// <inheritdoc cref="Parameters"/>    
        protected sealed override DbParameterCollection DbParameterCollection => Parameters;

        /// <summary>
        /// Gets the <see cref="ClickHouseTableProviderCollection"/>.
        /// </summary>
        /// <returns>
        /// The tables which should be sent along with the query. The default is an empty collection.
        /// </returns>
        public ClickHouseTableProviderCollection TableProviders { get; } = new ClickHouseTableProviderCollection();

        /// <summary>
        /// Gets or sets the transaction within which the command executes. Always returns <b>null</b>.
        /// </summary>
        /// <returns><b>null</b></returns>
        /// <exception cref="NotSupportedException">The value set is not <b>null</b>.</exception>
        protected override DbTransaction? DbTransaction
        {
            get => null;
            set
            {
                if (value != null)
                    throw new NotSupportedException($"{nameof(DbTransaction)} is read only.'");
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether the command object should be visible in a customized interface control.
        /// </summary>
        /// <returns><see langword="true"/>, if the command object should be visible in a control; otherwise <see langword="false"/>. The default is <see langword="true"/>.</returns>
        public override bool DesignTimeVisible { get; set; } = true;

        /// <summary>
        /// Gets or sets value indicating whether the query should be executed with an explicitly defined values of the property 'extremes'.
        /// </summary>
        public bool? Extremes { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether profile events should be ignored while reading data.
        /// </summary>
        /// <returns><see langword="true"/>, if the data reader should skip profile events. <see langword="false"/>,
        /// if the data reader should return profile events as a recordset. The default value is <see langword="true"/>.</returns>
        public bool IgnoreProfileEvents { get; set; } = true;

        /// <summary>
        /// Gets or sets the mode of passing parameters to the query. The value of this property overrides <see cref="ClickHouseConnection.ParametersMode"/>.
        /// </summary>
        /// <returns>The mode of passing parameters to the query. The default value is <see cref="ClickHouseParameterMode.Inherit"/>.</returns>
        public ClickHouseParameterMode ParametersMode { get; set; } = ClickHouseParameterMode.Inherit;

        /// <summary>
        /// Creates a new instance of <see cref="ClickHouseCommand"/>.
        /// </summary>
        public ClickHouseCommand()
        {
        }

        internal ClickHouseCommand(ClickHouseConnection connection)
        {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        /// <summary>
        /// Not supported. To cancel a command execute it asyncronously with an appropriate cancellation token.
        /// </summary>
        /// <exception cref="NotImplementedException">Always throws <see cref="NotImplementedException"/>.</exception>
        public override void Cancel()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Executes a SQL statement against a connection object.
        /// </summary>
        /// <returns>The number of rows affected. The returned value is negative when the actual number of rows is greater than <see cref="int.MaxValue"/>.</returns>
        public override int ExecuteNonQuery()
        {
            var result = TaskHelper.WaitNonAsyncTask(ExecuteNonQuery(false, CancellationToken.None));

            if (result > int.MaxValue)
                return int.MinValue;

            return (int) result;
        }

        /// <summary>
        /// Executes a SQL statement against a connection object asyncronously.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> representing the asynchronous operation. The result (<see cref="Task{TResult}.Result"/>) is the 
        /// number of affected rows. The result is negative when the actual number of rows is greater than <see cref="int.MaxValue"/>.
        /// </returns>
        public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            var result = await ExecuteNonQuery(true, cancellationToken);
            
            if (result > int.MaxValue)
                return int.MinValue;

            return (int) result;
        }

        private async ValueTask<ulong> ExecuteNonQuery(bool async, CancellationToken cancellationToken)
        {
            ClickHouseTcpClient.Session? session = null;

            bool cancelOnFailure = false;            
            try
            {
                session = await OpenSession(false, async, cancellationToken);
                var query = await SendQuery(session, CommandBehavior.Default, async, cancellationToken);

                cancelOnFailure = true;
                (ulong read, ulong written) result = (0, 0), progress = (0, 0);
                while (true)
                {
                    var message = await session.ReadMessage(async, cancellationToken);
                    switch (message.MessageCode)
                    {
                        case ServerMessageCode.Data:
                        case ServerMessageCode.Totals:
                        case ServerMessageCode.Extremes:
                            var dataMessage = (ServerDataMessage) message;
                            var blockHeader = await session.SkipTable(dataMessage, async, cancellationToken);
                            if (blockHeader.Columns.Count == 0 && blockHeader.RowCount == 0)
                            {
                                result = (result.read + progress.read, result.written + progress.written);
                                progress = (0, 0);
                            }
                            continue;

                        case ServerMessageCode.ProfileEvents:
                            var profileEventsMessage = (ServerDataMessage)message;
                            await session.SkipTable(profileEventsMessage, async, cancellationToken);
                            continue;

                        case ServerMessageCode.Error:
                            throw ((ServerErrorMessage) message).Exception.CopyWithQuery(query);

                        case ServerMessageCode.EndOfStream:
                            result = (result.read + progress.read, result.written + progress.written);
                            await session.Dispose(async);

                            if (result.written > 0)
                            {
                                // INSERT command could also return the number of parsed rows. Return only the number of inserted rows.
                                return result.written;
                            }

                            return result.read;

                        case ServerMessageCode.Progress:
                            var progressMessage = (ServerProgressMessage) message;
                            progress = (progressMessage.Rows, progressMessage.WrittenRows);
                            continue;

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
            }
            catch (ClickHouseHandledException ex)
            {
                // Exception can't be handled at this level
                if (session != null)
                {
                    var aggrEx = await session.SetFailed(ex.InnerException, cancelOnFailure, async);
                    if (aggrEx != null)
                        throw aggrEx;
                }

                throw;
            }
            catch (Exception ex)
            {
                if (session != null)
                {
                    var aggrEx = await session.SetFailed(ex, cancelOnFailure, async);
                    if (aggrEx != null)
                        throw aggrEx;
                }

                throw;
            }
            finally
            {
                if (session != null)
                    await session.Dispose(async);
            }
        }

        /// <summary>
        /// Executes the query and returns the first column of the first row in the result set returned by the query.
        /// All other columns and rows are ignored.
        /// </summary>
        /// <returns>
        /// The first row of the first columns in the result set or <see cref="DBNull.Value"/> if the result set is empty.
        /// </returns>
        public override object ExecuteScalar()
        {
            return TaskHelper.WaitNonAsyncTask(ExecuteScalar(null, false, CancellationToken.None));
        }

        /// <summary>
        /// Executes the query and returns the first column of the first row in the result set returned by the query.
        /// All other columns and rows are ignored.
        /// </summary>
        /// <param name="columnSettings">Optional parameter. Settings for the first column in the result set.</param>
        /// <returns>
        /// The first row of the first columns in the result set or <see cref="DBNull.Value"/> if the result set is empty.
        /// </returns>
        public object ExecuteScalar(ClickHouseColumnSettings? columnSettings)
        {
            return TaskHelper.WaitNonAsyncTask(ExecuteScalar(columnSettings, false, CancellationToken.None));
        }

        /// <summary>
        /// Executes the query and returns the first column of the first row in the result set returned by the query.
        /// All other columns and rows are ignored.
        /// </summary>
        /// <typeparam name="T">The expected type of the first column in the result set.</typeparam>
        /// <returns>
        /// The first row of the first columns in the result set.
        /// </returns>
        public T ExecuteScalar<T>()
        {
            return TaskHelper.WaitNonAsyncTask(ExecuteScalar<T>(null, false, CancellationToken.None));
        }

        /// <summary>
        /// Executes the query and returns the first column of the first row in the result set returned by the query.
        /// All other columns and rows are ignored.
        /// </summary>
        /// <typeparam name="T">The expected type of the first column in the result set.</typeparam>
        /// <param name="columnSettings">Optional parameter. Settings for the first column in the result set.</param>
        /// <returns>
        /// The first row of the first columns in the result set.
        /// </returns>
        public T ExecuteScalar<T>(ClickHouseColumnSettings? columnSettings)
        {
            return TaskHelper.WaitNonAsyncTask(ExecuteScalar<T>(columnSettings, false, CancellationToken.None));
        }

        /// <summary>
        /// Executes the query asyncronously and returns the first column of the first row in the result set returned by the query.
        /// All other columns and rows are ignored.
        /// </summary>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> representing the asynchronous operation. The result (<see cref="Task{TResult}.Result"/>) is
        /// the first row of the first columns in the result set or <see cref="DBNull.Value"/> if the result set is empty.
        /// </returns>
        public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            return await ExecuteScalar(null, true, cancellationToken);
        }

        /// <summary>
        /// Executes the query asyncronously and returns the first column of the first row in the result set returned by the query.
        /// All other columns and rows are ignored.
        /// </summary>
        /// <param name="columnSettings">Optional parameter. Settings for the first column in the result set.</param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> representing the asynchronous operation. The result (<see cref="Task{TResult}.Result"/>) is
        /// the first row of the first columns in the result set or <see cref="DBNull.Value"/> if the result set is empty.
        /// </returns>
        public async Task<object> ExecuteScalarAsync(ClickHouseColumnSettings? columnSettings)
        {
            return await ExecuteScalar(columnSettings, true, CancellationToken.None);
        }

        /// <summary>
        /// Executes the query asyncronously and returns the first column of the first row in the result set returned by the query.
        /// All other columns and rows are ignored.
        /// </summary>
        /// <param name="columnSettings">Optional parameter. Settings for the first column in the result set.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> representing the asynchronous operation. The result (<see cref="Task{TResult}.Result"/>) is
        /// the first row of the first columns in the result set or <see cref="DBNull.Value"/> if the result set is empty.
        /// </returns>
        public async Task<object> ExecuteScalarAsync(ClickHouseColumnSettings? columnSettings, CancellationToken cancellationToken)
        {
            return await ExecuteScalar(columnSettings, true, cancellationToken);
        }

        /// <summary>
        /// Executes the query asyncronously and returns the first column of the first row in the result set returned by the query.
        /// All other columns and rows are ignored.
        /// </summary>
        /// <typeparam name="T">The expected type of the first column in the result set.</typeparam>
        /// <returns>
        /// A <see cref="Task{TResult}"/> representing the asynchronous operation. The result (<see cref="Task{TResult}.Result"/>) is
        /// the first row of the first columns in the result set.
        /// </returns>
        public async Task<T> ExecuteScalarAsync<T>()
        {
            return await ExecuteScalar<T>(null, true, CancellationToken.None);
        }

        /// <summary>
        /// Executes the query asyncronously and returns the first column of the first row in the result set returned by the query.
        /// All other columns and rows are ignored.
        /// </summary>
        /// <typeparam name="T">The expected type of the first column in the result set.</typeparam>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> representing the asynchronous operation. The result (<see cref="Task{TResult}.Result"/>) is
        /// the first row of the first columns in the result set.
        /// </returns>
        public async Task<T> ExecuteScalarAsync<T>(CancellationToken cancellationToken)
        {
            return await ExecuteScalar<T>(null, true, cancellationToken);
        }

        /// <summary>
        /// Executes the query asyncronously and returns the first column of the first row in the result set returned by the query.
        /// All other columns and rows are ignored.
        /// </summary>
        /// <typeparam name="T">The expected type of the first column in the result set.</typeparam>
        /// <param name="columnSettings">Optional parameter. Settings for the first column in the result set.</param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> representing the asynchronous operation. The result (<see cref="Task{TResult}.Result"/>) is
        /// the first row of the first columns in the result set.
        /// </returns>
        public async Task<T> ExecuteScalarAsync<T>(ClickHouseColumnSettings? columnSettings)
        {
            return await ExecuteScalar<T>(columnSettings, true, CancellationToken.None);
        }

        /// <summary>
        /// Executes the query asyncronously and returns the first column of the first row in the result set returned by the query.
        /// All other columns and rows are ignored.
        /// </summary>
        /// <typeparam name="T">The expected type of the first column in the result set.</typeparam>
        /// <param name="columnSettings">Optional parameter. Settings for the first column in the result set.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> representing the asynchronous operation. The result (<see cref="Task{TResult}.Result"/>) is
        /// the first row of the first columns in the result set.
        /// </returns>
        public async Task<T> ExecuteScalarAsync<T>(ClickHouseColumnSettings? columnSettings, CancellationToken cancellationToken)
        {
            return await ExecuteScalar<T>(columnSettings, true, cancellationToken);
        }

        private async ValueTask<T> ExecuteScalar<T>(ClickHouseColumnSettings? columnSettings, bool async, CancellationToken cancellationToken)
        {
            var result = await ExecuteScalar(columnSettings, reader => reader.GetFieldValue<T>(0), async, cancellationToken);
            return (T) result;
        }

        private ValueTask<object> ExecuteScalar(ClickHouseColumnSettings? columnSettings, bool async, CancellationToken cancellationToken)
        {
            return ExecuteScalar(columnSettings, reader => reader.GetValue(0), async, cancellationToken);
        }

        private async ValueTask<object> ExecuteScalar(ClickHouseColumnSettings? columnSettings, Func<ClickHouseDataReader, object?> valueSelector, bool async, CancellationToken cancellationToken)
        {
            ClickHouseDataReader? reader = null;
            try
            {
                reader = await ExecuteDbDataReader(CommandBehavior.Default, true, async, cancellationToken);
                bool hasAnyColumn = reader.FieldCount > 0;
                if (!hasAnyColumn)
                    return DBNull.Value;

                if (columnSettings != null)
                    reader.ConfigureColumn(0, columnSettings);

                bool hasAnyRow = async ? await reader.ReadAsync(cancellationToken) : reader.Read();
                if (!hasAnyRow)
                    return DBNull.Value;

                if (reader.IsDBNull(0))
                    return DBNull.Value;

                var result = valueSelector(reader);

                if (async)
                    await reader.CloseAsync();
                else
                    reader.Close();

                return result ?? DBNull.Value;
            }
            finally
            {
                if (async)
                {
                    if (reader != null)
                        await reader.DisposeAsync();
                }
                else
                {
                    reader?.Dispose();
                }
            }
        }

        /// <summary>
        /// Not supported. A preparation of the command is not implemented.
        /// </summary>
        /// <exception cref="NotImplementedException">Always throws <see cref="NotImplementedException"/>.</exception>
        public override void Prepare()
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc cref="Prepare"/>
        public override Task PrepareAsync(CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Creates a new <see cref="ClickHouseParameter"/> object with the default name and adds it to the collection of parameters (<see cref="Parameters"/>).
        /// </summary>
        /// <returns>A new <see cref="ClickHouseParameter"/> object.</returns>
        protected override DbParameter CreateDbParameter()
        {
            const string baseParamName = "param";
            int i = 0;
            string paramName;
            do
            {
                paramName = string.Format(CultureInfo.InvariantCulture, "{{{0}{1}}}", baseParamName, ++i);
            } while (Parameters.Contains(paramName));

            return new ClickHouseParameter(paramName);
        }

        /// <summary>
        /// Executes the query asyncronously and builds a <see cref="ClickHouseDataReader"/> with the default command behavior.
        /// </summary>
        /// <returns>A <see cref="Task{TResult}"/> representing the asynchronous operation.</returns>
        public new async Task<ClickHouseDataReader> ExecuteReaderAsync()
        {
            return await ExecuteDbDataReader(CommandBehavior.Default, IgnoreProfileEvents, true, CancellationToken.None);
        }

        /// <summary>
        /// Executes the query asyncronously and builds a <see cref="ClickHouseDataReader"/> with the default command behavior.
        /// </summary>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A <see cref="Task{TResult}"/> representing the asynchronous operation.</returns>
        public new async Task<ClickHouseDataReader> ExecuteReaderAsync(CancellationToken cancellationToken)
        {
            return await ExecuteDbDataReader(CommandBehavior.Default, IgnoreProfileEvents, true, cancellationToken);
        }

        /// <summary>
        /// Executes the query asyncronously and builds a <see cref="ClickHouseDataReader"/>.
        /// </summary>
        /// <param name="behavior">
        /// The set of flags determining the behavior of the command.
        /// The flag <see cref="CommandBehavior.KeyInfo"/> is not supported.
        /// The flag <see cref="CommandBehavior.SequentialAccess"/> is ignored.
        /// </param>
        /// <returns>A <see cref="Task{TResult}"/> representing the asynchronous operation.</returns>
        public new async Task<ClickHouseDataReader> ExecuteReaderAsync(CommandBehavior behavior)
        {
            return await ExecuteDbDataReader(behavior, IgnoreProfileEvents, true, CancellationToken.None);
        }

        /// <summary>
        /// Executes the query asyncronously and builds a <see cref="ClickHouseDataReader"/>.
        /// </summary>
        /// <param name="behavior">
        /// The set of flags determining the behavior of the command.
        /// The flag <see cref="CommandBehavior.KeyInfo"/> is not supported.
        /// The flag <see cref="CommandBehavior.SequentialAccess"/> is ignored.
        /// </param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A <see cref="Task{TResult}"/> representing the asynchronous operation.</returns>
        public new async Task<ClickHouseDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            return await ExecuteDbDataReader(behavior, IgnoreProfileEvents, true, cancellationToken);
        }

        /// <inheritdoc cref="ExecuteReaderAsync(CommandBehavior, CancellationToken)"/>
        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            return await ExecuteDbDataReader(behavior, IgnoreProfileEvents, true, cancellationToken);
        }

        /// <summary>
        /// Executes the query and builds a <see cref="ClickHouseDataReader"/> with the default command behavior.
        /// </summary>
        /// <returns>A <see cref="ClickHouseDataReader"/> object.</returns>
        public new ClickHouseDataReader ExecuteReader()
        {
            return TaskHelper.WaitNonAsyncTask(ExecuteDbDataReader(CommandBehavior.Default, IgnoreProfileEvents, false, CancellationToken.None));
        }

        /// <summary>
        /// Executes the query and builds a <see cref="ClickHouseDataReader"/>.
        /// </summary>
        /// <param name="behavior">
        /// The set of flags determining the behavior of the command.
        /// The flag <see cref="CommandBehavior.KeyInfo"/> is not supported.
        /// The flag <see cref="CommandBehavior.SequentialAccess"/> is ignored.
        /// </param>
        /// <returns>A <see cref="ClickHouseDataReader"/> object.</returns>
        public new ClickHouseDataReader ExecuteReader(CommandBehavior behavior)
        {
            return TaskHelper.WaitNonAsyncTask(ExecuteDbDataReader(behavior, IgnoreProfileEvents, false, CancellationToken.None));
        }

        /// <inheritdoc cref="ExecuteReader(CommandBehavior)"/>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            return TaskHelper.WaitNonAsyncTask(ExecuteDbDataReader(behavior, IgnoreProfileEvents, false, CancellationToken.None));
        }

        private async ValueTask<ClickHouseDataReader> ExecuteDbDataReader(CommandBehavior behavior, bool ignoreProfileEvents, bool async, CancellationToken cancellationToken)
        {
            const CommandBehavior knownBehaviorFlags =
                CommandBehavior.CloseConnection |
                CommandBehavior.KeyInfo |
                CommandBehavior.SchemaOnly |
                CommandBehavior.SequentialAccess |
                CommandBehavior.SingleResult |
                CommandBehavior.SingleRow;

            var unknownBehaviorFlags = behavior & ~knownBehaviorFlags;
            if (unknownBehaviorFlags != 0)
                throw new ArgumentException($"Command behavior has unknown flags ({unknownBehaviorFlags}).", nameof(behavior));

            if (behavior.HasFlag(CommandBehavior.KeyInfo))
            {
                throw new ArgumentException(
                    $"Command behavior has unsupported flag {nameof(CommandBehavior.KeyInfo)}." + Environment.NewLine +
                    "Please, report an issue if you have any idea of how this flag should affect the result (https://github.com/Octonica/ClickHouseClient/issues).",
                    nameof(behavior));
            }

            ClickHouseDataReaderRowLimit rowLimit;
            if (behavior.HasFlag(CommandBehavior.SchemaOnly))
            {
                if (behavior.HasFlag(CommandBehavior.SingleRow))
                    throw new ArgumentException($"Command behavior's flags {nameof(CommandBehavior.SchemaOnly)} and {nameof(CommandBehavior.SingleRow)} are mutualy exclusive.", nameof(behavior));

                rowLimit = ClickHouseDataReaderRowLimit.Zero;
            }
            else if (behavior.HasFlag(CommandBehavior.SingleRow))
            {
                rowLimit = ClickHouseDataReaderRowLimit.OneRow;
            }
            else if (behavior.HasFlag(CommandBehavior.SingleResult))
            {
                rowLimit = ClickHouseDataReaderRowLimit.OneResult;
            }
            else
            {
                rowLimit = ClickHouseDataReaderRowLimit.Infinite;
            }

            ClickHouseTcpClient.Session? session = null;
            bool cancelOnFailure = false;
            try
            {
                session = await OpenSession(behavior.HasFlag(CommandBehavior.CloseConnection), async, cancellationToken);
                var query = await SendQuery(session, behavior, async, cancellationToken);

                cancelOnFailure = true;
                var message = await session.ReadMessage(async, cancellationToken);
                switch (message.MessageCode)
                {
                    case ServerMessageCode.Data:
                        break;

                    case ServerMessageCode.Error:
                        throw ((ServerErrorMessage) message).Exception.CopyWithQuery(query);

                    case ServerMessageCode.EndOfStream:
                        throw ClickHouseHandledException.Wrap(new ClickHouseException(ClickHouseErrorCodes.QueryTypeMismatch, "There is no table in the server's response."));

                    default:
                        throw new ClickHouseException(ClickHouseErrorCodes.QueryTypeMismatch, "There is no table in the server's response.");
                }

                var firstTable = await session.ReadTable((ServerDataMessage) message, null, async, cancellationToken);
                if (rowLimit == ClickHouseDataReaderRowLimit.Zero)
                    await session.SendCancel(async);

                return new ClickHouseDataReader(firstTable, session, rowLimit, ignoreProfileEvents);
            }
            catch (ClickHouseHandledException)
            {
                if (session != null)
                    await session.Dispose(async);

                throw;
            }
            catch (ClickHouseServerException)
            {
                if (session != null)
                    await session.Dispose(async);

                throw;
            }
            catch (Exception ex)
            {
                Exception? aggrEx = null;
                if (session != null)
                    aggrEx = await session.SetFailed(ex, cancelOnFailure, async);

                if (aggrEx != null)
                    throw aggrEx;

                throw;
            }
        }

        private async ValueTask<string> SendQuery(ClickHouseTcpClient.Session session, CommandBehavior behavior, bool async, CancellationToken cancellationToken)
        {
            string commandText;
            List<IClickHouseTableWriter>? tableWriters = null;

            try
            {
                var parametersTable = $"_{Guid.NewGuid():N}";
                commandText = PrepareCommandText(session.TypeInfoProvider, parametersTable, out var parameters);

                if (parameters != null)
                {
                    tableWriters = new List<IClickHouseTableWriter>(TableProviders.Count + 1);
                    tableWriters.Add(CreateParameterTableWriter(session.TypeInfoProvider, parametersTable));
                }
                if (TableProviders.Count > 0)
                {
                    tableWriters ??= new List<IClickHouseTableWriter>(TableProviders.Count);
                    foreach(var tableProvider in TableProviders)
                    {
                        if (tableProvider.ColumnCount == 0)
                            continue;

                        var tableWriter = await CreateTableWriter(tableProvider, session.TypeInfoProvider, async, cancellationToken);
                        tableWriters.Add(tableWriter);
                    }
                }
            }
            catch (Exception ex)
            {
                throw ClickHouseHandledException.Wrap(ex);
            }

            List<KeyValuePair<string, string>>? setting = null;
            if (Extremes != null)
            {
                setting = new List<KeyValuePair<string, string>>(1) {new KeyValuePair<string, string>("extremes", Extremes.Value ? "1" : "0")};
            }

            if (session.ServerInfo.Revision >= ClickHouseProtocolRevisions.MinRevisionWithSettingsSerializedAsStrings)
            {
                if (behavior.HasFlag(CommandBehavior.SchemaOnly) || behavior.HasFlag(CommandBehavior.SingleRow))
                {
                    // https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Settings.h
                    // This settings are hints for the server. The result may contain more than one row.

                    setting ??= new List<KeyValuePair<string, string>>(2);
                    setting.Add(new KeyValuePair<string, string>("max_result_rows", "1"));
                    setting.Add(new KeyValuePair<string, string>("result_overflow_mode", "break"));
                }
            }

            var messageBuilder = new ClientQueryMessage.Builder {QueryKind = QueryKind.InitialQuery, Query = commandText, Settings = setting};
            await session.SendQuery(messageBuilder, tableWriters, async, cancellationToken);

            return commandText;
        }

        private async ValueTask<ClickHouseTcpClient.Session> OpenSession(bool closeConnection, bool async, CancellationToken cancellationToken)
        {
            var connection = Connection;
            if (connection == null)
                throw new InvalidOperationException("The connection is not set. The command can't be executed without a connection.");

            SessionResources? resources = null;
            try
            {
                var timeout = GetCommandTimeout(connection);
                CancellationTokenSource? sessionTokenSource = null;
                if (timeout > TimeSpan.Zero)
                    sessionTokenSource = new CancellationTokenSource(timeout);

                if (closeConnection || sessionTokenSource != null)
                    resources = new SessionResources(closeConnection ? connection : null, sessionTokenSource);
                
                return await connection.OpenSession(async, resources, sessionTokenSource?.Token ?? CancellationToken.None, cancellationToken);
            }
            catch
            {
                if (resources != null)
                    await resources.Release(async);

                throw;
            }
        }

        private TimeSpan GetCommandTimeout(ClickHouseConnection? connection)
        {
            return _commandTimeout ?? connection?.CommandTimeSpan ?? TimeSpan.FromSeconds(ClickHouseConnectionStringBuilder.DefaultCommandTimeout);
        }

        private ClickHouseParameterMode GetParametersMode(ClickHouseConnection? connection)
        {
            var mode = ParametersMode;
            if (mode != ClickHouseParameterMode.Inherit)
                return mode;

            return connection?.ParametersMode ?? ClickHouseParameterMode.Default;
        }

        private IClickHouseTableWriter CreateParameterTableWriter(IClickHouseTypeInfoProvider typeInfoProvider, string tableName)
        {
            return new ClickHouseTableWriter(tableName, 1, Parameters.Select(p => p.CreateParameterColumnWriter(typeInfoProvider)));
        }

        private static async ValueTask<IClickHouseTableWriter> CreateTableWriter(IClickHouseTableProvider tableProvider, IClickHouseTypeInfoProvider typeInfoProvider, bool async, CancellationToken cancellationToken)
        {
            var factories = new List<IClickHouseColumnWriterFactory>(tableProvider.ColumnCount);

            var rowCount = tableProvider.RowCount;
            for (int i = 0; i < tableProvider.ColumnCount; i++)
            {
                var columnDescriptor = tableProvider.GetColumnDescriptor(i);
                var typeInfo = typeInfoProvider.GetTypeInfo(columnDescriptor);
                var columnInfo = new ColumnInfo(columnDescriptor.ColumnName, typeInfo);
                var column = tableProvider.GetColumn(i);

                var factory = await ClickHouseColumnWriter.CreateColumnWriterFactory(columnInfo, column, i, rowCount, columnDescriptor.Settings, async, cancellationToken);
                factories.Add(factory);
            }

            return new ClickHouseTableWriter(tableProvider.TableName, rowCount, factories.Select(f => f.Create(0, rowCount)));
        }

        private string PrepareCommandText(IClickHouseTypeInfoProvider typeInfoProvider, string parametersTable, out HashSet<string>? parameters)
        {
            var query = CommandText;
            if (string.IsNullOrEmpty(query))
                throw new InvalidOperationException("Command text is not defined.");

            var parameterPositions = GetParameterPositions(query);
            if (parameterPositions.Count == 0)
            {
                parameters = null;
                return query;
            }

            parameters = null;
            var inheritParameterMode = GetParametersMode(Connection);
            var queryStringBuilder = new StringBuilder(query.Length);
            for (int i = 0; i < parameterPositions.Count; i++)
            {
                var (offset, length, typeSeparatorIdx) = parameterPositions[i];

                var start = i > 0 ? parameterPositions[i - 1].offset + parameterPositions[i - 1].length : 0;
                queryStringBuilder.Append(query, start, parameterPositions[i].offset - start);

                var parameterName = typeSeparatorIdx < 0 ? query.Substring(offset, length) : query.Substring(offset + 1, typeSeparatorIdx - 1);
                if (!Parameters.TryGetValue(parameterName, out var parameter))
                    throw new ClickHouseException(ClickHouseErrorCodes.QueryParameterNotFound, $"Parameter \"{parameterName}\" not found.");

                var parameterMode = parameter.GetParameterMode(inheritParameterMode);
                switch (parameterMode)
                {
                    case ClickHouseParameterMode.Interpolate:
                        var specifiedType = typeSeparatorIdx >= 0 ? query.AsMemory().Slice(offset + typeSeparatorIdx + 1, length - typeSeparatorIdx - 2) : ReadOnlyMemory<char>.Empty;
                        parameter.OutputParameterValue(queryStringBuilder, specifiedType, typeInfoProvider);
                        break;

                    case ClickHouseParameterMode.Default:
                    case ClickHouseParameterMode.Binary:
                        if (parameters == null)
                            parameters = new HashSet<string>();

                        if (!parameters.Contains(parameter.ParameterName))
                            parameters.Add(parameter.ParameterName);

                        if (typeSeparatorIdx >= 0)
                            queryStringBuilder.Append("(CAST(");

                        queryStringBuilder.Append("(SELECT ").Append(parametersTable).Append('.').Append(parameter.Id).Append(" FROM ").Append(parametersTable).Append(')');

                        if (typeSeparatorIdx >= 0)
                            queryStringBuilder.Append(" AS ").Append(query, offset + typeSeparatorIdx + 1, length - typeSeparatorIdx - 2).Append("))");
                        break;

                    default:
                        throw new ClickHouseException(ClickHouseErrorCodes.InternalError, $"Internal error. Unexpected parameter mode: {parameterMode}.");
                }
            }

            var lastPartStart = parameterPositions[^1].offset + parameterPositions[^1].length;
            queryStringBuilder.Append(query, lastPartStart, query.Length - lastPartStart);

            return queryStringBuilder.ToString();
        }

        private static List<(int offset, int length, int typeSeparatorIdx)> GetParameterPositions(string query)
        {
            // Searching parameters outside of comments, string literals and escaped identifiers
            // https://github.com/ClickHouse/ClickHouse/blob/master/docs/en/query_language/syntax.md

            var identifierRegex = new Regex(@"^[a-zA-Z_][0-9a-zA-Z_]*(\:.+)?$");
            var simpleIdentifierRegex = new Regex("^[a-zA-Z_][0-9a-zA-Z_]*");
            ReadOnlySpan<char> significantChars = stackalloc char[7] { '-', '\'', '"', '`', '/', '{' , '@' };
            ReadOnlySpan<char> querySlice = query;

            var parameterPositions = new List<(int offset, int length, int typeSeparatorIdx)>();

            int position = 0;
            while (!querySlice.IsEmpty)
            {
                var idx = querySlice.IndexOfAny(significantChars);
                if (idx < 0)
                    break;

                var ch = querySlice[idx];

                position += idx + 1;
                querySlice = querySlice.Slice(idx + 1);
                if (querySlice.IsEmpty)
                    break;

                switch (ch)
                {
                    case '-':
                        if (querySlice[0] != '-')
                            break;

                        var endOfCommentIdx = querySlice.IndexOfAny('\r', '\n');
                        if (endOfCommentIdx < 0)
                        {
                            position += querySlice.Length;
                            querySlice = ReadOnlySpan<char>.Empty;
                        }
                        else
                        {
                            position += endOfCommentIdx + 1;
                            querySlice = querySlice.Slice(endOfCommentIdx + 1);
                        }

                        break;

                    case '\'':
                    case '"':
                    case '`':
                        var tokenLen = ClickHouseSyntaxHelper.GetQuotedTokenLength(((ReadOnlySpan<char>) query).Slice(position - 1), ch);
                        if (tokenLen < 0)
                        {
                            position += querySlice.Length;
                            querySlice = ReadOnlySpan<char>.Empty;
                        }
                        else
                        {
                            position += tokenLen - 1;
                            querySlice = querySlice.Slice(tokenLen - 1);
                        }

                        break;

                    case '/':
                        if (querySlice[0] != '*')
                            break;

                        ++position;
                        querySlice = querySlice.Slice(1);
                        while (!querySlice.IsEmpty)
                        {
                            var endOfMultilineCommentIdx = querySlice.IndexOf('*');
                            if (endOfMultilineCommentIdx < 0)
                            {
                                position += querySlice.Length;
                                querySlice = ReadOnlySpan<char>.Empty;
                            }
                            else
                            {
                                position += endOfMultilineCommentIdx + 1;
                                querySlice = querySlice.Slice(endOfMultilineCommentIdx + 1);
                                if (querySlice.Length > 0 && querySlice[0] == '/')
                                {
                                    ++position;
                                    querySlice = querySlice.Slice(1);
                                    break;
                                }
                            }
                        }

                        break;

                    case '{':
                        var closeIdx = querySlice.IndexOf('}');
                        if (closeIdx < 0)
                        {
                            position += querySlice.Length;
                            querySlice = ReadOnlySpan<char>.Empty;
                            break;
                        }

                        var match = identifierRegex.Match(query, position, closeIdx);
                        if (match.Success)
                        {
                            if (match.Groups[1].Success)
                                parameterPositions.Add((position - 1, closeIdx + 2, match.Groups[1].Index - position + 1));
                            else
                                parameterPositions.Add((position - 1, closeIdx + 2, -1));
                        }

                        position += closeIdx + 1;
                        querySlice = querySlice.Slice(closeIdx + 1);
                        break;

                    case '@':
                        var simpleMatch = simpleIdentifierRegex.Match(query, position, querySlice.Length);
                        if (simpleMatch.Success)
                        {
                            var len = simpleMatch.Groups[0].Length;
                            parameterPositions.Add((position - 1, len + 1, -1));
                            
                            position += len;
                            querySlice = querySlice.Slice(len);
                        }

                        break;                        

                    default:
                        throw new NotSupportedException($"Internal error. Unexpected character \"{ch}\".");
                }
            }

            return parameterPositions;
        }

        /// <inheritdoc/>
        public override ValueTask DisposeAsync()
        {
            return base.DisposeAsync();
        }

        /// <inheritdoc/>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
        }

        internal sealed class SessionResources : IClickHouseSessionExternalResources
        {
            private readonly ClickHouseConnection? _connection;
            private readonly CancellationTokenSource? _tokenSource;

            public SessionResources(ClickHouseConnection? connection, CancellationTokenSource? tokenSource)
            {
                _connection = connection;
                _tokenSource = tokenSource;
            }

            public ValueTask Release(bool async)
            {
                _tokenSource?.Dispose();
                return _connection?.Close(async) ?? default;
            }

            public async ValueTask<Exception?> ReleaseOnFailure(Exception? exception, bool async)
            {
                await Release(async);
                return null;
            }
        }
    }
}
