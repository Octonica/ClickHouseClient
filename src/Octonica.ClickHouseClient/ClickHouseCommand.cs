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
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
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
    public class ClickHouseCommand : DbCommand
    {
        private string? _commandText;
        private TimeSpan? _commandTimeout;

        [AllowNull]
        public override string CommandText
        {
            get => _commandText ?? string.Empty;
            set => _commandText = value;
        }

        public override int CommandTimeout
        {
            get => (int)CommandTimeoutSpan.TotalSeconds;
            set => CommandTimeoutSpan = TimeSpan.FromSeconds(value);
        }

        public TimeSpan CommandTimeoutSpan
        {
            get => GetCommandTimeout(Connection);
            set => _commandTimeout = value;
        }

        public override CommandType CommandType { get; set; }

        public override UpdateRowSource UpdatedRowSource { get; set; }

        public new ClickHouseConnection? Connection { get; set; }

        protected override DbConnection? DbConnection
        {
            get => Connection;
            set => Connection = (ClickHouseConnection?) value;
        }

        public new ClickHouseParameterCollection Parameters { get; } = new ClickHouseParameterCollection();

        protected sealed override DbParameterCollection DbParameterCollection => Parameters;

        protected override DbTransaction? DbTransaction
        {
            get => null;
            set => throw new NotSupportedException($"{nameof(DbTransaction)} is read only.'");
        }

        public override bool DesignTimeVisible { get; set; }

        /// <summary>
        /// Overrides the 'extremes' setting for the query
        /// </summary>
        public bool? Extremes { get; set; }

        public ClickHouseCommand()
        {
        }

        internal ClickHouseCommand(ClickHouseConnection connection)
        {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        public override void Cancel()
        {
            throw new NotImplementedException();
        }

        public override int ExecuteNonQuery()
        {
            var result = TaskHelper.WaitNonAsyncTask(ExecuteNonQuery(false, CancellationToken.None));

            if (result > int.MaxValue)
                return int.MinValue;

            return (int) result;
        }

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

        public override object ExecuteScalar()
        {
            return TaskHelper.WaitNonAsyncTask(ExecuteScalar(null, false, CancellationToken.None));
        }

        public object ExecuteScalar(ClickHouseColumnSettings? columnSettings)
        {
            return TaskHelper.WaitNonAsyncTask(ExecuteScalar(columnSettings, false, CancellationToken.None));
        }

        public T ExecuteScalar<T>()
        {
            return TaskHelper.WaitNonAsyncTask(ExecuteScalar<T>(null, false, CancellationToken.None));
        }

        public T ExecuteScalar<T>(ClickHouseColumnSettings? columnSettings)
        {
            return TaskHelper.WaitNonAsyncTask(ExecuteScalar<T>(columnSettings, false, CancellationToken.None));
        }

        public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            return await ExecuteScalar(null, true, cancellationToken);
        }

        public async Task<object> ExecuteScalarAsync(ClickHouseColumnSettings? columnSettings)
        {
            return await ExecuteScalar(columnSettings, true, CancellationToken.None);
        }

        public async Task<object> ExecuteScalarAsync(ClickHouseColumnSettings? columnSettings, CancellationToken cancellationToken)
        {
            return await ExecuteScalar(columnSettings, true, cancellationToken);
        }

        public async Task<T> ExecuteScalarAsync<T>()
        {
            return await ExecuteScalar<T>(null, true, CancellationToken.None);
        }

        public async Task<T> ExecuteScalarAsync<T>(CancellationToken cancellationToken)
        {
            return await ExecuteScalar<T>(null, true, cancellationToken);
        }

        public async Task<T> ExecuteScalarAsync<T>(ClickHouseColumnSettings? columnSettings)
        {
            return await ExecuteScalar<T>(columnSettings, true, CancellationToken.None);
        }

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
                reader = await ExecuteDbDataReader(CommandBehavior.Default, async, cancellationToken);
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

        public override void Prepare()
        {
            throw new NotImplementedException();
        }

        public override Task PrepareAsync(CancellationToken cancellationToken = new CancellationToken())
        {
            return base.PrepareAsync(cancellationToken);
        }

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

        public new async Task<ClickHouseDataReader> ExecuteReaderAsync()
        {
            return await ExecuteDbDataReader(CommandBehavior.Default, true, CancellationToken.None);
        }

        public new async Task<ClickHouseDataReader> ExecuteReaderAsync(CancellationToken cancellationToken)
        {
            return await ExecuteDbDataReader(CommandBehavior.Default, true, cancellationToken);
        }

        public new async Task<ClickHouseDataReader> ExecuteReaderAsync(CommandBehavior behavior)
        {
            return await ExecuteDbDataReader(behavior, true, CancellationToken.None);
        }

        public new async Task<ClickHouseDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            return await ExecuteDbDataReader(behavior, true, cancellationToken);
        }

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            return await ExecuteDbDataReader(behavior, true, cancellationToken);
        }

        public new ClickHouseDataReader ExecuteReader()
        {
            return TaskHelper.WaitNonAsyncTask(ExecuteDbDataReader(CommandBehavior.Default, false, CancellationToken.None));
        }

        public new ClickHouseDataReader ExecuteReader(CommandBehavior behavior)
        {
            return TaskHelper.WaitNonAsyncTask(ExecuteDbDataReader(behavior, false, CancellationToken.None));
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            return TaskHelper.WaitNonAsyncTask(ExecuteDbDataReader(behavior, false, CancellationToken.None));
        }

        private async ValueTask<ClickHouseDataReader> ExecuteDbDataReader(CommandBehavior behavior, bool async, CancellationToken cancellationToken)
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

                return new ClickHouseDataReader(firstTable, session, rowLimit);
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
            IClickHouseTableWriter[]? tableWriters = null;

            try
            {
                var parametersTable = $"_{Guid.NewGuid():N}";
                commandText = PrepareCommandText(parametersTable, out var parameters);

                if (parameters != null)
                    tableWriters = new[] {CreateParameterTableWriter(session.TypeInfoProvider, parametersTable)};
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

            if (session.ServerInfo.Revision >= Revisions.MinRevisionWithSettingsSerializedAsStrings)
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

        private IClickHouseTableWriter CreateParameterTableWriter(IClickHouseTypeInfoProvider typeInfoProvider, string tableName)
        {
            return new ClickHouseTableWriter(tableName, 1, Parameters.Select(p => p.CreateParameterColumnWriter(typeInfoProvider)));
        }

        private string PrepareCommandText(string parametersTable, out HashSet<string>? parameters)
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

            parameters = new HashSet<string>(parameterPositions.Count);
            var queryStringBuilder = new StringBuilder(query.Length);
            for (int i = 0; i < parameterPositions.Count; i++)
            {
                var (offset, length, typeSeparatorIdx) = parameterPositions[i];

                var start = i > 0 ? parameterPositions[i - 1].offset + parameterPositions[i - 1].length : 0;
                queryStringBuilder.Append(query, start, parameterPositions[i].offset - start);

                var parameterName = query.Substring(offset + 1, typeSeparatorIdx < 0 ? length - 2 : typeSeparatorIdx - 1);
                if (!Parameters.TryGetValue(parameterName, out var parameter))
                    throw new ClickHouseException(ClickHouseErrorCodes.QueryParameterNotFound, $"Parameter \"{parameterName}\" not found.");

                if (!parameters.Contains(parameter.ParameterName))
                    parameters.Add(parameter.ParameterName);

                if (typeSeparatorIdx >= 0)
                    queryStringBuilder.Append("(CAST(");

                queryStringBuilder.Append("(SELECT ").Append(parametersTable).Append('.').Append(parameter.Id).Append(" FROM ").Append(parametersTable).Append(')');

                if (typeSeparatorIdx >= 0)
                    queryStringBuilder.Append(" AS ").Append(query, offset + typeSeparatorIdx + 1, length - typeSeparatorIdx - 2).Append("))");
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
            ReadOnlySpan<char> significantChars = stackalloc char[6] { '-', '\'', '"', '`', '/', '{' };
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
                        do
                        {
                            var nextCharacterIdx = querySlice.IndexOfAny('\\', ch);
                            if (nextCharacterIdx < 0)
                            {
                                position += querySlice.Length;
                                querySlice = ReadOnlySpan<char>.Empty;
                            }
                            else if (querySlice[nextCharacterIdx] == '\\')
                            {
                                position += nextCharacterIdx + 1;
                                querySlice = querySlice.Slice(nextCharacterIdx + 1);
                                if (querySlice.Length > 0)
                                {
                                    ++position;
                                    querySlice = querySlice.Slice(1);
                                }
                            }
                            else
                            {
                                Debug.Assert(querySlice[nextCharacterIdx] == ch);
                                position += nextCharacterIdx + 1;
                                querySlice = querySlice.Slice(nextCharacterIdx + 1);
                                break;
                            }

                        } while (!querySlice.IsEmpty);

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

                    default:
                        throw new NotSupportedException($"Internal error. Unexpected character \"{querySlice[idx]}\".");
                }
            }

            return parameterPositions;
        }

        public override ValueTask DisposeAsync()
        {
            return base.DisposeAsync();
        }

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
        }
    }
}
