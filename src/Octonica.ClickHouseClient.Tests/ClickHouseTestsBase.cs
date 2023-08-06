#region License Apache 2.0
/* Copyright 2019-2023 Octonica
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
using System.Threading;
using System.Threading.Tasks;

namespace Octonica.ClickHouseClient.Tests
{
    public abstract class ClickHouseTestsBase
    {
        private ClickHouseConnectionSettings? _settings;

        public static readonly IReadOnlyCollection<object[]> ParameterModes = new[]
        {
            new object[] { ClickHouseParameterMode.Binary },
            new object[] { ClickHouseParameterMode.Interpolate }
        };

        public ClickHouseConnectionSettings GetDefaultConnectionSettings(Action<ClickHouseConnectionStringBuilder>? updateSettings = null)
        {
            if (_settings != null)
            {
                return _settings;
            }

            _settings = ConnectionSettingsHelper.GetConnectionSettings(updateSettings);
            return _settings;
        }

        public async Task<ClickHouseConnection> OpenConnectionAsync(ClickHouseConnectionSettings settings, CancellationToken cancellationToken)
        {
            ClickHouseConnection connection = new ClickHouseConnection(settings);
            await connection.OpenAsync(cancellationToken);

            return connection;
        }

        public Task<ClickHouseConnection> OpenConnectionAsync(ClickHouseParameterMode parameterMode, CancellationToken cancellationToken = default)
        {
            return OpenConnectionAsync(builder => builder.ParametersMode = parameterMode, cancellationToken);
        }

        public async Task<ClickHouseConnection> OpenConnectionAsync(Action<ClickHouseConnectionStringBuilder>? updateSettings = null, CancellationToken cancellationToken = default)
        {
            return await OpenConnectionAsync(GetDefaultConnectionSettings(updateSettings), cancellationToken);
        }

        public ClickHouseConnection OpenConnection(Action<ClickHouseConnectionStringBuilder>? updateSettings = null)
        {
            ClickHouseConnection connection = new ClickHouseConnection(GetDefaultConnectionSettings(updateSettings));
            connection.Open();

            return connection;
        }

        protected Task WithTemporaryTable(string tableNameSuffix, string columns, Func<ClickHouseConnection, string, Task> runTest)
        {
            return WithTemporaryTable(tableNameSuffix, columns, (cn, tableName, _) => runTest(cn, tableName));
        }

        protected async Task WithTemporaryTable(string tableNameSuffix, string columns, Func<ClickHouseConnection, string, CancellationToken, Task> runTest, CancellationToken ct = default)
        {
            var tableName = GetTempTableName(tableNameSuffix);
            try
            {
                await using var connection = await OpenConnectionAsync();

                var cmd = connection.CreateCommand($"DROP TABLE IF EXISTS {tableName}");
                await cmd.ExecuteNonQueryAsync();

                cmd = connection.CreateCommand($"CREATE TABLE {tableName}({columns}) ENGINE=Memory");
                await cmd.ExecuteNonQueryAsync();

                await runTest(connection, tableName, ct);
            }
            finally
            {
                await using var connection = await OpenConnectionAsync();
                var cmd = connection.CreateCommand($"DROP TABLE IF EXISTS {tableName}");
                await cmd.ExecuteNonQueryAsync();
            }
        }

        protected virtual string GetTempTableName(string tableNameSuffix)
        {
            return $"clickhouse_client_test_{tableNameSuffix}";
        }
    }
}
