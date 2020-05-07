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
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;


namespace Octonica.ClickHouseClient.Benchmark
{
    public class ClickHouseBaseConnection
    {
        private const string ConfigExample = "host=domain.com; port=9000; user=default; password=pw";

        private ClickHouseConnectionSettings? _settings;

        public ClickHouseConnectionSettings GetDefaultConnectionSettings()
        {
            if (_settings != null)
            {
                return _settings;
            }

            _settings = this.ReadConfigFile().BuildSettings();

            return _settings;
        }

        public async Task<ClickHouseConnection> OpenConnectionAsync(ClickHouseConnectionSettings settings, CancellationToken cancellationToken)
        {
            ClickHouseConnection connection = new ClickHouseConnection(settings);
            await connection.OpenAsync(cancellationToken);

            return connection;
        }

        public async Task<ClickHouseConnection> OpenConnectionAsync()
        {
            return await OpenConnectionAsync(GetDefaultConnectionSettings(), CancellationToken.None);
        }

        public ClickHouseConnection OpenConnection()
        {
            ClickHouseConnection connection = new ClickHouseConnection(GetDefaultConnectionSettings());
            connection.Open();

            return connection;
        }

        private ClickHouseConnectionStringBuilder ReadConfigFile()
        {

            var configEnv = Environment.GetEnvironmentVariable("dbconfig", EnvironmentVariableTarget.User);

            if (configEnv == null)
            {
                throw new Exception($"Need user enviroment \"dbconfig\" with params {ConfigExample}.");
            }

            ClickHouseConnectionStringBuilder builder = new ClickHouseConnectionStringBuilder(configEnv);

            return builder;
        }
    }

}
