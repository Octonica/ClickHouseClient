#region License Apache 2.0
/* Copyright 2020-2023 Octonica
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
using System.IO;

namespace Octonica.ClickHouseClient
{
    internal static class ConnectionSettingsHelper
    {
        public static ClickHouseConnectionSettings GetConnectionSettings(Action<ClickHouseConnectionStringBuilder>? updateSettings = null)
        {
            return GetConnectionSettingsInternal(updateSettings).settings;
        }

        public static string GetConnectionString()
        {
            return GetConnectionSettingsInternal(null).connectionString;
        }

        public static string GetConnectionString(Action<ClickHouseConnectionStringBuilder> updateSettings)
        {
            var settings = GetConnectionSettings(updateSettings);
            var builder = new ClickHouseConnectionStringBuilder(settings);
            return builder.ConnectionString;
        }

        private static (ClickHouseConnectionSettings settings, string connectionString) GetConnectionSettingsInternal(Action<ClickHouseConnectionStringBuilder>? updateSettings)
        {
            const string envVariableName = "CLICKHOUSE_TEST_CONNECTION";
            const string configFileName = "clickHouse.dbconfig";
            const string conStrExample = "host=clickhouse.example.com; port=9000; user=default;";

            var configTextFromEnvVar = Environment.GetEnvironmentVariable(envVariableName);
            if (configTextFromEnvVar != null)
            {
                try
                {
                    var builder = new ClickHouseConnectionStringBuilder(configTextFromEnvVar);
                    updateSettings?.Invoke(builder);
                    return (builder.BuildSettings(), configTextFromEnvVar);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"The connection string from the environment variable '{envVariableName}' is not valid. Connection string example: '{conStrExample}'. {ex.Message}", ex);
                }
            }

            string configPath = Path.Combine(AppContext.BaseDirectory, configFileName);
            if (!File.Exists(configPath))
            {
                throw new InvalidOperationException(
                    "The connection string is required. " +
                    $"Please, set the environment variable '{envVariableName}' or write the connection string to the file '{configFileName}'. " +
                    $"Connection string example: '{conStrExample}'.");
            }
            
            string configText = File.ReadAllText(configPath);
            try
            {
                var builder = new ClickHouseConnectionStringBuilder(configText);
                updateSettings?.Invoke(builder);
                return (builder.BuildSettings(), configText);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"The connection string from the file '{configFileName}' is not valid. Connection string example: '{conStrExample}'. {ex.Message}", ex);
            }
        }
    }
}