#region License Apache 2.0
/* Copyright 2020 Octonica
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
        public static ClickHouseConnectionSettings GetConnectionSettings()
        {
            const string configFileName = "clickHouse.dbconfig";
            const string conStrExample = "host=domain.com; port=9000; user=default;";

            string configPath = Path.Combine(AppContext.BaseDirectory, configFileName);
            if (!File.Exists(configPath))
                throw new FileNotFoundException($"File '{configFileName}' not found. This file is required. It should contain the connection string. Connection string example: '{conStrExample}'.");
            
            string configText = File.ReadAllText(configPath);
            try
            {
                var builder = new ClickHouseConnectionStringBuilder(configText);
                return builder.BuildSettings();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"The connection string from the file '{configFileName}' is not valid. Connection string example: {ex.Message}", ex);
            }
        }
    }
}