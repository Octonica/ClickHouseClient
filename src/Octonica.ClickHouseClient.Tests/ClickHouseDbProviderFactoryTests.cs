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

using System.Data;
using System.Data.Common;
using Xunit;

namespace Octonica.ClickHouseClient.Tests
{
    public class ClickHouseDbProviderFactoryTests
    {
        private static readonly DbProviderFactory Factory = new ClickHouseDbProviderFactory();

        [Fact]
        public void SupportedFeatures()
        {
            Assert.False(Factory.CanCreateCommandBuilder);
            Assert.False(Factory.CanCreateDataAdapter);
            Assert.False(Factory.CanCreateDataSourceEnumerator);

            var p = Factory.CreateParameter();
            Assert.IsAssignableFrom<ClickHouseParameter>(p);

            var cmd = Factory.CreateCommand();
            Assert.IsAssignableFrom<ClickHouseCommand>(cmd);
            cmd.Dispose();

            var cn = Factory.CreateConnection();
            Assert.IsAssignableFrom<ClickHouseConnection>(cn);
            cn.Dispose();

            var sb = Factory.CreateConnectionStringBuilder();
            Assert.IsAssignableFrom<ClickHouseConnectionStringBuilder>(sb);
        }

        [Fact]
        public void CreateCommand()
        {
            var connectionString = ConnectionSettingsHelper.GetConnectionString();
            var sb = Factory.CreateConnectionStringBuilder();
            Assert.NotNull(sb);
            sb.ConnectionString = connectionString;

            using var connection = Factory.CreateConnection();
            Assert.NotNull(connection);

            using var cmd = Factory.CreateCommand();
            Assert.NotNull(cmd);

            var parameterName = cmd.CreateParameter().ParameterName;
            cmd.Parameters.RemoveAt(parameterName);
            Assert.Empty(cmd.Parameters);

            var parameter = Factory.CreateParameter();
            Assert.NotNull(parameter);
            
            cmd.CommandText = $"SELECT * FROM system.one WHERE dummy < {parameterName}";
            cmd.Parameters.Add(parameter);

            parameter.ParameterName = parameterName;
            parameter.DbType = DbType.Int32;
            parameter.Value = int.MaxValue;

            connection.ConnectionString = sb.ToString();
            connection.Open();

            cmd.Connection = connection;

            var result = cmd.ExecuteScalar();
            Assert.Equal((byte) 0, result);
        }
    }
}
