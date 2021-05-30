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

using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Octonica.ClickHouseClient.Exceptions;
using Xunit;

namespace Octonica.ClickHouseClient.Tests
{
    public class ClickHouseConnectionTests : ClickHouseTestsBase
    {
        [Fact]
        public void ShouldUnwrapConnectionOpenExceptions()
        {
            var sb = new ClickHouseConnectionStringBuilder { Host = "none.example.com" };
            using var conn = new ClickHouseConnection(sb);
            var exception = Assert.ThrowsAny<SocketException>(() => conn.Open());
            Assert.Equal(SocketError.HostNotFound, exception.SocketErrorCode);
        }

        [Fact]
        public async Task CanConnectWithUserAndPassword()
        {
            var settings = GetDefaultConnectionSettings();
            settings = new ClickHouseConnectionStringBuilder(settings) { Database = null }.BuildSettings();

            Assert.False(string.IsNullOrEmpty(settings.User));
            Assert.Null(settings.Database);

            await using var conn = new ClickHouseConnection(settings);
            await conn.OpenAsync();
            Assert.Equal(string.Empty, conn.Database);

            var currentUser = await conn.CreateCommand("select currentUser()").ExecuteScalarAsync<string>();
            var currentDb = await conn.CreateCommand("select currentDatabase()").ExecuteScalarAsync<string>();

            Assert.Equal(settings.User, currentUser);
            Assert.False(string.IsNullOrEmpty(currentDb));
        }

        [Fact]
        public async Task TryPing()
        {
            await using var cn = new ClickHouseConnection(GetDefaultConnectionSettings());

            var ex = await Assert.ThrowsAnyAsync<ClickHouseException>(() => cn.TryPingAsync());
            Assert.Equal(ClickHouseErrorCodes.ConnectionClosed, ex.ErrorCode);

            await cn.OpenAsync();
            Assert.True(await cn.TryPingAsync());

            var cmd = cn.CreateCommand("SELECT * FROM system.one");
            Assert.True(await cn.TryPingAsync());
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                Assert.False(await cn.TryPingAsync());
                Assert.True(await reader.ReadAsync());
                Assert.False(await cn.TryPingAsync());
                Assert.False(await reader.ReadAsync());
                Assert.True(await cn.TryPingAsync());
            }

            await using (await cmd.ExecuteReaderAsync())
            {
                Assert.False(await cn.TryPingAsync());
            }

            Assert.True(await cn.TryPingAsync());

            await WithTemporaryTable(
                "ping",
                "id Int32",
                async (_, tableName) =>
                {
                    await using (var writer = await cn.CreateColumnWriterAsync($"INSERT INTO {tableName} VALUES", CancellationToken.None))
                    {
                        Assert.False(await cn.TryPingAsync());
                        await writer.EndWriteAsync(CancellationToken.None);
                        Assert.True(await cn.TryPingAsync());
                    }

                    Assert.True(await cn.TryPingAsync());
                    await using (await cn.CreateColumnWriterAsync($"INSERT INTO {tableName} VALUES", CancellationToken.None))
                    {
                        Assert.False(await cn.TryPingAsync());
                    }

                    Assert.True(await cn.TryPingAsync());
                });
        }
    }
}
