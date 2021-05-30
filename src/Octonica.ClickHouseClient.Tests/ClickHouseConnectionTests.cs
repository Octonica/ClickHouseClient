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
using System.Linq;
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

        [Fact]
        public async Task OpenConncetionInParallel()
        {
            // Only one thread should open the connection and other threads should fail

            int counter = 0;
            var settings = GetDefaultConnectionSettings();
            await using var connection = new ClickHouseConnection(settings);
            connection.StateChange += OnStateChanged;

            var tcs = new TaskCompletionSource<bool>();

            var tasks = Enumerable.Range(0, 32).Select(_ => Open()).ToList();            

            tcs.SetResult(true);
            await Assert.ThrowsAnyAsync<Exception>(() => Task.WhenAll(tasks));

            Task? notFailedTask = null;
            foreach (var task in tasks)
            {
                if (task.Exception == null)
                {
                    Assert.Null(notFailedTask);
                    notFailedTask = task;
                    continue;
                }

                var aggrEx = Assert.IsAssignableFrom<AggregateException>(task.Exception);
                Assert.Single(aggrEx.InnerExceptions);
                var ex = Assert.IsAssignableFrom<ClickHouseException>(aggrEx.InnerExceptions[0]);
                Assert.Equal(ClickHouseErrorCodes.InvalidConnectionState, ex.ErrorCode);
            }

            Assert.NotNull(notFailedTask);
            Assert.Equal(ConnectionState.Open, connection.State);
            Assert.Equal(0x10001, counter);

            void OnStateChanged(object sender, StateChangeEventArgs e)
            {
                Assert.Same(sender, connection);

                if (e.OriginalState == ConnectionState.Closed && e.CurrentState == ConnectionState.Connecting)
                    Interlocked.Increment(ref counter);

                if (e.OriginalState == ConnectionState.Connecting && e.CurrentState == ConnectionState.Open)
                    Interlocked.Add(ref counter, 0x10000);
            }

            async Task Open()
            {
                await tcs.Task;
                await connection.OpenAsync();
            }
        }

        [Theory]
        [InlineData(ConnectionState.Connecting, ConnectionState.Closed)]
        [InlineData(ConnectionState.Open, ConnectionState.Open)]
        [InlineData(ConnectionState.Closed, ConnectionState.Closed)]
        public async Task HandleCallbackException(ConnectionState throwOnState, ConnectionState expectedState)
        {
            var settings = GetDefaultConnectionSettings();
            await using var connection = new ClickHouseConnection(settings);
            connection.StateChange += OnStateChanged;

            var ex = await Assert.ThrowsAnyAsync<ClickHouseException>(async () =>
            {
                await connection.OpenAsync();
                await connection.CloseAsync();
            });

            Assert.Equal(expectedState, connection.State);
            Assert.Equal(ClickHouseErrorCodes.CallbackError, ex.ErrorCode);
            Assert.NotNull(ex.InnerException);
            Assert.Equal("You shall not pass!", ex.InnerException!.Message);

            void OnStateChanged(object sender, StateChangeEventArgs e)
            {
                Assert.Same(sender, connection);

                if (e.CurrentState == throwOnState)
                    throw new Exception("You shall not pass!");
            }
        }

        [Fact]
        public async Task HandleDoubleCallbackException()
        {
            var settings = GetDefaultConnectionSettings();
            await using var connection = new ClickHouseConnection(settings);
            connection.StateChange += OnStateChanged;

            var ex = await Assert.ThrowsAnyAsync<AggregateException>(() => connection.OpenAsync());

            Assert.Equal(ConnectionState.Closed, connection.State);
            Assert.Equal(2, ex.InnerExceptions.Count);
            Assert.Equal("You shall not pass!", ex.InnerExceptions[0].Message);
            Assert.Equal("How dare you!", ex.InnerExceptions[1].Message);

            void OnStateChanged(object sender, StateChangeEventArgs e)
            {
                Assert.Same(sender, connection);

                if (e.CurrentState == ConnectionState.Closed)
                    throw new Exception("How dare you!");

                if (e.CurrentState == ConnectionState.Connecting)
                    throw new Exception("You shall not pass!");
            }
        }

        [Fact]
        public async Task DisposeCallback()
        {
            bool disposed = false;
            await using(var connection=await OpenConnectionAsync())
            {
                connection.Disposed += OnDisposed;
            }

            Assert.True(disposed);

            void OnDisposed(object? sender, EventArgs eventArgs)
            {
                disposed = true;
            }
        }
    }
}
