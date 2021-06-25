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

using Octonica.ClickHouseClient.Exceptions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Octonica.ClickHouseClient.Tests
{
    public class ClickHouseCommandTests : ClickHouseTestsBase
    {
        [Fact]
        public async Task SimpleExecuteScalar()
        {
            await using var cn = await OpenConnectionAsync();

            await using var cmd = cn.CreateCommand();
            cmd.CommandText = "select 42";
            var value = await cmd.ExecuteScalarAsync(CancellationToken.None);
            Assert.Equal((byte)42, value);
        }

        [Fact]
        public async Task ExecuteNonQuery()
        {
            await using var cn = await OpenConnectionAsync();

            await using var cmd = cn.CreateCommand();
            cmd.CommandText = "select 42 as answer union all select 43 as not_an_answer";

            var rowCount = await cmd.ExecuteNonQueryAsync(CancellationToken.None);
            Assert.Equal(2, rowCount);

            cmd.CommandText = "select 44";
            var value = cmd.ExecuteScalar();

            var byteValue = Assert.IsType<byte>(value);
            Assert.Equal<byte>(44, byteValue);
        }

        [Fact]
        public async Task Params()
        {
            await using var cn = await OpenConnectionAsync();
            await using var cmd = cn.CreateCommand();

            cmd.CommandText =
                "select {a:UUID} a, {b} b, {long_parameter_name} /*+{_}*/ c, {d} d, {e123_456_789e} e, {_} f, {g} g, '{e123_456_789e}' h, '/v\\\\'`/v\\\\`, \"{a}\" i--{g} should not be replaced" +
                Environment.NewLine +
                "from (select 'some real value' as `{a}`)";

            var now = DateTime.Now;
            now = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second, DateTimeKind.Local);
            var id = Guid.NewGuid();

            cmd.Parameters.Add(new ClickHouseParameter("{a}") { DbType = DbType.String, Value = id.ToString("D") });
            cmd.Parameters.Add(new ClickHouseParameter("{B}") { DbType = DbType.Guid, Value = DBNull.Value });
            cmd.Parameters.Add(new ClickHouseParameter("LONG_parameter_NAME") { DbType = DbType.Int32 });
            cmd.Parameters.Add(new ClickHouseParameter("D") { Value = 42m });
            cmd.Parameters.Add(new ClickHouseParameter("{e123_456_789E}") { Value = "e123_456_789e" });
            cmd.Parameters.Add(new ClickHouseParameter("_"));
            cmd.Parameters.Add(new ClickHouseParameter("g") { Value = now });

            await using var reader = cmd.ExecuteReader();
            Assert.True(await reader.ReadAsync());

            Assert.Equal(id, reader.GetGuid(reader.GetOrdinal("a")));
            Assert.True(reader.IsDBNull(reader.GetOrdinal("b")));
            Assert.True(reader.IsDBNull(reader.GetOrdinal("c")));
            Assert.Equal(42m, reader.GetDecimal(reader.GetOrdinal("d")));
            Assert.Equal("e123_456_789e", reader.GetString(reader.GetOrdinal("e")));
            Assert.True(reader.IsDBNull(reader.GetOrdinal("f")));
            Assert.Equal(now, reader.GetDateTime(reader.GetOrdinal("g")));
            Assert.Equal("{e123_456_789e}", reader.GetString(reader.GetOrdinal("h")));
            Assert.Equal("some real value", reader.GetString(reader.GetOrdinal("i")));
            Assert.Equal(@"/v\", reader.GetString(reader.GetOrdinal(@"/v\")));

            Assert.False(await reader.ReadAsync());
        }

        [Fact]
        public async Task MsSqlLikeParams()
        {
            await using var cn = await OpenConnectionAsync();
            await using var cmd = cn.CreateCommand();

            cmd.CommandText =
                "select cast(@a as UUID) a, @b b, @long_parameter_name /*+@_*/ c, @d d, {e123_456_789e} e, @_ f, {g} g, '@e123_456_789e' h, '/v\\\\\'`/v\\\\`, \"@a\" i--@g should not be replaced" +
                Environment.NewLine +
                "from (select 'some real value' as `@a`)";

            var now = DateTime.Now;
            now = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second, DateTimeKind.Local);
            var id = Guid.NewGuid();

            cmd.Parameters.Add(new ClickHouseParameter("{a}") { DbType = DbType.String, Value = id.ToString("D") });
            cmd.Parameters.Add(new ClickHouseParameter("@B") { DbType = DbType.Guid, Value = DBNull.Value });
            cmd.Parameters.Add(new ClickHouseParameter("@LONG_parameter_NAME") { DbType = DbType.Int32 });
            cmd.Parameters.Add(new ClickHouseParameter("@D") { Value = 42m });
            cmd.Parameters.Add(new ClickHouseParameter("e123_456_789E") { Value = "e123_456_789e" });
            cmd.Parameters.Add(new ClickHouseParameter("@_"));
            cmd.Parameters.Add(new ClickHouseParameter("g") { Value = now });

            await using var reader = cmd.ExecuteReader();
            Assert.True(await reader.ReadAsync());

            Assert.Equal(id, reader.GetGuid(reader.GetOrdinal("a")));
            Assert.True(reader.IsDBNull(reader.GetOrdinal("b")));
            Assert.True(reader.IsDBNull(reader.GetOrdinal("c")));
            Assert.Equal(42m, reader.GetDecimal(reader.GetOrdinal("d")));
            Assert.Equal("e123_456_789e", reader.GetString(reader.GetOrdinal("e")));
            Assert.True(reader.IsDBNull(reader.GetOrdinal("f")));
            Assert.Equal(now, reader.GetDateTime(reader.GetOrdinal("g")));
            Assert.Equal("@e123_456_789e", reader.GetString(reader.GetOrdinal("h")));
            Assert.Equal("some real value", reader.GetString(reader.GetOrdinal("i")));
            Assert.Equal(@"/v\", reader.GetString(reader.GetOrdinal(@"/v\")));

            Assert.False(await reader.ReadAsync());
        }

        [Fact]
        public async Task EmptyStringParam()
        {
            const string query = @"select {url}, {data}";

            await using var cn = await OpenConnectionAsync();
            await using var cmd = cn.CreateCommand(query);

            cmd.Parameters.AddWithValue("url", "");
            cmd.Parameters.AddWithValue("data", "{\"value\":107}");
            await using var reader = await cmd.ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());

            Assert.Equal((string)cmd.Parameters["url"].Value!, reader.GetString(0));
            Assert.Equal((string)cmd.Parameters["data"].Value!, reader.GetString(1));

            Assert.False(await reader.ReadAsync());
        }

        [Fact]
        public async Task TypedParams()
        {
            await using var cn = await OpenConnectionAsync();

            await using var cmd = cn.CreateCommand();

            cmd.CommandText = "SELECT {param:UInt32} intVal, {PaRaM:Decimal128(9)} decVal, {pArAm:String} strVal";

            cmd.Parameters.Add(new ClickHouseParameter("{Param}") { Value = "42" });

            await using var reader = cmd.ExecuteReader();
            Assert.True(await reader.ReadAsync());

            Assert.Equal<uint>(42, reader.GetFieldValue<uint>(0));
            Assert.Equal(42, reader.GetDecimal(1));
            Assert.Equal("42", reader.GetString(2));

            Assert.False(await reader.ReadAsync());
        }

        [Fact]
        public async Task UseConnectionInParallel()
        {
            await using var cn = await OpenConnectionAsync();

            var res = await Task.WhenAll(Sleep(cn, 1), Sleep(cn, 2), Sleep(cn, 3));

            Assert.Equal(new[] { 1, 2, 3 }, res);

            async Task<int> Sleep(ClickHouseConnection connection, int value)
            {
                await using var cmd = connection.CreateCommand(string.Format(CultureInfo.InvariantCulture, "SELECT sleep(0.2)+{0}", value));
                return await cmd.ExecuteScalarAsync<int>();
            }
        }

        [Fact]
        public async Task ReadScalarFromLargeResultSet()
        {
            await using var cn = await OpenConnectionAsync();

            await using var cmd = cn.CreateCommand("SELECT number+42 FROM system.numbers");

            var cts = new CancellationTokenSource();

            var expectedTask = cmd.ExecuteScalarAsync<ulong>(cts.Token);
            var task = await Task.WhenAny(expectedTask, Task.Delay(2000, CancellationToken.None));

            if (!ReferenceEquals(expectedTask, task))
                cts.Cancel(true);

            var value = await expectedTask;
            Assert.Equal<ulong>(42, value);
        }

        [Fact]
        public async Task UseCommandInParallel()
        {
            await using var cn = await OpenConnectionAsync();

            await using var cmd = cn.CreateCommand("SELECT sleep(0.2) + 42");

            var result = await Task.WhenAll(cmd.ExecuteScalarAsync<uint>(), cmd.ExecuteScalarAsync<uint>(), cmd.ExecuteScalarAsync<uint>());
            Assert.Equal(new uint[] { 42, 42, 42 }, result);
        }

#if DEBUG
        [Fact]
        public async Task CancelOnSocketTimeout()
        {
            // Attempt to emulate a network error. It doesn't work sometimes because ClickHouse server sends Progress messages

            var csb = new ClickHouseConnectionStringBuilder(GetDefaultConnectionSettings()) { ReadWriteTimeout = 25 };
            await using (var cn = new ClickHouseConnection(csb))
            {
                await cn.OpenAsync();
                await using var cmd = cn.CreateCommand("SELECT sleep(3)");

                var ioEx = Assert.Throws<IOException>(() => cmd.ExecuteNonQuery());
                var socketEx = Assert.IsType<SocketException>(ioEx.InnerException);
                Assert.Equal(SocketError.TimedOut, socketEx.SocketErrorCode);
            }
        }
#endif

        [Fact]
        public async Task CancelOnCommandTimeout()
        {
            var csb = new ClickHouseConnectionStringBuilder(GetDefaultConnectionSettings()) { CommandTimeout = 2 };
            await using (var cn = new ClickHouseConnection(csb))
            {
                await cn.OpenAsync();
                await using var cmd = cn.CreateCommand("SELECT sleep(3)");

                Assert.Throws<OperationCanceledException>(() => cmd.ExecuteNonQuery());
            }
        }

        [Fact]
        public async Task CancelOnTokenTimeout()
        {
            await using (var cn = await OpenConnectionAsync())
            {
                await using var cmd = cn.CreateCommand("SELECT sleep(3)");

                var tokenSource = new CancellationTokenSource();
                tokenSource.CancelAfter(500);
                var ex = await Assert.ThrowsAsync<OperationCanceledException>(() => cmd.ExecuteNonQueryAsync(tokenSource.Token));
                Assert.Equal(tokenSource.Token, ex.CancellationToken);
            }
        }

        [Fact]
        public void ShouldUnwrapCommandExecuteScalar()
        {
            using var conn = OpenConnection();
            Assert.Throws<ClickHouseServerException>(() => conn.CreateCommand("select nothing").ExecuteScalar());
        }

        [Fact]
        public async Task ExecuteScalarShouldReturnSingleResult()
        {
            await using var connection = await OpenConnectionAsync();

            await using var cmdDrop = connection.CreateCommand("select 2+2");

            var result = Convert.ToInt32(await cmdDrop.ExecuteScalarAsync());
            Assert.Equal(4, result);
        }

        [Fact]
        public async Task CommandBehaviorCloseConnection()
        {
            await using var cn = await OpenConnectionAsync();

            await using var cmd = cn.CreateCommand("SELECT * FROM system.numbers LIMIT 100");
            await using (var reader = await cmd.ExecuteReaderAsync(CommandBehavior.CloseConnection))
            {
                Assert.True(await reader.ReadAsync());
                Assert.Equal(ConnectionState.Open, cn.State);
            }

            Assert.Equal(ConnectionState.Closed, cn.State);

            await cn.OpenAsync();
            await using (var reader = await cmd.ExecuteReaderAsync(CommandBehavior.CloseConnection))
            {
                Assert.True(await reader.ReadAsync());
                Assert.Equal(ConnectionState.Open, cn.State);

                await reader.CloseAsync();
                Assert.Equal(ConnectionState.Closed, cn.State);
            }

            await cn.OpenAsync();
            await using (var reader = await cmd.ExecuteReaderAsync(CommandBehavior.CloseConnection))
            {
                int count = 0;
                while (await reader.ReadAsync())
                {
                    Assert.Equal(ConnectionState.Open, cn.State);
                    ++count;
                }

                Assert.Equal(100, count);
                Assert.Equal(ConnectionState.Closed, cn.State);

                Assert.False(await reader.ReadAsync());
            }

            cmd.CommandText = "select x, sum(y) as v from (SELECT number%3 + 1 as x, number as y FROM numbers(10)) group by x with totals;";
            await cn.OpenAsync();

            await using (var reader = await cmd.ExecuteReaderAsync(CommandBehavior.CloseConnection))
            {
                int count = 0;
                while (await reader.ReadAsync())
                {
                    Assert.Equal(ConnectionState.Open, cn.State);
                    ++count;
                }

                Assert.Equal(3, count);
                Assert.Equal(ConnectionState.Open, cn.State);

                Assert.True(await reader.NextResultAsync());
                Assert.True(await reader.ReadAsync());
                Assert.Equal(ConnectionState.Open, cn.State);
                Assert.False(await reader.ReadAsync());
                Assert.Equal(ConnectionState.Closed, cn.State);

                Assert.False(await reader.ReadAsync());
                Assert.False(await reader.NextResultAsync());
            }

            await cn.OpenAsync();

            cmd.CommandText = "It IS NOT a query...";
            await Assert.ThrowsAsync<ClickHouseServerException>(() => cmd.ExecuteReaderAsync(CommandBehavior.CloseConnection));
            Assert.Equal(ConnectionState.Closed, cn.State);
        }

        [Fact]
        public async Task TableParameterSingleColumn()
        {
            await using var cn = await OpenConnectionAsync();

            var cmd = cn.CreateCommand("SELECT toInt32(number) FROM numbers(100000) WHERE number IN param_table");
            
            var tableProvider = new ClickHouseTableProvider("param_table", 100);
            tableProvider.AddColumn(Enumerable.Range(500, int.MaxValue / 2));

            cmd.TableProviders.Add(tableProvider);

            var expectedValues = new HashSet<int>(Enumerable.Range(500, 100));
            await using var reader = await cmd.ExecuteReaderAsync();

            while(await reader.ReadAsync())
            {
                var value = reader.GetInt32(0);
                Assert.True(expectedValues.Remove(value));
            }

            Assert.Empty(expectedValues);
        }

        [Fact]
        public async Task TableParameterAndScalarParameter()
        {
            await using var cn = await OpenConnectionAsync();

            var cmd = cn.CreateCommand("SELECT toInt32(number) FROM numbers(100000) WHERE number IN (SELECT {val}*val FROM param_table)");

            var tableProvider = new ClickHouseTableProvider("param_table", 100);
            tableProvider.AddColumn("val", Enumerable.Range(500, int.MaxValue / 2));

            cmd.TableProviders.Add(tableProvider);

            cmd.Parameters.AddWithValue("val", 3);

            var expectedValues = new HashSet<int>(Enumerable.Range(500, 100).Select(v => v * 3));
            await using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var value = reader.GetInt32(0);
                Assert.True(expectedValues.Remove(value));
            }

            Assert.Empty(expectedValues);
        }

        [Fact]
        public async Task MultipleTableParameters()
        {
            await using var cn = await OpenConnectionAsync();

            var cmd = cn.CreateCommand("SELECT T2.id AS id, T1.id AS user_id, T1.value AS user, T2.value AS ip FROM q_user AS T1 LEFT JOIN q_addr AS T2 ON T1.id = T2.user_id WHERE T2.id%{param} != 0 ORDER BY T2.id");

            var users = new (int id, string name)[] { (1, "user1"), (2, "user2"), (3, "admin1"), (4, "admin2") };
            var addr = new (int id, string? ip)[] { (1, "8.8.8.8"), (2, "9.9.9.9"), (3, null), (4, null), (3, "127.0.0.1"), (4, "127.0.0.1"), (1, "2001:0db8:0000:0000:0000:ff00:0042:8329"), (4, "::ffff:192.0.2.1"), (2, "fe80::883b:771b:71c6:7c31") };

            var usersTable = new ClickHouseTableProvider("q_user", users.Length);
            usersTable.AddColumn("id", users.Select(u => u.id));
            usersTable.AddColumn("value", users.Select(u => u.name));

            cmd.TableProviders.Add(usersTable);

            var addrTable = new ClickHouseTableProvider("q_addr", addr.Length);
            addrTable.AddColumn("id", Enumerable.Range(0, addr.Length));
            addrTable.AddColumn("user_id", addr.Select(a => a.id));
            var ipColumn = addrTable.AddColumn("value", addr.Select(a => a.ip));
            ipColumn.ClickHouseDbType = ClickHouseDbType.IpV6;
            ipColumn.IsNullable = true;

            cmd.TableProviders.Add(addrTable);

            cmd.Parameters.AddWithValue("param", 7);

            await using var reader = await cmd.ExecuteReaderAsync();
            int expectedId = 1;
            while(await reader.ReadAsync())
            {
                var id = reader.GetInt32(0);
                Assert.Equal(expectedId, id);

                var userId = reader.GetInt32(1);
                var user = reader.GetString(2);
                var ip = reader.GetFieldValue<IPAddress>(3, null);

                Assert.Equal(addr[expectedId].id, userId);
                var expectedUser = users.Single(u => u.id == userId).name;
                Assert.Equal(expectedUser, user);

                var expectedIp = addr[expectedId].ip == null ? null : IPAddress.Parse(addr[expectedId].ip).MapToIPv6();
                Assert.Equal(expectedIp, ip);

                if (++expectedId % 7 == 0)
                    ++expectedId;
            }

            Assert.Equal(addr.Length, expectedId);
        }
    }
}
