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
using System.Globalization;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Octonica.ClickHouseClient.Exceptions;
using Xunit;

namespace Octonica.ClickHouseClient.Tests
{
    public class TransportTests: ClickHouseTestsBase
    {
        [Fact]
        public async Task SimpleExecuteScalar()
        {
            await using var cn = await OpenConnectionAsync();

            await using var cmd = cn.CreateCommand();
            cmd.CommandText = "select 42";
            var value = await cmd.ExecuteScalarAsync(CancellationToken.None);
            Assert.Equal((byte) 42, value);
        }

        [Fact]
        public async Task SimpleReader()
        {
            await using var cn = await OpenConnectionAsync();

            await using var cmd = cn.CreateCommand();
            cmd.CommandText = "select 42 as answer";

            await using var reader = await cmd.ExecuteReaderAsync(CancellationToken.None);
            var columnIndex = reader.GetOrdinal("answer");
            Assert.True(await reader.ReadAsync(CancellationToken.None));
            var value = reader.GetByte(columnIndex);
            Assert.False(await reader.ReadAsync());

            Assert.Equal<byte>(42, value);
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
        public async Task ParamsTest()
        {
            await using var cn = await OpenConnectionAsync();
            await using var cmd = cn.CreateCommand();

            cmd.CommandText =
                "select {a:UUID} a, {b} b, {long_parameter_name} /*+{_}*/ c, {d} d, {e123_456_789e} e, {_} f, {g} g, '{e123_456_789e}' h, \"{a}\" i--{g} should not be replaced" +
                Environment.NewLine +
                "from (select 'some real value' as `{a}`)";

            var now = DateTime.Now;
            now = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second, DateTimeKind.Local);
            var id = Guid.NewGuid();

            cmd.Parameters.Add(new ClickHouseParameter("{a}") {DbType = DbType.String, Value = id.ToString("D")});
            cmd.Parameters.Add(new ClickHouseParameter("{B}") {DbType = DbType.Guid, Value = DBNull.Value});
            cmd.Parameters.Add(new ClickHouseParameter("LONG_parameter_NAME") {DbType = DbType.Int32});
            cmd.Parameters.Add(new ClickHouseParameter("D") {Value = 42m});
            cmd.Parameters.Add(new ClickHouseParameter("{e123_456_789E}") {Value = "e123_456_789e"});
            cmd.Parameters.Add(new ClickHouseParameter("_"));
            cmd.Parameters.Add(new ClickHouseParameter("g") {Value = now});

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

            Assert.False(await reader.ReadAsync());
        }

        [Fact]
        public async Task EmptyStringParamTest()
        {
            const string query = @"select {url}, {data}";

            await using var cn = await OpenConnectionAsync();
            await using var cmd = cn.CreateCommand(query);

            cmd.Parameters.AddWithValue("url", "");
            cmd.Parameters.AddWithValue("data", "{\"value\":107}");
            await using var reader = await cmd.ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());

            Assert.Equal((string) cmd.Parameters["url"].Value!, reader.GetString(0));
            Assert.Equal((string) cmd.Parameters["data"].Value!, reader.GetString(1));

            Assert.False(await reader.ReadAsync());
        }

        [Fact]
        public async Task TypedParamsTest()
        {
            await using var cn = await OpenConnectionAsync();

            await using var cmd = cn.CreateCommand();

            cmd.CommandText = "SELECT {param:UInt32} intVal, {PaRaM:Decimal128(9)} decVal, {pArAm:String} strVal";

            cmd.Parameters.Add(new ClickHouseParameter("{Param}") {Value = "42"});

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

            Assert.Equal(new[] {1, 2, 3}, res);

            async Task<int> Sleep(ClickHouseConnection connection, int value)
            {
                await using var cmd = connection.CreateCommand(string.Format(CultureInfo.InvariantCulture, "SELECT sleep(0.2)+{0}", value));
                return await cmd.ExecuteScalarAsync<int>();
            }
        }

        [Fact]
        public async Task CloseReaderWithoutReading()
        {
            await using var cn = await OpenConnectionAsync();

            await using (var cmd = cn.CreateCommand("SELECT * FROM system.numbers"))
            {
                await using (var reader = await cmd.ExecuteReaderAsync())
                {
                    ulong value = 0;
                    while (value < 100 && await reader.ReadAsync())
                        value = reader.GetFieldValue<ulong>(0);

                    Assert.Equal<ulong>(100, value);
                }

                await using var reader2 = await cmd.ExecuteReaderAsync(CancellationToken.None);
                Assert.True(await reader2.ReadAsync());

                var value2 = reader2.GetFieldValue<ulong>(0);
                Assert.Equal<ulong>(0, value2);
            }

            await using var cmd2 = cn.CreateCommand("SELECT * FROM system.one");
            
            await (await cmd2.ExecuteReaderAsync()).DisposeAsync();

            cmd2.CommandText = "SELECT * FROM system.numbers LIMIT 10000 OFFSET 42";

            await using var reader3 = await cmd2.ExecuteReaderAsync();
            Assert.True(await reader3.ReadAsync());
            var value3 = reader3.GetFieldValue<ulong>(0);

            Assert.Equal<ulong>(42, value3);
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
            Assert.Equal(new uint[] {42, 42, 42}, result);
        }

#if DEBUG
        [Fact]
        public async Task CancelOnSocketTimeout()
        {
            // Attempt to emulate a network error. It doesn't work sometimes because ClickHouse server sends Progress messages

            var csb = new ClickHouseConnectionStringBuilder(GetDefaultConnectionSettings()) {ReadWriteTimeout = 25};
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
        public void ShouldUnwrapConnectionOpenExceptions()
        {
            var sb = new ClickHouseConnectionStringBuilder {Host = "none.example.com"};
            using var conn = new ClickHouseConnection(sb);
            var exception = Assert.ThrowsAny<SocketException>(() => conn.Open());
            Assert.Equal(SocketError.HostNotFound, exception.SocketErrorCode);
        }

        [Fact]
        public void ShouldUnwrapCommandExecuteScalar()
        {
            using var conn = OpenConnection();
            Assert.Throws<ClickHouseServerException>(() => conn.CreateCommand("select nothing").ExecuteScalar());
        }

        [Fact]
        public async Task CanConnectWithUserAndPassword()
        {
            var settings = GetDefaultConnectionSettings();
            settings = new ClickHouseConnectionStringBuilder(settings) {Database = null}.BuildSettings();

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
        public async Task ExecuteScalarShouldReturnSingleResult()
        {
            await using var connection = await OpenConnectionAsync();

            await using var cmdDrop = connection.CreateCommand("select 2+2");

            var result = Convert.ToInt32(await cmdDrop.ExecuteScalarAsync());
            Assert.Equal(4, result);
        }

        [Fact]
        public async Task TotalsWithNextResult()
        {
            await using var cn = await OpenConnectionAsync();

            await using var cmd = cn.CreateCommand();
            cmd.CommandText = "select x, sum(y) as v from (SELECT number%2 + 1 as x, number as y FROM numbers(10)) group by x with totals;";

            using var reader = await cmd.ExecuteReaderAsync();
            Assert.Equal(ClickHouseDataReaderState.Data, reader.State);

            ulong rowsTotal = 0;
            while (reader.Read())
            {
                Assert.Equal(ClickHouseDataReaderState.Data, reader.State);
                rowsTotal += reader.GetFieldValue<ulong>(1);
            }

            Assert.Equal(ClickHouseDataReaderState.NextResultPending, reader.State);
            var hasTotals = reader.NextResult();
            Assert.True(hasTotals);
            Assert.Equal(ClickHouseDataReaderState.Totals, reader.State);

            Assert.True(reader.Read());

            Assert.Equal(ClickHouseDataReaderState.Totals, reader.State);
            var total = reader.GetFieldValue<ulong>(1);
            Assert.Equal(rowsTotal, total);

            Assert.False(reader.Read());

            Assert.Equal(ClickHouseDataReaderState.Closed, reader.State);
        }

        [Fact]
        public async Task ExtremesWithNextResult()
        {
            await using var cn = await OpenConnectionAsync();

            await using var cmd = cn.CreateCommand();
            cmd.Extremes = true;
            cmd.CommandText = "select x, sum(y) as v from (SELECT number%2 + 1 as x, number as y FROM numbers(10)) group by x;";

            using var reader = await cmd.ExecuteReaderAsync();
            Assert.Equal(ClickHouseDataReaderState.Data, reader.State);

            ulong minX = ulong.MaxValue, maxX = ulong.MinValue, minSum = ulong.MaxValue, maxSum = ulong.MinValue;
            while (reader.Read())
            {
                Assert.Equal(ClickHouseDataReaderState.Data, reader.State);
                var x = reader.GetFieldValue<ulong>(0);
                var sum = reader.GetFieldValue<ulong>(1);

                minX = Math.Min(minX, x);
                maxX = Math.Max(maxX, x);

                minSum = Math.Min(minSum, sum);
                maxSum = Math.Max(maxSum, sum);
            }

            Assert.Equal(ClickHouseDataReaderState.NextResultPending, reader.State);

            Assert.True(reader.NextResult());

            Assert.True(reader.Read());
            var extremeX = reader.GetFieldValue<ulong>(0);
            var extremeSum = reader.GetFieldValue<ulong>(1);
            Assert.Equal(minX, extremeX);
            Assert.Equal(minSum, extremeSum);

            Assert.True(reader.Read());
            extremeX = reader.GetFieldValue<ulong>(0);
            extremeSum = reader.GetFieldValue<ulong>(1);
            Assert.Equal(maxX, extremeX);
            Assert.Equal(maxSum, extremeSum);

            Assert.False(reader.Read());

            Assert.Equal(ClickHouseDataReaderState.Closed, reader.State);
        }

        [Fact]
        public async Task TotalsAndExtremes()
        {
            await using var cn = await OpenConnectionAsync();

            await using var cmd = cn.CreateCommand();
            cmd.Extremes = true;
            cmd.CommandText = "select x, sum(y) as v from (SELECT number%2 + 1 as x, number as y FROM numbers(10)) group by x with totals;";

            using var reader = await cmd.ExecuteReaderAsync();
            Assert.Equal(ClickHouseDataReaderState.Data, reader.State);

            ulong rowsTotal = 0;
            ulong minX = ulong.MaxValue, maxX = ulong.MinValue, minSum = ulong.MaxValue, maxSum = ulong.MinValue;
            while (reader.Read())
            {
                Assert.Equal(ClickHouseDataReaderState.Data, reader.State);
                var x = reader.GetFieldValue<ulong>(0);
                var sum = reader.GetFieldValue<ulong>(1);
                
                rowsTotal += sum;

                minX = Math.Min(minX, x);
                maxX = Math.Max(maxX, x);

                minSum = Math.Min(minSum, sum);
                maxSum = Math.Max(maxSum, sum);
            }

            Assert.Equal(ClickHouseDataReaderState.NextResultPending, reader.State);
            var hasTotals = reader.NextResult();
            Assert.True(hasTotals);
            Assert.Equal(ClickHouseDataReaderState.Totals, reader.State);

            Assert.True(reader.Read());

            Assert.Equal(ClickHouseDataReaderState.Totals, reader.State);
            var total = reader.GetFieldValue<ulong>(1);
            Assert.Equal(rowsTotal, total);

            Assert.False(reader.Read());

            Assert.Equal(ClickHouseDataReaderState.NextResultPending, reader.State);

            Assert.True(reader.NextResult());
            
            Assert.True(reader.Read());
            var extremeX = reader.GetFieldValue<ulong>(0);
            var extremeSum = reader.GetFieldValue<ulong>(1);
            Assert.Equal(minX, extremeX);
            Assert.Equal(minSum, extremeSum);

            Assert.True(reader.Read());
            extremeX = reader.GetFieldValue<ulong>(0);
            extremeSum = reader.GetFieldValue<ulong>(1);
            Assert.Equal(maxX, extremeX);
            Assert.Equal(maxSum, extremeSum);

            Assert.False(reader.Read());

            Assert.Equal(ClickHouseDataReaderState.Closed, reader.State);
        }

        [Theory]
        [InlineData(ClickHouseDataReaderState.Data, 3)]
        [InlineData(ClickHouseDataReaderState.Extremes, 2)]
        [InlineData(ClickHouseDataReaderState.Totals, 1)]
        public async Task SkipNextResult(ClickHouseDataReaderState readBlock, int expectedCount)
        {
            await using var cn = await OpenConnectionAsync();

            await using var cmd = cn.CreateCommand();
            cmd.Extremes = true;
            cmd.CommandText = "select x, sum(y) as v from (SELECT number%3 + 1 as x, number as y FROM numbers(10)) group by x with totals;";

            await using var reader = await cmd.ExecuteReaderAsync();
            Assert.Equal(ClickHouseDataReaderState.Data, reader.State);

            do
            {
                switch (reader.State)
                {
                    case ClickHouseDataReaderState.Data:
                    case ClickHouseDataReaderState.Totals:
                    case ClickHouseDataReaderState.Extremes:
                        if (reader.State == readBlock)
                            break;

                        continue;

                    default:
                        Assert.True(false, $"Unexpected state: {reader.State}.");
                        break;
                }

                int count = 0;
                while (await reader.ReadAsync())
                    ++count;

                Assert.False(await reader.ReadAsync());
                Assert.Equal(expectedCount, count);
                Assert.True(reader.State == ClickHouseDataReaderState.NextResultPending || reader.State == ClickHouseDataReaderState.Closed);

            } while (await reader.NextResultAsync());

            Assert.Equal(ClickHouseDataReaderState.Closed, reader.State);
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
        public async Task CommandBehaviorSchemaOnly()
        {
            await using var cn = await OpenConnectionAsync();

            await using var cmd = cn.CreateCommand("SELECT * FROM system.numbers");
            await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);

            Assert.False(await reader.ReadAsync());
            Assert.False(await reader.ReadAsync());
            Assert.False(await reader.NextResultAsync());
            Assert.False(await reader.ReadAsync());

            Assert.Equal(1, reader.FieldCount);
        }

        [Fact]
        public async Task CommandBehaviorSingleRow()
        {
            await using var cn = await OpenConnectionAsync();

            await using var cmd = cn.CreateCommand("SELECT * FROM system.numbers");
            await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SingleRow);

            Assert.Equal(1, reader.FieldCount);
            Assert.True(await reader.ReadAsync());

            Assert.False(await reader.ReadAsync());
            Assert.False(await reader.ReadAsync());
            Assert.False(await reader.NextResultAsync());
            Assert.False(await reader.ReadAsync());
        }

        [Fact]
        public async Task CommandBehaviorSingleResult()
        {
            await using var cn = await OpenConnectionAsync();

            await using var cmd = cn.CreateCommand("select x, sum(y) as v from (SELECT number%7 + 1 as x, number as y FROM numbers(100)) group by x with totals;");
            cmd.Extremes = true;

            await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SingleResult);

            int count = 0;
            while (await reader.ReadAsync())
                ++count;

            Assert.False(reader.Read());
            Assert.NotEqual(ClickHouseDataReaderState.NextResultPending, reader.State);

            Assert.False(reader.NextResult());
            Assert.False(reader.Read());
        }

        [Fact]
        public async Task CommandBehaviorSequentialAccess()
        {
            // SequentialAccess is ignored

            await using var cn = await OpenConnectionAsync();
            await using var cmd = cn.CreateCommand("SELECT 41,42,43");
            await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess);

            Assert.True(await reader.ReadAsync());

            Assert.Equal(41, reader.GetInt32(0));
            Assert.Equal(42, reader.GetInt32(1));
            Assert.Equal(43, reader.GetInt32(2));

            Assert.False(await reader.ReadAsync());
        }
        
        [Fact]
        public async Task CommandBehaviorKeyInfo()
        {
            // KeyInfo is not supported

            await using var cn = await OpenConnectionAsync();
            await using var cmd = cn.CreateCommand("SELECT 42");
            var ex = await Assert.ThrowsAsync<ArgumentException>(() => cmd.ExecuteReaderAsync(CommandBehavior.KeyInfo));
            Assert.Equal("behavior", ex.ParamName);
        }
    }
}
