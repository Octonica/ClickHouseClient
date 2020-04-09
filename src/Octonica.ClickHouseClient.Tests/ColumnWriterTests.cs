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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Octonica.ClickHouseClient.Protocol;
using Xunit;

namespace Octonica.ClickHouseClient.Tests
{
    public class ColumnWriterTests : ClickHouseTestsBase, IClassFixture<ColumnWriterTests.TableFixture>, IClassFixture<EncodingFixture>
    {
        private const string TestTableName = "stream_insert_test";

        [Fact]
        public async Task InsertValues()
        {
            await using var con = await OpenConnectionAsync();

            await using (var writer = await con.CreateColumnWriterAsync($"INSERT INTO {TestTableName} VALUES", CancellationToken.None))
            {
                var columns = new object?[writer.FieldCount];
                columns[writer.GetOrdinal("id")] = new[] {10000, 10001};
                columns[writer.GetOrdinal("num")] = new List<decimal?> {49999.99m, -999999.99999m};

                await writer.WriteTableAsync(columns, 2, CancellationToken.None);
            }

            var cmd = con.CreateCommand($"SELECT id, str, num FROM {TestTableName} WHERE id>=10000 AND id<20000");
            int count = 0;
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var id = reader.GetInt32(0);
                    var str = reader.GetFieldValue(1, "NULL");
                    var num = reader.GetDecimal(2);

                    Assert.Equal("NULL", str);
                    switch (id - 10000)
                    {
                        case 0:
                            Assert.Equal(49999.99m, num);
                            break;
                        case 1:
                            Assert.Equal(-999999.99999m, num);
                            break;
                        default:
                            Assert.False(true, $"Unexpected id: {id}");
                            break;
                    }

                    ++count;
                }
            }

            Assert.Equal(2, count);
        }

        [Fact]
        public async Task InsertValuesFromGeneratedColumns()
        {
            await using var con = await OpenConnectionAsync();

            await using (var writer = await con.CreateColumnWriterAsync($"INSERT INTO {TestTableName} VALUES", CancellationToken.None))
            {
                var columns = new Dictionary<string, object>
                {
                    ["id"] = Enumerable.Range(20_000, int.MaxValue - 20_000),
                    ["str"] = Enumerable.Range(0, int.MaxValue).Select(i => i % 3 == 0 ? i % 5 == 0 ? "FizzBuzz" : "Fizz" : i % 5 == 0 ? "Buzz" : i.ToString())
                };

                await writer.WriteTableAsync(columns, 100, CancellationToken.None);
            }

            var cmd = con.CreateCommand($"SELECT id, str, num FROM {TestTableName} WHERE id>=20000 AND id<30000");
            int count = 0;
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var id = reader.GetInt32(0);
                    var str = reader.GetString(1);
                    var num = reader.GetFieldValue<decimal>(2, null);
                    Assert.Null(num);

                    var i = id - 20_000;
                    switch (str)
                    {
                        case "Fizz":
                            Assert.True(i % 3 == 0);
                            break;

                        case "Buzz":
                            Assert.True(i % 5 == 0);
                            break;

                        case "FizzBuzz":
                            Assert.True(i % 3 == 0);
                            Assert.True(i % 5 == 0);
                            break;

                        default:
                            Assert.NotNull(str);
                            Assert.True(int.TryParse(str, out var parsedStr));
                            Assert.Equal(i, parsedStr);
                            break;
                    }

                    ++count;
                }
            }

            Assert.Equal(100, count);
        }

        [Fact]
        public async Task InsertValuesFromAsyncEnumerableColumn()
        {
            await using var con = await OpenConnectionAsync();

            await using (var writer = await con.CreateColumnWriterAsync($"INSERT INTO {TestTableName} VALUES", CancellationToken.None))
            {
                var columns = new object?[writer.FieldCount];
                columns[writer.GetOrdinal("id")] = Enumerable.Range(30_000, int.MaxValue - 30_000);
                columns[writer.GetOrdinal("num")] = new AsyncTestFibSequence();

                await writer.WriteTableAsync(columns, 63, CancellationToken.None);
            }

            var cmd = con.CreateCommand($"SELECT id, str, num FROM {TestTableName} WHERE id>=30000 AND id<40000 ORDER BY id");
            int count = 0;
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                decimal? previous = null, current = null;
                while (await reader.ReadAsync())
                {
                    var id = reader.GetInt32(0);
                    var str = reader.GetString(1, null);
                    var num = reader.GetFieldValue<decimal>(2, null);
                    Assert.Null(str);

                    var i = id - 30_000;
                    Assert.Equal(count, i);

                    if (current == null)
                        current = 1;
                    else if (previous == null)
                        previous = 1;
                    else
                    {
                        var next = previous + current;
                        previous = current;
                        current = next;
                    }

                    Assert.Equal(current, num);
                    ++count;
                }
            }

            Assert.Equal(63, count);
        }

        [Fact]
        public async Task InsertStringsWithEncoding()
        {
            await using var con = await OpenConnectionAsync();

            var values = new List<string>
                {"ноль", "один", "два", "три", "четыре", "пять", "шесть", "семь", "восемь", "девять", "десять", "одиннадцать", "двенадцать", "тринадцать", "четырнадцать", "пятнадцать"};
            await using (var writer = await con.CreateColumnWriterAsync($"INSERT INTO {TestTableName} VALUES", CancellationToken.None))
            {
                writer.ConfigureColumn("str", new ClickHouseColumnSettings(Encoding.GetEncoding("UTF-7")));
                var columns = new object?[writer.FieldCount];
                columns[writer.GetOrdinal("id")] = Enumerable.Range(40_000, int.MaxValue - 40_000);
                columns[writer.GetOrdinal("str")] = values;

                await writer.WriteTableAsync(columns, values.Count, CancellationToken.None);
            }

            var cmd = con.CreateCommand($"SELECT CAST(id - 40000 AS Int32), convertCharset(str, 'UTF-7', 'cp1251'), num FROM {TestTableName} WHERE id>=40000 AND id<50000 ORDER BY id");
            int count = 0;
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                reader.ConfigureColumn(1, new ClickHouseColumnSettings(Encoding.GetEncoding("windows-1251")));

                while (await reader.ReadAsync())
                {
                    var id = reader.GetInt32(0);
                    var str = reader.GetString(1, null);
                    var num = reader.GetFieldValue<decimal>(2, null);
                    Assert.Null(num);

                    var originalValue = values[id];
                    Assert.Equal(originalValue, str);

                    ++count;
                }
            }

            Assert.Equal(values.Count, count);
        }

        [Fact]
        public async Task InsertArrayValues()
        {
            try
            {
                await using var cn = await OpenConnectionAsync();

                var cmd = cn.CreateCommand($"DROP TABLE IF EXISTS {TestTableName}_arr");
                cmd.ExecuteNonQuery();

                cmd = cn.CreateCommand($"CREATE TABLE {TestTableName}_arr(id Int32, arr Array(Nullable(String)), multi_arr Array(Array(Nullable(Decimal64(4))))) ENGINE=Memory");
                cmd.ExecuteNonQuery();

                var arr = new List<List<string?>>
                    {new List<string?> {"foo", null, "bar"}, new List<string?> {null, null, null, null}, new List<string?>(0), new List<string?> {"1", "2", "Lorem ipsum"}};
                var multiArr = new[]
                {
                    new List<decimal?[]>(0),
                    new List<decimal?[]>
                    {
                        new decimal?[] {1, 2, 3, null, 4, 5},
                        new decimal?[0],
                        new decimal?[] {null, null, 6, null}
                    },
                    new List<decimal?[]>
                    {
                        new decimal?[0]
                    },
                    new List<decimal?[]>
                    {
                        new decimal?[0],
                        new decimal?[0],
                        new decimal?[0],
                        new decimal?[] {7, 8, 9, 10}
                    }
                };

                await using (var writer = await cn.CreateColumnWriterAsync($"INSERT INTO {TestTableName}_arr VALUES", CancellationToken.None))
                {
                    var columns = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["ID"] = Enumerable.Range(0, int.MaxValue),
                        ["ARR"] = arr,
                        ["multi_ARR"] = multiArr
                    };

                    await writer.WriteTableAsync(columns, Math.Max(arr.Count, multiArr.Length), CancellationToken.None);

                    Assert.False(writer.IsClosed);
                    writer.EndWrite();
                    Assert.True(writer.IsClosed);
                }

                cmd = cn.CreateCommand($"SELECT id, multi_arr, arr FROM {TestTableName}_arr");
                int count = 0;
                await using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var id = reader.GetInt32(0);
                        var resultMultiArr = reader.GetFieldValue<decimal?[][]>(1);
                        var resultArr = reader.GetFieldValue<string?[]>(2);

                        var expectedMultiArr = multiArr[id];
                        Assert.Equal(expectedMultiArr.Count, resultMultiArr.Length);

                        for (int i = 0; i < expectedMultiArr.Count; i++)
                            Assert.Equal(expectedMultiArr[i], resultMultiArr[i]);

                        var expectedArr = arr[id];
                        Assert.Equal(expectedArr, resultArr);

                        ++count;
                    }
                }

                Assert.Equal(count, arr.Count);
            }
            finally
            {
                await using var cn = await OpenConnectionAsync();

                var cmd = cn.CreateCommand($"DROP TABLE IF EXISTS {TestTableName}_arr");
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public async Task InsertTupleValues()
        {
            var tuples = new[]
            {
                new Tuple<int, string?, ulong, decimal?, string, DateTime?, byte, Tuple<short?, ulong?, decimal>>(
                    0,
                    null,
                    4,
                    3434.35m,
                    "0123456",
                    null,
                    1,
                    new Tuple<short?, ulong?, decimal>(19, 3838838383, 53356.2343m)),

                new Tuple<int, string?, ulong, decimal?, string, DateTime?, byte, Tuple<short?, ulong?, decimal>>(
                    1,
                    "foo_bar",
                    534343434343434678,
                    8949679.5555m,
                    "5432106",
                    new DateTime(2020, 1, 1, 11, 11, 11),
                    255,
                    new Tuple<short?, ulong?, decimal>(-6598, 8984, 85760704949.4567m)),

                new Tuple<int, string?, ulong, decimal?, string, DateTime?, byte, Tuple<short?, ulong?, decimal>>(
                    2,
                    null,
                    0,
                    34987134.35m,
                    "6543210",
                    null,
                    128,
                    new Tuple<short?, ulong?, decimal>(-19, 38383, 0.2343m)),

                new Tuple<int, string?, ulong, decimal?, string, DateTime?, byte, Tuple<short?, ulong?, decimal>>(
                    3,
                    "one more value",
                    42,
                    -0.6548m,
                    "0246135",
                    new DateTime(2019, 12, 31, 23, 59, 59),
                    15,
                    new Tuple<short?, ulong?, decimal>(null, null, -1234.567m)),

                new Tuple<int, string?, ulong, decimal?, string, DateTime?, byte, Tuple<short?, ulong?, decimal>>(
                    4,
                    null,
                    ulong.MinValue,
                    null,
                    "1352460",
                    null,
                    99,
                    new Tuple<short?, ulong?, decimal>(null, null, -0.0001m)),

                new Tuple<int, string?, ulong, decimal?, string, DateTime?, byte, Tuple<short?, ulong?, decimal>>(
                    5,
                    "Five!",
                    ulong.MaxValue,
                    1_000_000.0001m,
                    "2350146",
                    null,
                    127,
                    new Tuple<short?, ulong?, decimal>(null, 2, -0.0042m))
            };

            try
            {
                await using var cn = new ClickHouseConnection(GetDefaultConnectionSettings());
                cn.Open();

                var cmd = cn.CreateCommand($"DROP TABLE IF EXISTS {TestTableName}_tuple");
                cmd.ExecuteNonQuery();

                cmd = cn.CreateCommand($@"CREATE TABLE {TestTableName}_tuple(ten Tuple(
                                                                                        Int32,
                                                                                        Nullable(String),
                                                                                        UInt64,
                                                                                        Nullable(Decimal64(4)),
                                                                                        FixedString(7),
                                                                                        Nullable(DateTime),
                                                                                        UInt8,
                                                                                        Nullable(Int16),
                                                                                        Nullable(UInt64),
                                                                                        Decimal64(4)
                                                                                        )) ENGINE = Memory");
                cmd.ExecuteNonQuery();

                await using (var writer = await cn.CreateColumnWriterAsync($"INSERT INTO {TestTableName}_tuple VALUES", CancellationToken.None))
                {
                    await writer.WriteTableAsync(new[] {tuples}, tuples.Length, CancellationToken.None);

                    Assert.False(writer.IsClosed);
                    await writer.EndWriteAsync(CancellationToken.None);
                    Assert.True(writer.IsClosed);
                }

                cmd = cn.CreateCommand($"SELECT * FROM {TestTableName}_tuple");
                int count = 0;
                await using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var value = reader.GetFieldValue<Tuple<int, string?, ulong, decimal?, string, DateTime?, byte, Tuple<short?, ulong?, decimal>>>(0);
                        Assert.Equal(tuples[value.Item1], value);

                        ++count;
                    }
                }

                Assert.Equal(count, tuples.Length);
            }
            finally
            {
                await using var cn = new ClickHouseConnection(GetDefaultConnectionSettings());
                cn.Open();
                var cmd = cn.CreateCommand($"DROP TABLE IF EXISTS {TestTableName}_tuple");
                cmd.ExecuteNonQuery();
            }
        }

        public class TableFixture : ClickHouseTestsBase, IDisposable
        {
            public TableFixture()
            {
                using var cn = OpenConnection();

                var cmd = cn.CreateCommand($"DROP TABLE IF EXISTS {TestTableName}");
                cmd.ExecuteNonQuery();

                cmd = cn.CreateCommand($"CREATE TABLE {TestTableName}(id Int32, str Nullable(String), num Nullable(Decimal64(6))) ENGINE=Memory");
                cmd.ExecuteNonQuery();
            }

            public void Dispose()
            {
                using var cn = OpenConnection();
                var cmd = cn.CreateCommand($"DROP TABLE IF EXISTS {TestTableName}");
                cmd.ExecuteNonQuery();
            }
        }
    }
}
