#region License Apache 2.0
/* Copyright 2019-2021, 2023-2024 Octonica
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
using Octonica.ClickHouseClient.Utils;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Octonica.ClickHouseClient.Tests
{
    public class ClickHouseColumnWriterTests : ClickHouseTestsBase, IClassFixture<ClickHouseColumnWriterTests.TableFixture>, IClassFixture<EncodingFixture>
    {
        private const string TestTableName = "stream_insert_test";

        private readonly TableFixture _tableFixture;

        public ClickHouseColumnWriterTests(TableFixture tableFixture)
        {
            _tableFixture = tableFixture;
        }

        [Fact]
        public async Task InsertValues()
        {
            await using var con = await OpenConnectionAsync();

            var rangeStart = _tableFixture.ReserveRange(100);
            await using (var writer = await con.CreateColumnWriterAsync($"INSERT INTO {TestTableName} VALUES", CancellationToken.None))
            {
                var columns = new object?[writer.FieldCount];
                columns[writer.GetOrdinal("id")] = new[] { rangeStart, rangeStart + 1 };
                columns[writer.GetOrdinal("num")] = new List<decimal?> {49999.99m, -999999.99999m};

                await writer.WriteTableAsync(columns, 2, CancellationToken.None);
            }

            var cmd = con.CreateCommand($"SELECT id, str, num FROM {TestTableName} WHERE id>={rangeStart} AND id<{rangeStart + 2}");
            int count = 0;
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var id = reader.GetInt32(0);
                    var str = reader.GetFieldValue(1, "NULL");
                    var num = reader.GetDecimal(2);

                    Assert.Equal("NULL", str);
                    switch (id - rangeStart)
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

            var rangeStart = _tableFixture.ReserveRange(10_000);
            await using (var writer = await con.CreateColumnWriterAsync($"INSERT INTO {TestTableName} VALUES", CancellationToken.None))
            {
                var columns = new Dictionary<string, object>
                {
                    ["id"] = Enumerable.Range(rangeStart, int.MaxValue - rangeStart),
                    ["str"] = Enumerable.Range(0, int.MaxValue).Select(i => i % 3 == 0 ? i % 5 == 0 ? "FizzBuzz" : "Fizz" : i % 5 == 0 ? "Buzz" : i.ToString())
                };

                await writer.WriteTableAsync(columns, 100, CancellationToken.None);
            }

            var cmd = con.CreateCommand($"SELECT id, str, num FROM {TestTableName} WHERE id>={rangeStart} AND id<{rangeStart + 10_000}");
            int count = 0;
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var id = reader.GetInt32(0);
                    var str = reader.GetString(1);
                    var num = reader.GetFieldValue<decimal>(2, null);
                    Assert.Null(num);

                    var i = id - rangeStart;
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

            var rangeStart = _tableFixture.ReserveRange(10_000);
            await using (var writer = await con.CreateColumnWriterAsync($"INSERT INTO {TestTableName} VALUES", CancellationToken.None))
            {
                var columns = new object?[writer.FieldCount];
                columns[writer.GetOrdinal("id")] = Enumerable.Range(rangeStart, int.MaxValue - rangeStart);
                columns[writer.GetOrdinal("num")] = new AsyncTestFibSequence();

                await writer.WriteTableAsync(columns, 63, CancellationToken.None);
            }

            var cmd = con.CreateCommand($"SELECT id, str, num FROM {TestTableName} WHERE id>={rangeStart} AND id<{rangeStart + 10_000} ORDER BY id");
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

                    var i = id - rangeStart;
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

            var rangeStart = _tableFixture.ReserveRange(10_000);
            await using (var writer = await con.CreateColumnWriterAsync($"INSERT INTO {TestTableName} VALUES", CancellationToken.None))
            {
                writer.ConfigureColumn("str", new ClickHouseColumnSettings(Encoding.UTF7));
                var columns = new object?[writer.FieldCount];
                columns[writer.GetOrdinal("id")] = Enumerable.Range(rangeStart, int.MaxValue - rangeStart);
                columns[writer.GetOrdinal("str")] = values;

                await writer.WriteTableAsync(columns, values.Count, CancellationToken.None);
            }

            var cmd = con.CreateCommand($"SELECT CAST(id - {rangeStart} AS Int32), convertCharset(str, 'UTF-7', 'cp1251'), num FROM {TestTableName} WHERE id>={rangeStart} AND id<{rangeStart + 10_000} ORDER BY id");
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
        public async Task InsertSingleRow()
        {
            await using var con = await OpenConnectionAsync();

            var rangeStart = _tableFixture.ReserveRange(100);
            await using (var writer = await con.CreateColumnWriterAsync($"INSERT INTO {TestTableName}(num, id, str) VALUES", CancellationToken.None))
            {
                await writer.WriteRowAsync(new List<object?> {42m, rangeStart, "Hello"}, CancellationToken.None);
                writer.WriteRow(null, rangeStart + 1, "world!");
                writer.WriteRow(new List<object?> { 42.5m, rangeStart + 2, DBNull.Value });

                await writer.EndWriteAsync(CancellationToken.None);
            }

            var cmd = con.CreateCommand($"SELECT cast(T.id - {rangeStart} AS Int32) id, T.str, T.num FROM {TestTableName} AS T WHERE T.id>={rangeStart} AND T.id<{rangeStart + 100}");
            int count = 0;
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var id = reader.GetInt32(0);
                    var str = reader.GetFieldValue(1, (string?) null);
                    var num = reader.GetFieldValue(2, (decimal?) null);

                    switch (id)
                    {
                        case 0:
                            Assert.Equal("Hello", str);
                            Assert.Equal(42m, num);
                            break;
                        case 1:
                            Assert.Equal("world!", str);
                            Assert.Null(num);
                            break;
                        case 2:
                            Assert.Null(str);
                            Assert.Equal(42.5m, num);
                            break;
                        default:
                            Assert.True(id >= 0 && id <= 3, "Id is out of range.");
                            break;
                    }

                    ++count;
                }
            }

            Assert.Equal(3, count);
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

        [Fact]
        public async Task InsertLowCardinalityValues()
        {
            try
            {
                await using var connection = await OpenConnectionAsync();

                var cmd = connection.CreateCommand($"DROP TABLE IF EXISTS {TestTableName}_low_cardinality");
                await cmd.ExecuteNonQueryAsync();

                cmd = connection.CreateCommand($"CREATE TABLE {TestTableName}_low_cardinality(id Int32, str LowCardinality(Nullable(String)), strNotNull LowCardinality(String)) ENGINE=Memory");
                await cmd.ExecuteNonQueryAsync();

                var idEnumerable = Enumerable.Range(0, 1000);
                var strEnumerable = Enumerable.Range(0, 1000).Select(NumToString);
                var strNotNullEnumerable = Enumerable.Range(0, 1000).Select(n => NumToString(n) ?? string.Empty);
                await using (var writer = connection.CreateColumnWriter($"INSERT INTO {TestTableName}_low_cardinality(id, str, strNotNull) VALUES"))
                {
                    var source = new object[] {idEnumerable, strEnumerable, strNotNullEnumerable};

                    await writer.WriteTableAsync(source, 250, CancellationToken.None);
                    await writer.WriteTableAsync(source, 250, CancellationToken.None);
                    await writer.WriteTableAsync(source, 500, CancellationToken.None);
                    await writer.EndWriteAsync(CancellationToken.None);
                }

                cmd.CommandText = $"SELECT id, str, strNotNull FROM {TestTableName}_low_cardinality";
                int count = 0;
                await using (var reader = cmd.ExecuteReader())
                {
                    while (await reader.ReadAsync())
                    {
                        var id = reader.GetInt32(0);
                        var str = reader.GetString(1, null);
                        var strNotNull = reader.GetString(2);

                        var expectedStr = NumToString(id);
                        Assert.Equal(expectedStr, str);
                        Assert.Equal(expectedStr ?? string.Empty, strNotNull);

                        ++count;
                    }
                }

                Assert.Equal(1000, count);
            }
            finally
            {
                await using var connection = await OpenConnectionAsync();
                var cmd = connection.CreateCommand($"DROP TABLE IF EXISTS {TestTableName}_low_cardinality");
                await cmd.ExecuteNonQueryAsync();
            }

            static string? NumToString(int num) => num % 15 == 0 ? (num % 2 == 0 ? null : string.Empty) : num % 3 == 0 ? "foo" : num % 5 == 0 ? "bar" : num % 2 == 0 ? "true" : "false";
        }

        [Fact]
        public async Task InsertEnumValues()
        {
            try
            {
                await using var connection = await OpenConnectionAsync();

                var cmd = connection.CreateCommand($"DROP TABLE IF EXISTS {TestTableName}_enums");
                await cmd.ExecuteNonQueryAsync();

                cmd = connection.CreateCommand($"CREATE TABLE {TestTableName}_enums(id Int16, e8 Enum8('min' = -128, 'zero' = 0, 'max' = 127), e16 Enum16('unknown value' = 0, 'well known value' = 42, 'foo' = -1024, 'bar' = 108)) ENGINE=Memory");
                await cmd.ExecuteNonQueryAsync();

                await using (var writer = connection.CreateColumnWriter($"INSERT INTO {TestTableName}_enums(id, e16, e8) VALUES"))
                {
                    var source = new object[]
                    {
                        Enumerable.Range(0, 1000).Select(num => (short) num),
                        Enumerable.Range(-500, 1000).Select(num => num % 108 == 0 ? "bar" : num < 0 ? "foo" : num == 42 ? "well known value" : "unknown value"),
                        Enumerable.Range(0, 1000).Select(num => num % 3 == 0 ? sbyte.MinValue : num % 3 == 1 ? (sbyte) 0 : sbyte.MaxValue)
                    };

                    await writer.WriteTableAsync(source, 1000, CancellationToken.None);
                    await writer.EndWriteAsync(CancellationToken.None);
                }

                cmd.CommandText = $"SELECT e8, e16 FROM {TestTableName}_enums ORDER BY id";

                int count = 0;
                await using var reader = await cmd.ExecuteReaderAsync();

                while (reader.Read())
                {
                    var e8 = reader.GetValue(0);
                    var e16 = reader.GetInt16(1);

                    Assert.Equal(count % 3 == 0 ? "min" : count % 3 == 1 ? "zero" : "max", e8);
                    Assert.Equal((count - 500) % 108 == 0 ? 108 : count - 500 < 0 ? -1024 : count - 500 == 42 ? 42 : 0, e16);
                    ++count;
                }

                Assert.Equal(1000, count);
            }
            finally
            {
                await using var connection = await OpenConnectionAsync();
                var cmd = connection.CreateCommand($"DROP TABLE IF EXISTS {TestTableName}_enums");
                await cmd.ExecuteNonQueryAsync();
            }
        }

        [Fact]
        public async Task InsertNullableEnumValues()
        {
            try
            {
                await using var connection = await OpenConnectionAsync();

                var cmd = connection.CreateCommand($"DROP TABLE IF EXISTS {TestTableName}_null_enums");
                await cmd.ExecuteNonQueryAsync();

                cmd = connection.CreateCommand($"CREATE TABLE {TestTableName}_null_enums(id Int16, e8 Nullable(Enum8('min' = -128, 'zero' = 0, 'max' = 127)), e16 Nullable(Enum16('unknown value' = 0, 'well known value' = 42, 'foo' = -1024, 'bar' = 108))) ENGINE=Memory");
                await cmd.ExecuteNonQueryAsync();

                await using (var writer = connection.CreateColumnWriter($"INSERT INTO {TestTableName}_null_enums(id, e16, e8) VALUES"))
                {
                    var source = new object[]
                    {
                        Enumerable.Range(0, 1000).Select(num => (short) num),
                        Enumerable.Range(-500, 1000).Select(num => num % 108 == 0 ? num % 5 == 0 ? null : "bar" : num < 0 ? "foo" : num == 42 ? "well known value" : "unknown value"),
                        Enumerable.Range(0, 1000).Select(num => num % 3 == 0 ? num % 7 == 0 ? (sbyte?) null : sbyte.MinValue : num % 3 == 1 ? (sbyte) 0 : sbyte.MaxValue)
                    };

                    await writer.WriteTableAsync(source, 1000, CancellationToken.None);
                    await writer.EndWriteAsync(CancellationToken.None);
                }

                cmd.CommandText = $"SELECT e8, e16 FROM {TestTableName}_null_enums ORDER BY id";

                int count = 0;
                await using var reader = await cmd.ExecuteReaderAsync();

                while (reader.Read())
                {
                    var e8 = reader.GetValue(0);
                    var e16 = reader.GetFieldValue(1, (short?)null);

                    Assert.Equal(count % 3 == 0 ? count % 7 == 0 ? (object)DBNull.Value : "min" : count % 3 == 1 ? "zero" : "max", e8);
                    Assert.Equal((count - 500) % 108 == 0 ? (count - 500) % 5 == 0 ? (short?)null : 108 : count - 500 < 0 ? -1024 : count - 500 == 42 ? (short?)42 : 0, e16);
                    ++count;
                }

                Assert.Equal(1000, count);
            }
            finally
            {
                await using var connection = await OpenConnectionAsync();
                var cmd = connection.CreateCommand($"DROP TABLE IF EXISTS {TestTableName}_null_enums");
                await cmd.ExecuteNonQueryAsync();
            }
        }

        [Theory]
        [InlineData(0)]
        [InlineData(null)]
        [InlineData(2111)]
        [InlineData(999_990)]
        public async Task InsertLargeTable(int? maxBlockSize)
        {
            // "Large" means that the size of the table is greater than the size of the buffer
            const int rowCount = 100_000;
            var startId = _tableFixture.ReserveRange(rowCount);

            var settings = new ClickHouseConnectionStringBuilder(GetDefaultConnectionSettings()) {BufferSize = 4096};

            await using var connection = new ClickHouseConnection(settings);
            await connection.OpenAsync();

            await using (var writer = connection.CreateColumnWriter($"INSERT INTO {TestTableName}(id, str) VALUES"))
            {
                // Leave a default value intact when maxBlockSize == 0
                if (maxBlockSize == null || maxBlockSize > 0)
                    writer.MaxBlockSize = maxBlockSize;                

                var table = new object[] {Enumerable.Range(startId, rowCount), Enumerable.Range(startId, rowCount).Select(num => num.ToString())};
                await writer.WriteTableAsync(table, rowCount, CancellationToken.None);
                await writer.EndWriteAsync(CancellationToken.None);
            }

            await using var cmd = connection.CreateCommand($"SELECT id, str FROM {TestTableName} WHERE id >= {{startId}} AND id < {{endId}} ORDER BY id");
            cmd.Parameters.AddWithValue("startId", startId);
            cmd.Parameters.AddWithValue("endId", startId + rowCount);

            await using var reader = cmd.ExecuteReader();
            int expectedId = startId;
            while (await reader.ReadAsync())
            {
                var id = reader.GetInt32(0);
                var str = reader.GetString(1);

                Assert.Equal(expectedId, id);
                Assert.Equal(expectedId.ToString(), str);

                ++expectedId;
            }

            Assert.Equal(startId + rowCount, expectedId);
        }

        [Fact]
        public async Task InsertFromSecondaryInterfaces()
        {
            const int rowCount = 100;
            var startId = _tableFixture.ReserveRange(rowCount);

            // Each wrapper implements only one interface
            var ids = new EnumerableListWrapper<int>(Enumerable.Range(startId, rowCount).ToList());
            var strings = new GenericEnumerableListWrapper<string>(Enumerable.Range(1, rowCount).Select(num => $"Str #{num}").ToList());
            var numbers = new ListWrapper<decimal?>(
                Enumerable.Range(0, rowCount).Select(num => num % 17 == 0 ? (decimal?) null : Math.Round(num / (decimal) 17, 6)).ToList());

            var connection = await OpenConnectionAsync();

            var writeCount = (int) (rowCount * 0.9);
            await using (var writer = connection.CreateColumnWriter($"INSERT INTO {TestTableName}(id, str, num) VALUES"))
            {
                var table = new object[] {ids, strings, numbers};
                await writer.WriteTableAsync(table, writeCount, CancellationToken.None);
                await writer.EndWriteAsync(CancellationToken.None);
            }

            await using var cmd = connection.CreateCommand($"SELECT id, str, num FROM {TestTableName} WHERE id >= {{startId}} AND id < {{endId}} ORDER BY id");
            cmd.Parameters.AddWithValue("startId", startId);
            cmd.Parameters.AddWithValue("endId", startId + rowCount);

            await using var reader = cmd.ExecuteReader();
            int count = 0;
            while (await reader.ReadAsync())
            {
                var id = reader.GetInt32(0);
                var str = reader.GetString(1);
                var num = reader.GetFieldValue<decimal>(2, null);

                Assert.Equal(ids.List[count], id);
                Assert.Equal(strings.List[count], str);
                Assert.Equal(numbers.List[count], num);

                ++count;
            }

            Assert.Equal(writeCount, count);
        }

        [Fact]
        public async Task InsertValuesWithLowRowCount()
        {
            const int rowCount = 100, rowCap = 71;
            var startId = _tableFixture.ReserveRange(rowCount);

            await using var con = await OpenConnectionAsync();

            var ids = Enumerable.Range(startId, rowCount).ToList();
            var nums = Enumerable.Range(0, rowCount).Select(v => -100 + Math.Round(200m / (rowCount - 1) * v, 6)).ToArray();
            await using (var writer = await con.CreateColumnWriterAsync($"INSERT INTO {TestTableName} VALUES", CancellationToken.None))
            {
                var columns = new object?[writer.FieldCount];
                columns[writer.GetOrdinal("id")] = ids;
                columns[writer.GetOrdinal("num")] = nums;

                await writer.WriteTableAsync(columns, rowCap, CancellationToken.None);
            }

            await using var cmd = con.CreateCommand($"SELECT id, str, num FROM {TestTableName} WHERE id >= {{startId}} AND id < {{endId}} ORDER BY id");
            cmd.Parameters.AddWithValue("startId", startId);
            cmd.Parameters.AddWithValue("endId", startId + rowCount);

            int count = 0;
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var id = reader.GetInt32(0);
                    var str = reader.GetFieldValue(1, "NULL");
                    var num = reader.GetDecimal(2);

                    Assert.Equal(ids[count], id);
                    Assert.Equal("NULL", str);
                    Assert.Equal(nums[count], num);

                    ++count;
                }
            }

            Assert.Equal(rowCap, count);
        }

        [Fact]
        public async Task InsertArrayFromMemory()
        {
            var ints = Enumerable.Range(0, 1000).ToArray();
            var nums = ints.Select(v => -100 + Math.Round(200m / (ints.Length - 1) * v, 6)).ToArray();

            var intsArr = new List<Memory<int>>();
            var numsArr = new List<ReadOnlyMemory<decimal>>();
            int expectedCount = 0;
            for (int i = 0; i < ints.Length;)
            {
                var size = i % 13 == 0 ? 13 : i % 7;
                if (size == 0)
                    size = 3;

                if (i + size > ints.Length)
                    break;
                
                var intsMem = new Memory<int>(ints, i, size);
                var numsMem = new Memory<decimal>(nums, i, size);

                intsArr.Add(intsMem);
                numsArr.Add(numsMem);

                i += size;
                expectedCount = i;
            }

            await WithTemporaryTable("mem", "idx Int32, id Array(Int32), num Array(Decimal64(6))", RunTest);

            async Task RunTest(ClickHouseConnection connection, string tableName)
            {
                await using (var writer = connection.CreateColumnWriter($"INSERT INTO {tableName}(num, id, idx) VALUES"))
                {
                    var source = new object[] { numsArr, intsArr, Enumerable.Range(0, numsArr.Count).ToArray() };

                    await writer.WriteTableAsync(source, numsArr.Count, CancellationToken.None);
                    await writer.EndWriteAsync(CancellationToken.None);
                }

                var cmd = connection.CreateCommand($"SELECT id, num, idx FROM {tableName}");

                int count = 0;
                await using var reader = await cmd.ExecuteReaderAsync();

                while (reader.Read())
                {
                    var idArr = reader.GetFieldValue<int[]>(0);
                    var numArr = reader.GetFieldValue<decimal[]>(1);
                    var idx = reader.GetInt32(2);

                    Assert.Equal(idArr.Length, numArr.Length);

                    for (int i = 0; i < idArr.Length; i++)
                    {
                        Assert.Equal(nums[idArr[i]], numArr[i]);
                        Assert.Equal(intsArr[idx].Span[i], idArr[i]);
                        Assert.Equal(numsArr[idx].Span[i], numArr[i]);
                    }

                    count += idArr.Length;
                }

                Assert.Equal(expectedCount, count);
            }
        }

        [Fact]
        public async Task InsertStringFromMemory()
        {
            const int rowCount = 400;
            var startId = _tableFixture.ReserveRange(rowCount);

            const string someText =
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
    "Donec varius tortor iaculis sapien malesuada, nec eleifend risus ultrices. " +
    "Suspendisse ac ligula nec nunc finibus lobortis sed ac ipsum. " +
    "Curabitur rutrum ligula feugiat, finibus enim id, vulputate purus. " +
    "Aliquam facilisis sem vel mattis fringilla. " +
    "Nullam in mauris feugiat, pulvinar nibh id, pretium quam. " +
    "Suspendisse hendrerit sapien et nisi rutrum, eu vestibulum magna convallis. " +
    "Nam sed turpis vulputate, volutpat augue eget, pulvinar sapien.";

            var ids = new List<int>(Enumerable.Range(0, 100));
            var mem = new List<Memory<char>>();
            var roMem = new List<ReadOnlyMemory<char>>();
            var bytes = new List<Memory<byte>>();
            var roBytes = new List<ReadOnlyMemory<byte>>();

            var someTextChars = someText.ToCharArray();
            var someTextBytes = Encoding.ASCII.GetBytes(someText);
            Assert.Equal(someTextChars.Length, someTextBytes.Length);

            int position = 0;
            char[] separators = { ' ', ',', '.' };
            for (int i = 0; i < ids.Count; i++)
            {
                while (separators.Contains(someText[position]))
                    position = (position + 1) % someText.Length;

                var idx = someText.IndexOfAny(separators, position);
                var len = idx < 0 ? someText.Length - position : idx - position;

                mem.Add(new Memory<char>(someTextChars, position, len));
                roMem.Add(someText.AsMemory(position, len));
                var bytesMem = new Memory<byte>(someTextBytes, position, len);
                bytes.Add(bytesMem);
                roBytes.Add(bytesMem);

                position = (position + len) % someText.Length;
            }

            var connection = await OpenConnectionAsync();

            await using (var writer = await connection.CreateColumnWriterAsync($"INSERT INTO {TestTableName} VALUES", CancellationToken.None))
            {
                var columns = new object?[writer.FieldCount];
                columns[writer.GetOrdinal("id")] = ids.Select(id => id + startId);
                columns[writer.GetOrdinal("str")] = mem;

                await writer.WriteTableAsync(columns, ids.Count, CancellationToken.None);

                columns[writer.GetOrdinal("id")] = ids.Select(id => id + startId + ids.Count);
                columns[writer.GetOrdinal("str")] = roMem;

                await writer.WriteTableAsync(columns, ids.Count, CancellationToken.None);

                columns[writer.GetOrdinal("id")] = ids.Select(id => id + startId + ids.Count * 2);
                columns[writer.GetOrdinal("str")] = bytes;

                await writer.WriteTableAsync(columns, ids.Count, CancellationToken.None);

                columns[writer.GetOrdinal("id")] = ids.Select(id => id + startId + ids.Count * 3);
                columns[writer.GetOrdinal("str")] = roBytes;

                await writer.WriteTableAsync(columns, ids.Count, CancellationToken.None);
            }

            await using var cmd = connection.CreateCommand($"SELECT id, str, num FROM {TestTableName} WHERE id >= {{startId}} AND id < {{endId}} ORDER BY id");
            cmd.Parameters.AddWithValue("startId", startId);
            cmd.Parameters.AddWithValue("endId", startId + rowCount);

            int count = 0;
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var id = reader.GetInt32(0);
                    var str = reader.GetString(1);
                    var num = reader.GetFieldValue(2, (decimal?) null);

                    Assert.Equal(mem[(id - startId) % ids.Count].ToString(), str);
                    Assert.Null(num);

                    ++count;
                }
            }

            Assert.Equal(rowCount, count);
        }

        [Fact]
        public async Task InsertFixedStringFromMemory()
        {
            const string someText =
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
                "Donec varius tortor iaculis sapien malesuada, nec eleifend risus ultrices. " +
                "Suspendisse ac ligula nec nunc finibus lobortis sed ac ipsum. " +
                "Curabitur rutrum ligula feugiat, finibus enim id, vulputate purus. " +
                "Aliquam facilisis sem vel mattis fringilla. " +
                "Nullam in mauris feugiat, pulvinar nibh id, pretium quam. " +
                "Suspendisse hendrerit sapien et nisi rutrum, eu vestibulum magna convallis. " +
                "Nam sed turpis vulputate, volutpat augue eget, pulvinar sapien.";

            var id = new List<int>(Enumerable.Range(0, 100));
            var mem = new List<Memory<char>>();
            var roMem = new List<ReadOnlyMemory<char>>();
            var bytes = new List<Memory<byte>>();
            var roBytes = new List<ReadOnlyMemory<byte>>();

            var someTextChars = someText.ToCharArray();
            var someTextBytes = Encoding.ASCII.GetBytes(someText);
            Assert.Equal(someTextChars.Length, someTextBytes.Length);

            int position = 0;
            int maxLength = 0;
            char[] separators = {' ', ',', '.'};
            for (int i = 0; i < id.Count; i++)
            {
                while (separators.Contains(someText[position]))
                    position = (position + 1) % someText.Length;

                var idx = someText.IndexOfAny(separators, position);
                var len = idx < 0 ? someText.Length - position : idx - position;

                mem.Add(new Memory<char>(someTextChars, position, len));
                roMem.Add(someText.AsMemory(position, len));
                var bytesMem = new Memory<byte>(someTextBytes, position, len);
                bytes.Add(bytesMem);
                roBytes.Add(bytesMem);

                maxLength = Math.Max(maxLength, len);
                position = (position + len) % someText.Length;
            }

            await WithTemporaryTable("fsm", $"id Int32, str FixedString({maxLength})", RunTest);

            async Task RunTest(ClickHouseConnection connection, string tableName)
            {
                await using (var writer = connection.CreateColumnWriter($"INSERT INTO {tableName}(id, str) VALUES"))
                {
                    writer.ConfigureColumn("str", new ClickHouseColumnSettings(Encoding.ASCII));
                    await writer.WriteTableAsync(new object[] {id, mem}, id.Count, CancellationToken.None);
                    await writer.WriteTableAsync(new object[] {id.Select(i => i + id.Count), roMem}, id.Count, CancellationToken.None);
                    await writer.WriteTableAsync(new object[] {id.Select(i => i + id.Count*2), bytes}, id.Count, CancellationToken.None);
                    await writer.WriteTableAsync(new object[] {id.Select(i => i + id.Count*3), roBytes}, id.Count, CancellationToken.None);
                    await writer.EndWriteAsync(CancellationToken.None);
                }

                var cmd = connection.CreateCommand($"SELECT id, str FROM {tableName}");

                int count = 0;
                await using var reader = await cmd.ExecuteReaderAsync();
                reader.ConfigureColumn("str", new ClickHouseColumnSettings(Encoding.ASCII));

                while (reader.Read())
                {
                    var idVal = reader.GetFieldValue<int>(0);
                    var strVal = reader.GetFieldValue<string>(1);

                    Assert.Equal(roMem[idVal % id.Count].ToString(), strVal);
                    count++;
                }

                Assert.Equal(id.Count * 4, count);
            }
        }

        [Fact]
        public async Task InsertFromCollectionOfObjects()
        {
            const int rowCount = 200;
            var startId = _tableFixture.ReserveRange(rowCount);

            // Each wrapper implements only one interface
            var ids = new EnumerableListWrapper<object>(Enumerable.Range(startId, rowCount / 2).Cast<object>().ToList());
            var strings = new GenericEnumerableListWrapper<object>(Enumerable.Range(1, rowCount / 2).Select(num => $"Str #1_{num}").ToList<object>());
            var numbers = new ListWrapper<object?>(
                Enumerable.Range(0, rowCount / 2).Select(num => num % 19 == 0 ? (object?)null : Math.Round(num / (decimal)19, 6)).ToList());

            var ids2 = new AsyncEnumerableListWrapper<object>(Enumerable.Range(startId + rowCount/2, rowCount / 2).Cast<object>().ToList());
            var strings2 = Enumerable.Range(1, rowCount / 2).Select(num => $"Str #2_{num}").ToArray<object>();
            var numbers2 = Enumerable.Range(rowCount / 2, rowCount / 2).Select(num => num % 19 == 0 ? (object?)null : Math.Round(num / (decimal)19, 6)).ToList();

            await using var connection = await OpenConnectionAsync();

            await using (var writer = await connection.CreateColumnWriterAsync($"INSERT INTO {TestTableName}(id, str, num) VALUES", CancellationToken.None))
            {
                await writer.WriteTableAsync(new object[] { ids, strings, numbers }, rowCount / 2, CancellationToken.None);
                await writer.WriteTableAsync(new object[] { ids2, strings2, numbers2 }, rowCount / 2, CancellationToken.None);                
            }

            await using var cmd = connection.CreateCommand($"SELECT id, str, num FROM {TestTableName} WHERE id >= {{startId}} AND id < {{endId}} ORDER BY id");
            cmd.Parameters.AddWithValue("startId", startId);
            cmd.Parameters.AddWithValue("endId", startId + rowCount);

            await using var reader = cmd.ExecuteReader();
            int count = 0;
            while (await reader.ReadAsync())
            {
                var id = reader.GetInt32(0);
                var str = reader.GetString(1);
                var num = reader.GetFieldValue<decimal>(2, null);

                Assert.Equal(count + startId, id);

                if (id - startId < rowCount / 2)
                {
                    Assert.Equal(strings.List[count], str);
                    Assert.Equal(numbers.List[count], num);
                }
                else
                {
                    Assert.Equal((string)strings2[count - rowCount / 2], str);
                    Assert.Equal((decimal?)numbers2[count - rowCount / 2], num);
                }

                ++count;
            }

            Assert.Equal(rowCount, count);
        }

        [Fact]
        public async Task InsertValuesOfSpecifiedType()
        {
            const int rowCount = 200;
            var startId = _tableFixture.ReserveRange(rowCount);

            var ids = Enumerable.Range(startId, 123).ToList();
            var list = new Int32ToUInt32MappedListWrapper(ids, v => (uint)v * 7);
            var table = new object?[] { list, list, null };

            await using var connection = await OpenConnectionAsync();

            await using (var writer = await connection.CreateColumnWriterAsync($"INSERT INTO {TestTableName}(id, num, str) VALUES", CancellationToken.None))
            {
                Assert.Equal(typeof(int), writer.GetFieldType(0));
                Assert.Equal(typeof(decimal), writer.GetFieldType(1));
                Assert.Equal(typeof(string), writer.GetFieldType(2));

                writer.ConfigureColumn("id", new ClickHouseColumnSettings(typeof(int)));
                writer.ConfigureColumn("num", new ClickHouseColumnSettings(typeof(uint)));

                Assert.Equal(typeof(int), writer.GetFieldType(0));
                Assert.Equal(typeof(uint), writer.GetFieldType(1));
                Assert.Equal(typeof(string), writer.GetFieldType(2));

                await writer.WriteTableAsync(table, ids.Count, CancellationToken.None);
            }

            await using var cmd = connection.CreateCommand($"SELECT id, num FROM {TestTableName} WHERE id >= {{startId}} AND id < {{endId}} ORDER BY id");
            cmd.Parameters.AddWithValue("startId", startId);
            cmd.Parameters.AddWithValue("endId", startId + rowCount);

            await using var reader = cmd.ExecuteReader();
            int count = 0;
            while (await reader.ReadAsync())
            {
                var id = reader.GetInt32(0);
                var num = reader.GetFieldValue<decimal>(1);

                Assert.Equal(count + startId, id);
                Assert.Equal(id * 7, num);

                ++count;
            }

            Assert.Equal(ids.Count, count);
        }

        [Fact]
        public async Task InsertValuesFromObjectArrays()
        {
            var tableData = new object?[][]
            {
                new object[]{1, 2, 3, 4, 5, 6, 7},
                new object?[]{"one", null, null, null, "five", "six", "seven"},
                new object?[]{"192.168.121.0", DBNull.Value, "127.0.0.1", DBNull.Value, DBNull.Value, "10.0.0.1", "8.8.8.8"},
                new object?[]{null, DBNull.Value, 12.34m, 12324.57m, 2195.99m, DBNull.Value, null},
                new object[]{(sbyte)-1, (sbyte)0, (sbyte)0, (sbyte)1, (sbyte)1, (sbyte)-1, (sbyte)0}

            };

            var expectedData = new object?[][]
            {
                tableData[0].Cast<int>().Select(v=>(object)(long)v).ToArray(),
                tableData[1],
                new object?[]{IPAddress.Parse("192.168.121.0"), null, IPAddress.Parse("127.0.0.1"), null, null, IPAddress.Parse("10.0.0.1"), IPAddress.Parse("8.8.8.8")},
                tableData[3],
                new object[]{"minus", "zero", "zero", "plus", "plus", "minus", "zero"}
            };

            await WithTemporaryTable("obj_arr", "id Int64, str Nullable(String), ip Nullable(IPv4), num Nullable(Decimal32(2)), sign Enum8('minus'=-1, 'zero'=0, 'plus'=1)", RunTest);

            async Task RunTest(ClickHouseConnection connection, string tableName)
            {
                await using (var writer = await connection.CreateColumnWriterAsync($"INSERT INTO {tableName}(id, str, ip, num, sign) VALUES", CancellationToken.None))
                {
                    writer.ConfigureColumn(0, new ClickHouseColumnSettings(tableData[0][0]!.GetType()));
                    writer.ConfigureColumn(1, new ClickHouseColumnSettings(tableData[1][0]!.GetType()));
                    writer.ConfigureColumn(2, new ClickHouseColumnSettings(tableData[2][0]!.GetType()));
                    writer.ConfigureColumn(4, new ClickHouseColumnSettings(tableData[4][0]!.GetType()));

                    await writer.WriteTableAsync(tableData, tableData[0].Length, CancellationToken.None);
                }

                var cmd = connection.CreateCommand($"SELECT id, str, ip, num, sign FROM {tableName}");
                await using var reader = await cmd.ExecuteReaderAsync();
                
                int count = 0;
                while (await reader.ReadAsync())
                {
                    for (int i = 0; i < tableData.Length; i++)
                    {
                        var expectedValue = expectedData[i][count];
                        bool isNull = expectedValue == null || expectedValue == DBNull.Value;
                        Assert.Equal(isNull, reader.IsDBNull(i));

                        if (isNull)
                            continue;

                        var value = reader.GetValue(i);
                        Assert.Equal(expectedValue, value);
                    }

                    ++count;
                }

                Assert.Equal(tableData[0].Length, count);
            }
        }

        [Fact]
        public async Task InsertMapValues()
        {
            var map1 = new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 };
            var map2 = new KeyValuePair<string, int>[] { new KeyValuePair<string, int>("c", 3) };
            var map3 = new List<Tuple<string, int>> { new Tuple<string, int>("d", 5), new Tuple<string, int>("e", 6) };
            var map4 = new List<(string key, int value)> { ("f", 7), ("g", 8), ("h", 9), ("i", 10) };

            await WithTemporaryTable("map", "id Int32, map Map(String, Int32)", Test);

            async Task Test(ClickHouseConnection cn, string tableName)
            {
                await using (var writer = await cn.CreateColumnWriterAsync($"INSERT INTO {tableName}(id, map) VALUES", CancellationToken.None))
                {
                    await writer.WriteTableAsync(new object[] { AsListOfOne(1), AsListOfOne(map1) }, 1, CancellationToken.None);
                    await writer.WriteTableAsync(new object[] { AsListOfOne(2), AsListOfOne(map2) }, 1, CancellationToken.None);
                    await writer.WriteTableAsync(new object[] { AsListOfOne(3), AsListOfOne(map3) }, 1, CancellationToken.None);
                    await writer.WriteTableAsync(new object[] { AsListOfOne(4), AsListOfOne(map4) }, 1, CancellationToken.None);
                }

                var cmd = cn.CreateCommand($"SELECT map FROM {tableName} ORDER BY id");

                await using var reader = await cmd.ExecuteReaderAsync();

                int count = 0;
                while(await reader.ReadAsync())
                {
                    KeyValuePair<string, int>[] expected;
                    switch (++count)
                    {
                        case 1:
                            expected = map1.ToArray();
                            break;
                        case 2:
                            expected = map2;
                            break;
                        case 3:
                            expected = map3.Select(t => new KeyValuePair<string, int>(t.Item1, t.Item2)).ToArray();
                            break;
                        case 4:
                            expected = map4.Select(t => new KeyValuePair<string, int>(t.key, t.value)).ToArray();
                            break;
                        default:
                            Assert.True(false, "Too many rows.");
                            throw new InvalidOperationException();
                    }

                    var actual = reader.GetFieldValue<KeyValuePair<string, int>[]>(0);
                    Assert.Equal(expected, actual);
                }

                Assert.Equal(4, count);
            }

            static IReadOnlyList<T> AsListOfOne<T>(T value)
            {
                return new[] { value };
            }
        }

        [Fact]
        public async Task InsertArrayLowCardinality()
        {
            var columns = new Dictionary<string, object?>()
            {
                ["id"] = Enumerable.Range(1, 10).ToList(),
                ["data"] = Enumerable.Range(1, 10).Select(o => new[] { $"test{ 1 + o * 2 % 3}", $"test{ 1 + (1 + o * 2) % 3}" }),
            };

            await WithTemporaryTable("arrlc", "id Int32, data Array(LowCardinality(String))", Test);

            async Task Test(ClickHouseConnection cn, string tableName)
            {
                await using (var writer = await cn.CreateColumnWriterAsync($"INSERT INTO {tableName}(id, data) VALUES", CancellationToken.None))
                {
                    await writer.WriteTableAsync(columns, 10, CancellationToken.None);
                }

                var cmd = cn.CreateCommand($"SELECT id, data FROM {tableName} ORDER BY id");
                await using var reader = await cmd.ExecuteReaderAsync();
                int count = 0;
                while(await reader.ReadAsync())
                {
                    var id = reader.GetInt32(0);
                    var data = reader.GetFieldValue<string[]>(1);

                    Assert.Equal(((List<int>?)columns["id"])?[count], id);
                    Assert.Equal(new[] { $"test{ 1 + (1 + count) * 2 % 3}", $"test{ 1 + (1 + (1 + count) * 2) % 3}" }, data);
                    ++count;
                }

                Assert.Equal(10, count);
            }
        }

        [Fact]
        public Task InsertBoolValues()
        {
            var columns = new Dictionary<string, object?>
            {
                ["id"] = Enumerable.Range(1, 100).ToList(),
                ["b1"] = Enumerable.Range(1, 100).Select(i => i % 2 == 0).ToList(),
                ["b2"] = Enumerable.Range(1, 100).Select(i => i % 3 == 0 ? (bool?)null : i % 4 == 0).ToList(),
                ["b3"] = Enumerable.Range(1, 100).Select(i => (byte)(i % 8)).ToList(),
                ["b4"] = Enumerable.Range(1, 100).Select(i => i % 5 == 0 ? null : (byte?)(i % 16))
            };

            return WithTemporaryTable("bool", "id Int32, b1 Boolean, b2 Nullable(Boolean), b3 Boolean, b4 Nullable(Boolean)", Test);

            async Task Test(ClickHouseConnection cn, string tableName)
            {
                await using (var writer = await cn.CreateColumnWriterAsync($"INSERT INTO {tableName}(id, b1, b2, b3, b4) VALUES", CancellationToken.None))
                {
                    await writer.WriteTableAsync(columns, 100, CancellationToken.None);
                }

                var cmd = cn.CreateCommand($"SELECT id, b1, b2, b3, b4 data FROM {tableName} ORDER BY id");
                await using var reader = await cmd.ExecuteReaderAsync();
                int count = 0;
                while (await reader.ReadAsync())
                {
                    var id = reader.GetInt32(0);

                    // Bool value can be true or false.
                    // Byte value can be 1 or 0, even if inserted value was different.

                    var b1 = reader.GetBoolean(1);
                    var b1Byte = reader.GetByte(1);
                    Assert.Equal(b1 ? 1 : 0, b1Byte);

                    var b2 = reader.GetFieldValue(2, (bool?)null);
                    var b2Byte = reader.GetFieldValue(2, (byte?)null);
                    Assert.Equal(b2 == null ? null : b2.Value ? (byte?)1 : 0, b2Byte);

                    var b3 = reader.GetValue(3);
                    var b3Byte = reader.GetByte(3);
                    var b3Bool = Assert.IsType<bool>(b3);
                    Assert.Equal(b3Bool ? 1 : 0, b3Byte);

                    var b4 = reader.GetValue(4);
                    var b4Byte = reader.GetFieldValue(4, (byte?)null);
                    var b4Bool = b4 == DBNull.Value ? (bool?)null : Assert.IsType<bool>(b4);
                    Assert.Equal(b4Bool == null ? null : b4Bool.Value ? (byte?)1 : 0, b4Byte);

                    Assert.Equal(count + 1, id);
                    Assert.Equal(id % 2 == 0, b1);
                    Assert.Equal(id % 3 == 0 ? (bool?)null : id % 4 == 0, b2);
                    Assert.Equal(id % 8 != 0, b3);
                    Assert.Equal(id % 5 == 0 ? DBNull.Value : (object)(id % 16 != 0), b4);

                    ++count;
                }

                Assert.Equal(100, count);
            }
        }

        [Fact]
        public Task TransactionModeBlock()
        {
            return WithTemporaryTable("tran_block", "id Int32", Test);

            async Task Test(ClickHouseConnection connection, string tableName, CancellationToken ct)
            {
                var list = MappedReadOnlyList<int, int>.Map(Enumerable.Range(0, 48).ToList(), i => i < 47 ? i : throw new IndexOutOfRangeException("Too long!"));
                await using (var writer = await connection.CreateColumnWriterAsync($"INSERT INTO {tableName}(id) VALUES", ct))
                {
                    // There will be three blocks in the list. The last block will produce an error, but first two blocks must be commited.
                    writer.MaxBlockSize = 16;

                    var ex = await Assert.ThrowsAnyAsync<ClickHouseHandledException>(() => writer.WriteTableAsync(new[] { list }, list.Count, ClickHouseTransactionMode.Block, ct));
                    Assert.NotNull(ex.InnerException);
                    Assert.IsType<IndexOutOfRangeException>(ex.InnerException);
                }

                var cmd = connection.CreateCommand($"SELECT * FROM {tableName} ORDER BY id");
                await using var reader = await cmd.ExecuteReaderAsync();
                int expected = 0;

                while (await reader.ReadAsync(ct))
                {
                    var id = reader.GetInt32(0);
                    Assert.Equal(expected++, id);
                }

                Assert.Equal(32, expected);
            }
        }

        [Fact]
        public Task TransactionModeManual()
        {
            return WithTemporaryTable("tran_manual", "id Int32", Test);

            static async Task Test(ClickHouseConnection connection, string tableName, CancellationToken ct)
            {
                await using (var writer = await connection.CreateColumnWriterAsync($"INSERT INTO {tableName}(id) VALUES", ct))
                {
                    await writer.WriteTableAsync(new[] { Enumerable.Range(0, 10) }, 10, ClickHouseTransactionMode.Manual, ct);
                    await writer.RollbackAsync(ct);
                    await writer.WriteTableAsync(new[] { Enumerable.Range(10, 10) }, 10, ClickHouseTransactionMode.Manual, ct);
                    await writer.CommitAsync(ct);
                    await writer.WriteTableAsync(new[] { Enumerable.Range(20, 10) }, 10, ClickHouseTransactionMode.Manual, ct);
                }

                await using (var writer = await connection.CreateColumnWriterAsync($"INSERT INTO {tableName}(id) VALUES", ct))
                {
                    await writer.WriteRowAsync(new object?[] { 18 }, false, ct);
                    await writer.WriteRowAsync(new object?[] { 19 }, false, ct);
                    await writer.RollbackAsync(ct);
                    await writer.WriteRowAsync(new object?[] { 20 }, false, ct);
                    await writer.WriteRowAsync(new object?[] { 21 }, true, ct);
                    await writer.CommitAsync(ct);
                    await writer.WriteRowAsync(new object?[] { 22 }, false, ct);
                    await writer.WriteRowAsync(new object?[] { 23 }, false, ct);
                }

                var cmd = connection.CreateCommand($"SELECT * FROM {tableName} ORDER BY id");
                await using var reader = await cmd.ExecuteReaderAsync();
                int expected = 10;

                while (await reader.ReadAsync(ct))
                {
                    var id = reader.GetInt32(0);
                    Assert.Equal(expected++, id);
                }

                Assert.Equal(22, expected);
            }
        }

        [Theory]
        [InlineData(ClickHouseTransactionMode.Default)]
        [InlineData(ClickHouseTransactionMode.Auto)]
        public Task TransactionModeAuto(ClickHouseTransactionMode mode)
        {
            return WithTemporaryTable("tran_auto", "id Int32", Test);

            async Task Test(ClickHouseConnection connection, string tableName, CancellationToken ct)
            {
                await using (var writer = await connection.CreateColumnWriterAsync($"INSERT INTO {tableName}(id) VALUES", ct))
                {
                    await writer.WriteTableAsync(new[] { Enumerable.Range(0, 10) }, 10, mode, ct);
                    await writer.RollbackAsync(ct);
                    await writer.WriteTableAsync(new[] { Enumerable.Range(10, 10) }, 10, mode, ct);
                    await writer.CommitAsync(ct);
                    await writer.WriteTableAsync(new[] { Enumerable.Range(20, 10) }, 10, mode, ct);
                }

                await using (var writer = await connection.CreateColumnWriterAsync($"INSERT INTO {tableName}(id) VALUES", ct))
                {
                    await writer.WriteRowAsync(new object?[] { 30 }, true, ct);
                    await writer.RollbackAsync(ct);
                    await writer.WriteRowAsync(new object?[] { 31 }, true, ct);
                    await writer.CommitAsync(ct);
                    await writer.WriteRowAsync(new object?[] { 32 }, true, ct);
                }

                var cmd = connection.CreateCommand($"SELECT * FROM {tableName} ORDER BY id");
                await using var reader = await cmd.ExecuteReaderAsync();
                int expected = 0;

                while (await reader.ReadAsync(ct))
                {
                    var id = reader.GetInt32(0);
                    Assert.Equal(expected++, id);
                }

                Assert.Equal(33, expected);
            }
        }

        [Fact]
        public Task InsertMapLowCardinalityValues()
        {
            return WithTemporaryTable("map_lc", "id Int32, data Map(LowCardinality(String), Int32)", Test);

            static async Task Test(ClickHouseConnection connection, string tableName, CancellationToken ct)
            {
                var ids = Enumerable.Range(0, 1000).ToList();
                var values = ids.Select(
                    id =>
                        id % 2 == 0
                        ? new[] { KeyValuePair.Create("key1", id * 3), KeyValuePair.Create("key2", id * 3 + 1) }
                        : new[] { KeyValuePair.Create("key2", id * 3 - 1) })
                    .ToList();

                await using (var writer = await connection.CreateColumnWriterAsync($"INSERT INTO {tableName}(id, data) VALUES", ct))
                    await writer.WriteTableAsync(new object[] { ids, values }, ids.Count, ct);

                var cmd = connection.CreateCommand($"SELECT id, data FROM {tableName} ORDER BY id");
                await using (var reader = await cmd.ExecuteReaderAsync(ct))
                {
                    int counter = 0;
                    while (await reader.ReadAsync(ct))
                    {
                        var id = reader.GetInt32(0);
                        var data = reader.GetFieldValue<KeyValuePair<string, int>[]>(1);

                        Assert.Equal(counter++, id);
                        Assert.Equal(values[id], data);
                    }
                    Assert.Equal(1000, counter);
                }

                // Skipping values
                await cmd.ExecuteNonQueryAsync(ct);
            }
        }

        [Fact]
        public Task TransactionModeAutoBackwardCompatibility()
        {
            // Check that the default transaction mode is 'Auto' when not specified
            return WithTemporaryTable("tran_auto_bc", "id Int32", Test);

            async Task Test(ClickHouseConnection connection, string tableName, CancellationToken ct)
            {
                await using (var writer = await connection.CreateColumnWriterAsync($"INSERT INTO {tableName}(id) VALUES", ct))
                {
                    await writer.WriteTableAsync(new[] { Enumerable.Range(0, 10) }, 10, ct);
                    await writer.RollbackAsync(ct);
                    await writer.WriteTableAsync(new[] { Enumerable.Range(10, 10) }, 10, ct);
                    await writer.CommitAsync(ct);
                    await writer.WriteTableAsync(new[] { Enumerable.Range(20, 10) }, 10, ct);
                }

                await using (var writer = await connection.CreateColumnWriterAsync($"INSERT INTO {tableName}(id) VALUES", ct))
                {
                    await writer.WriteRowAsync(new object?[] { 30 }, ct);
                    await writer.RollbackAsync(ct);
                    await writer.WriteRowAsync(new object?[] { 31 }, ct);
                    await writer.CommitAsync(ct);
                    await writer.WriteRowAsync(new object?[] { 32 }, ct);
                }

                var cmd = connection.CreateCommand($"SELECT * FROM {tableName} ORDER BY id");
                await using var reader = await cmd.ExecuteReaderAsync();
                int expected = 0;

                while (await reader.ReadAsync(ct))
                {
                    var id = reader.GetInt32(0);
                    Assert.Equal(expected++, id);
                }

                Assert.Equal(33, expected);
            }
        }

        [Fact]
        public Task InsertIPv6Values()
        {
            return WithTemporaryTable("ipv6", "id Int32, ip IPv6", Test, csb => csb.BufferSize = 33);

            static async Task Test(ClickHouseConnection connection, string tableName, CancellationToken ct)
            {
                var ids = Enumerable.Range(0, 255).ToList();
                var ips = ids.Select(id => string.Format(CultureInfo.InvariantCulture, "192.168.0.{0}", id)).ToList();

                await using (var writer = await connection.CreateColumnWriterAsync($"INSERT INTO {tableName}(id, ip) VALUES", ct))
                    await writer.WriteTableAsync(new object[] { ids, ips }, ids.Count, ct);

                var cmd = connection.CreateCommand($"SELECT id, ip FROM {tableName}");
                await using (var reader = await cmd.ExecuteReaderAsync(ct))
                {
                    int counter = 0;
                    while (await reader.ReadAsync(ct))
                    {
                        var id = reader.GetInt32(0);
                        var ip = reader.GetFieldValue<IPAddress>(1);

                        Assert.Equal(counter++, id);
                        Assert.Equal(IPAddress.Parse("::ffff:" + ips[id]), ip);
                    }

                    Assert.Equal(255, counter);
                }
            }
        }

        protected override string GetTempTableName(string tableNameSuffix)
        {
            return $"{TestTableName}_{tableNameSuffix}";
        }

        public class TableFixture : ClickHouseTestsBase, IDisposable
        {
            private int _identity;

            public TableFixture()
            {
                using var cn = OpenConnection();

                var cmd = cn.CreateCommand($"DROP TABLE IF EXISTS {TestTableName}");
                cmd.ExecuteNonQuery();

                cmd = cn.CreateCommand($"CREATE TABLE {TestTableName}(id Int32, str Nullable(String), num Nullable(Decimal64(6))) ENGINE=Memory");
                cmd.ExecuteNonQuery();
            }

            /// <summary>
            /// Reserves a range of unique sequential identifiers
            /// </summary>
            /// <param name="length">The length of a range</param>
            /// <returns>The first identifier of the reserved range</returns>
            public int ReserveRange(int length)
            {
                Assert.True(length > 0);

                var identity = _identity;
                while (true)
                {
                    var nextIdentity = identity + length;
                    var originalValue = Interlocked.CompareExchange(ref _identity, nextIdentity, identity);
                    if (originalValue == identity)
                        return identity;

                    identity = originalValue;
                }
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
