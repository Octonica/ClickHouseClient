#region License Apache 2.0
/* Copyright 2019-2023 Octonica
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
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Types;
using Octonica.ClickHouseClient.Utils;
using Xunit;

namespace Octonica.ClickHouseClient.Tests
{
    public class TypeTests : ClickHouseTestsBase, IClassFixture<EncodingFixture>
    {
        [Fact]
        public async Task ReadFixedStringScalar()
        {
            await using var connection = await OpenConnectionAsync();

            await using var cmd = connection.CreateCommand("SELECT cast('1234йё' AS FixedString(10))");
            
            var result = await cmd.ExecuteScalarAsync();
            var resultBytes = Assert.IsType<byte[]>(result);

            Assert.Equal(10, resultBytes.Length);
            Assert.Equal(0, resultBytes[^1]);
            Assert.Equal(0, resultBytes[^2]);

            var resultString = Encoding.UTF8.GetString(resultBytes, 0, 8);
            Assert.Equal("1234йё", resultString);
        }

        [Fact]
        public async Task ReadFixedStringWithEncoding()
        {
            const string str = "аaбbсc";
            var encoding = Encoding.GetEncoding("windows-1251");

            await using var connection = await OpenConnectionAsync();

            await using var cmd = connection.CreateCommand($"SELECT cast(convertCharset('{str}', 'UTF8', 'windows-1251') AS FixedString(10))");

            var result = await cmd.ExecuteScalarAsync();
            var resultBytes = Assert.IsType<byte[]>(result);

            Assert.Equal(10, resultBytes.Length);
            Assert.Equal(0, resultBytes[^1]);
            Assert.Equal(0, resultBytes[^2]);
            Assert.Equal(0, resultBytes[^3]);
            Assert.Equal(0, resultBytes[^4]);
            Assert.NotEqual(0, resultBytes[^5]);

            await using var reader = await cmd.ExecuteReaderAsync();
            reader.ConfigureDataReader(new ClickHouseColumnSettings(encoding));

            var success = await reader.ReadAsync();
            Assert.True(success);

            var strResult = reader.GetString(0);
            Assert.Equal(str, strResult);

            success = await reader.ReadAsync();
            Assert.False(success);

            var error = Assert.Throws<ClickHouseException>(() => reader.ConfigureDataReader(new ClickHouseColumnSettings(Encoding.UTF8)));
            Assert.Equal(ClickHouseErrorCodes.DataReaderError, error.ErrorCode);
        }

        [Fact]
        public async Task ReadNullableFixedStringAsArray()
        {
            await WithTemporaryTable("fixed_string_as_array", "id Int32, str Nullable(FixedString(32))", Test);

            async Task Test(ClickHouseConnection connection, string tableName)
            {
                var stringValues = new string?[] { null, "фываasdf", "abcdef", "ghijkl", "null", "пролджэzcvgb", "ячсмить" };
                var byteValues = stringValues.Select(v => v == null ? null : Encoding.UTF8.GetBytes(v)).ToArray();

                await using (var writer = await connection.CreateColumnWriterAsync($"INSERT INTO {tableName}(id, str) VALUES", CancellationToken.None))
                {
                    writer.ConfigureColumn("str", new ClickHouseColumnSettings(Encoding.UTF8));
                    await writer.WriteTableAsync(new object[] { Enumerable.Range(0, 10_000), stringValues }, stringValues.Length, CancellationToken.None);
                }

                var cmd = connection.CreateCommand($"SELECT id, str FROM {tableName} ORDER BY id");

                int count = 0;
                await using var reader = await cmd.ExecuteReaderAsync();
                reader.ConfigureColumn(1, new ClickHouseColumnSettings(Encoding.UTF8));
                char[] charBuffer = new char[stringValues.Select(v => v?.Length ?? 0).Max() + 7];
                byte[] byteBuffer = new byte[32 + 3];
                while (await reader.ReadAsync())
                {
                    var id = reader.GetInt32(0);
                    Assert.Equal(count, id);

                    var expectedStr = stringValues[id];
                    var valueAsCharArray = reader.GetFieldValue<char[]>(1, null);
                    Assert.Equal(expectedStr, valueAsCharArray == null ? null : new string(valueAsCharArray));

                    var valueAsByteArray = reader.GetFieldValue<byte[]>(1, null);
                    byte[]? expectedByteArray = byteValues[id] == null ? null : new byte[32];
                    byteValues[id]?.CopyTo((Memory<byte>)expectedByteArray);
                    Assert.Equal(expectedByteArray, valueAsByteArray);

                    if (expectedStr == null)
                    {
                        Assert.True(reader.IsDBNull(1));
                    }
                    else
                    {
                        var len = (int)reader.GetChars(1, 0, charBuffer, 0, charBuffer.Length);
                        Assert.Equal(expectedStr.Length, len);
                        Assert.Equal(expectedStr, new string(((ReadOnlySpan<char>)charBuffer).Slice(0, len)));

                        len = (int)reader.GetBytes(1, 0, byteBuffer, 0, byteBuffer.Length);
                        Assert.Equal(expectedByteArray!.Length, len);
                        Assert.Equal(expectedByteArray, byteBuffer.Take(len));

                        len = 0;
                        while (len < charBuffer.Length - 7)
                        {
                            var size = Math.Min(3, charBuffer.Length - len - 7);
                            var currentLen = (int)reader.GetChars(1, len, charBuffer, len + 7, size);
                            len += currentLen;

                            if (currentLen < size)
                                break;
                        }

                        Assert.Equal(expectedStr.Length, len);
                        Assert.Equal(expectedStr, new string(((ReadOnlySpan<char>)charBuffer).Slice(7, len)));

                        len = 0;
                        while (len < byteBuffer.Length - 3)
                        {
                            var size = Math.Min(3, byteBuffer.Length - len - 3);
                            var currentLen = (int)reader.GetBytes(1, len, byteBuffer, len + 3, size);
                            len += currentLen;

                            if (currentLen < size)
                                break;
                        }

                        Assert.Equal(expectedByteArray!.Length, len);
                        Assert.Equal(expectedByteArray, byteBuffer.Skip(3).Take(len));
                    }

                    ++count;
                }

                Assert.Equal(stringValues.Length, count);
            }
        }

        [Fact]
        public async Task ReadStringWithEncoding()
        {
            const string str = "АБВГДЕ";
            var encoding = Encoding.GetEncoding("windows-1251");

            await using var connection = await OpenConnectionAsync();
            await using var cmd = connection.CreateCommand($"SELECT convertCharset('{str}', 'UTF8', 'windows-1251') AS c");

            await using var reader = await cmd.ExecuteReaderAsync();
            reader.ConfigureDataReader(new ClickHouseColumnSettings(encoding));

            var success = await reader.ReadAsync();
            Assert.True(success);

            var error = Assert.Throws<ClickHouseException>(() => reader.ConfigureColumn("c", new ClickHouseColumnSettings(Encoding.UTF8)));
            Assert.Equal(ClickHouseErrorCodes.DataReaderError, error.ErrorCode);

            var strResult = reader.GetString(0);
            Assert.Equal(str, strResult);

            success = await reader.ReadAsync();
            Assert.False(success);
        }

        [Fact]
        public async Task ReadStringWithEncodingScalar()
        {
            const string str = "АБВГДЕ";
            var encoding = Encoding.GetEncoding("windows-1251");

            await using var connection = await OpenConnectionAsync();
            await using var cmd = connection.CreateCommand($"SELECT convertCharset('{str}', 'UTF8', 'windows-1251') AS c");

            var strResult = await cmd.ExecuteScalarAsync(new ClickHouseColumnSettings(encoding));
            Assert.Equal(str, strResult);
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadStringParameterScalar(ClickHouseParameterMode parameterMode)
        {
            var strings = new[] {null, "", "abcde", "fghi", "jklm", "nopq", "rst", "uvwxy","z"};

            var byteArray = Encoding.UTF8.GetBytes(strings[4]!);
            var byteMemory = Encoding.UTF8.GetBytes(strings[5]!).AsMemory();
            var byteRoMemory = (ReadOnlyMemory<byte>) Encoding.UTF8.GetBytes(strings[6]!).AsMemory();
            var charMemory = strings[7]!.ToCharArray().AsMemory();
            var charArray = strings[8]!.ToCharArray();
            var values = new object?[] {strings[0], strings[1], strings[2], strings[3].AsMemory(), byteArray, byteMemory, byteRoMemory, charMemory, charArray};

            await using var connection = await OpenConnectionAsync(parameterMode);
            await using var cmd = connection.CreateCommand("SELECT {val}");
            var param = cmd.Parameters.AddWithValue("val", "some_value", DbType.String);
            for (var i = 0; i < values.Length; i++)
            {
                param.Value = values[i];
                var result = await cmd.ExecuteScalarAsync(CancellationToken.None);
                if (values[i] == null)
                    Assert.Equal(result, DBNull.Value);
                else
                    Assert.Equal(strings[i], Assert.IsType<string>(result));
            }
        }

        [Fact]
        public async Task ReadGuidScalar()
        {
            var guidValue = new Guid("74D47928-2423-4FE2-AD45-82E296BF6058");

            await using var connection = await OpenConnectionAsync();
            await using var cmd = connection.CreateCommand($"SELECT cast('{guidValue:D}' AS UUID)");

            var result = await cmd.ExecuteScalarAsync();
            var resultGuid = Assert.IsType<Guid>(result);

            Assert.Equal(guidValue, resultGuid);
        }

        [Fact]
        public async Task ReadDecimal128Scalar()
        {
            var testData = new[] {decimal.Zero, decimal.One, decimal.MinusOne, decimal.MinValue / 100, decimal.MaxValue / 100, decimal.One / 100, decimal.MinusOne / 100};
            
            await using var connection = await OpenConnectionAsync();

            foreach (var testValue in testData)
            {
                await using var cmd = connection.CreateCommand($"SELECT cast('{testValue.ToString(CultureInfo.InvariantCulture)}' AS Decimal128(2))");

                var result = await cmd.ExecuteScalarAsync();
                var resultDecimal = Assert.IsType<decimal>(result);

                Assert.Equal(testValue, resultDecimal);
            }

            await using (var cmd2 = connection.CreateCommand("SELECT cast('-108.4815162342' AS Decimal128(35))"))
            {
                var result2 = await cmd2.ExecuteScalarAsync<decimal>();
                Assert.Equal(-108.4815162342m, result2);
            }

            await using (var cmd2 = connection.CreateCommand("SELECT cast('-999.9999999999999999999999999' AS Decimal128(35))"))
            {
                var result2 = await cmd2.ExecuteScalarAsync<decimal>();
                Assert.Equal(-999.9999999999999999999999999m, result2);
            }
        }

        [Fact]
        public async Task ReadDecimal64Scalar()
        {
            var testData = new[] { decimal.Zero, decimal.One, decimal.MinusOne, 999_999_999_999_999.999m, -999_999_999_999_999.999m, decimal.One / 1000, decimal.MinusOne / 1000 };

            await using var connection = await OpenConnectionAsync();

            foreach (var testValue in testData)
            {
                await using var cmd = connection.CreateCommand($"SELECT cast('{testValue.ToString(CultureInfo.InvariantCulture)}' AS Decimal64(3))");

                var result = await cmd.ExecuteScalarAsync();
                var resultDecimal = Assert.IsType<decimal>(result);

                Assert.Equal(testValue, resultDecimal);
            }
        }

        [Fact]
        public async Task ReadDecimal32Scalar()
        {
            var testData = new[] { decimal.Zero, decimal.One, decimal.MinusOne, 9.9999999m, -9.9999999m, decimal.One / 100_000_000, decimal.MinusOne / 100_000_000 };

            await using var connection = await OpenConnectionAsync();

            foreach (var testValue in testData)
            {
                await using var cmd = connection.CreateCommand($"SELECT cast('{testValue.ToString(CultureInfo.InvariantCulture)}' AS Decimal32(8))");

                var result = await cmd.ExecuteScalarAsync();
                var resultDecimal = Assert.IsType<decimal>(result);

                Assert.Equal(testValue, resultDecimal);
            }
        }

        [Fact]
        public async Task ReadDateTimeScalar()
        {
            await using var connection = await OpenConnectionAsync();

            await using var cmd = connection.CreateCommand("SELECT cast('2015-04-21 14:59:44' AS DateTime)");

            var result = await cmd.ExecuteScalarAsync();
            var resultDateTime = Assert.IsType<DateTimeOffset>(result);

            Assert.Equal(new DateTime(2015, 4, 21, 14, 59, 44), resultDateTime.DateTime);
        }

        [Fact]
        public async Task ReadDateTimeWithTimezoneScalar()
        {
            await using var connection = await OpenConnectionAsync();

            var tzName = TimeZoneHelper.GetTimeZoneId(TimeZoneInfo.Local);

            await using var cmd = connection.CreateCommand($"SELECT toDateTime('2015-04-21 14:59:44', '{tzName}')");

            var result = await cmd.ExecuteScalarAsync();
            var resultDateTime = Assert.IsType<DateTimeOffset>(result);

            Assert.Equal(new DateTime(2015, 4, 21, 14, 59, 44), resultDateTime);
        }

        [Fact]
        public async Task ReadDateTime64Scalar()
        {
            await using var connection = await OpenConnectionAsync();

            await using var cmd = connection.CreateCommand("SELECT cast('2015-04-21 14:59:44.12345' AS DateTime64)");

            var result = await cmd.ExecuteScalarAsync();
            var resultDateTime = Assert.IsType<DateTimeOffset>(result);

            Assert.Equal(new DateTime(2015, 4, 21, 14, 59, 44).AddMilliseconds(123), resultDateTime.DateTime);
        }

        [Fact]
        public async Task ReadDateTime64WithTimezoneScalar()
        {
            await using var connection = await OpenConnectionAsync();

            var tzName = TimeZoneHelper.GetTimeZoneId(TimeZoneInfo.Local);

            await using var cmd = connection.CreateCommand($"SELECT cast('2015-04-21 14:59:44.123456789' AS DateTime64(9,'{tzName}'))");

            var result = await cmd.ExecuteScalarAsync();
            var resultDateTime = Assert.IsType<DateTimeOffset>(result);

            Assert.Equal(new DateTime(2015, 4, 21, 14, 59, 44).Add(TimeSpan.FromMilliseconds(123.4567)), resultDateTime);
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadDateTime64ParameterScalar(ClickHouseParameterMode parameterMode)
        {
            await using var connection = await OpenConnectionAsync(parameterMode);
            var timeZone = connection.GetServerTimeZone();
            var unixEpochOffset = timeZone.GetUtcOffset(DateTime.UnixEpoch);

            var values = new[]
            {
                default,
                DateTime.UnixEpoch.Add(TimeSpan.FromMilliseconds(1111.1111) + unixEpochOffset),
                new DateTime(2531, 3, 5, 7, 9, 23).Add(TimeSpan.FromMilliseconds(123.45)),
                new DateTime(1984, 4, 21, 14, 59, 44).Add(TimeSpan.FromMilliseconds(123.4567))
            };

            await using var cmd = connection.CreateCommand("SELECT {v}");
            var parameter = new ClickHouseParameter("v") {ClickHouseDbType = ClickHouseDbType.DateTime64};
            cmd.Parameters.Add(parameter);

            for (int precision = 0; precision < 10; precision++)
            {
                parameter.Precision = (byte) precision;
                var div = TimeSpan.TicksPerSecond / (long) Math.Pow(10, precision);
                if (div == 0)
                    div = -(long) Math.Pow(10, precision) / TimeSpan.TicksPerSecond;

                foreach (var value in values)
                {
                    parameter.Value = value;

                    var result = await cmd.ExecuteScalarAsync();
                    var resultDateTime = Assert.IsType<DateTimeOffset>(result);

                    DateTime expectedValue;
                    if (value == default)
                        expectedValue = default;
                    else if (div > 0)
                        expectedValue = new DateTime(value.Ticks / div * div);
                    else
                        expectedValue = value;

                    Assert.Equal(expectedValue, resultDateTime.DateTime);
                }

                // Min and max values must be adjusted to the server's timezone
                var unixEpochValue = new DateTime(DateTime.UnixEpoch.Ticks + unixEpochOffset.Ticks);
                parameter.Value = unixEpochValue;

                var unixEpochResult = await cmd.ExecuteScalarAsync();
                var unixEpochResultDto = Assert.IsType<DateTimeOffset>(unixEpochResult);

                Assert.Equal(default, unixEpochResultDto);

                if (div > 0)
                {
                    var offset = timeZone.GetUtcOffset(DateTime.MaxValue);
                    long ticks = DateTime.MaxValue.Ticks;
                    if (offset < TimeSpan.Zero)
                        ticks += offset.Ticks;

                    var maxValue = new DateTimeOffset(ticks, offset).DateTime;
                    parameter.Value = maxValue;
                    var result = await cmd.ExecuteScalarAsync<DateTime>();

                    var expectedValue = new DateTime(maxValue.Ticks / div * div);
                    Assert.Equal(expectedValue, result);
                }
                else
                {
                    DateTimeOffset maxValueDto;
                    if (div == -100)
                        maxValueDto = DateTimeOffset.ParseExact("2554-07-21T23:34:33.7095516Z", "O", CultureInfo.InvariantCulture);
                    else
                        maxValueDto = DateTimeOffset.ParseExact("7815-07-17T19:45:37.0955161Z", "O", CultureInfo.InvariantCulture);

                    var offset = timeZone.GetUtcOffset(maxValueDto);
                    var maxValue = new DateTime(maxValueDto.Ticks + offset.Ticks);

                    parameter.Value = maxValue;
                    var result = await cmd.ExecuteScalarAsync<DateTime>();
                    Assert.Equal(maxValue, result);

                    parameter.Value = maxValue.AddTicks(1);
                    var exception = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync());
                    Assert.IsAssignableFrom<OverflowException>(exception.InnerException);
                }
            }
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadDateTimeParameterWithTimezoneScalar(ClickHouseParameterMode parameterMode)
        {
            var valueShort = new DateTime(2014, 7, 5, 12, 13, 14);
            var value = valueShort.Add(TimeSpan.FromMilliseconds(123.4567));

            const string targetTzCode = "Asia/Magadan";
            var targetTz = TimeZoneHelper.GetTimeZoneInfo(targetTzCode);

            await using var connection = await OpenConnectionAsync(parameterMode);
            await using var cmd = connection.CreateCommand($"SELECT toTimeZone({{d}}, '{targetTzCode}')");
            var parameter = new ClickHouseParameter("d") {Value = value, TimeZone = TimeZoneHelper.GetTimeZoneInfo("Pacific/Niue"), Precision = 4};
            cmd.Parameters.Add(parameter);
            var deltaOffset = targetTz.GetUtcOffset(valueShort) - parameter.TimeZone.GetUtcOffset(valueShort);

            object? resultObj;
            DateTimeOffset result;
            foreach (var parameterType in new ClickHouseDbType?[] {null, ClickHouseDbType.DateTime, ClickHouseDbType.DateTimeOffset})
            {
                if (parameterType != null)
                    parameter.ClickHouseDbType = parameterType.Value;

                resultObj = await cmd.ExecuteScalarAsync();
                result = Assert.IsType<DateTimeOffset>(resultObj);
                Assert.Equal(valueShort + deltaOffset, result.DateTime);
                Assert.Equal(targetTz.GetUtcOffset(result), result.Offset);
            }

            parameter.ClickHouseDbType = ClickHouseDbType.DateTime2;
            resultObj = await cmd.ExecuteScalarAsync();
            result = Assert.IsType<DateTimeOffset>(resultObj);
            Assert.Equal(value + deltaOffset, result.DateTime);
            Assert.Equal(targetTz.GetUtcOffset(result), result.Offset);

            parameter.ClickHouseDbType = ClickHouseDbType.DateTime64;
            resultObj = await cmd.ExecuteScalarAsync();
            result = Assert.IsType<DateTimeOffset>(resultObj);
            Assert.Equal(valueShort + deltaOffset + TimeSpan.FromMilliseconds(123.4), result.DateTime);
            Assert.Equal(targetTz.GetUtcOffset(result), result.Offset);

            parameter.ResetDbType();
            resultObj = await cmd.ExecuteScalarAsync();
            result = Assert.IsType<DateTimeOffset>(resultObj);
            deltaOffset = targetTz.GetUtcOffset(valueShort) - connection.GetServerTimeZone().GetUtcOffset(valueShort);
            Assert.Equal(valueShort + deltaOffset, result.DateTime);
        }

        [Fact]
        public async Task ReadFloatScalar()
        {
            await using var connection = await OpenConnectionAsync();

            var expectedValue = 1234567890.125f;
            await using var cmd = connection.CreateCommand($"SELECT CAST('{expectedValue:#.#}' AS Float32)");

            var result = await cmd.ExecuteScalarAsync();
            var resultFloat = Assert.IsType<float>(result);

            Assert.Equal(expectedValue, resultFloat);
        }

        [Fact]
        public async Task ReadDoubleScalar()
        {
            await using var connection = await OpenConnectionAsync();

            await using var cmd = connection.CreateCommand("SELECT -123456789109876.125");

            var result = await cmd.ExecuteScalarAsync();
            var resultDouble = Assert.IsType<double>(result);

            Assert.Equal(-123456789109876.125, resultDouble);
        }

        [Fact]
        public async Task ReadNothingScalar()
        {
            await using var connection = await OpenConnectionAsync();

            await using var cmd = connection.CreateCommand("SELECT NULL");

            var result = await cmd.ExecuteScalarAsync();
            Assert.IsType<DBNull>(result);
        }

        [Fact]
        public async Task ReadEmptyArrayScalar()
        {
            await using var connection = await OpenConnectionAsync();

            await using var cmd = connection.CreateCommand("SELECT []");

            var result = await cmd.ExecuteScalarAsync();
            var objResult = Assert.IsType<object[]>(result);
            Assert.Empty(objResult);
        }

        [Fact]
        public async Task ReadByteArrayScalar()
        {
            await using var connection = await OpenConnectionAsync();

            await using var cmd = connection.CreateCommand("SELECT [4, 8, 15, 16, 23, 42]");

            var result = await cmd.ExecuteScalarAsync<byte[]>();

            Assert.Equal(new byte[] {4, 8, 15, 16, 23, 42}, result);
        }

        [Fact]
        public async Task ReadNullableByteArrayScalar()
        {
            await using var connection = await OpenConnectionAsync();

            await using var cmd = connection.CreateCommand("SELECT [4, NULL, 8, NULL, 15, NULL, 16, NULL, 23, NULL, 42]");

            var result = await cmd.ExecuteScalarAsync();
            var resultArr = Assert.IsType<byte?[]>(result);

            Assert.Equal(new byte?[] { 4, null, 8, null, 15, null, 16, null, 23, null, 42 }, resultArr);
        }

        [Fact]
        public async Task ReadArrayOfArraysOfArraysScalar()
        {
            const string query = @"SELECT 
                                        [
                                            [
                                                [1],
                                                [],
                                                [2, NULL]
                                            ],
                                            [
                                                [3]
                                            ]
                                        ]";

            await using var connection = await OpenConnectionAsync();

            await using var cmd = connection.CreateCommand(query);

            var result = await cmd.ExecuteScalarAsync();
            var resultArr = Assert.IsType<byte?[][][]>(result);

            Assert.NotNull(resultArr);
            Assert.Equal(2, resultArr.Length);

            Assert.NotNull(resultArr[0]);
            Assert.Equal(3, resultArr[0].Length);

            Assert.Equal(new byte?[] {1}, resultArr[0][0]);
            Assert.Equal(new byte?[0], resultArr[0][1]);
            Assert.Equal(new byte?[] {2, null}, resultArr[0][2]);

            Assert.NotNull(resultArr[1]);
            Assert.Equal(1, resultArr[1].Length);

            Assert.Equal(new byte?[] {3}, resultArr[1][0]);
        }

        [Fact]
        public async Task ReadNullableByteArrayAsUInt64ArrayScalar()
        {
            await using var connection = await OpenConnectionAsync();

            await using var cmd = connection.CreateCommand("SELECT [4, NULL, 8, NULL, 15, NULL, 16, NULL, 23, NULL, 42]");

            var result = await cmd.ExecuteScalarAsync<ulong?[]>();

            Assert.Equal(new ulong?[] { 4, null, 8, null, 15, null, 16, null, 23, null, 42 }, result);
        }

        [Fact]
        public async Task ReadNullableStringArrayScalar()
        {
            await using var connection = await OpenConnectionAsync();

            await using var cmd = connection.CreateCommand("SELECT ['All', NULL, 'your', NULL, 'base', NULL, 'are', NULL, 'belong', NULL, 'to', NULL, 'us!']");

            var result = await cmd.ExecuteScalarAsync<string?[]>();

            Assert.Equal(new[] {"All", null, "your", null, "base", null, "are", null, "belong", null, "to", null, "us!"}, result);
        }

        [Fact]
        public async Task ReadNullableNothingArrayScalar()
        {
            await using var connection = await OpenConnectionAsync();

            await using var cmd = connection.CreateCommand("SELECT [NULL,NULL,NULL]");

            var result = await cmd.ExecuteScalarAsync();
            var resultArr = Assert.IsType<object?[]>(result);

            Assert.Equal(new object[3], resultArr);
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadArrayParameterScalar(ClickHouseParameterMode parameterMode)
        {
            await using var connection = await OpenConnectionAsync(parameterMode);

            await using var cmd = connection.CreateCommand("SELECT {p}");
            var expectedResult = new[] {4, 8, 15, 16, 23, 42};
            var param = cmd.Parameters.AddWithValue("p", expectedResult);

            var result = await cmd.ExecuteScalarAsync();
            var intResult = Assert.IsType<int[]>(result);

            Assert.Equal(expectedResult, intResult);

            param.IsArray = true;
            param.DbType = DbType.Decimal;

            result = await cmd.ExecuteScalarAsync();
            var decResult = Assert.IsType<decimal[]>(result);

            Assert.Equal(expectedResult.Select(v => (decimal) v), decResult);
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadArrayOfArraysParameterScalar(ClickHouseParameterMode parameterMode)
        {
            await using var connection = await OpenConnectionAsync(parameterMode);

            await using var cmd = connection.CreateCommand("SELECT {p}");
            var expectedResult = new List<uint[]> {new uint[] {4, 8}, new uint[] {15, 16, 23}, new uint[] {42}};
            var param = cmd.Parameters.AddWithValue("p", expectedResult);

            var result = await cmd.ExecuteScalarAsync();
            var intResult = Assert.IsType<uint[][]>(result);

            Assert.Equal(expectedResult.Count, intResult.Length);
            for (int i = 0; i < expectedResult.Count; i++)
                Assert.Equal(expectedResult[i], intResult[i]);

            param.ArrayRank = 2;
            param.DbType = DbType.UInt64;
            param.IsNullable = true;

            result = await cmd.ExecuteScalarAsync();
            var decResult = Assert.IsType<ulong?[][]>(result);

            Assert.Equal(expectedResult.Count, decResult.Length);
            for (int i = 0; i < decResult.Length; i++)
                Assert.Equal(expectedResult[i].Select(v => (ulong?) v), decResult[i]);
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadMultidimensionalArrayParameterScalar(ClickHouseParameterMode parameterMode)
        {
            await using var connection = await OpenConnectionAsync(parameterMode);

            await using var cmd = connection.CreateCommand("SELECT {p}");
            var expectedResult = new List<int?[,]> {new[,] {{(int?) 4, 8}, {15, 16}, {23, 42}}, new[,] {{1}, {(int?) null}, {3}}, new[,] {{(int?) -4, -8, -15}, {-16, -23, -42}}};
            var param = cmd.Parameters.AddWithValue("p", expectedResult);

            var result = await cmd.ExecuteScalarAsync();
            var intResult = Assert.IsType<int?[][][]>(result);

            Assert.Equal(expectedResult.Count, intResult.Length);
            for (int i = 0; i < expectedResult.Count; i++)
            {
                var expectedLengthI = expectedResult[i].GetLength(0);
                var expectedLengthJ = expectedResult[i].GetLength(1);
                Assert.Equal(expectedLengthI, intResult[i].Length);
                for (int j = 0; j < expectedLengthI; j++)
                {
                    Assert.Equal(expectedLengthJ, intResult[i][j].Length);
                    for (int k = 0; k < expectedLengthJ; k++)
                        Assert.Equal(expectedResult[i][j, k], intResult[i][j][k]);
                }
            }

            param.ArrayRank = 3;
            param.DbType = DbType.Decimal;

            result = await cmd.ExecuteScalarAsync();
            var decResult = Assert.IsType<decimal?[][][]>(result);

            Assert.Equal(expectedResult.Count, decResult.Length);
            for (int i = 0; i < expectedResult.Count; i++)
            {
                var expectedLengthI = expectedResult[i].GetLength(0);
                var expectedLengthJ = expectedResult[i].GetLength(1);
                Assert.Equal(expectedLengthI, decResult[i].Length);
                for (int j = 0; j < expectedLengthI; j++)
                {
                    Assert.Equal(expectedLengthJ, decResult[i][j].Length);
                    for (int k = 0; k < expectedLengthJ; k++)
                        Assert.Equal(expectedResult[i][j, k], decResult[i][j][k]);
                }
            }
        }

        [Fact]
        public async Task ReadEnumColumn()
        {
            await using var connection = await OpenConnectionAsync();

            await using var cmd = connection.CreateCommand(@"SELECT CAST(T.col AS Enum(''=0, '\\e\s\c\\a\p\\e'=1, '\'val\''=2, '\r\n\t\d\\\r\n'=3,'\a\b\c\d\e\f\g\h\i\j\k\l\m\n\o\p\q\r\s\t\u\v\w\x20\y\z'=4)) AS enumVal, toString(enumVal) AS strVal
    FROM (SELECT 0 AS col UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) AS T");

            await using var reader = await cmd.ExecuteReaderAsync();
            var columnType = reader.GetFieldTypeInfo(0);
            var typeNames = new Dictionary<int, string>();
            for(int i=0; i<columnType.TypeArgumentsCount; i++)
            {
                var obj = columnType.GetTypeArgument(i);
                var pair = Assert.IsType<KeyValuePair<string, sbyte>>(obj);
                typeNames.Add(pair.Value, pair.Key);
            }

            int bitmap = 0;
            while (await reader.ReadAsync())
            {
                var value = reader.GetFieldValue<int>(0);
                var defaultValue = reader.GetValue(0);
                var strValue = Assert.IsType<string>(defaultValue);
                var expectedStrValue = reader.GetFieldValue<string>(1);
                
                Assert.Equal(expectedStrValue, strValue);

                switch (value)
                {
                    case 0:
                        Assert.Equal(string.Empty, strValue);
                        break;
                    case 1:
                        Assert.Equal(@"\e\s\c\a\p\e", strValue);
                        break;
                    case 2:
                        Assert.Equal("'val'", strValue);
                        break;
                    case 3:
                        Assert.Equal("\r\n\t\\d\\\r\n", strValue);
                        break;
                    case 4:
                        Assert.Equal("\a\b\\c\\d\u001b\f\\g\\h\\i\\j\\k\\l\\m\n\\o\\p\\q\r\\s\t\\u\v\\w\x20\\y\\z", strValue);
                        break;
                    default:
                        Assert.True(false, $"Unexpected value: {value}.");
                        break;
                }

                Assert.Equal(strValue, typeNames[value]);

                bitmap ^= 1 << value;
            }

            Assert.Equal(31, bitmap);
        }

        [Fact]
        public async Task ReadEnumScalar()
        {
            await using var connection = await OpenConnectionAsync();

            await using var cmd = connection.CreateCommand("SELECT CAST(42 AS Enum('' = 0, 'b' = -129, 'Hello, world! :)' = 42))");

            var result = await cmd.ExecuteScalarAsync();
            Assert.Equal("Hello, world! :)", result);
        }

        [Fact]
        public async Task ReadClrEnumScalar()
        {
            await using var connection = await OpenConnectionAsync();

            await using var cmd = connection.CreateCommand("SELECT CAST(42 AS Enum('' = 0, 'b' = -129, 'Hello, world! :)' = 42))");

            var settings = new ClickHouseColumnSettings(new ClickHouseEnumConverter<TestEnum>());
            var result = await cmd.ExecuteScalarAsync(settings);
            Assert.Equal(TestEnum.Value1, result);
        }

        [Fact]
        public async Task ReadInt32ArrayColumn()
        {
            int?[]?[] expected = new int?[10][];

            var queryBuilder = new StringBuilder("SELECT T.* FROM (").AppendLine();
            for (int i = 0, k = 0; i < expected.Length; i++)
            {
                if (i > 0)
                    queryBuilder.AppendLine().Append("UNION ALL ");

                queryBuilder.Append("SELECT ").Append(i).Append(" AS num, [");
                var expectedArray = expected[i] = new int?[i + 1];
                for (int j = 0; j < expectedArray.Length; j++, k++)
                {
                    if (j > 0)
                        queryBuilder.Append(", ");

                    if ((k % 3 == 0) == (k % 5 == 0))
                    {
                        queryBuilder.Append("CAST(").Append(k).Append(" AS Nullable(Int32))");
                        expectedArray[j] = k;
                    }
                    else
                    {
                        queryBuilder.Append("CAST(NULL AS Nullable(Int32))");
                    }
                }

                queryBuilder.Append("] AS arr");
            }

            var queryString = queryBuilder.AppendLine(") AS T").Append("ORDER BY T.num DESC").ToString();

            await using var connection = await OpenConnectionAsync();

            var cmd = connection.CreateCommand(queryString);
            await using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var num = reader.GetInt32(0);

                var expectedArray = expected[num];
                Assert.NotNull(expectedArray);

                var value = reader.GetValue(1);
                var array = Assert.IsType<int?[]>(value);

                Assert.Equal(expectedArray, array);

                expected[num] = null;
            }

            Assert.All(expected, Assert.Null);
        }

        [Fact]
        public async Task SkipInt32ArrayColumn()
        {
            int?[]?[] expected = new int?[10][];

            var queryBuilder = new StringBuilder("SELECT T.* FROM (").AppendLine();
            for (int i = 0, k = 0; i < expected.Length; i++)
            {
                if (i > 0)
                    queryBuilder.AppendLine().Append("UNION ALL ");

                queryBuilder.Append("SELECT ").Append(i).Append(" AS num, [");
                var expectedArray = expected[i] = new int?[i + 1];
                for (int j = 0; j < expectedArray.Length; j++, k++)
                {
                    if (j > 0)
                        queryBuilder.Append(", ");

                    if ((k % 3 == 0) == (k % 5 == 0))
                    {
                        queryBuilder.Append("CAST(").Append(k).Append(" AS Nullable(Int32))");
                        expectedArray[j] = k;
                    }
                    else
                    {
                        queryBuilder.Append("CAST(NULL AS Nullable(Int32))");
                    }
                }

                queryBuilder.Append("] AS arr");
            }

            var queryString = queryBuilder.AppendLine(") AS T").Append("ORDER BY T.num DESC").ToString();

            await using var connection = await OpenConnectionAsync();

            var cmd = connection.CreateCommand(queryString);
            await cmd.ExecuteNonQueryAsync();
        }

        [Fact]
        public async Task ReadTuplesWithDifferentLength()
        {
            var tupleSb = new StringBuilder("tuple(");
            var querySb = new StringBuilder("SELECT").AppendLine();

            for (int i = 0; i < 15; i++)
            {
                if (i > 0)
                {
                    tupleSb.Append(", ");
                    querySb.AppendLine(", ");
                }

                tupleSb.Append("CAST(").Append(i + 1).Append(" AS Int32)");
                querySb.Append(tupleSb, 0, tupleSb.Length).Append($") AS t{i}");
            }

            var queryString = querySb.ToString();

            await using var connection = await OpenConnectionAsync();

            var cmd = connection.CreateCommand(queryString);
            await using var reader = await cmd.ExecuteReaderAsync();

            var success = await reader.ReadAsync();
            Assert.True(success);

            var expected = new object[]
            {
                Tuple.Create(1),
                Tuple.Create(1, 2),
                Tuple.Create(1, 2, 3),
                Tuple.Create(1, 2, 3, 4),
                Tuple.Create(1, 2, 3, 4, 5),
                Tuple.Create(1, 2, 3, 4, 5, 6),
                Tuple.Create(1, 2, 3, 4, 5, 6, 7),
                Tuple.Create(1, 2, 3, 4, 5, 6, 7, 8),
                new Tuple<int, int, int, int, int, int, int, Tuple<int, int>>(1, 2, 3, 4, 5, 6, 7, Tuple.Create(8, 9)),
                new Tuple<int, int, int, int, int, int, int, Tuple<int, int, int>>(1, 2, 3, 4, 5, 6, 7, Tuple.Create(8, 9, 10)),
                new Tuple<int, int, int, int, int, int, int, Tuple<int, int, int, int>>(1, 2, 3, 4, 5, 6, 7, Tuple.Create(8, 9, 10, 11)),
                new Tuple<int, int, int, int, int, int, int, Tuple<int, int, int, int, int>>(1, 2, 3, 4, 5, 6, 7, Tuple.Create(8, 9, 10, 11, 12)),
                new Tuple<int, int, int, int, int, int, int, Tuple<int, int, int, int, int, int>>(1, 2, 3, 4, 5, 6, 7, Tuple.Create(8, 9, 10, 11, 12, 13)),
                new Tuple<int, int, int, int, int, int, int, Tuple<int, int, int, int, int, int, int>>(1, 2, 3, 4, 5, 6, 7, Tuple.Create(8, 9, 10, 11, 12, 13, 14)),
                new Tuple<int, int, int, int, int, int, int, Tuple<int, int, int, int, int, int, int, Tuple<int>>>(1, 2, 3, 4, 5, 6, 7, Tuple.Create(8, 9, 10, 11, 12, 13, 14, 15))
            };

            for (int i = 0; i < 15; i++)
            {
                var columnType = reader.GetFieldType(i);
                Assert.Equal(expected[i].GetType(), columnType);

                var columnValue = reader.GetValue(i);

                if (i == 12)
                {
                    var reinterpretedValue = reader.GetFieldValue<Tuple<int?, long, long?, int, int?, int, int?, Tuple<long, long?, int, int?, int, int?>>>(i);
                    var expectedValue = new Tuple<int?, long, long?, int, int?, int, int?, Tuple<long, long?, int, int?, int, int?>>(
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        Tuple.Create((long) 8, (long?) 9, 10, (int?) 11, 12, (int?) 13));

                    Assert.Equal(expectedValue, reinterpretedValue);
                }

                Assert.Equal(expected[i].GetType(), columnValue.GetType());
                Assert.Equal(columnValue, expected[i]);
            }

            Assert.False(await reader.ReadAsync());
        }

        [Fact]
        public async Task ReadValueTuplesWithDifferentLength()
        {
            var tupleSb = new StringBuilder("tuple(");
            var querySb = new StringBuilder("SELECT").AppendLine();

            for (int i = 0; i < 15; i++)
            {
                if (i > 0)
                {
                    tupleSb.Append(", ");
                    querySb.AppendLine(", ");
                }

                tupleSb.Append("CAST(").Append(i + 1).Append(" AS Int32)");
                querySb.Append(tupleSb, 0, tupleSb.Length).Append($") AS t{i}");
            }

            var queryString = querySb.ToString();

            await using var connection = await OpenConnectionAsync();

            var cmd = connection.CreateCommand(queryString);
            await using var reader = await cmd.ExecuteReaderAsync();

            var success = await reader.ReadAsync();
            Assert.True(success);
            Assert.Equal(15, reader.FieldCount);

            for (int i = 0; i < reader.FieldCount; i++)
            {
                switch (i)
                {
                    case 0:
                        AssertEqual(reader, i, new ValueTuple<int>(1));
                        break;
                    case 1:
                        AssertEqual(reader, i, (1, (int?) 2));
                        break;
                    case 2:
                        AssertEqual(reader, i, (1, 2, 3));
                        break;
                    case 3:
                        AssertEqual(reader, i, (1, 2, 3, 4));
                        break;
                    case 4:
                        AssertEqual(reader, i, (1, 2, 3, 4, 5));
                        break;
                    case 5:
                        AssertEqual(reader, i, (1, 2, 3, 4, 5, 6));
                        break;
                    case 6:
                        AssertEqual(reader, i, (1, 2, 3, 4, 5, 6, 7));
                        break;
                    case 7:
                        AssertEqual(reader, i, (1, 2, 3, 4, 5, 6, 7, (long) 8));
                        break;
                    case 8:
                        AssertEqual(reader, i, (1, 2, 3, 4, (int?) 5, 6, 7, (long?) 8, 9));
                        break;
                    case 9:
                        AssertEqual(reader, i, (1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
                        break;
                    case 10:
                        AssertEqual(reader, i, (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11));
                        break;
                    case 11:
                        AssertEqual(reader, i, (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12));
                        break;
                    case 12:
                        AssertEqual<(int?, long, long?, int, int?, int, int?, long, long?, int, int?, int, int?)>(reader, i, (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13));
                        break;
                    case 13:
                        AssertEqual(reader, i, (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14));
                        break;
                    case 14:
                        AssertEqual(reader, i, (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, (long?) 15));
                        break;
                    default:
                        Assert.True(i >= 0 && i < 15, "Too many columns.");
                        break;
                }
            }

            Assert.False(await reader.ReadAsync());

            static void AssertEqual<T>(DbDataReader reader, int ordinal, T expectedValue)
            {
                var value = reader.GetFieldValue<T>(ordinal);
                Assert.Equal(expectedValue, value);
            }
        }

        [Fact]
        public async Task ReadTupleColumn()
        {
            const string query = @"SELECT T.tval
    FROM (
        SELECT tuple(cast(1 as Decimal(13, 4)), cast('one' as Nullable(String)), cast('1999-09-09 09:09:09' as Nullable(DateTime))) AS tval
        UNION ALL SELECT tuple(2, 'two', cast('2019-12-11 16:55:54' as DateTime('Asia/Yekaterinburg')))
        UNION ALL SELECT tuple(3, null, cast('2007-01-11 05:32:48' as DateTime))) T
    ORDER BY T.tval.3";

            await using var connection = await OpenConnectionAsync();

            var cmd = connection.CreateCommand(query);
            await using var reader = await cmd.ExecuteReaderAsync();

            int count = 0;
            while (await reader.ReadAsync())
            {
                var value = reader.GetFieldValue<Tuple<decimal, string?, DateTime?>>(0);
                DateTime dt;
                switch (count)
                {
                    case 0:
                        Assert.Equal(1, value.Item1);
                        Assert.Equal("one", value.Item2);
                        dt = new DateTime(1999, 9, 9, 9, 9, 9);
                        Assert.Equal(dt, value.Item3);
                        break;

                    case 1:
                        Assert.Equal(3, value.Item1);
                        Assert.Null(value.Item2);
                        dt = new DateTime(2007, 1, 11, 5, 32, 48);
                        Assert.Equal(dt, value.Item3);
                        break;

                    case 2:
                        Assert.Equal(2, value.Item1);
                        Assert.Equal("two", value.Item2);
                        var tz = TimeZoneHelper.GetTimeZoneInfo("Asia/Yekaterinburg");
                        dt = TimeZoneInfo.ConvertTime(new DateTime(2019, 12, 11, 16, 55, 54), tz, connection.GetServerTimeZone());
                        Assert.Equal(dt, value.Item3);
                        break;

                    default:
                        Assert.False(true, "Too many rows.");
                        break;
                }

                ++count;
            }

            Assert.Equal(3, count);
        }

        [Fact]
        public async Task ReadValueTupleColumn()
        {
            const string query = @"SELECT T.tval
    FROM (
        SELECT tuple(cast(1 as Decimal(13, 4)), cast('one' as Nullable(String)), cast('1999-09-09 09:09:09' as Nullable(DateTime))) AS tval
        UNION ALL SELECT tuple(2, 'two', cast('2019-12-11 16:55:54' as DateTime('Asia/Yekaterinburg')))
        UNION ALL SELECT tuple(3, null, cast('2007-01-11 05:32:48' as DateTime))) T
    ORDER BY T.tval.3";

            await using var connection = await OpenConnectionAsync();

            var cmd = connection.CreateCommand(query);
            await using var reader = await cmd.ExecuteReaderAsync();

            int count = 0;
            while (await reader.ReadAsync())
            {
                var value = reader.GetFieldValue<(decimal number, string? str, DateTime? date)>(0);
                DateTime dt;
                switch (count)
                {
                    case 0:
                        Assert.Equal(1, value.number);
                        Assert.Equal("one", value.str);
                        dt = new DateTime(1999, 9, 9, 9, 9, 9);
                        Assert.Equal(dt, value.date);
                        break;

                    case 1:
                        Assert.Equal(3, value.number);
                        Assert.Null(value.str);
                        dt = new DateTime(2007, 1, 11, 5, 32, 48);
                        Assert.Equal(dt, value.date);
                        break;

                    case 2:
                        Assert.Equal(2, value.number);
                        Assert.Equal("two", value.str);
                        dt = new DateTime(2019, 12, 11, 16, 55, 54);
                        dt = TimeZoneInfo.ConvertTime(dt, TimeZoneHelper.GetTimeZoneInfo("Asia/Yekaterinburg"), connection.GetServerTimeZone());
                        Assert.Equal(dt, value.date);
                        break;

                    default:
                        Assert.False(true, "Too many rows.");
                        break;
                }

                ++count;
            }

            Assert.Equal(3, count);
        }

        [Fact]
        public async Task ReadNamedTupleScalar()
        {
            await using var connection = await OpenConnectionAsync();
            var cmd = connection.CreateCommand("SELECT CAST(('hello', 1, -1) AS Tuple(name String, id UInt32, `e \\`e\\` e` Int32))");
            var result = await cmd.ExecuteScalarAsync<(string firstItem, uint secondItem, int thirdItem)>();
            Assert.Equal(("hello", 1u, -1), result);
        }

        [Fact]
        public async Task SkipTupleColumn()
        {
            const string query = @"SELECT T.tval
    FROM (
        SELECT tuple(cast(1 as Decimal(13, 4)), cast('one' as Nullable(String)), cast('1999-09-09 09:09:09' as Nullable(DateTime))) AS tval
        UNION ALL SELECT tuple(2, 'two', cast('2019-12-11 16:55:54' as DateTime('Asia/Yekaterinburg')))
        UNION ALL SELECT tuple(3, null, cast('2007-01-11 05:32:48' as DateTime))) T
    ORDER BY T.tval.3";

            await using var connection = await OpenConnectionAsync();

            var cmd = connection.CreateCommand(query);
            await cmd.ExecuteNonQueryAsync();
        }

        [Fact]
        public async Task ReadIpV4Column()
        {
            try
            {
                await using var connection = await OpenConnectionAsync();

                var cmd = connection.CreateCommand("DROP TABLE IF EXISTS ip4_test");
                await cmd.ExecuteNonQueryAsync();

                cmd = connection.CreateCommand("CREATE TABLE ip4_test(val IPv4, strVal String) ENGINE=Memory");
                await cmd.ExecuteNonQueryAsync();

                cmd.CommandText = "INSERT INTO ip4_test(val, strVal) VALUES ('116.253.40.133','116.253.40.133')('10.0.151.56','10.0.151.56')('192.0.121.234','192.0.121.234')";
                await cmd.ExecuteNonQueryAsync();

                cmd.CommandText = "SELECT val, strVal FROM ip4_test";
                int count = 0;
                await using (var reader = cmd.ExecuteReader())
                {
                    while (await reader.ReadAsync())
                    {
                        var ipAddr = reader.GetFieldValue<IPAddress>(0);
                        var ipAddrStr = reader.GetFieldValue<string>(1);
                        var expectedIpAddr = IPAddress.Parse(ipAddrStr);

                        Assert.Equal(expectedIpAddr, ipAddr);
                        ++count;
                    }
                }

                Assert.Equal(3, count);
            }
            finally
            {
                await using var connection = await OpenConnectionAsync();
                var cmd = connection.CreateCommand("DROP TABLE IF EXISTS ip4_test");
                await cmd.ExecuteNonQueryAsync();
            }
        }

        [Fact]
        public async Task ReadIpV6Column()
        {
            try
            {
                await using var connection = await OpenConnectionAsync();

                var cmd = connection.CreateCommand("DROP TABLE IF EXISTS ip6_test");
                await cmd.ExecuteNonQueryAsync();

                cmd = connection.CreateCommand("CREATE TABLE ip6_test(val IPv6, strVal String) ENGINE=Memory");
                await cmd.ExecuteNonQueryAsync();

                cmd.CommandText = "INSERT INTO ip6_test(val, strVal) VALUES ('2001:0db8:11a3:09d7:1f34:8a2e:07a0:765d','2001:0db8:11a3:09d7:1f34:8a2e:07a0:765d')('2a02:aa08:e000:3100::2','2a02:aa08:e000:3100::2')('::ffff:192.0.121.234','::ffff:192.0.121.234')";
                await cmd.ExecuteNonQueryAsync();

                cmd.CommandText = "SELECT val, strVal FROM ip6_test";
                int count = 0;
                await using (var reader = cmd.ExecuteReader())
                {
                    while (await reader.ReadAsync())
                    {
                        var ipAddr = reader.GetFieldValue<IPAddress>(0);
                        var ipAddrStr = reader.GetFieldValue<string>(1);
                        var expectedIpAddr = IPAddress.Parse(ipAddrStr);

                        Assert.Equal(expectedIpAddr, ipAddr);
                        ++count;
                    }
                }

                Assert.Equal(3, count);
            }
            finally
            {
                await using var connection = await OpenConnectionAsync();
                var cmd = connection.CreateCommand("DROP TABLE IF EXISTS ip6_test");
                await cmd.ExecuteNonQueryAsync();
            }
        }

        [Fact]
        public async Task ReadLowCardinalityColumn()
        {
            try
            {
                await using var connection = await OpenConnectionAsync();

                var cmd = connection.CreateCommand("DROP TABLE IF EXISTS low_cardinality_test");
                await cmd.ExecuteNonQueryAsync();

                cmd = connection.CreateCommand("CREATE TABLE low_cardinality_test(id Int32, str LowCardinality(String)) ENGINE=Memory");
                await cmd.ExecuteNonQueryAsync();

                cmd.CommandText = "SELECT * FROM low_cardinality_test";
                await using (var reader = cmd.ExecuteReader())
                {
                    Assert.False(await reader.ReadAsync());
                }

                cmd.CommandText = "INSERT INTO low_cardinality_test(id, str) VALUES (1,'foo')(2,'bar')(4,'bar')(6,'bar')(3,'foo')(7,'foo')(8,'bar')(5,'foobar')";
                await cmd.ExecuteNonQueryAsync();

                cmd.CommandText = "SELECT id, str FROM low_cardinality_test";
                int count = 0;
                await using (var reader = cmd.ExecuteReader())
                {
                    while (await reader.ReadAsync())
                    {
                        var id = reader.GetInt32(0);
                        var str = reader.GetString(1);

                        var expected = id == 5 ? "foobar" : id % 2 == 1 ? "foo" : "bar";
                        Assert.Equal(expected, str);
                        ++count;
                    }
                }

                Assert.Equal(8, count);
            }
            finally
            {
                await using var connection = await OpenConnectionAsync();
                var cmd = connection.CreateCommand("DROP TABLE IF EXISTS low_cardinality_test");
                await cmd.ExecuteNonQueryAsync();
            }
        }

        [Fact]
        public async Task ReadNullableLowCardinalityColumn()
        {
            try
            {
                await using var connection = await OpenConnectionAsync();

                var cmd = connection.CreateCommand("DROP TABLE IF EXISTS low_cardinality_null_test");
                await cmd.ExecuteNonQueryAsync();

                cmd = connection.CreateCommand("CREATE TABLE low_cardinality_null_test(id Int32, str LowCardinality(Nullable(String))) ENGINE=Memory");
                await cmd.ExecuteNonQueryAsync();

                cmd.CommandText = "SELECT * FROM low_cardinality_null_test";
                await using (var reader = cmd.ExecuteReader())
                {
                    Assert.False(await reader.ReadAsync());
                }

                cmd.CommandText = "INSERT INTO low_cardinality_null_test(id, str) SELECT number, number%50 == 0 ? NULL : toString(number%200) FROM system.numbers LIMIT 30000";
                await cmd.ExecuteNonQueryAsync();

                cmd.CommandText = "INSERT INTO low_cardinality_null_test(id, str) SELECT number, number%50 == 0 ? NULL : toString(number%200) FROM system.numbers WHERE number>=30000 LIMIT 30000";
                await cmd.ExecuteNonQueryAsync();

                cmd.CommandText = "INSERT INTO low_cardinality_null_test(id, str) SELECT number, number%50 == 0 ? NULL : toString(number%400) FROM system.numbers WHERE number>=60000 LIMIT 30000";
                await cmd.ExecuteNonQueryAsync();

                cmd.CommandText = "SELECT id, str FROM low_cardinality_null_test";
                int count = 0;
                await using (var reader = cmd.ExecuteReader())
                {
                    while (await reader.ReadAsync())
                    {
                        var id = reader.GetInt32(0);
                        var str = reader.GetString(1, null);

                        if (id % 50 == 0)
                            Assert.Null(str);
                        else if (id < 60000)
                            Assert.Equal((id % 200).ToString(), str);
                        else
                            Assert.Equal((id % 400).ToString(), str);

                        ++count;
                    }
                }

                Assert.Equal(90000, count);
            }
            finally
            {
                await using var connection = await OpenConnectionAsync();
                var cmd = connection.CreateCommand("DROP TABLE IF EXISTS low_cardinality_null_test");
                await cmd.ExecuteNonQueryAsync();
            }
        }

        [Fact]
        public async Task ReadNullableStringLowCardinalityColumnAsArray()
        {
            await WithTemporaryTable("low_cardinality_as_array", "id Int32, str LowCardinality(Nullable(String))", Test);

            async Task Test(ClickHouseConnection connection, string tableName)
            {
                var stringValues = new string?[] { null, string.Empty, "фываasdf", "abcdef", "ghijkl", "null", "пролджэzcvgb", "ячсмить" };
                var byteValues = stringValues.Select(v => v == null ? null : Encoding.UTF8.GetBytes(v)).ToArray();

                const int rowCount = 150;
                await using (var writer = await connection.CreateColumnWriterAsync($"INSERT INTO {tableName}(id, str) VALUES", CancellationToken.None))
                {
                    writer.ConfigureColumn("str", new ClickHouseColumnSettings(Encoding.UTF8));
                    await writer.WriteTableAsync(new object[] { Enumerable.Range(0, rowCount), Enumerable.Range(0, rowCount).Select(i => stringValues[i % stringValues.Length]) }, rowCount, CancellationToken.None);
                }

                var cmd = connection.CreateCommand($"SELECT id, str FROM {tableName} ORDER BY id");

                int count = 0;
                await using var reader = await cmd.ExecuteReaderAsync();
                reader.ConfigureColumn(1, new ClickHouseColumnSettings(Encoding.UTF8));
                char[] charBuffer = new char[stringValues.Select(v => v?.Length ?? 0).Max() + 7];
                byte[] byteBuffer = new byte[byteValues.Select(v => v?.Length ?? 0).Max() + 3];
                while(await reader.ReadAsync())
                {
                    var id = reader.GetInt32(0);
                    Assert.Equal(count, id);

                    var expectedStr = stringValues[id % stringValues.Length];
                    var valueAsCharArray = reader.GetFieldValue<char[]>(1, null);
                    Assert.Equal(expectedStr, valueAsCharArray == null ? null : new string(valueAsCharArray));

                    var valueAsByteArray = reader.GetFieldValue<byte[]>(1, null);
                    var expectedByteArray = byteValues[id % byteValues.Length];
                    Assert.Equal(expectedByteArray, valueAsByteArray);

                    if (expectedStr == null)
                    {
                        Assert.True(reader.IsDBNull(1));
                    }
                    else 
                    {
                        var len = (int)reader.GetChars(1, 0, charBuffer, 0, charBuffer.Length);
                        Assert.Equal(expectedStr.Length, len);
                        Assert.Equal(expectedStr, new string(((ReadOnlySpan<char>)charBuffer).Slice(0, len)));

                        len = (int)reader.GetBytes(1, 0, byteBuffer, 0, byteBuffer.Length);
                        Assert.Equal(expectedByteArray!.Length, len);
                        Assert.Equal(expectedByteArray, byteBuffer.Take(len));

                        len = 0;
                        while (len < charBuffer.Length - 7)
                        {
                            var size = Math.Min(3, charBuffer.Length - len - 7);
                            var currentLen = (int)reader.GetChars(1, len, charBuffer, len + 7, size);
                            len += currentLen;

                            if (currentLen < size)
                                break;
                        }

                        Assert.Equal(expectedStr.Length, len);
                        Assert.Equal(expectedStr, new string(((ReadOnlySpan<char>)charBuffer).Slice(7, len)));

                        len = 0;
                        while (len < byteBuffer.Length - 3)
                        {
                            var size = Math.Min(3, byteBuffer.Length - len - 3);
                            var currentLen = (int)reader.GetBytes(1, len, byteBuffer, len + 3, size);
                            len += currentLen;

                            if (currentLen < size)
                                break;
                        }

                        Assert.Equal(expectedByteArray!.Length, len);
                        Assert.Equal(expectedByteArray, byteBuffer.Skip(3).Take(len));
                    }

                    ++count;
                }

                Assert.Equal(rowCount, count);
            }
        }

        [Fact]
        public async Task ReadStringLowCardinalityColumnAsArray()
        {
            await WithTemporaryTable("low_cardinality_not_null_as_array", "id Int32, str LowCardinality(String)", Test);

            async Task Test(ClickHouseConnection connection, string tableName)
            {
                var stringValues = new string[] { string.Empty, "фываasdf", "abcdef", "ghijkl", "null", "пролджэzcvgb", "ячсмить" };
                var byteValues = stringValues.Select(v => Encoding.UTF8.GetBytes(v)).ToArray();

                const int rowCount = 150;
                await using (var writer = await connection.CreateColumnWriterAsync($"INSERT INTO {tableName}(id, str) VALUES", CancellationToken.None))
                {
                    writer.ConfigureColumn("str", new ClickHouseColumnSettings(Encoding.UTF8));
                    await writer.WriteTableAsync(new object[] { Enumerable.Range(0, rowCount), Enumerable.Range(0, rowCount).Select(i => stringValues[i % stringValues.Length]) }, rowCount, CancellationToken.None);
                }

                var cmd = connection.CreateCommand($"SELECT id, str FROM {tableName} ORDER BY id");

                int count = 0;
                await using var reader = await cmd.ExecuteReaderAsync();
                reader.ConfigureColumn(1, new ClickHouseColumnSettings(Encoding.UTF8));
                char[] charBuffer = new char[stringValues.Select(v => v.Length).Max() + 7];
                byte[] byteBuffer = new byte[byteValues.Select(v => v.Length).Max() + 3];
                while (await reader.ReadAsync())
                {
                    var id = reader.GetInt32(0);
                    Assert.Equal(count, id);

                    var expectedStr = stringValues[id % stringValues.Length];
                    var valueAsCharArray = reader.GetFieldValue<char[]>(1);
                    Assert.Equal(expectedStr, new string(valueAsCharArray));

                    var valueAsByteArray = reader.GetFieldValue<byte[]>(1);
                    var expectedByteArray = byteValues[id % byteValues.Length];
                    Assert.Equal(expectedByteArray, valueAsByteArray);

                    var len = (int)reader.GetChars(1, 0, charBuffer, 0, charBuffer.Length);
                    Assert.Equal(expectedStr.Length, len);
                    Assert.Equal(expectedStr, new string(((ReadOnlySpan<char>)charBuffer).Slice(0, len)));

                    len = (int)reader.GetBytes(1, 0, byteBuffer, 0, byteBuffer.Length);
                    Assert.Equal(expectedByteArray!.Length, len);
                    Assert.Equal(expectedByteArray, byteBuffer.Take(len));

                    len = 0;
                    while (len < charBuffer.Length - 7)
                    {
                        var size = Math.Min(3, charBuffer.Length - len - 7);
                        var currentLen = (int)reader.GetChars(1, len, charBuffer, len + 7, size);
                        len += currentLen;

                        if (currentLen < size)
                            break;
                    }

                    Assert.Equal(expectedStr.Length, len);
                    Assert.Equal(expectedStr, new string(((ReadOnlySpan<char>)charBuffer).Slice(7, len)));

                    len = 0;
                    while (len < byteBuffer.Length - 3)
                    {
                        var size = Math.Min(3, byteBuffer.Length - len - 3);
                        var currentLen = (int)reader.GetBytes(1, len, byteBuffer, len + 3, size);
                        len += currentLen;

                        if (currentLen < size)
                            break;
                    }

                    Assert.Equal(expectedByteArray!.Length, len);
                    Assert.Equal(expectedByteArray, byteBuffer.Skip(3).Take(len));

                    ++count;
                }

                Assert.Equal(rowCount, count);
            }
        }

        [Fact]
        public async Task SkipLowCardinalityColumn()
        {
            try
            {
                await using var connection = await OpenConnectionAsync();

                var cmd = connection.CreateCommand("DROP TABLE IF EXISTS low_cardinality_skip_test");
                await cmd.ExecuteNonQueryAsync();

                cmd = connection.CreateCommand("CREATE TABLE low_cardinality_skip_test(id Int32, str LowCardinality(Nullable(String))) ENGINE=Memory");
                await cmd.ExecuteNonQueryAsync();

                cmd.CommandText = "INSERT INTO low_cardinality_skip_test(id, str) SELECT number, number%50 == 0 ? NULL : toString(number%200) FROM system.numbers LIMIT 10000";
                await cmd.ExecuteNonQueryAsync();

                cmd.CommandText = "INSERT INTO low_cardinality_skip_test(id, str) SELECT number, number%50 == 0 ? NULL : toString(number%200) FROM system.numbers WHERE number>=10000 LIMIT 10000";
                await cmd.ExecuteNonQueryAsync();

                cmd.CommandText = "SELECT id, str FROM low_cardinality_skip_test";
                int count = 0;
                await using (var reader = cmd.ExecuteReader())
                {
                    while (await reader.ReadAsync())
                    {
                        var id = reader.GetInt32(0);
                        var str = reader.GetString(1, null);

                        if (id % 50 == 0)
                            Assert.Null(str);
                        else
                            Assert.Equal((id % 200).ToString(), str);

                        if (++count == 100)
                            break;
                    }
                }

                Assert.Equal(100, count);

                cmd.CommandText = "SELECT count(*) FROM low_cardinality_skip_test";
                count = (int) await cmd.ExecuteScalarAsync<ulong>();
                Assert.Equal(20000, count);
            }
            finally
            {
                await using var connection = await OpenConnectionAsync();
                var cmd = connection.CreateCommand("DROP TABLE IF EXISTS low_cardinality_skip_test");
                await cmd.ExecuteNonQueryAsync();
            }
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadFixedStringParameterScalar(ClickHouseParameterMode parameterMode)
        {
            var values = new[] {string.Empty, "0", "12345678", "abcdefg", "1234", "abcd", "абвг"};

            await using var connection = await OpenConnectionAsync(parameterMode);

            await using var cmd = connection.CreateCommand("SELECT {param}");
            var param = new ClickHouseParameter("param") {DbType = DbType.StringFixedLength, Size = 8};
            cmd.Parameters.Add(param);
            
            foreach (var testValue in values)
            {
                param.Value = testValue;

                var value = await cmd.ExecuteScalarAsync<byte[]>();
                var len = value.Length - value.Reverse().TakeWhile(b => b == 0).Count();
                var strValue = Encoding.UTF8.GetString(value, 0, len);
                Assert.Equal(testValue, strValue);
            }

            param.Value = "123456789";
            var exception = await Assert.ThrowsAnyAsync<ClickHouseException>(() => cmd.ExecuteScalarAsync<byte[]>());
            Assert.Equal(ClickHouseErrorCodes.InvalidQueryParameterConfiguration, exception.ErrorCode);

            param.Value = "абвг0";
            exception = await Assert.ThrowsAnyAsync<ClickHouseException>(() => cmd.ExecuteScalarAsync<byte[]>());
            Assert.Equal(ClickHouseErrorCodes.InvalidQueryParameterConfiguration, exception.ErrorCode);
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadGuidParameterScalar(ClickHouseParameterMode parameterMode)
        {
            var parameterValue = Guid.Parse("7FCFFE2D-E9A6-49E0-B8ED-9617603F5584");

            await using var connection = await OpenConnectionAsync(parameterMode);

            await using var cmd = connection.CreateCommand("SELECT {param}");
            var param = new ClickHouseParameter("param") { DbType = DbType.Guid };
            cmd.Parameters.Add(param);
            param.Value = parameterValue;
            
            var result = await cmd.ExecuteScalarAsync<Guid>();
            Assert.Equal(parameterValue, result);
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadDecimalParameterScalar(ClickHouseParameterMode parameterMode)
        {
            // The default ClickHouse type for decimal is Decimal128(9)
            const decimal minValueByDefault = 1m / 1_000_000_000;
            const decimal binarySparseValue = 281479271677952m * 4294967296m;

            var testData = new[]
            {
                decimal.Zero, decimal.One, decimal.MinusOne, decimal.MinValue, decimal.MaxValue, decimal.MinValue / 100, decimal.MaxValue / 100, decimal.One / 100, decimal.MinusOne / 100,
                minValueByDefault, -minValueByDefault, minValueByDefault / 10, -minValueByDefault / 10, binarySparseValue, -binarySparseValue
            };

            await using var connection = await OpenConnectionAsync(parameterMode);

            await using var cmd = connection.CreateCommand("SELECT {param}");
            var param = new ClickHouseParameter("param") {DbType = DbType.Decimal};
            cmd.Parameters.Add(param);
            
            foreach (var testValue in testData)
            {
                param.Value = testValue;

                var result = await cmd.ExecuteScalarAsync();
                var resultDecimal = Assert.IsType<decimal>(result);

                if (Math.Abs(testValue) >= minValueByDefault)
                    Assert.Equal(testValue, resultDecimal);
                else
                    Assert.Equal(0, resultDecimal);
            }
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadCurrencyParameterScalar(ClickHouseParameterMode parameterMode)
        {
            const decimal maxCurrencyValue = 922_337_203_685_477.5807m, minCurrencyValue = -922_337_203_685_477.5808m, binarySparseValue = 7_205_759_833_289.5232m, currencyEpsilon = 0.0001m;

            var testData = new[]
            {
                decimal.Zero, decimal.One, decimal.MinusOne, minCurrencyValue, maxCurrencyValue, decimal.One / 100, decimal.MinusOne / 100,
                binarySparseValue, -binarySparseValue, currencyEpsilon, -currencyEpsilon, currencyEpsilon / 10, -currencyEpsilon / 10
            };

            await using var connection = await OpenConnectionAsync(parameterMode);

            await using var cmd = connection.CreateCommand("SELECT {param}");
            var param = new ClickHouseParameter("param") { DbType = DbType.Currency };
            cmd.Parameters.Add(param);

            param.Value = minCurrencyValue - currencyEpsilon;
            var handledException = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync());
            Assert.IsType<OverflowException>(handledException.InnerException);

            param.Value = maxCurrencyValue + currencyEpsilon;
            handledException = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync());
            Assert.IsType<OverflowException>(handledException.InnerException);

            foreach (var testValue in testData)
            {
                param.Value = testValue;

                var result = await cmd.ExecuteScalarAsync();
                var resultDecimal = Assert.IsType<decimal>(result);

                if (Math.Abs(testValue) >= currencyEpsilon)
                    Assert.Equal(testValue, resultDecimal);
                else
                    Assert.Equal(0, resultDecimal);
            }
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadVarNumericParameter(ClickHouseParameterMode parameterMode)
        {
            await using var connection = await OpenConnectionAsync(parameterMode);

            await using var cmd = connection.CreateCommand("SELECT {param} AS p, toString(p)");
            var param = new ClickHouseParameter("param") {DbType = DbType.VarNumeric, Precision = 7, Scale = 3};
            cmd.Parameters.Add(param);

            {
                const decimal epsilon = 0.001m, formalMax = 9_999.999m, actualMax = 2_147_483.647m, actualMin = -2_147_483.648m;
                var values = new[] {decimal.Zero, decimal.One, decimal.MinusOne, epsilon, -epsilon, formalMax, -formalMax, actualMax, actualMin, epsilon / 10, -epsilon / 10};
                foreach (var testValue in values)
                {
                    param.Value = testValue;

                    await using var reader = await cmd.ExecuteReaderAsync();
                    Assert.True(await reader.ReadAsync());

                    var resultDecimal = reader.GetDecimal(0);
                    var resultStr = reader.GetString(1);

                    Assert.False(await reader.ReadAsync());

                    if (resultStr.StartsWith("--"))
                    {
                        Assert.True(testValue < -formalMax);
                        resultStr = resultStr.Substring(1);
                    }

                    var parsedValue = decimal.Parse(resultStr, CultureInfo.InvariantCulture);
                    if (Math.Abs(testValue) >= epsilon)
                    {
                        Assert.Equal(testValue, resultDecimal);
                        Assert.Equal(testValue, parsedValue);
                    }
                    else
                    {
                        Assert.Equal(0, resultDecimal);
                        Assert.Equal(0, parsedValue);
                    }
                }

                param.Value = actualMax + epsilon;
                var handledException = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync());
                Assert.IsType<OverflowException>(handledException.InnerException);

                param.Value = actualMin - epsilon;
                handledException = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync());
                Assert.IsType<OverflowException>(handledException.InnerException);
            }

            param.Precision = 18;
            param.Scale = 6;
            {
                const decimal epsilon = 0.000_001m, formalMax = 999_999_999_999.999_999m, actualMax = 9_223_372_036_854.775807m, actualMin = -9_223_372_036_854.775808m;
                var values = new[] { decimal.Zero, decimal.One, decimal.MinusOne, epsilon, -epsilon, formalMax, -formalMax, actualMax, actualMin, epsilon / 10, -epsilon / 10 };
                foreach (var testValue in values)
                {
                    param.Value = testValue;

                    await using var reader = await cmd.ExecuteReaderAsync();
                    Assert.True(await reader.ReadAsync());

                    var resultDecimal = reader.GetDecimal(0);
                    var resultStr = reader.GetString(1);

                    Assert.False(await reader.ReadAsync());

                    if (resultStr.StartsWith("--"))
                    {
                        Assert.True(testValue < -formalMax);
                        resultStr = resultStr.Substring(1);
                    }

                    var parsedValue = decimal.Parse(resultStr, CultureInfo.InvariantCulture);
                    if (Math.Abs(testValue) >= epsilon)
                    {
                        Assert.Equal(testValue, resultDecimal);
                        Assert.Equal(testValue, parsedValue);
                    }
                    else
                    {
                        Assert.Equal(0, resultDecimal);
                        Assert.Equal(0, parsedValue);
                    }
                }

                param.Value = actualMax + epsilon;
                var handledException = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync()); 
                Assert.IsType<OverflowException>(handledException.InnerException);

                param.Value = actualMin - epsilon;
                handledException = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync());
                Assert.IsType<OverflowException>(handledException.InnerException);
            }

            param.Precision = 35;
            param.Scale = 30;
            {
                const decimal formalMax = 99_999.999_999_999_999_999_999_999_99m, actualMax = 170_141_183.460_469_231_731_687_303_71m, actualMin = -actualMax, epsilon= 0.000_000_000_000_000_000_01m;
                var values = new[] {decimal.Zero, decimal.One, decimal.MinusOne, epsilon, -epsilon, formalMax, -formalMax, actualMax, actualMin};
                foreach (var testValue in values)
                {
                    param.Value = testValue;

                    await using var reader = await cmd.ExecuteReaderAsync();
                    Assert.True(await reader.ReadAsync());

                    var resultDecimal = reader.GetDecimal(0);
                    var resultStr = reader.GetString(1);

                    Assert.False(await reader.ReadAsync());

                    if (resultStr.StartsWith("--"))
                    {
                        Assert.True(testValue < -formalMax);
                        resultStr = resultStr.Substring(1);
                    }

                    var parsedValue = decimal.Parse(resultStr, CultureInfo.InvariantCulture);
                    Assert.Equal(testValue, resultDecimal);
                    Assert.Equal(testValue, parsedValue);
                }

                param.Value = actualMax + epsilon;
                var handledException = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync());
                Assert.IsType<OverflowException>(handledException.InnerException);

                param.Value = actualMin - epsilon;
                handledException = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync());
                Assert.IsType<OverflowException>(handledException.InnerException);
            }
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ClickHouseDecimalTypeNames(ClickHouseParameterMode parameterMode)
        {
            await using var connection = await OpenConnectionAsync(parameterMode);

            await using var cmd = connection.CreateCommand("SELECT toTypeName({param})");
            var param = new ClickHouseParameter("param") {DbType = DbType.Decimal, Value = 0m};
            cmd.Parameters.Add(param);

            var typeName = await cmd.ExecuteScalarAsync<string>();
            Assert.Matches(GetRegex(38, 9), typeName);

            param.DbType = DbType.Currency;
            typeName = await cmd.ExecuteScalarAsync<string>();
            Assert.Matches(GetRegex(18, 4), typeName);

            param.DbType = DbType.VarNumeric;
            typeName = await cmd.ExecuteScalarAsync<string>();
            Assert.Matches(GetRegex(38, 9), typeName);

            param.Scale = 2;
            param.Precision = 3;
            typeName = await cmd.ExecuteScalarAsync<string>();
            Assert.Matches(GetRegex(3, 2), typeName);

            param.Scale = 14;
            param.Precision = 14;
            typeName = await cmd.ExecuteScalarAsync<string>();
            Assert.Matches(GetRegex(14, 14), typeName);

            param.Scale = 0;
            param.Precision = 1;
            typeName = await cmd.ExecuteScalarAsync<string>();
            Assert.Matches(GetRegex(1, 0), typeName);

            param.Scale = 32;
            param.Precision = 33;
            typeName = await cmd.ExecuteScalarAsync<string>();
            Assert.Matches(GetRegex(33, 32), typeName);

            static string GetRegex(int precision, int scale)
            {
                // ClickHouse server may wrap the parameter's type into Nullable
                return string.Format(CultureInfo.InvariantCulture, @"^(Nullable\()?Decimal\({0},\s{1}\)\)?$", precision, scale);
            }
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadDateTimeParameterScalar(ClickHouseParameterMode parameterMode)
        {
            var now = DateTime.Now;
            now = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second, now.Kind);

            var testData = new[] {now, default, new DateTime(1980, 12, 15, 3, 8, 58), new DateTime(2015, 1, 1, 18, 33, 55)};

            await using var connection = await OpenConnectionAsync(parameterMode);

            await using var cmd = connection.CreateCommand("SELECT {param}");
            var param = new ClickHouseParameter("param");
            cmd.Parameters.Add(param);

            foreach (var testValue in testData)
            {
                param.Value = testValue;

                var result = await cmd.ExecuteScalarAsync<DateTime>();
                Assert.Equal(testValue, result);
            }

            param.Value = DateTime.UnixEpoch.AddMonths(-1);
            var handledException = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync());
            Assert.IsType<OverflowException>(handledException.InnerException);

            param.Value = DateTime.UnixEpoch.AddSeconds(uint.MaxValue).AddMonths(1);
            handledException = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync());
            Assert.IsType<OverflowException>(handledException.InnerException);
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadDateTimeOffsetParameterScalar(ClickHouseParameterMode parameterMode)
        {
            var now = DateTime.Now;
            now = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second, now.Kind);

            var testData = new[]
            {
                now, default, new DateTimeOffset(new DateTime(1980, 12, 15, 3, 8, 58), new TimeSpan(0, -5, 0, 0)), new DateTimeOffset(new DateTime(2015, 1, 1, 18, 33, 55), new TimeSpan(0, 3, 15, 0)),
                new DateTimeOffset(DateTime.UnixEpoch.AddSeconds(1)).ToOffset(new TimeSpan(0, -11, 0, 0)),
                new DateTimeOffset(DateTime.UnixEpoch.AddSeconds(uint.MaxValue)).ToOffset(new TimeSpan(0, 11, 0, 0))
            };

            await using var connection = await OpenConnectionAsync(parameterMode);

            await using var cmd = connection.CreateCommand("SELECT {param}");
            var param = new ClickHouseParameter("param");
            cmd.Parameters.Add(param);

            foreach (var testValue in testData)
            {
                param.Value = testValue;

                var result = await cmd.ExecuteScalarAsync<DateTimeOffset>();
                Assert.Equal(0, (result - testValue).Ticks);
            }

            param.Value = DateTimeOffset.UnixEpoch;
            var unixEpochResult = await cmd.ExecuteScalarAsync<DateTimeOffset>();
            Assert.Equal(default, unixEpochResult);

            param.Value = new DateTimeOffset(DateTime.UnixEpoch.AddSeconds(-1)).ToOffset(new TimeSpan(0, -11, 0, 0));
            var handledException = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync());
            Assert.IsType<OverflowException>(handledException.InnerException);
            
            param.Value = new DateTimeOffset(DateTime.UnixEpoch.AddSeconds((double) uint.MaxValue + 1)).ToOffset(new TimeSpan(0, 11, 0, 0));
            handledException = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync());
            Assert.IsType<OverflowException>(handledException.InnerException);
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadFloatParameterScalar(ClickHouseParameterMode parameterMode)
        {
            var testData = new[]
                {float.MinValue, float.MaxValue, float.Epsilon * 2, -float.Epsilon * 2, 1, -1, (float) Math.PI, (float) Math.Exp(1)};

            await using var connection = await OpenConnectionAsync(parameterMode);

            await using var cmd = connection.CreateCommand("SELECT CAST({param}/2 AS Float32)");
            var param = new ClickHouseParameter("param") { DbType = DbType.Single };
            cmd.Parameters.Add(param);

            foreach (var testValue in testData)
            {
                param.Value = testValue;

                var result = await cmd.ExecuteScalarAsync();
                var resultFloat = Assert.IsType<float>(result);

                Assert.Equal(testValue / 2, resultFloat);
            }
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadDoubleParameterScalar(ClickHouseParameterMode parameterMode)
        {
            var testData = new[]
                {double.MinValue, double.MaxValue, double.Epsilon * 2, -double.Epsilon * 2, 1, -1, Math.PI, Math.Exp(1)};

            await using var connection = await OpenConnectionAsync(parameterMode);

            await using var cmd = connection.CreateCommand("SELECT {param}/2");
            var param = new ClickHouseParameter("param") { DbType = DbType.Double };
            cmd.Parameters.Add(param);

            foreach (var testValue in testData)
            {
                param.Value = testValue;

                var result = await cmd.ExecuteScalarAsync();
                var resultDouble = Assert.IsType<double>(result);

                Assert.Equal(testValue / 2, resultDouble);
            }
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadNothingParameterScalar(ClickHouseParameterMode parameterMode)
        {
            await using var connection = await OpenConnectionAsync(parameterMode);

            await using var cmd = connection.CreateCommand("SELECT {param}");
            var param = new ClickHouseParameter("param");
            cmd.Parameters.Add(param);

            var result = await cmd.ExecuteScalarAsync();
            Assert.Equal(DBNull.Value, result);
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadIpV4ParameterScalar(ClickHouseParameterMode parameterMode)
        {
            await using var connection = await OpenConnectionAsync(parameterMode);

            await using var cmd = connection.CreateCommand("SELECT {param}");
            var param = new ClickHouseParameter("param") {Value = IPAddress.Parse("10.0.121.1")};
            Assert.Equal(ClickHouseDbType.IpV4, param.ClickHouseDbType);

            cmd.Parameters.Add(param);
            var result = await cmd.ExecuteScalarAsync();
            Assert.Equal(param.Value, result);

            param.Value = "::ffff:192.0.2.1";
            param.ClickHouseDbType = ClickHouseDbType.IpV4;
            result = await cmd.ExecuteScalarAsync<string>();
            Assert.Equal("192.0.2.1", result);
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadIpV6ParameterScalar(ClickHouseParameterMode parameterMode)
        {
            await using var connection = await OpenConnectionAsync(parameterMode);

            await using var cmd = connection.CreateCommand("SELECT {param}");
            var param = new ClickHouseParameter("param") { Value = IPAddress.Parse("2001:0db8:11a3:09d7:1f34:8a2e:07a0:765d") };
            Assert.Equal(ClickHouseDbType.IpV6, param.ClickHouseDbType);

            cmd.Parameters.Add(param);
            var result = await cmd.ExecuteScalarAsync();
            Assert.Equal(param.Value, result);

            param.Value = "192.0.121.234";
            param.ClickHouseDbType = ClickHouseDbType.IpV6;
            result = await cmd.ExecuteScalarAsync<string>();
            Assert.Equal("::ffff:192.0.121.234", result);

            param.Value = null;
            result = await cmd.ExecuteScalarAsync();
            Assert.Equal(DBNull.Value, result);
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadIntegerParameterScalar(ClickHouseParameterMode parameterMode)
        {
            object[] values =
            {
                sbyte.MinValue, sbyte.MaxValue, (sbyte) 0,
                byte.MinValue, byte.MaxValue, 
                short.MinValue, short.MaxValue, (short) 0,
                ushort.MinValue, ushort.MaxValue,
                int.MinValue, int.MaxValue, (int) 0,
                uint.MinValue, uint.MaxValue,
                long.MinValue, long.MaxValue, (long) 0, 
                ulong.MinValue, ulong.MaxValue
            };

            await using var connection = await OpenConnectionAsync(parameterMode);

            await using var cmd = connection.CreateCommand("SELECT {integerParam}");
            var param = new ClickHouseParameter("integerParam");
            cmd.Parameters.Add(param);
            foreach (var value in values)
            {
                param.Value = value;
                var result = await cmd.ExecuteScalarAsync();

                Assert.IsType(value.GetType(), result);
                Assert.Equal(result, value);
            }
        }

        [Fact]
        public async Task ReadValueWithOverridenType()
        {
            await WithTemporaryTable("col_settings_type", "id UInt16, ip Nullable(IPv4), enum Nullable(Enum16('min'=-512, 'avg'=0, 'max'=512)), num Nullable(Int32)", RunTest);

            static async Task RunTest(ClickHouseConnection connection, string tableName)
            {
                var cmd = connection.CreateCommand($"INSERT INTO {tableName}(id, ip, enum, num) VALUES" +
                    "(124, null, 'min', 1234)" +
                    "(125, '10.0.0.1', 'avg', null)" +
                    "(126, null, null, null)" +
                    "(127, '127.0.0.1', 'max', -8990)" +
                    "(128, '4.8.15.16', null, 12789)");

                await cmd.ExecuteNonQueryAsync();

                cmd.CommandText = $"SELECT * FROM {tableName} ORDER BY id";
                await using var reader = await cmd.ExecuteReaderAsync();
                var idIdx = reader.GetOrdinal("id");
                var numIdx = reader.GetOrdinal("num");
                var enumIdx = reader.GetOrdinal("enum");
                var ipIdx = reader.GetOrdinal("ip");                

                Assert.Equal(typeof(ushort), reader.GetFieldType(idIdx));
                Assert.Equal(typeof(int), reader.GetFieldType(numIdx));
                Assert.Equal(typeof(string), reader.GetFieldType(enumIdx));
                Assert.Equal(typeof(IPAddress), reader.GetFieldType(ipIdx));

                reader.ConfigureColumn("id", new ClickHouseColumnSettings(typeof(int)));
                reader.ConfigureColumn("num", new ClickHouseColumnSettings(typeof(long)));
                reader.ConfigureColumn("enum", new ClickHouseColumnSettings(typeof(short?)));
                reader.ConfigureColumn("ip", new ClickHouseColumnSettings(typeof(string)));

                Assert.Equal(typeof(int), reader.GetFieldType(idIdx));
                Assert.Equal(typeof(long), reader.GetFieldType(numIdx));
                Assert.Equal(typeof(short), reader.GetFieldType(enumIdx));
                Assert.Equal(typeof(string), reader.GetFieldType(ipIdx));

                var expectedData = new object[][]
                {
                    new object[] {124, DBNull.Value, (short)-512, 1234L},
                    new object[] {125, "10.0.0.1", (short)0, DBNull.Value},
                    new object[] {126, DBNull.Value, DBNull.Value, DBNull.Value},
                    new object[] {127, "127.0.0.1", (short)512, -8990L},
                    new object[] {128, "4.8.15.16", DBNull.Value, 12789L}
                };

                int count = 0;
                while(await reader.ReadAsync())
                {
                    Assert.Equal(expectedData[count][0], reader.GetValue(idIdx));
                    Assert.Equal(expectedData[count][1], reader.GetValue(ipIdx));
                    Assert.Equal(expectedData[count][2], reader.GetValue(enumIdx));
                    Assert.Equal(expectedData[count][3], reader.GetValue(numIdx));

                    ++count;
                }

                Assert.Equal(expectedData.Length, count);                
            }
        }

        [Fact]
        public async Task ReadGuidColumn()
        {
            var guids = new List<Guid> { Guid.Parse("74D47928-2423-4FE2-AD45-82E296BF6058"), Guid.Parse("2879D474-2324-E24F-AD45-82E296BF6058"), Guid.Empty };
            guids.AddRange(Enumerable.Range(1, 100 - guids.Count).Select(_ => Guid.NewGuid()));

            await WithTemporaryTable("uuid", "id Int32, guid UUID, str String", RunTest);

            async Task RunTest(ClickHouseConnection connection, string tableName)
            {
                await using (var writer = await connection.CreateColumnWriterAsync($"INSERT INTO {tableName}(id, guid, str) VALUES", CancellationToken.None))
                {
                    await writer.WriteTableAsync(new object[] { Enumerable.Range(0, guids.Count), guids, guids.Select(v => v.ToString("D")) }, guids.Count, CancellationToken.None);
                }

                var cmd = connection.CreateCommand($"SELECT id, guid, CAST(guid AS String) strGuid, (guid = CAST(str AS UUID)) eq FROM {tableName} ORDER BY id");

                await using var reader = await cmd.ExecuteReaderAsync();

                int count = 0;
                while (await reader.ReadAsync())
                {
                    var id = reader.GetInt32(0);
                    var guid = reader.GetGuid(1);
                    var str = reader.GetString(2);
                    var eq = reader.GetBoolean(3);

                    Assert.Equal(count, id);
                    Assert.Equal(guids[id], guid);
                    Assert.True(Guid.TryParse(str, out var strGuid));
                    Assert.Equal(guids[id], strGuid);
                    Assert.True(eq);

                    ++count;
                }

                Assert.Equal(guids.Count, count);
            }
        }

        [Fact]
        public async Task ReadMapScalar()
        {
            await using var cn = await OpenConnectionAsync();

            var cmd = cn.CreateCommand("SELECT CAST(([1, 2, 3, 0], ['Ready', 'Steady', 'Go', null]), 'Map(UInt8, Nullable(String))') AS map");
            var result = await cmd.ExecuteScalarAsync();

            Assert.Equal(new[] { new KeyValuePair<byte, string?>(1, "Ready"), new KeyValuePair<byte, string?>(2, "Steady"), new KeyValuePair<byte, string?>(3, "Go"), new KeyValuePair<byte, string?>(0, null) }, result);            
        }

        [Fact]
        public async Task ReadMapColumn()
        {
            await WithTemporaryTable("map", "id Int32, map Map(String, Nullable(Int32))", Test);

            static async Task Test(ClickHouseConnection cn, string tableName)
            {
                var expectedDict = new Dictionary<string, int?>(StringComparer.OrdinalIgnoreCase)
                {
                    ["Ready"] = 1,
                    ["Steady"] = 2,
                    ["Go"] = 3,
                    ["Unknown"] = null,
                    ["Ok"] = 1,
                    ["NotOk"] = -1,
                    ["Oh"] = 199,
                    ["hh"] = null,
                    ["hO"] = 201,
                    ["null"] = null,
                };

                var expectedLists = new string[][]
                {
                    new string[]{ "Ready", "Steady", "Go", "Unknown" },
                    new string[]{ "Ok", "NotOk" },
                    new string[]{ "Oh", "hh", "hO" },
                    new string[0],
                    new string[]{ "null" },
                };

                var cmd = cn.CreateCommand(@$"INSERT INTO {tableName}(id, map)
SELECT 1, CAST((['Ready', 'Steady', 'Go', 'Unknown'], [1, 2, 3, null]), 'Map(String, Nullable(Int32))') AS map
UNION ALL SELECT 2, CAST((['Ok', 'NotOk'], [1, -1]), 'Map(String, Nullable(Int32))')
UNION ALL SELECT 3, CAST((['Oh', 'hh', 'hO'], [199, null, 201]), 'Map(String, Nullable(Int32))')
UNION ALL SELECT 4, CAST(([], []), 'Map(String, Nullable(Int32))')
UNION ALL SELECT 5, CAST((['null'], [null]), 'Map(String, Nullable(Int32))')");

                await cmd.ExecuteNonQueryAsync();

                cmd.CommandText = $"SELECT map FROM {tableName} ORDER BY id";
                await using var reader = await cmd.ExecuteReaderAsync();

                var columnType = reader.GetFieldTypeInfo(0);
                Assert.Equal("Map", columnType.TypeName);
                Assert.Equal("Map(String, Nullable(Int32))", columnType.ComplexTypeName);
                Assert.Equal(2, columnType.GenericArgumentsCount);
                Assert.Equal(2, columnType.TypeArgumentsCount);

                var keyArg = columnType.GetGenericArgument(0);
                var typeArg1 = columnType.GetTypeArgument(0);
                Assert.Same(keyArg, typeArg1);
                Assert.Equal("String", keyArg.ComplexTypeName);

                var valueArg = columnType.GetGenericArgument(1);
                var typeArg2 = columnType.GetTypeArgument(1);
                Assert.Same(valueArg, typeArg2);
                Assert.Equal("Nullable(Int32)", valueArg.ComplexTypeName);

                Assert.Equal(typeof(KeyValuePair<string, int?>[]), columnType.GetFieldType());

                int count = 0;
                while (await reader.ReadAsync())
                {
                    var value = reader.GetValue(0);
                    var pairs = Assert.IsType<KeyValuePair<string, int?>[]>(value);
                    var expectedKeys = expectedLists[count];

                    Assert.Equal(expectedKeys.Length, pairs.Length);
                    for (int i = 0; i < expectedKeys.Length; i++)
                    {
                        Assert.Equal(expectedKeys[i], pairs[i].Key);
                        Assert.Equal(expectedDict[expectedKeys[i]], pairs[i].Value);
                    }

                    ++count;
                }

                Assert.Equal(expectedLists.Length, count);                
            }            
        }

        [Fact]
        public async Task ReadInt128Column()
        {
            var minValue = -(BigInteger.One << 127);
            var maxValue = (BigInteger.One << 127) - BigInteger.One;

            var maxStrLen = maxValue.ToString().Length;
            var sb = new StringBuilder(maxStrLen + 1).Append('-');
            for (int i = 1; i <= maxStrLen; i++)
                sb.Append((char)('0' + (i % 10)));

            var strValues = new[] { minValue.ToString(), sb.ToString(), "-1", "0", "1", sb.ToString(1, maxStrLen), maxValue.ToString() };

            await using var cn = await OpenConnectionAsync();
            var cmd = cn.CreateCommand("SELECT CAST(value AS Int128) AS v, v = bigIntValue AS testPassed FROM ptable ORDER BY id");

            var tableProvider = new ClickHouseTableProvider("ptable", strValues.Length);
            tableProvider.Columns.AddColumn("id", Enumerable.Range(1, strValues.Length));
            tableProvider.Columns.AddColumn("value", strValues);
            var bigIntColumn = tableProvider.Columns.AddColumn("bigIntValue", strValues.Select(v => BigInteger.Parse(v)));
            bigIntColumn.ClickHouseDbType = ClickHouseDbType.Int128;
            cmd.TableProviders.Add(tableProvider);

            await using var reader = await cmd.ExecuteReaderAsync();

            var valueColumnType = reader.GetFieldTypeInfo(0);
            Assert.Equal("Int128", valueColumnType.ComplexTypeName);
            Assert.Equal(ClickHouseDbType.Int128, valueColumnType.GetDbType());

            int count = 0;
            while (await reader.ReadAsync())
            {
                var value = reader.GetValue(0);
                var expectedValue = BigInteger.Parse(strValues[count]);
                Assert.Equal(expectedValue, value);

                var testPassed = reader.GetBoolean(1);
                Assert.True(testPassed);

                ++count;
            }

            Assert.Equal(strValues.Length, count);
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadInt128ParameterScalar(ClickHouseParameterMode parameterMode)
        {
            await using var cn = await OpenConnectionAsync(parameterMode);

            var cmd = cn.CreateCommand("SELECT {v}");
            var parameter = new ClickHouseParameter("v") { ClickHouseDbType = ClickHouseDbType.Int128 };
            cmd.Parameters.Add(parameter);

            var pairs = new (object value, BigInteger expected)[] 
            {
                (0, 0),
                (-1, -1),
                (1, 1),
                (byte.MaxValue, byte.MaxValue),
                (sbyte.MinValue, sbyte.MinValue),
                (sbyte.MaxValue, sbyte.MaxValue),
                (short.MinValue, short.MinValue),
                (short.MaxValue, short.MaxValue),
                (ushort.MaxValue, ushort.MaxValue),
                (int.MinValue, int.MinValue),
                (int.MaxValue, int.MaxValue),
                (uint.MaxValue, uint.MaxValue),
                (long.MinValue, long.MinValue),
                (long.MaxValue, long.MaxValue),
                (ulong.MaxValue, ulong.MaxValue),
                (-(BigInteger.One << 127), -(BigInteger.One << 127)),
                ((BigInteger.One << 127) - 1, (BigInteger.One << 127) - 1),
            };

            foreach(var pair in pairs)
            {
                parameter.Value = pair.value;
                var result = await cmd.ExecuteScalarAsync();
                var bigIntResult = Assert.IsType<BigInteger>(result);
                Assert.Equal(pair.expected, bigIntResult);
            }            
        }

        [Fact]
        public async Task ReadUInt128Column()
        {
            var maxValue = (BigInteger.One << 128) - BigInteger.One;

            var maxStrLen = maxValue.ToString().Length;
            var sb = new StringBuilder(maxStrLen);
            for (int i = 1; i <= maxStrLen; i++)
                sb.Append((char)('0' + (i % 10)));

            var strValues = new[] { "0", "1", sb.ToString(), maxValue.ToString() };

            await using var cn = await OpenConnectionAsync();
            var cmd = cn.CreateCommand("SELECT CAST(value AS UInt128) AS v, v = bigIntValue AS testPassed FROM ptable ORDER BY id");

            var tableProvider = new ClickHouseTableProvider("ptable", strValues.Length);
            tableProvider.Columns.AddColumn("id", Enumerable.Range(1, strValues.Length));
            tableProvider.Columns.AddColumn("value", strValues);
            var bigIntColumn = tableProvider.Columns.AddColumn("bigIntValue", strValues.Select(v => BigInteger.Parse(v)));
            bigIntColumn.ClickHouseDbType = ClickHouseDbType.UInt128;
            cmd.TableProviders.Add(tableProvider);

            await using var reader = await cmd.ExecuteReaderAsync();

            var valueColumnType = reader.GetFieldTypeInfo(0);
            Assert.Equal("UInt128", valueColumnType.ComplexTypeName);
            Assert.Equal(ClickHouseDbType.UInt128, valueColumnType.GetDbType());

            int count = 0;
            while (await reader.ReadAsync())
            {
                var value = reader.GetValue(0);
                var expectedValue = BigInteger.Parse(strValues[count]);
                Assert.Equal(expectedValue, value);

                var testPassed = reader.GetBoolean(1);
                Assert.True(testPassed);

                ++count;
            }

            Assert.Equal(strValues.Length, count);
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadUInt128ParameterScalar(ClickHouseParameterMode parameterMode)
        {
            await using var cn = await OpenConnectionAsync(parameterMode);

            var cmd = cn.CreateCommand("SELECT {v}");
            var parameter = new ClickHouseParameter("v") { ClickHouseDbType = ClickHouseDbType.UInt128 };
            cmd.Parameters.Add(parameter);

            var pairs = new (object value, BigInteger expected)[]
            {
                (0u, 0u),
                (1u, 1u),
                (byte.MaxValue, byte.MaxValue),
                (ushort.MaxValue, ushort.MaxValue),
                (uint.MaxValue, uint.MaxValue),
                (ulong.MaxValue, ulong.MaxValue),
                ((BigInteger.One << 128) - 1, (BigInteger.One << 128) - 1),
            };

            foreach (var pair in pairs)
            {
                parameter.Value = pair.value;
                var result = await cmd.ExecuteScalarAsync();
                var bigIntResult = Assert.IsType<BigInteger>(result);
                Assert.Equal(pair.expected, bigIntResult);
            }
        }

        [Fact]
        public async Task ReadInt256Column()
        {
            var minValue = -(BigInteger.One << 255);            
            var maxValue = (BigInteger.One << 255) - BigInteger.One;

            var maxStrLen = maxValue.ToString().Length;
            var sb = new StringBuilder(maxStrLen + 1).Append('-');
            for (int i = 1; i <= maxStrLen; i++)
                sb.Append((char)('0' + (i % 10)));

            var strValues = new[] { minValue.ToString(), sb.ToString(), "-1", "0", "1", sb.ToString(1, maxStrLen), maxValue.ToString() };

            await using var cn = await OpenConnectionAsync();
            var cmd = cn.CreateCommand("SELECT CAST(value AS Int256) AS v, v = bigIntValue AS testPassed FROM ptable ORDER BY id");

            var tableProvider = new ClickHouseTableProvider("ptable", strValues.Length);
            tableProvider.Columns.AddColumn("id", Enumerable.Range(1, strValues.Length));
            tableProvider.Columns.AddColumn("value", strValues);
            tableProvider.Columns.AddColumn("bigIntValue", strValues.Select(v => BigInteger.Parse(v)));            
            cmd.TableProviders.Add(tableProvider);

            await using var reader = await cmd.ExecuteReaderAsync();

            var valueColumnType = reader.GetFieldTypeInfo(0);
            Assert.Equal("Int256", valueColumnType.ComplexTypeName);
            Assert.Equal(ClickHouseDbType.Int256, valueColumnType.GetDbType());

            int count = 0;
            while(await reader.ReadAsync())
            {
                var value = reader.GetValue(0);
                var expectedValue = BigInteger.Parse(strValues[count]);
                Assert.Equal(expectedValue, value);

                var testPassed = reader.GetBoolean(1);
                Assert.True(testPassed);

                ++count;
            }

            Assert.Equal(strValues.Length, count);
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadInt256ParameterScalar(ClickHouseParameterMode parameterMode)
        {
            await using var cn = await OpenConnectionAsync(parameterMode);

            var cmd = cn.CreateCommand("SELECT {v}");
            var parameter = new ClickHouseParameter("v") { ClickHouseDbType = ClickHouseDbType.Int256 };
            cmd.Parameters.Add(parameter);

            var pairs = new (object value, BigInteger expected)[]
            {
                (0, 0),
                (-1, -1),
                (1, 1),
                (byte.MaxValue, byte.MaxValue),
                (sbyte.MinValue, sbyte.MinValue),
                (sbyte.MaxValue, sbyte.MaxValue),
                (short.MinValue, short.MinValue),
                (short.MaxValue, short.MaxValue),
                (ushort.MaxValue, ushort.MaxValue),
                (int.MinValue, int.MinValue),
                (int.MaxValue, int.MaxValue),
                (uint.MaxValue, uint.MaxValue),
                (long.MinValue, long.MinValue),
                (long.MaxValue, long.MaxValue),
                (ulong.MaxValue, ulong.MaxValue),
                (-(BigInteger.One << 255), -(BigInteger.One << 255)),
                ((BigInteger.One << 255) - 1, (BigInteger.One << 255) - 1),
            };

            foreach (var pair in pairs)
            {
                if (pair.value is BigInteger)
                    parameter.ResetDbType();

                parameter.Value = pair.value;
                var result = await cmd.ExecuteScalarAsync();
                var bigIntResult = Assert.IsType<BigInteger>(result);
                Assert.Equal(pair.expected, bigIntResult);
            }
        }

        [Fact]
        public async Task ReadUInt256Column()
        {
            var maxValue = (BigInteger.One << 256) - BigInteger.One;

            var maxStrLen = maxValue.ToString().Length;
            var sb = new StringBuilder(maxStrLen);
            sb.Append("11");
            for (int i = 3; i <= maxStrLen; i++)
                sb.Append((char)('0' + (i % 10)));

            var strValues = new[] { "0", "1", sb.ToString(), maxValue.ToString() };

            await using var cn = await OpenConnectionAsync();
            var cmd = cn.CreateCommand("SELECT CAST(value AS UInt256) AS v, v = bigIntValue AS testPassed FROM ptable ORDER BY id");

            var tableProvider = new ClickHouseTableProvider("ptable", strValues.Length);
            tableProvider.Columns.AddColumn("id", Enumerable.Range(1, strValues.Length));
            tableProvider.Columns.AddColumn("value", strValues);
            var bigIntColumn = tableProvider.Columns.AddColumn("bigIntValue", strValues.Select(v => BigInteger.Parse(v)));
            bigIntColumn.ClickHouseDbType = ClickHouseDbType.UInt256;
            cmd.TableProviders.Add(tableProvider);

            await using var reader = await cmd.ExecuteReaderAsync();

            var valueColumnType = reader.GetFieldTypeInfo(0);
            Assert.Equal("UInt256", valueColumnType.ComplexTypeName);
            Assert.Equal(ClickHouseDbType.UInt256, valueColumnType.GetDbType());            

            int count = 0;
            while (await reader.ReadAsync())
            {
                var value = reader.GetValue(0);
                var expectedValue = BigInteger.Parse(strValues[count]);
                Assert.Equal(expectedValue, value);

                var testPassed = reader.GetBoolean(1);
                Assert.True(testPassed);

                ++count;
            }

            Assert.Equal(strValues.Length, count);
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadUInt256ParameterScalar(ClickHouseParameterMode parameterMode)
        {
            await using var cn = await OpenConnectionAsync(parameterMode);

            var cmd = cn.CreateCommand("SELECT {v}");
            var parameter = new ClickHouseParameter("v") { ClickHouseDbType = ClickHouseDbType.UInt256 };
            cmd.Parameters.Add(parameter);

            var pairs = new (object value, BigInteger expected)[]
            {
                (0u, 0u),
                (1u, 1u),
                (byte.MaxValue, byte.MaxValue),
                (ushort.MaxValue, ushort.MaxValue),
                (uint.MaxValue, uint.MaxValue),
                (ulong.MaxValue, ulong.MaxValue),
                ((BigInteger.One << 256) - 1, (BigInteger.One << 256) - 1),
            };

            foreach (var pair in pairs)
            {
                parameter.Value = pair.value;
                var result = await cmd.ExecuteScalarAsync();
                var bigIntResult = Assert.IsType<BigInteger>(result);
                Assert.Equal(pair.expected, bigIntResult);
            }
        }

        [Fact]
        public async Task ReadArrayLowCardinality()
        {
            await WithTemporaryTable("arrlc", "id Int32, arr Array(LowCardinality(String))", Test);

            static async Task Test(ClickHouseConnection cn, string tableName)
            {
                var cmd = cn.CreateCommand($"SELECT * FROM {tableName}");
                await using (var reader = await cmd.ExecuteReaderAsync())
                    Assert.False(await reader.ReadAsync());

                cmd.CommandText = $"INSERT INTO {tableName}(id, arr) VALUES (1, [])";
                await cmd.ExecuteNonQueryAsync();

                cmd.CommandText = $"SELECT arr FROM {tableName}";

                await using (var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly))
                {
                    // Skipping all rows
                    Assert.False(await reader.ReadAsync());
                }

                await using (var reader = await cmd.ExecuteReaderAsync())
                {
                    Assert.True(await reader.ReadAsync());
                    Assert.Equal(Array.Empty<string>(), reader.GetValue(0));
                    Assert.False(await reader.ReadAsync());
                }

                cmd.CommandText = $"INSERT INTO {tableName}(id, arr) VALUES (2, ['abc', 'def'])(3, ['def', 'ghi'])(4, ['def'])(5, ['ghi', 'abc'])";
                await cmd.ExecuteNonQueryAsync();

                var expectedValues = new Dictionary<int, string[]>
                {
                    [1] = Array.Empty<string>(),
                    [2] = new[] { "abc", "def" },
                    [3] = new[] { "def", "ghi" },
                    [4] = new[] { "def" },
                    [5] = new[] { "ghi", "abc" }
                };

                cmd.CommandText = $"SELECT id, arr FROM {tableName}";
                await using (var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly))
                {
                    // Skipping all rows
                    Assert.False(await reader.ReadAsync());
                }

                await using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var id = reader.GetInt32(0);
                        var value = reader.GetValue(1);
                        var arr = Assert.IsType<string[]>(value);
                        Assert.True(expectedValues.Remove(id, out var expectedArr));
                        Assert.Equal(expectedArr, arr);
                    }
                }

                Assert.Empty(expectedValues);
            }
        }

        [Theory]
        [InlineData("2021-11-09", 2021, 11, 09)]
        [InlineData("1970-1-1", 0, 0, 0)] // Default value
        [InlineData("1970-1-2", 1970, 1, 2)]
        [InlineData("2149-06-06", 2149, 6, 6)]
        public async Task ReadDateScalar(string str, int year, int month, int day)
        {
            DateTime expectedDateTime = default;
#if NET6_0_OR_GREATER
            DateOnly expectedDate = default;
#endif
            if (year != 0 || month != 0 || day != 0)
            {
                expectedDateTime = new DateTime(year, month, day);
#if NET6_0_OR_GREATER
                expectedDate = new DateOnly(year, month, day);
#endif
            }

            await using var connection = await OpenConnectionAsync();

            await using var cmd = connection.CreateCommand($"SELECT cast('{str}' AS Date)");

            var result = await cmd.ExecuteScalarAsync();

            DateTime resultDateTime;
#if NET6_0_OR_GREATER
            var resultDateOnly = Assert.IsType<DateOnly>(result);
            Assert.Equal(expectedDate, resultDateOnly);
#else
            resultDateTime = Assert.IsType<DateTime>(result);
            Assert.Equal(expectedDateTime, resultDateTime);
#endif

            resultDateTime = await cmd.ExecuteScalarAsync<DateTime>();
            Assert.Equal(expectedDateTime, resultDateTime);
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadDateParameterScalar(ClickHouseParameterMode parameterMode)
        {
            var now = DateTime.Now;
            now = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second, now.Kind);

            var testData = new[] { now, default, new DateTime(1980, 12, 15, 3, 8, 58), new DateTime(2015, 1, 1, 18, 33, 55) };

            await using var connection = await OpenConnectionAsync(parameterMode);

            await using var cmd = connection.CreateCommand("SELECT {param} v, toString(v)");
            var param = new ClickHouseParameter("param") { ClickHouseDbType = ClickHouseDbType.Date };
            cmd.Parameters.Add(param);

            foreach (var testValue in testData)
            {
                param.Value = testValue;

                await using var reader = await cmd.ExecuteReaderAsync();
                Assert.True(await reader.ReadAsync());

                var result = reader.GetFieldValue<DateTime>(0);
                Assert.Equal(testValue.Date, result);

                if (testValue == default)
                    continue;

                var resultStr = reader.GetString(1);
                Assert.Equal(testValue.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), resultStr);
            }

            param.Value = DateTime.UnixEpoch.AddMonths(-1);
            var handledException = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync());
            Assert.IsType<OverflowException>(handledException.InnerException);

            param.Value = DateTime.UnixEpoch.AddDays(ushort.MaxValue).AddMonths(1);
            handledException = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync());
            Assert.IsType<OverflowException>(handledException.InnerException);
        }

#if NET6_0_OR_GREATER
        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadDateParameterScalarNet6(ClickHouseParameterMode parameterMode)
        {
            var nowDateTime = DateTime.Now;
            var now = new DateOnly(nowDateTime.Year, nowDateTime.Month, nowDateTime.Day);

            var testData = new[] { now, default, new DateOnly(1980, 12, 15), new DateOnly(2015, 1, 1) };

            await using var connection = await OpenConnectionAsync(parameterMode);

            await using var cmd = connection.CreateCommand("SELECT {param} v, toString(v)");
            var param = new ClickHouseParameter("param") { ClickHouseDbType = ClickHouseDbType.Date };
            cmd.Parameters.Add(param);

            foreach (var testValue in testData)
            {
                param.Value = testValue;

                Assert.Equal(ClickHouseDbType.Date, param.ClickHouseDbType);

                await using var reader = await cmd.ExecuteReaderAsync();
                Assert.True(await reader.ReadAsync());

                var result = reader.GetValue(0);
                var resultDateOnly = Assert.IsType<DateOnly>(result);
                Assert.Equal(testValue, resultDateOnly);

                if (testValue == default)
                    continue;

                var resultStr = reader.GetString(1);
                Assert.Equal(testValue.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), resultStr);
            }

            param.Value = DateOnly.FromDateTime(DateTime.UnixEpoch.AddMonths(-1));
            var handledException = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync());
            Assert.IsType<OverflowException>(handledException.InnerException);

            param.Value = DateOnly.FromDateTime(DateTime.UnixEpoch.AddDays(ushort.MaxValue).AddMonths(1));
            handledException = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync());
            Assert.IsType<OverflowException>(handledException.InnerException);
        }
#endif

        [Theory]
        [InlineData("2021-11-09", 2021, 11, 09)]
        [InlineData("1925-01-01", 0, 0, 0)] // Default value
        [InlineData("1925-1-02", 1925, 1, 2)]
        [InlineData("1970-1-1", 1970, 1, 1)]
        [InlineData("2283-11-11", 2283, 11, 11)]
        public async Task ReadDate32Scalar(string str, int year, int month, int day)
        {
            DateTime expectedDateTime = default;
#if NET6_0_OR_GREATER
            DateOnly expectedDate = default;
#endif
            if (year != 0 || month != 0 || day != 0)
            {
                expectedDateTime = new DateTime(year, month, day);
#if NET6_0_OR_GREATER
                expectedDate = new DateOnly(year, month, day);
#endif
            }

            await using var connection = await OpenConnectionAsync();

            await using var cmd = connection.CreateCommand($"SELECT cast('{str}' AS Date32)");

            var result = await cmd.ExecuteScalarAsync();

            DateTime resultDateTime;
#if NET6_0_OR_GREATER
            var resultDateOnly = Assert.IsType<DateOnly>(result);
            Assert.Equal(expectedDate, resultDateOnly);
#else
            resultDateTime = Assert.IsType<DateTime>(result);
            Assert.Equal(expectedDateTime, resultDateTime);
#endif

            resultDateTime = await cmd.ExecuteScalarAsync<DateTime>();
            Assert.Equal(expectedDateTime, resultDateTime);
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadDate32ParameterScalar(ClickHouseParameterMode parameterMode)
        {
            var now = DateTime.Now;
            var minValue = new DateTime(1925, 1, 1);
            var maxValue = new DateTime(2283, 11, 11);
            now = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second, now.Kind);

            var testData = new[] { now, default, new DateTime(1980, 12, 15, 3, 8, 58), new DateTime(2015, 1, 1, 18, 33, 55), minValue.AddDays(1), maxValue, DateTime.UnixEpoch };

            await using var connection = await OpenConnectionAsync(parameterMode);

            // toString(Date32) doesn't work well for all range https://github.com/ClickHouse/ClickHouse/issues/31924
            await using var cmd = connection.CreateCommand("SELECT {param} v, concat(toString(year(v)), '-', toString(month(v)), '-', toString(day(v)))");
            var param = new ClickHouseParameter("param") { ClickHouseDbType = ClickHouseDbType.Date32 };
            cmd.Parameters.Add(param);

            foreach (var testValue in testData)
            {
                param.Value = testValue;

                await using var reader = await cmd.ExecuteReaderAsync();
                Assert.True(await reader.ReadAsync());

                var result = reader.GetFieldValue<DateTime>(0);
                Assert.Equal(testValue.Date, result);

                if (testValue == default)
                    continue;

                var resultStr = reader.GetString(1);
                Assert.Equal(testValue.ToString("yyyy-M-d", CultureInfo.InvariantCulture), resultStr);
            }

            param.Value = minValue.AddMonths(-1);
            var handledException = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync());
            Assert.IsType<OverflowException>(handledException.InnerException);

            param.Value = maxValue.AddMonths(1);
            handledException = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync());
            Assert.IsType<OverflowException>(handledException.InnerException);
        }

#if NET6_0_OR_GREATER
        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadDate32ParameterScalarNet6(ClickHouseParameterMode parameterMode)
        {
            var nowDateTime = DateTime.Now;
            var now = new DateOnly(nowDateTime.Year, nowDateTime.Month, nowDateTime.Day);
            var minValue = new DateOnly(1925, 1, 1);
            var maxValue = new DateOnly(2283, 11, 11);
            var testData = new[] { now, default, new DateOnly(1980, 12, 15), new DateOnly(2015, 1, 1), minValue.AddDays(1), maxValue, DateOnly.FromDateTime(DateTime.UnixEpoch) };

            await using var connection = await OpenConnectionAsync(parameterMode);

            // toString(Date32) doesn't work well for all range https://github.com/ClickHouse/ClickHouse/issues/31924
            await using var cmd = connection.CreateCommand("SELECT {param} AS v, concat(toString(year(v)), '-', toString(month(v)), '-', toString(day(v)))");
            var param = new ClickHouseParameter("param") { ClickHouseDbType = ClickHouseDbType.Date32 };
            cmd.Parameters.Add(param);

            foreach (var testValue in testData)
            {
                param.Value = testValue;

                await using var reader = await cmd.ExecuteReaderAsync();
                Assert.True(await reader.ReadAsync());

                var result = reader.GetValue(0);
                var resultDateOnly = Assert.IsType<DateOnly>(result);
                Assert.Equal(testValue, resultDateOnly);

                if (testValue == default)
                    continue;

                var resultStr = reader.GetString(1);
                Assert.Equal(testValue.ToString("yyyy-M-d", CultureInfo.InvariantCulture), resultStr);
            }

            param.Value = minValue.AddMonths(-1);
            var handledException = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync());
            Assert.IsType<OverflowException>(handledException.InnerException);

            param.Value = maxValue.AddMonths(1);
            handledException = await Assert.ThrowsAsync<ClickHouseHandledException>(() => cmd.ExecuteScalarAsync());
            Assert.IsType<OverflowException>(handledException.InnerException);
        }
#endif

        [Fact]
        public async Task ReadMultidimensionalArrayLowCardinality()
        {
            await WithTemporaryTable("arrlc", "id Int32, arr Array(Array(Array(LowCardinality(Nullable(String)))))", Test);

            static async Task Test(ClickHouseConnection cn, string tableName)
            {
                var cmd = cn.CreateCommand($"SELECT * FROM {tableName}");
                await using (var reader = await cmd.ExecuteReaderAsync())
                    Assert.False(await reader.ReadAsync());

                cmd.CommandText = $"INSERT INTO {tableName}(id, arr) VALUES (1, [[[]]])";
                await cmd.ExecuteNonQueryAsync();

                cmd.CommandText = $"SELECT arr FROM {tableName}";
                await using (var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly))
                {
                    // Skipping all rows
                    Assert.False(await reader.ReadAsync());
                }

                await using (var reader = await cmd.ExecuteReaderAsync())
                {
                    Assert.True(await reader.ReadAsync());
                    Assert.Equal(new[] { new[] { Array.Empty<string>() } }, reader.GetValue(0));
                    Assert.False(await reader.ReadAsync());
                }

                cmd.CommandText = $"INSERT INTO {tableName}(id, arr) VALUES " +
                    "(2, [[['foo', 'bar'], [''], ['foo', 'bar', null, 'baz']]])" +
                    "(3, [[['foo', 'bar'], ['bar']], []])" +
                    "(4, [[['foo']], [[]], [['baz', null], ['bar', 'foo']]])" +
                    "(5, [[[NULL]], [[], ['baz']]])" +
                    "(6, [])" +
                    "(7, [[]])";

                await cmd.ExecuteNonQueryAsync();

                var expectedValues = new Dictionary<int, string?[][][]>
                {
                    [1] = new[] { new[] { Array.Empty<string>() } },
                    [2] = new[] { new[] { new[] { "foo", "bar" }, new[] { string.Empty }, new[] { "foo", "bar", null, "baz" } } },
                    [3] = new[] { new[] { new[] { "foo", "bar" }, new[] { "bar" } }, Array.Empty<string?[]>() },
                    [4] = new[] { new[] { new[] { "foo" } }, new[] { Array.Empty<string>() }, new[] { new[] { "baz", null }, new[] { "bar", "foo" } } },
                    [5] = new[] { new[] { new[] { default(string) } }, new[] { Array.Empty<string>(), new[] { "baz" } } },
                    [6] = Array.Empty<string?[][]>(),
                    [7] = new[] { Array.Empty<string?[]>() }
                };

                cmd.CommandText = $"SELECT id, arr FROM {tableName}";
                await using (var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly))
                {
                    // Skipping all rows
                    Assert.False(await reader.ReadAsync());
                }

                await using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while(await reader.ReadAsync())
                    {
                        var id = reader.GetInt32(0);
                        var value = reader.GetValue(1);
                        var arr = Assert.IsType<string?[][][]>(value);
                        Assert.True(expectedValues.Remove(id, out var expectedArr));
                        Assert.Equal(expectedArr, arr);
                    }
                }

                Assert.Empty(expectedValues);                
            }
        }

        [Theory]
        [InlineData("true::Bool", true)]
        [InlineData("false::Bool", false)]
        [InlineData("1::Bool", true)]
        [InlineData("0::Bool", false)]
        [InlineData("NULL::Nullable(Bool)", null)]
        public async Task ReadBoolScalar(string value, bool? expectedValue)
        {
            await using var connection = await OpenConnectionAsync();

            var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT {value}";

            var result = await cmd.ExecuteScalarAsync();
            Assert.Equal((object?)expectedValue ?? DBNull.Value, result);
        }

        [Theory]
        [MemberData(nameof(ParameterModes))]
        public async Task ReadBoolParameter(ClickHouseParameterMode parameterMode)
        {
            var testData = new[] { false, true, (object?)null, DBNull.Value };

            await using var connection = await OpenConnectionAsync(parameterMode);

            var sb = new StringBuilder("SELECT ");
            var cmd = connection.CreateCommand();
            for (int i = 0; i < testData.Length * 2; i++)
            {
                var value = testData[i % testData.Length];
                ClickHouseParameter p;
                if (value is bool)
                    p = cmd.Parameters.AddWithValue($"p{i + 1}", value);
                else
                    p = cmd.Parameters.AddWithValue($"p{i + 1}", value, ClickHouseDbType.Boolean);

                if (i > 0)
                    sb.Append(", ");

                if (i > testData.Length)
                    p.ParameterMode = ClickHouseParameterMode.Interpolate;

                sb.Append($"{{p{i + 1}}} AS p{i + 1}");
            }

            cmd.CommandText = sb.ToString();
            await using var reader = await cmd.ExecuteReaderAsync();
            Assert.Equal(testData.Length*2, reader.FieldCount);

            Assert.True(await reader.ReadAsync());
            for (int i = 0; i < testData.Length * 2; i++)
            {
                Assert.Equal(ClickHouseDbType.Boolean, reader.GetFieldTypeInfo(i).GetDbType());
                var value = testData[i % testData.Length];

                if (value is bool boolVal)
                {
                    var result = reader.GetValue(i);
                    Assert.Equal(boolVal, result);
                }
                else
                {
                    Assert.True(reader.IsDBNull(i));
                }
            }

            Assert.False(await reader.ReadAsync());
        }

        [Fact]
        public async Task CreateInsertSelectAllKnownNullable()
        {
            const string ddl = @"
CREATE TABLE clickhouse_test_nullable (
    int8     Nullable(Int8),
    int16    Nullable(Int16),
    int32    Nullable(Int32),
    int64    Nullable(Int64),
    uint8    Nullable(UInt8),
    uint16   Nullable(UInt16),
    uint32   Nullable(UInt32),
    uint64   Nullable(UInt64),
    float32  Nullable(Float32),
    float64  Nullable(Float64),
    string   Nullable(String),
    fString  Nullable(FixedString(2)),
    date     Nullable(Date),
    datetime Nullable(DateTime),
    enum8    Nullable(Enum8 ('a' = 127, 'b' = 2)),
    enum16   Nullable(Enum16('c' = -32768, 'd' = 2, '' = 42))
) Engine=Memory;";

            const string dml = @"
INSERT INTO clickhouse_test_nullable (
    int8
    ,int16
    ,int32
    ,int64
    ,uint8
    ,uint16
    ,uint32
    ,uint64
    ,float32
    ,float64
    ,string
    ,fString
    ,date
    ,datetime
    ,enum8
    ,enum16
) SELECT
    8
    ,16
    ,32
    ,64
    ,18
    ,116
    ,132
    ,165
    ,1.1
    ,1.2
    ,'RU'
    ,'UA'
    ,now()
    ,now()
    ,'a'
    ,'c'";

            const string query = @"
SELECT
    int8
    ,int16
    ,int32
    ,int64
    ,uint8
    ,uint16
    ,uint32
    ,uint64
    ,float32
    ,float64
    ,string
    ,fString
    ,date
    ,datetime
    ,enum8
    ,enum16
FROM clickhouse_test_nullable";

            try
            {
                await using var connection = await OpenConnectionAsync();
                
                await using var cmdDrop = connection.CreateCommand("DROP TABLE IF EXISTS clickhouse_test_nullable ");
                var ddlResult = await cmdDrop.ExecuteNonQueryAsync();
                Assert.Equal(0, ddlResult);

                await using var cmd = connection.CreateCommand(ddl);
                var result = await cmd.ExecuteNonQueryAsync();
                Assert.Equal(0, result);

                await using var dmlcmd = connection.CreateCommand(dml);
                result = await dmlcmd.ExecuteNonQueryAsync();
                if (!connection.ServerVersion.StartsWith("21.11."))
                {
                    // The server of the version 21.11 doesn't send profile events.
                    Assert.Equal(1, result);
                }

                await using var queryCmd = connection.CreateCommand(query);
                var r = await queryCmd.ExecuteReaderAsync();
                while (r.Read())
                {
                    Assert.Equal(typeof(sbyte), r.GetFieldType(0));
                    Assert.Equal((sbyte) 8, r.GetFieldValue<sbyte>(0)); //int8
                    Assert.Equal((sbyte) 8, r.GetValue(0));

                    Assert.Equal(typeof(short), r.GetFieldType(1));
                    Assert.Equal((short) 16, r.GetInt16(1));
                    Assert.Equal((short) 16, r.GetValue(1));

                    Assert.Equal(typeof(int), r.GetFieldType(2));
                    Assert.Equal((int) 32, r.GetInt32(2));
                    Assert.Equal((int) 32, r.GetValue(2));

                    Assert.Equal(typeof(long), r.GetFieldType(3));
                    Assert.Equal((long) 64, r.GetInt64(3));
                    Assert.Equal((long) 64, r.GetValue(3));

                    Assert.Equal(typeof(byte), r.GetFieldType(4));
                    Assert.Equal((byte) 18, r.GetFieldValue<byte>(4)); //uint8
                    Assert.Equal((byte) 18, r.GetValue(4));

                    Assert.Equal(typeof(ushort), r.GetFieldType(5));
                    Assert.Equal((ushort) 116, r.GetFieldValue<ushort>(5));
                    Assert.Equal((ushort) 116, r.GetValue(5));

                    Assert.Equal(typeof(uint), r.GetFieldType(6));
                    Assert.Equal((uint) 132, r.GetFieldValue<uint>(6));
                    Assert.Equal((uint) 132, r.GetValue(6));

                    Assert.Equal(typeof(ulong), r.GetFieldType(7));
                    Assert.Equal((UInt64) 165, r.GetFieldValue<UInt64>(7));
                    Assert.Equal((UInt64) 165, r.GetValue(7));

                    Assert.Equal(typeof(float), r.GetFieldType(8));
                    Assert.Equal((float) 1.1, r.GetFloat(8));
                    Assert.Equal((float) 1.1, r.GetValue(8));

                    Assert.Equal(typeof(double), r.GetFieldType(9));
                    Assert.Equal((double) 1.2, r.GetDouble(9));
                    Assert.Equal((double) 1.2, r.GetValue(9));

                    Assert.Equal(typeof(string), r.GetFieldType(10));
                    Assert.Equal("RU", r.GetString(10));
                    Assert.Equal("RU", r.GetValue(10));

                    Assert.Equal(typeof(byte[]), r.GetFieldType(11));
                    var fixedStringBytes = r.GetFieldValue<byte[]>(11);
                    var fixedStringBytesAsValue = r.GetValue(11) as byte[];
                    Assert.Equal(fixedStringBytes, fixedStringBytesAsValue);
                    Assert.NotNull(fixedStringBytes as byte[]);
                    Assert.Equal("UA", Encoding.Default.GetString(fixedStringBytes as byte[]));

                    Assert.Equal(typeof(string), r.GetFieldType(14));
                    Assert.Equal("a", r.GetValue(14));
                    Assert.Equal(127, r.GetFieldValue<sbyte>(14));

                    Assert.Equal(typeof(string), r.GetFieldType(15));
                    Assert.Equal("c", r.GetValue(15));
                    Assert.Equal(-32768, r.GetInt16(15));
                }
            }
            finally
            {
                await using var connection = await OpenConnectionAsync();
                await using var cmdDrop = connection.CreateCommand("DROP TABLE IF EXISTS clickhouse_test_nullable ");
                await cmdDrop.ExecuteNonQueryAsync();
            }
        }
    }
}
