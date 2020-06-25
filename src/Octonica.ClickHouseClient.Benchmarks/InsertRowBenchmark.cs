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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Octonica.ClickHouseClient.Benchmarks
{
    public class InsertRowBenchmark
    {
        private ClickHouseConnectionSettings _connectionSettings;

        private List<Guid> _id;
        private List<string?> _str;
        private List<DateTime> _dt;
        private List<decimal> _val;

        [Params(50, 200)]
        public int Rows;

        [Params(true, false)]
        public bool KeepConnection;

        [GlobalSetup]
        public void Setup()
        {
            _connectionSettings = ConnectionSettingsHelper.GetConnectionSettings();

            using (var connection = new ClickHouseConnection(_connectionSettings))
            {
                connection.Open();

                var cmd = connection.CreateCommand("DROP TABLE IF EXISTS ColumnWriterInsertBenchmarks");
                cmd.ExecuteNonQuery();

                cmd.CommandText = "CREATE TABLE ColumnWriterInsertBenchmarks(id UUID, str Nullable(String), dt DateTime, val Decimal64(4)) ENGINE = Memory";
                cmd.ExecuteNonQuery();
            }

            _id = new List<Guid>(Rows);
            _str = new List<string?>(Rows);
            _dt = new List<DateTime>(Rows);
            _val = new List<decimal>(Rows);

            const string strSource =
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

            var now = DateTime.Now;
            var rnd = new Random(2306);
            for (int i = 0; i < Rows; i++)
            {
                _id.Add(Guid.NewGuid());
                _str.Add(i % strSource.Length == 0 ? null : strSource.Substring(0, i % strSource.Length));
                _dt.Add(now.AddSeconds(i));
                _val.Add(Math.Round((decimal)(rnd.NextDouble() - 0.5) * 100_000, 4));
            }
        }

        [Benchmark]
        public async Task InsertWithColumnWriter()
        {
            var id = new List<Guid>(1) {Guid.Empty};
            var str = new List<string?>(1) {null};
            var dt = new List<DateTime>(1) {DateTime.MinValue};
            var val = new List<decimal>(1) {0};

            if (KeepConnection)
            {
                await using var connection = new ClickHouseConnection(_connectionSettings);
                await connection.OpenAsync();
                for (int i = 0; i < Rows; i++)
                {
                    await using var writer = connection.CreateColumnWriter("INSERT INTO ColumnWriterInsertBenchmarks VALUES");

                    var columns = new object[4];
                    columns[writer.GetOrdinal("id")] = id;
                    columns[writer.GetOrdinal("str")] = str;
                    columns[writer.GetOrdinal("dt")] = dt;
                    columns[writer.GetOrdinal("val")] = val;


                    id[0] = _id[i];
                    str[0] = _str[i];
                    dt[0] = _dt[i];
                    val[0] = _val[i];

                    await writer.WriteTableAsync(columns, 1, CancellationToken.None);
                    await writer.EndWriteAsync(CancellationToken.None);
                }
            }
            else
            {
                for (int i = 0; i < Rows; i++)
                {
                    await using var connection = new ClickHouseConnection(_connectionSettings);
                    await connection.OpenAsync();

                    await using var writer = connection.CreateColumnWriter("INSERT INTO ColumnWriterInsertBenchmarks VALUES");

                    var columns = new object[4];
                    columns[writer.GetOrdinal("id")] = id;
                    columns[writer.GetOrdinal("str")] = str;
                    columns[writer.GetOrdinal("dt")] = dt;
                    columns[writer.GetOrdinal("val")] = val;

                    id[0] = _id[i];
                    str[0] = _str[i];
                    dt[0] = _dt[i];
                    val[0] = _val[i];

                    await writer.WriteTableAsync(columns, 1, CancellationToken.None);
                    //await writer.EndWriteAsync(CancellationToken.None);
                }
            }
        }

        [Benchmark]
        public async Task InsertFromSelect()
        {
            if (KeepConnection)
            {
                await using var connection = new ClickHouseConnection(_connectionSettings);
                await connection.OpenAsync();

                for (int i = 0; i < Rows; i++)
                {
                    await using var cmd = connection.CreateCommand("INSERT INTO ColumnWriterInsertBenchmarks(id, str, dt, val) SELECT {id:UUID}, {str}, {dt}, {val}");

                    var idParam = new ClickHouseParameter("id") {Value = _id[i].ToString(), ClickHouseDbType = ClickHouseDbType.String};
                    cmd.Parameters.Add(idParam);

                    var strParam = new ClickHouseParameter("str") {Value = _str[i], ClickHouseDbType = ClickHouseDbType.String};
                    cmd.Parameters.Add(strParam);

                    var dtParam = new ClickHouseParameter("dt") {Value = _dt[i], ClickHouseDbType = ClickHouseDbType.DateTime};
                    cmd.Parameters.Add(dtParam);

                    var valParam = new ClickHouseParameter("val") {Value = _val[i], ClickHouseDbType = ClickHouseDbType.Decimal};
                    cmd.Parameters.Add(valParam);

                    await cmd.ExecuteNonQueryAsync();
                }
            }
            else
            {
                for (int i = 0; i < Rows; i++)
                {
                    await using var connection = new ClickHouseConnection(_connectionSettings);
                    await connection.OpenAsync();

                    await using var cmd = connection.CreateCommand("INSERT INTO ColumnWriterInsertBenchmarks(id, str, dt, val) SELECT {id:UUID}, {str}, {dt}, {val}");

                    var idParam = new ClickHouseParameter("id") {Value = _id[i].ToString(), ClickHouseDbType = ClickHouseDbType.String};
                    cmd.Parameters.Add(idParam);

                    var strParam = new ClickHouseParameter("str") {Value = _str[i], ClickHouseDbType = ClickHouseDbType.String};
                    cmd.Parameters.Add(strParam);

                    var dtParam = new ClickHouseParameter("dt") {Value = _dt[i], ClickHouseDbType = ClickHouseDbType.DateTime};
                    cmd.Parameters.Add(dtParam);

                    var valParam = new ClickHouseParameter("val") {Value = _val[i], ClickHouseDbType = ClickHouseDbType.Decimal};
                    cmd.Parameters.Add(valParam);

                    await cmd.ExecuteNonQueryAsync();

                    if (!KeepConnection)
                    {
                        await cmd.DisposeAsync();
                        await connection.DisposeAsync();
                    }
                }
            }
        }
    }
}
