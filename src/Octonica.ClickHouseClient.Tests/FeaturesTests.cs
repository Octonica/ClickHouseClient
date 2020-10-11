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

using System.Threading.Tasks;
using Xunit;

namespace Octonica.ClickHouseClient.Tests
{
    public class FeaturesTests : ClickHouseTestsBase
    {
        [Fact]
        public async Task TotalsWithNextResult()
        {
            await using var cn = await OpenConnectionAsync();

            await using var cmd = cn.CreateCommand();
            cmd.CommandText = "select x, sum(y) as v from (SELECT number%2 + 1 as x, number as y FROM numbers(10)) group by x with totals;";
            using var reader = await cmd.ExecuteReaderAsync();
            ulong rowsTotal = 0;
            while (reader.Read())
            {
                rowsTotal+= reader.GetFieldValue<ulong>(1);

            }
            var hasTotals = reader.NextResult();
            Assert.True(hasTotals);
            while (reader.Read())
            {
                var total = reader.GetFieldValue<ulong>(1);
                Assert.Equal(rowsTotal, total);
            }
        }
    }
}
