#region License Apache 2.0
/* Copyright 2019-2021, 2023 Octonica
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
using Octonica.ClickHouseClient.Protocol;
using Xunit;

namespace Octonica.ClickHouseClient.Tests
{
    public class ClickHouseConnectionPoolTests : ClickHouseTestsBase
    {
        [Fact]
        public async Task RentAsync_ConnectionsAreReturnedToPool()
        {
            var settings = GetDefaultConnectionSettings();
            using var pool = new ClickHouseConnectionPool(settings, maxPoolSize: 3);

            // Rent 3 connections
            var conn1 = await pool.RentAsync();
            var conn2 = await pool.RentAsync();
            var conn3 = await pool.RentAsync();

            // If the number of connection pools exceeds the limit, an exception is thrown
            await Assert.ThrowsAnyAsync<Exception>(() => pool.RentAsync());
            
            Assert.Equal(ConnectionState.Open, conn1.State);
            Assert.Equal(ConnectionState.Open, conn2.State);
            Assert.Equal(ConnectionState.Open, conn3.State);
            
            // Return connect
            pool.Return(conn1);
            pool.Return(conn2);
            pool.Return(conn2);
            
        }

    }
}
