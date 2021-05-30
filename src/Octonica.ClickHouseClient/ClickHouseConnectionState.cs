#region License Apache 2.0
/* Copyright 2020-2021 Octonica
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

using System.Data;

namespace Octonica.ClickHouseClient
{
    internal sealed class ClickHouseConnectionState
    {
        public ConnectionState State { get; }

        public ClickHouseConnectionSettings? Settings { get; }

        public int Counter { get; }

        public ClickHouseTcpClient? TcpClient { get; }

        public ClickHouseConnectionState()
            : this(ConnectionState.Closed, null, null, 0)
        {
        }

        public ClickHouseConnectionState(ConnectionState state, ClickHouseTcpClient? tcpClient, ClickHouseConnectionSettings? settings, int counter)
        {
            State = state;
            TcpClient = tcpClient;
            Settings = settings;
            Counter = counter;
        }
    }
}
