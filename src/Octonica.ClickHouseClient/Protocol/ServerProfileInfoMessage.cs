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

using System.Threading;
using System.Threading.Tasks;

namespace Octonica.ClickHouseClient.Protocol
{
    internal sealed class ServerProfileInfoMessage : IServerMessage
    {
        public ServerMessageCode MessageCode => ServerMessageCode.ProfileInfo;

        public int Rows { get; }

        public int Blocks { get; }

        public int Bytes { get; }

        public bool LimitApplied { get; }

        public int RowsBeforeLimit { get; }

        public int CalculatedRowsBeforeLimit { get; }

        private ServerProfileInfoMessage(int rows, int blocks, int bytes, bool limitApplied, int rowsBeforeLimit, int calculatedRowsBeforeLimit)
        {
            Rows = rows;
            Blocks = blocks;
            Bytes = bytes;
            LimitApplied = limitApplied;
            RowsBeforeLimit = rowsBeforeLimit;
            CalculatedRowsBeforeLimit = calculatedRowsBeforeLimit;
        }

        public static async ValueTask<ServerProfileInfoMessage> Read(ClickHouseBinaryProtocolReader reader, bool async, CancellationToken cancellationToken)
        {
            int rows = await reader.Read7BitInt32(async, cancellationToken);
            int blocks = await reader.Read7BitInt32(async, cancellationToken);
            int bytes = await reader.Read7BitInt32(async, cancellationToken);
            bool limitApplied = await reader.ReadBool(async, cancellationToken);
            int rowsBeforeLimit = await reader.Read7BitInt32(async, cancellationToken);
            int calculatedRowsBeforeLimit = await reader.Read7BitInt32(async, cancellationToken);

            return new ServerProfileInfoMessage(rows, blocks, bytes, limitApplied, rowsBeforeLimit, calculatedRowsBeforeLimit);
        }
    }
}
