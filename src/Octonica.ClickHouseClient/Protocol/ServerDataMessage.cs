#region License Apache 2.0
/* Copyright 2019-2020 Octonica LLC
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
    internal sealed class ServerDataMessage : IServerMessage
    {
        public ServerMessageCode MessageCode { get; }

        public string? TempTableName { get; }

        private ServerDataMessage(ServerMessageCode messageCode, string? tempTableName)
        {
            MessageCode = messageCode;
            TempTableName = tempTableName;
        }

        public static async ValueTask<ServerDataMessage> Read(ClickHouseBinaryProtocolReader reader, ServerMessageCode messageCode, bool async, CancellationToken cancellationToken)
        {
            string? tempTableName = await reader.ReadString(async, cancellationToken);
            if (tempTableName == string.Empty)
                tempTableName = null;

            return new ServerDataMessage(messageCode, tempTableName);
        }
    }
}
