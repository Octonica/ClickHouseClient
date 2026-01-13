#region License Apache 2.0
/* Copyright 2019-2020, 2023 Octonica
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
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Octonica.ClickHouseClient.Protocol
{
    internal sealed class ServerHelloMessage : IServerMessage
    {
        public ServerMessageCode MessageCode => ServerMessageCode.Hello;

        public ClickHouseServerInfo ServerInfo { get; }

        private ServerHelloMessage(ClickHouseServerInfo serverInfo)
        {
            ServerInfo = serverInfo ?? throw new ArgumentNullException(nameof(serverInfo));
        }

        public static async Task<ServerHelloMessage> Read(ClickHouseBinaryProtocolReader reader, int protocolRevision, bool async, CancellationToken cancellationToken)
        {
            string serverName = await reader.ReadString(async, cancellationToken);
            int mj = await reader.Read7BitInt32(async, cancellationToken);
            int mr = await reader.Read7BitInt32(async, cancellationToken);
            int rv = await reader.Read7BitInt32(async, cancellationToken);
            if (rv < ClickHouseProtocolRevisions.MinSupportedRevision)
            {
                throw new ClickHouseException(
                    ClickHouseErrorCodes.ProtocolRevisionNotSupported,
                    $"The revision {rv} of ClickHouse server is not supported. Minimal supported revision is {ClickHouseProtocolRevisions.MinSupportedRevision}.");
            }

            string tz = await reader.ReadString(async, cancellationToken);
            string displayName = await reader.ReadString(async, cancellationToken);
            int versionPatch = await reader.Read7BitInt32(async, cancellationToken);
            ClickHouseVersion serverVersion = new(mj, mr, versionPatch);

            int negotiatedRevision = Math.Min(rv, protocolRevision);
            List<ClickHousePasswordComplexityRule>? complexityRules = null;
            if (negotiatedRevision >= ClickHouseProtocolRevisions.MinRevisionWithPasswordComplexityRules)
            {
                int rulesCount = await reader.Read7BitInt32(async, cancellationToken);
                if (rulesCount > 0)
                {
                    complexityRules = new List<ClickHousePasswordComplexityRule>(rulesCount);
                    for (int i = 0; i < rulesCount; i++)
                    {
                        string pattern = await reader.ReadString(async, cancellationToken);
                        string message = await reader.ReadString(async, cancellationToken);
                        ClickHousePasswordComplexityRule rule = new(pattern, message);
                        complexityRules.Add(rule);
                    }
                }
            }

            if (negotiatedRevision >= ClickHouseProtocolRevisions.MinRevisionWithInterserverSecretV2)
            {
                await reader.SkipBytes(8, async, cancellationToken); // nonce
            }

            ClickHouseServerInfo serverInfo = new(serverName, serverVersion, serverRevision: rv, revision: negotiatedRevision, tz, displayName, complexityRules?.AsReadOnly());
            return new ServerHelloMessage(serverInfo);
        }
    }
}
