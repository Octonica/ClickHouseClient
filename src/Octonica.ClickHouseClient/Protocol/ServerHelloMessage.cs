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

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Octonica.ClickHouseClient.Exceptions;

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
            var serverName = await reader.ReadString(async, cancellationToken);
            var mj = await reader.Read7BitInt32(async, cancellationToken);
            var mr = await reader.Read7BitInt32(async, cancellationToken);
            var rv = await reader.Read7BitInt32(async, cancellationToken);
            if (rv < ClickHouseProtocolRevisions.MinSupportedRevision)
            {
                throw new ClickHouseException(
                    ClickHouseErrorCodes.ProtocolRevisionNotSupported,
                    $"The revision {rv} of ClickHouse server is not supported. Minimal supported revision is {ClickHouseProtocolRevisions.MinSupportedRevision}.");
            }

            var tz = await reader.ReadString(async, cancellationToken);
            var displayName = await reader.ReadString(async, cancellationToken);
            var versionPatch = await reader.Read7BitInt32(async, cancellationToken);
            var serverVersion = new ClickHouseVersion(mj, mr, versionPatch);

            var negotiatedRevision = Math.Min(rv, protocolRevision);
            List<ClickHousePasswordComplexityRule>? complexityRules = null;
            if (negotiatedRevision >= ClickHouseProtocolRevisions.MinRevisionWithPasswordComplexityRules)
            {
                var rulesCount = await reader.Read7BitInt32(async, cancellationToken);
                if (rulesCount > 0)
                {
                    complexityRules = new List<ClickHousePasswordComplexityRule>(rulesCount);
                    for (int i = 0; i < rulesCount; i++)
                    {
                        var pattern = await reader.ReadString(async, cancellationToken);
                        var message = await reader.ReadString(async, cancellationToken);
                        var rule = new ClickHousePasswordComplexityRule(pattern, message);
                        complexityRules.Add(rule);
                    }
                }
            }

            var serverInfo = new ClickHouseServerInfo(serverName, serverVersion, serverRevision: rv, revision: negotiatedRevision, tz, displayName, complexityRules?.AsReadOnly());
            return new ServerHelloMessage(serverInfo);
        }
    }
}
