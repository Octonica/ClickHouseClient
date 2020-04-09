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

using System;
using System.Threading;
using System.Threading.Tasks;
using Octonica.ClickHouseClient.Exceptions;

namespace Octonica.ClickHouseClient.Protocol
{
    internal sealed class ServerErrorMessage : IServerMessage
    {
        public ServerMessageCode MessageCode => ServerMessageCode.Error;

        public ClickHouseServerException Exception { get; }

        private ServerErrorMessage(ClickHouseServerException exception)
        {
            Exception = exception ?? throw new ArgumentNullException(nameof(exception));
        }

        public static async ValueTask<ServerErrorMessage> Read(ClickHouseBinaryProtocolReader reader, bool async, CancellationToken cancellationToken)
        {
            var errorCode = await reader.ReadInt32(async, cancellationToken);
            var name = await reader.ReadString(async, cancellationToken);
            var errorMessage = await reader.ReadString(async, cancellationToken);
            var stackTrace = await reader.ReadString(async, cancellationToken);

            bool hasNested = await reader.ReadBool(async, cancellationToken);
            ClickHouseServerException exception;
            if (hasNested)
            {
                var nested = await Read(reader, async, cancellationToken);
                exception = new ClickHouseServerException(errorCode, name, errorMessage, stackTrace, nested.Exception);
            }
            else
            {
                exception = new ClickHouseServerException(errorCode, name, errorMessage, stackTrace);
            }

            return new ServerErrorMessage(exception);
        }
    }
}
