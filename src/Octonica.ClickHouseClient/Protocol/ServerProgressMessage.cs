#region License Apache 2.0
/* Copyright 2019-2020, 2023, 2026 Octonica
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
    internal sealed class ServerProgressMessage : IServerMessage
    {
        public ServerMessageCode MessageCode => ServerMessageCode.Progress;

        public ClickHouseQueryExecutionProgress ExecutionProgress { get; }

        private ServerProgressMessage(ulong rows, ulong bytes, ulong totalRows, ulong totalBytes, ulong writtenRows, ulong writtenBytes, ulong elapsedNanoseconds)
        {
            ExecutionProgress = new ClickHouseQueryExecutionProgress(rows, bytes, totalRows, totalBytes, writtenRows, writtenBytes, elapsedNanoseconds);
        }

        public static async ValueTask<ServerProgressMessage> Read(ClickHouseBinaryProtocolReader reader, int protocolRevision, bool async, CancellationToken cancellationToken)
        {
            ulong rows = await reader.Read7BitUInt64(async, cancellationToken);
            ulong bytes = await reader.Read7BitUInt64(async, cancellationToken);
            ulong totalRows = await reader.Read7BitUInt64(async, cancellationToken);

            ulong totalBytes = 0;
            if (protocolRevision >= ClickHouseProtocolRevisions.MinRevisionWithTotalBytesInProgress)
                totalBytes = await reader.Read7BitUInt64(async, cancellationToken);

            ulong writtenRows = await reader.Read7BitUInt64(async, cancellationToken);
            ulong writtenBytes = await reader.Read7BitUInt64(async, cancellationToken);

            ulong elapsedNanoseconds = 0;
            if (protocolRevision >= ClickHouseProtocolRevisions.MinRevisionWithServerQueryTimeInProgress)
                elapsedNanoseconds = await reader.Read7BitUInt64(async, cancellationToken);

            return new ServerProgressMessage(rows, bytes, totalRows, totalBytes, writtenRows, writtenBytes, elapsedNanoseconds);
        }
    }
}
