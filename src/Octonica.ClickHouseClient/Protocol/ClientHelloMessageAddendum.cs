#region License Apache 2.0
/* Copyright 2026 Octonica
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

namespace Octonica.ClickHouseClient.Protocol
{
    internal sealed class ClientHelloMessageAddendum
    {
        public string? QuotaKey { get; }

        public bool SendChunked { get; }

        public bool ReceiveChunked { get; }

        public int ProtocolRevision { get; }

        private ClientHelloMessageAddendum(Builder builder)
        {
            if (builder.ProtocolRevision < ClickHouseProtocolRevisions.MinSupportedRevision)
                throw new ArgumentException($"Internal error. The protocol version must be greater than {ClickHouseProtocolRevisions.MinSupportedRevision}.", nameof(ProtocolRevision));

            if (builder.ProtocolRevision < ClickHouseProtocolRevisions.MinRevisionWithChunkedPackets)
            {
                string? invalidChunkedProp = null;
                if (builder.SendChunked)
                    invalidChunkedProp = nameof(SendChunked);
                else if (builder.ReceiveChunked)
                    invalidChunkedProp = nameof(ReceiveChunked);

                if (invalidChunkedProp != null)
                    throw new ArgumentException($"Internal error. Chunked packets are not supported by the protocol version {builder.ProtocolRevision}.", invalidChunkedProp);
            }

            QuotaKey = builder.QuotaKey;
            SendChunked = builder.SendChunked;
            ReceiveChunked = builder.ReceiveChunked;
            ProtocolRevision = builder.ProtocolRevision;
        }

        public void Write(ClickHouseBinaryProtocolWriter writer)
        {
            writer.WriteString(QuotaKey ?? string.Empty);

            if (ProtocolRevision >= ClickHouseProtocolRevisions.MinRevisionWithChunkedPackets)
            {
                writer.WriteString(SendChunked ? "chunked" : "notchunked");
                writer.WriteString(ReceiveChunked ? "chunked" : "notchunked");
            }
        }

        internal sealed class Builder
        {
            public string? QuotaKey { get; set; }

            public bool SendChunked { get; set; }

            public bool ReceiveChunked { get; set; }

            public int ProtocolRevision { get; set; }

            public ClientHelloMessageAddendum Build()
            {
                return new ClientHelloMessageAddendum(this);
            }
        }
    }
}
