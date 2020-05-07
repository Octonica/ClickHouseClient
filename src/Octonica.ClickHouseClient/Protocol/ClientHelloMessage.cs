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

namespace Octonica.ClickHouseClient.Protocol
{
    internal class ClientHelloMessage : IClientMessage
    {
        public ClientMessageCode MessageCode => ClientMessageCode.Hello;

        public string ClientName { get;  }

        public ClickHouseVersion ClientVersion { get; }

        public int ProtocolRevision { get;  }

        public string? Database { get;  }

        public string User { get;  }

        public string? Password { get; }

        private ClientHelloMessage(Builder builder)
        {
            ClientName = builder.ClientName ?? throw new ArgumentException("The name of the client can't be null.", nameof(ClientName));
            ClientVersion = builder.ClientVersion ?? throw new ArgumentException("The version of the client can't be null.", nameof(ClientVersion));
            ProtocolRevision = builder.ProtocolRevision ?? throw new ArgumentException("The revision of the protocol is required.", nameof(ProtocolRevision));
            Database = builder.Database;
            User = builder.User ?? throw new ArgumentException("The name of the user is required.", nameof(User));
            Password = builder.Password;
        }

        public void Write(ClickHouseBinaryProtocolWriter writer)
        {
            writer.Write7BitInt32((int) MessageCode);

            writer.WriteString(ClientName);
            writer.Write7BitInt32(ClientVersion.Major);
            writer.Write7BitInt32(ClientVersion.Minor);
            writer.Write7BitInt32(ProtocolRevision);

            writer.WriteString(Database ?? string.Empty);
            writer.WriteString(User);
            writer.WriteString(Password ?? string.Empty);
        }

        internal class Builder
        {
            /// <summary>
            /// Required
            /// </summary>
            public string? ClientName { get; set; }

            /// <summary>
            /// Required
            /// </summary>
            public ClickHouseVersion? ClientVersion { get; set; }

            /// <summary>
            /// Required
            /// </summary>
            public int? ProtocolRevision { get; set; }

            /// <summary>
            /// Required
            /// </summary>
            public string? Database { get; set; }

            /// <summary>
            /// Required
            /// </summary>
            public string? User { get; set; }

            /// <summary>
            /// Optional
            /// </summary>
            public string? Password { get; set; }

            public ClientHelloMessage Build()
            {
                return new ClientHelloMessage(this);
            }
        }
    }
}
