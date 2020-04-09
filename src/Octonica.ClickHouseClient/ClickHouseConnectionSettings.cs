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

namespace Octonica.ClickHouseClient
{
    public class ClickHouseConnectionSettings
    {
        public string Host { get; }

        public ushort Port { get; }

        public string User { get; }

        public string? Password { get; }

        public string Database { get; }

        public int ReadWriteTimeout { get; }

        public string ClientName { get; }

        public ClickHouseVersion ClientVersion { get; }

        public int BufferSize { get; }

        public bool Compress { get; }

        public int CommandTimeout { get; }

        internal readonly int CompressionBlockSize = 1024 * 8; // Maybe it should be configurable

        internal ClickHouseConnectionSettings(ClickHouseConnectionStringBuilder builder)
        {
            if (string.IsNullOrWhiteSpace(builder.Host))
                throw new ArgumentException("The host is not defined.", nameof(builder));

            if (builder.BufferSize <= 0)
                throw new ArgumentException("The size of the buffer must be a positive number.", nameof(builder));

            Host = builder.Host;
            Port = builder.Port;
            User = builder.User;
            Password = builder.Password;
            Database = builder.Database;
            ReadWriteTimeout = builder.ReadWriteTimeout;
            BufferSize = builder.BufferSize;
            ClientName = builder.ClientName;
            ClientVersion = builder.ClientVersion;
            Compress = builder.Compress;
            CommandTimeout = builder.CommandTimeout;
        }
    }
}
