#region License Apache 2.0
/* Copyright 2019-2023 Octonica
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
    /// <summary>
    /// Represents an immutable set of properties applied to the connection. This class can't be inherited.
    /// </summary>
    public sealed class ClickHouseConnectionSettings
    {
        /// <summary>
        /// Gets the name or the IP address of the host.
        /// </summary>
        public string Host { get; }

        /// <summary>
        /// Gets the IP port of the server.
        /// </summary>
        public ushort Port { get; }

        /// <summary>
        /// Gets the name of the user.
        /// </summary>
        public string User { get; }

        /// <summary>
        /// Gets the password.
        /// </summary>
        public string? Password { get; }

        /// <summary>
        /// Get the name of the default database.
        /// </summary>
        public string? Database { get; }

        /// <summary>
        /// Gets the network socket timeout in <b>milliseconds</b>. This timeout will be used to initialize properties <see cref= "System.Net.Sockets.TcpClient.SendTimeout"/> and 
        /// <see cref= "System.Net.Sockets.TcpClient.ReceiveTimeout"/>.
        /// </summary>
        public int ReadWriteTimeout { get; }

        /// <summary>
        /// Gets the name of the client. The name of the client is passed to the ClickHouse server as a part of the client's identifier.
        /// </summary>
        public string ClientName { get; }

        /// <summary>
        /// Gets the version of the client. The first two parts of the version (<see cref="ClickHouseVersion.Major"/> and <see cref="ClickHouseVersion.Minor"/>)
        /// are passed to the ClickHouse server as a part of the client's identifier.
        /// </summary>
        public ClickHouseVersion ClientVersion { get; }

        /// <summary>
        /// Gets the preferred size of the internal buffer in bytes.
        /// </summary>
        public int BufferSize { get; }

        /// <summary>
        /// Gets the value indicating whether the compression (LZ4) of data is enabled.
        /// </summary>
        public bool Compress { get; }

        /// <summary>
        /// Gets the command timeout in <b>seconds</b>. This timeout will be used to initialize the property <see cref= "ClickHouseCommand.CommandTimeout"/> of the command.
        /// </summary>
        public int CommandTimeout { get; }

        /// <summary>
        /// Gets the TLS mode for the connection. See <see cref="ClickHouseTlsMode"/> for details.
        /// </summary>
        public ClickHouseTlsMode TlsMode { get; }

        /// <summary>
        /// Gets the path to the file that contains a certificate (*.crt) or a list of certificates (*.pem).
        /// When performing TLS hanshake any of these certificates will be treated as a valid root for the certificate chain.
        /// </summary>
        public string? RootCertificate { get; }

        /// <summary>
        /// Gets the hash of the server's certificate. When performing TLS handshake the remote certificate with the specified
        /// hash will be treated as a valid certificate despite any other certificate chain validation errors (e.g. invalid hostname).
        /// </summary>
        public ReadOnlyMemory<byte> ServerCertificateHash { get; }

        /// <summary>
        /// Gets the default mode of passing parameters to the query for the connection.
        /// </summary>
        public ClickHouseParameterMode ParametersMode { get; }

        /// <summary>
        /// Gets the 'quota key' passed with the query. This key is used by the ClickHouse server for tracking quotas.
        /// </summary>
        public string? QuotaKey { get; }

        internal readonly int CompressionBlockSize = 1024 * 8; // Maybe it should be configurable

        internal ClickHouseConnectionSettings(ClickHouseConnectionStringBuilder builder)
        {
            if (string.IsNullOrWhiteSpace(builder.Host))
            {
                throw new ArgumentException("The host is not defined.", nameof(builder));
            }

            if (builder.BufferSize <= 0)
            {
                throw new ArgumentException("The size of the buffer must be a positive number.", nameof(builder));
            }

            Host = builder.Host;
            Port = builder.Port;
            User = builder.User;
            Password = string.IsNullOrEmpty(builder.Password) ? null : builder.Password;
            Database = string.IsNullOrEmpty(builder.Database) ? null : builder.Database;
            ReadWriteTimeout = builder.ReadWriteTimeout;
            BufferSize = builder.BufferSize;
            ClientName = builder.ClientName;
            ClientVersion = builder.ClientVersion;
            Compress = builder.Compress;
            CommandTimeout = builder.CommandTimeout;
            TlsMode = builder.TlsMode;
            RootCertificate = builder.RootCertificate;
            ServerCertificateHash = ParseHashString(builder.ServerCertificateHash);
            ParametersMode = builder.ParametersMode;
            QuotaKey = string.IsNullOrEmpty(builder.QuotaKey) ? null : builder.QuotaKey;
        }

        private static byte[]? ParseHashString(string? hashString)
        {
            if (string.IsNullOrEmpty(hashString))
            {
                return null;
            }

            int resultPos = 0;
            byte[] result = new byte[hashString.Length / 2];
            for (int i = 0; i < hashString.Length; i++)
            {
                char ch = hashString[i];
                if (char.IsWhiteSpace(ch) || ch == '-')
                {
                    continue;
                }

                if (i + 1 == hashString.Length)
                {
                    throw new ArgumentException("Unexpected end of the hash string. Expected at least one more significant character.", nameof(hashString));
                }

                byte byteVal = ch is >= '0' and <= '9'
                    ? (byte)(ch - '0')
                    : ch is >= 'a' and <= 'f'
                        ? (byte)(ch - 'a' + 0xA)
                        : ch is >= 'A' and <= 'F'
                    ? (byte)(ch - 'A' + 0xA)
                    : throw new ArgumentException($"Unexpected character '{ch}' at the position {i} in the hash string.", nameof(hashString));
                byteVal <<= 4;
                ch = hashString[++i];
                if (ch is >= '0' and <= '9')
                {
                    byteVal |= (byte)(ch - '0');
                }
                else if (ch is >= 'a' and <= 'f')
                {
                    byteVal |= (byte)(ch - 'a' + 0xA);
                }
                else if (ch is >= 'A' and <= 'F')
                {
                    byteVal |= (byte)(ch - 'A' + 0xA);
                }
                else
                {
                    throw new ArgumentException($"Unexpected character '{ch}' at the position {i} in the hash string.", nameof(hashString));
                }

                result[resultPos++] = byteVal;
            }

            if (resultPos == 0)
            {
                return null;
            }

            if (result.Length != resultPos)
            {
                Array.Resize(ref result, resultPos);
            }

            return result;
        }
    }
}
