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
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// Provides a set of methods for working with connection settings and connection strings.
    /// </summary>
    public class ClickHouseConnectionStringBuilder : DbConnectionStringBuilder
    {
        private static readonly HashSet<string> AllProperties;

        /// <summary>
        /// The default IP port of the server (9000).
        /// </summary>
        public const ushort DefaultPort = 9000;

        /// <summary>
        /// The default name of the user ('default').
        /// </summary>
        public const string DefaultUser = "default";

        /// <summary>
        /// The default network socket timeout in <b>milliseconds</b> (10000).
        /// </summary>
        public const int DefaultReadWriteTimeout = 10000;

        /// <summary>
        /// The default command timeout in <b>seconds</b> (15).
        /// </summary>
        public const int DefaultCommandTimeout = 15;

        /// <summary>
        /// The default size of the internal buffer in bytes (4096).
        /// </summary>
        public const int DefaultBufferSize = 4096;

        /// <summary>
        /// The default name of the client ('Octonica.ClickHouseClient'). 
        /// </summary>
        public const string DefaultClientName = "Octonica.ClickHouseClient";

        /// <summary>
        /// The default value (<see langword="true"/>). indicating whether the compression (LZ4) is enabled.
        /// </summary>
        public const bool DefaultCompress = true;

        /// <summary>
        /// The default version of the client. This value is equal to the version of the assembly Octonica.ClickHouseClient.
        /// </summary>
        public static readonly ClickHouseVersion DefaultClientVersion;

        /// <summary>
        /// The default value for the TLS mode is <see cref="ClickHouseTlsMode.Disable"/>.
        /// </summary>
        public const ClickHouseTlsMode DefaultTlsMode = ClickHouseTlsMode.Disable;

        /// <summary>
        /// The default value for the mode of passing parameters to the query is <see cref="ClickHouseParameterMode.Default"/>.
        /// </summary>
        public const ClickHouseParameterMode DefaultParametersMode = ClickHouseParameterMode.Default;

        /// <summary>
        /// Gets or sets the name or the IP address of the host.
        /// </summary>
        /// <returns>The name or the IP address of the host.</returns>
        public string? Host
        {
            get => GetString(nameof(Host));
            set => this[nameof(Host)] = value;
        }

        /// <summary>
        /// Gets or sets the IP port of the server.
        /// </summary>
        /// <returns>The IP port of the server. The default value is <see cref="DefaultPort"/>.</returns>
        public ushort Port
        {
            get => (ushort) GetInt32OrDefault(nameof(Port), DefaultPort);
            set => this[nameof(Port)] = value;
        }

        /// <summary>
        /// Gets or sets the name of the user.
        /// </summary>
        /// <returns>The name of the user. The default value is <see cref="DefaultUser"/>.</returns>
        public string User
        {
            get => GetStringOrDefault(nameof(User), DefaultUser);
            set => this[nameof(User)] = value;
        }

        /// <summary>
        /// Gets or sets the password.
        /// </summary>
        /// <returns>The password.</returns>
        public string? Password
        {
            get => GetString(nameof(Password));
            set => this[nameof(Password)] = value;
        }

        /// <summary>
        /// Gets or sets the name of the default database.
        /// </summary>
        /// <returns>The name of the default database. <see langword="null"/> if the default database in not specified.</returns>
        public string? Database
        {
            get => GetString(nameof(Database));
            set => this[nameof(Database)] = value;
        }

        /// <summary>
        /// Gets or sets the network socket timeout in <b>milliseconds</b>. This timeout will be used to initialize properties <see cref= "System.Net.Sockets.TcpClient.SendTimeout"/> and 
        /// <see cref= "System.Net.Sockets.TcpClient.ReceiveTimeout"/>.
        /// </summary>
        /// <returns>The network socket timeout in <b>milliseconds</b>. The default value is <see cref="DefaultReadWriteTimeout"/>.</returns>
        public int ReadWriteTimeout
        {
            get => GetInt32OrDefault(nameof(ReadWriteTimeout), DefaultReadWriteTimeout);
            set => this[nameof(ReadWriteTimeout)] = value;
        }

        /// <summary>
        /// Gets or sets the preferred size of the internal buffer in bytes.
        /// </summary>
        /// <returns>the preferred size of the internal buffer in bytes. The default value is <see cref="DefaultBufferSize"/>.</returns>
        public int BufferSize
        {
            get => GetInt32OrDefault(nameof(BufferSize), DefaultBufferSize);
            set => this[nameof(BufferSize)] = value;
        }

        /// <summary>
        /// Gets or sets the name of the client. The name of the client is passed to the ClickHouse server as a part of the client's identifier.
        /// </summary>
        /// <returns>The name of the client. The default value is <see cref="DefaultClientName"/>.</returns>
        public string ClientName
        {
            get => GetStringOrDefault(nameof(ClientName), DefaultClientName);
            set => this[nameof(ClientName)] = value;
        }

        /// <summary>
        /// Gets or sets the value indicating whether the compression (LZ4) of data is enabled.
        /// </summary>
        /// <returns><see langword="true"/> if compression is enabled; otherwise <see langword="true"/>. The default value is <see cref="DefaultCompress"/>.</returns>
        public bool Compress
        {
            get => GetBoolOrDefault(nameof(Compress), DefaultCompress);
            set => this[nameof(Compress)] = value;
        }

        /// <summary>
        /// Gets or sets the command timeout in <b>seconds</b>. This timeout will be used to initialize the property <see cref= "ClickHouseCommand.CommandTimeout"/> of the command.
        /// </summary>
        /// <returns>The command timeout in seconds. The default value is <see cref="DefaultCommandTimeout"/>.</returns>
        public int CommandTimeout
        {
            get => GetInt32OrDefault(nameof(CommandTimeout), DefaultCommandTimeout);
            set => this[nameof(CommandTimeout)] = value;
        }

        /// <summary>
        /// Gets or sets the version of the client. The first two parts of the version (<see cref="ClickHouseVersion.Major"/> and <see cref="ClickHouseVersion.Minor"/>)
        /// are passed to the ClickHouse server as a part of the client's identifier.
        /// </summary>
        /// <returns>The version of the client. The default value is <see cref="DefaultClientVersion"/>.</returns>
        public ClickHouseVersion ClientVersion
        {
            get
            {
                var value = GetString(nameof(ClientVersion));
                if (value == null)
                    return DefaultClientVersion;

                return ClickHouseVersion.Parse(value);
            }
            set => this[nameof(ClientVersion)] = value.ToString();
        }

        /// <summary>
        /// Gets or sets the TLS mode for the connection. See <see cref="ClickHouseTlsMode"/> for details.
        /// </summary>
        /// <returns>The TLS mode for the connection. The default value is <see cref="DefaultTlsMode"/>.</returns>
        public ClickHouseTlsMode TlsMode
        {
            get => GetEnumOrDefault(nameof(TlsMode), DefaultTlsMode);
            set => this[nameof(TlsMode)] = value == DefaultTlsMode ? null : value.ToString("G");
        }

        /// <summary>
        /// Gets or sets the path to the file that contains a certificate (*.crt) or a list of certificates (*.pem).
        /// When performing TLS hanshake any of these certificates will be treated as a valid root for the certificate chain.
        /// </summary>
        /// <returns>The path to the file that contains a certificate (*.crt) or a list of certificates (*.pem). The default value is <see langword="null"/>.</returns>
        public string? RootCertificate
        {
            get
            {
                var value = GetString(nameof(RootCertificate));
                if (string.IsNullOrWhiteSpace(value))
                    return null;

                return value;
            }
            set => this[nameof(RootCertificate)] = value;
        }

        /// <summary>
        /// Gets or sets the hash of the server's certificate in the hexadecimal format.
        /// When performing TLS handshake the remote certificate with the specified hash will be treated as a valid certificate
        /// despite any other certificate chain validation errors (e.g. invalid hostname).
        /// </summary>
        /// <returns>The hash of the server's certificate in the hexadecimal format. The default value is <see langword="null"/>.</returns>
        public string? ServerCertificateHash
        {
            get
            {
                var value = GetString(nameof(ServerCertificateHash));
                if (string.IsNullOrWhiteSpace(value))
                    return null;

                return value;
            }
            set => this[nameof(ServerCertificateHash)] = value;
        }

        /// <summary>
        /// Gets the default mode of passing parameters to the query for the connection.
        /// </summary>
        /// <returns>The default mode of passing parameters to the query for the connection. The default value is <see cref="DefaultParametersMode"/>.</returns>
        public ClickHouseParameterMode ParametersMode
        {
            get => GetEnumOrDefault(nameof(ParametersMode), DefaultParametersMode);
            set => this[nameof(ParametersMode)] = value == DefaultParametersMode ? null : value.ToString("G");
        }

        /// <summary>
        /// Gets the 'quota key' passed with the query. This key is used by the ClickHouse server for tracking quotas.
        /// </summary>
        /// <returns>The value of 'quota key' passed with the query.</returns>
        public string? QuotaKey
        {
            get => GetString(nameof(QuotaKey));
            set => this[nameof(QuotaKey)] = value;
        }

        static ClickHouseConnectionStringBuilder()
        {
            var asm = typeof(ClickHouseConnectionStringBuilder).Assembly;
            var version = asm.GetName().Version;
            DefaultClientVersion = new ClickHouseVersion(version?.Major ?? 1, version?.Minor ?? 0, version?.Build ?? 0);

            AllProperties = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                nameof(BufferSize),
                nameof(ClientName),
                nameof(ClientVersion),
                nameof(CommandTimeout),
                nameof(Compress),
                nameof(Database),
                nameof(Host),
                nameof(Password),
                nameof(ReadWriteTimeout),
                nameof(Port),
                nameof(User),
                nameof(TlsMode),
                nameof(RootCertificate),
                nameof(ServerCertificateHash),
                nameof(ParametersMode),
                nameof(QuotaKey)
            };
        }

        /// <summary>
        /// Initializes a new instance of <see cref="ClickHouseConnectionStringBuilder"/> with the default settings.
        /// </summary>
        public ClickHouseConnectionStringBuilder()
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="ClickHouseConnectionStringBuilder"/> with the settings specified in the connection string.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public ClickHouseConnectionStringBuilder(string connectionString)
        {
            ConnectionString = connectionString;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="ClickHouseConnectionStringBuilder"/> with the specified.
        /// </summary>
        /// <param name="settings">The settings.</param>
        public ClickHouseConnectionStringBuilder(ClickHouseConnectionSettings settings)
        {
            if (settings == null)
                throw new ArgumentNullException(nameof(settings));

            Host = settings.Host;
            Port = settings.Port;
            User = settings.User;
            Password = settings.Password;
            Database = settings.Database;
            ReadWriteTimeout = settings.ReadWriteTimeout;
            BufferSize = settings.BufferSize;
            Compress = settings.Compress;
            CommandTimeout = settings.CommandTimeout;
            TlsMode = settings.TlsMode;
            RootCertificate = settings.RootCertificate;
            ServerCertificateHash = HashToString(settings.ServerCertificateHash);
            ParametersMode = settings.ParametersMode;
            QuotaKey = settings.QuotaKey;

            if (settings.ClientName != DefaultClientName)
                ClientName = settings.ClientName;

            if (settings.ClientVersion != DefaultClientVersion)
                ClientVersion = settings.ClientVersion;
        }

        /// <inheritdoc/>
        [AllowNull]
        public override object this[string keyword]
        {
            get => base[keyword];
            set
            {
                if (!AllProperties.Contains(keyword))
                    throw new ArgumentException($"\"{keyword}\" is not a valid connection parameter name.", nameof(keyword));

                base[keyword] = value;
            }
        }

        /// <summary>
        /// Creates and returns a new instance of the <see cref="ClickHouseConnectionSettings"/>.
        /// </summary>
        /// <returns>A new instance of the <see cref="ClickHouseConnectionSettings"/>.</returns>
        public ClickHouseConnectionSettings BuildSettings()
        {
            return new ClickHouseConnectionSettings(this);
        }

        private string? GetString(string key)
        {
            return TryGetValue(key, out var value) ? (string) value : null;
        }

        private string GetStringOrDefault(string key, string defaultValue)
        {
            if (!TryGetValue(key, out var value))
                return defaultValue;

            return (string) value ?? defaultValue;
        }

        private int GetInt32OrDefault(string key, int defaultValue)
        {
            if (!TryGetValue(key, out var value))
                return defaultValue;

            if (value is string strValue)
            {
                if (!int.TryParse(strValue.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var result))
                    throw new InvalidOperationException($"The value of the property \"{key}\" must be an integer value.");

                return result;
            }

            return (int?) value ?? defaultValue;
        }

        private bool GetBoolOrDefault(string key, bool defaultValue)
        {
            if (!TryGetValue(key, out var value))
                return defaultValue;

            if (value is string strValue)
            {
                switch (strValue.Trim().ToLowerInvariant())
                {
                    case "on":
                    case "true":
                    case "1":
                        return true;

                    case "off":
                    case "false":
                    case "0":
                        return false;

                    default:
                        throw new InvalidOperationException($"The value of the property \"{key}\" is not a valid boolean value.");
                }
            }

            return (bool?) value ?? defaultValue;
        }

        private TEnum GetEnumOrDefault<TEnum>(string key, TEnum defaultValue)
            where TEnum : struct
        {
            if (!TryGetValue(key, out var value) || value == null)
                return defaultValue;

            if(value is string strValue)
            {
                if (string.IsNullOrWhiteSpace(strValue))
                    return defaultValue;

                // Enum.TryParse parses an integer value and casts it into enum without additional check.
                // Check that the value is not an integer before performing an actual enum parsing.
                if (int.TryParse(strValue.Trim(), out _) || !Enum.TryParse<TEnum>(strValue, true, out var result))
                    throw new InvalidOperationException($"The value \"{strValue}\" is not a valid value for the property \"{key}\".");

                return result;
            }

            return (TEnum)value;
        }

        private static string? HashToString(ReadOnlyMemory<byte> hashBytes)
        {
            if (hashBytes.Length == 0)
                return null;

            return string.Create(hashBytes.Length * 2, hashBytes, HashToString);
        }

        static void HashToString(Span<char> span, ReadOnlyMemory<byte> hashBytes)
        {
            var byteSpan = hashBytes.Span;
            for (int i = 0; i < hashBytes.Length; i++)
            {
                var val = byteSpan[i] >> 4;
                span[i * 2] = (char)(val + (val >= 0xA ? 'A' - 0xA : '0'));

                val = byteSpan[i] & 0x0F;
                span[i * 2 + 1] = (char)(val + (val >= 0xA ? 'A' - 0xA : '0'));
            }
        }
    }
}
