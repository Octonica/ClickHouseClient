#region License Apache 2.0
/* Copyright 2019-2021 Octonica
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
                nameof(User)
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
    }
}
