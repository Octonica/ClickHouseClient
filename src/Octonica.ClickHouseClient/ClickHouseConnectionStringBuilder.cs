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
    public class ClickHouseConnectionStringBuilder : DbConnectionStringBuilder
    {
        private static readonly HashSet<string> AllProperties;

        public const ushort DefaultPort = 9000;
        public const string DefaultUser = "default";
        public const int DefaultReadWriteTimeout = 10000;
        public const int DefaultCommandTimeout = 15;
        public const int DefaultBufferSize = 4096;
        public const string DefaultClientName = "Octonica.ClickHouseClient";
        public const bool DefaultCompress = true;

        public static readonly ClickHouseVersion DefaultClientVersion;

        public string? Host
        {
            get => GetString(nameof(Host));
            set => this[nameof(Host)] = value;
        }

        public ushort Port
        {
            get => (ushort) GetInt32OrDefault(nameof(Port), DefaultPort);
            set => this[nameof(Port)] = value;
        }

        public string User
        {
            get => GetStringOrDefault(nameof(User), DefaultUser);
            set => this[nameof(User)] = value;
        }

        public string? Password
        {
            get => GetString(nameof(Password));
            set => this[nameof(Password)] = value;
        }

        public string? Database
        {
            get => GetString(nameof(Database));
            set => this[nameof(Database)] = value;
        }

        public int ReadWriteTimeout
        {
            get => GetInt32OrDefault(nameof(ReadWriteTimeout), DefaultReadWriteTimeout);
            set => this[nameof(ReadWriteTimeout)] = value;
        }

        public int BufferSize
        {
            get => GetInt32OrDefault(nameof(BufferSize), DefaultBufferSize);
            set => this[nameof(BufferSize)] = value;
        }

        public string ClientName
        {
            get => GetStringOrDefault(nameof(ClientName), DefaultClientName);
            set => this[nameof(ClientName)] = value;
        }

        public bool Compress
        {
            get => GetBoolOrDefault(nameof(Compress), DefaultCompress);
            set => this[nameof(Compress)] = value;
        }

        public int CommandTimeout
        {
            get => GetInt32OrDefault(nameof(CommandTimeout), DefaultCommandTimeout);
            set => this[nameof(CommandTimeout)] = value;
        }

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

        public ClickHouseConnectionStringBuilder()
        {
        }

        public ClickHouseConnectionStringBuilder(string connectionString)
        {
            ConnectionString = connectionString;
        }

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
