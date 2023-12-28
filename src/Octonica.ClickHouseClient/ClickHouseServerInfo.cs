#region License Apache 2.0
/* Copyright 2019-2021, 2023 Octonica
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
using System.Collections.ObjectModel;

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// Describes a ClickHouse server. This class can't be inherited.
    /// </summary>
    public sealed class ClickHouseServerInfo
    {
        /// <summary>
        /// Gets the name of the server provided by the server when opening a connection.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the version of the server.
        /// </summary>
        public ClickHouseVersion Version { get; }

        /// <summary>
        /// Gets the revision of the ClickHouse binary protocol negotiated between the client and the server.
        /// </summary>
        public int Revision { get; }

        /// <summary>
        /// Gets the revision of the ClickHouse binary protocol supported by the server. This value can't be less than the negotiated revision (<see cref="Revision"/>).
        /// </summary>
        public int ServerRevision { get; }

        /// <summary>
        /// Gets the default timezone of the server.
        /// </summary>
        public string Timezone { get; }

        /// <summary>
        /// Gets the display name of the server provided by the server when opening a connection.
        /// </summary>
        public string DisplayName { get; }

        /// <summary>
        /// Gets password complexity rules provided by the server when opening a connection.
        /// </summary>
        public ReadOnlyCollection<ClickHousePasswordComplexityRule>? PasswordComplexityRules { get; }

        /// <summary>
        /// Initializes a new instance of <see cref="ClickHouseServerInfo"/> with specified arguments.
        /// </summary>
        /// <param name="name">The name of the server provided by the server when opening a connection.</param>
        /// <param name="version">The version of the server.</param>
        /// <param name="serverRevision">The revision of the ClickHouse binary protocol supported by the server.</param>
        /// <param name="revision">The revision of the ClickHouse binary protocol negotiated between the client and the server.</param>
        /// <param name="timezone">The default timezone of the server.</param>
        /// <param name="displayName">The display name of the server provided by the server when opening a connection.</param>
        /// <param name="passwordComplexityRules">Password complexity rules provided by the server when opening a connection.</param>
        public ClickHouseServerInfo(string name, ClickHouseVersion version, int serverRevision, int revision, string timezone, string displayName, ReadOnlyCollection<ClickHousePasswordComplexityRule>? passwordComplexityRules)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Version = version;
            ServerRevision = serverRevision;
            Revision = revision;
            Timezone = timezone;
            DisplayName = displayName;
            PasswordComplexityRules = passwordComplexityRules;
        }

        /// <summary>
        /// Creates a copy of the server info with the specified timezone.
        /// </summary>
        /// <param name="timezone">The default timezone of the server.</param>
        /// <returns>A new instance of <see cref="ClickHouseServerInfo"/> with the specified timezone.</returns>
        public ClickHouseServerInfo WithTimezone(string timezone)
        {
            return new ClickHouseServerInfo(
                name: Name,
                version: Version,
                serverRevision: ServerRevision,
                revision: Revision,
                timezone: timezone,
                displayName: DisplayName,
                passwordComplexityRules: PasswordComplexityRules);
        }
    }
}
