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
        /// Gets the default timezone of the server.
        /// </summary>
        public string Timezone { get; }

        /// <summary>
        /// Gets the display name of the server provided by the server when opening a connection.
        /// </summary>
        public string DisplayName { get; }

        /// <summary>
        /// Initializes a new instance of <see cref="ClickHouseServerInfo"/> with specified arguments.
        /// </summary>
        /// <param name="name">The name of the server provided by the server when opening a connection.</param>
        /// <param name="version">The version of the server.</param>
        /// <param name="revision">The revision of the ClickHouse binary protocol negotiated between the client and the server.</param>
        /// <param name="timezone">The default timezone of the server.</param>
        /// <param name="displayName">The display name of the server provided by the server when opening a connection.</param>
        public ClickHouseServerInfo(string name, ClickHouseVersion version, int revision, string timezone, string displayName)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Version = version;
            Revision = revision;
            Timezone = timezone;
            DisplayName = displayName;
        }
    }
}
