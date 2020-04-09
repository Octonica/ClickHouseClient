#region License Apache 2.0
/* Copyright 2019-2020 Octonica LLC
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
    public class ClickHouseServerInfo
    {
        public string Name { get; }

        public ClickHouseVersion Version { get; }

        public int Revision { get; }

        public string Timezone { get; }

        public string DisplayName { get; }

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
