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

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// Provides information about a server setting.
    /// </summary>
    public class ClickHouseServerSetting
    {
        /// <summary>
        /// Gets the setting name.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the value of the setting.
        /// </summary>
        public string Value { get; }

        /// <summary>
        /// Gets the flags of the setting
        /// </summary>
        public ClickHouseServerSettingFlags Flags { get; }

        /// <summary>
        /// Initializes a new instance of <see cref="ClickHouseServerSetting"/> with specified arguments.
        /// </summary>
        /// <param name="name">The setting name.</param>
        /// <param name="value">The value of the setting.</param>
        /// <param name="flags">The flags of the setting.</param>
        public ClickHouseServerSetting(string name, string value, ClickHouseServerSettingFlags flags)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("The name of a server setting must be a non-empty string.", nameof(name));

            Name = name;
            Value = value??throw new ArgumentNullException(nameof(value));
            Flags = flags;
        }
    }

    /// <summary>
    /// Specifies the flags for <see cref="ClickHouseServerSetting"/>
    /// </summary>
    public enum ClickHouseServerSettingFlags
    {
        /// <summary>
        /// Zero value. No flags
        /// </summary>
        None = 0x00,

        /// <summary>
        /// Setting affects query results, cannot be ignored by older versions
        /// </summary>
        Important = 0x01,

        /// <summary>
        /// User-defined custom setting
        /// </summary>
        Custom = 0x02,

        /// <summary>
        /// 0b1100 == 2 bits for tier level (PRODUCTION/BETA/EXPERIMENTAL)
        /// </summary>
        Tier = 0x0c,

        /// <summary>
        /// Flag indicating that changes from config can be picked up without server restart.
        /// Currently only works in CoordinationSettings.
        /// </summary>
        HotReload = 0x80,
    }
}