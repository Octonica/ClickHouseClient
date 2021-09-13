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

namespace Octonica.ClickHouseClient.Protocol
{
    /// <summary>
    /// The static class that provides access to the ClickHouse's binary protocol revision numbers.
    /// </summary>
    public static class ClickHouseProtocolRevisions
    {
        /// <summary>
        /// The number of the current revision. It is the latest revision supported by the client.
        /// </summary>
        public const int CurrentRevision = 54441;

        /// <summary>
        /// The number of the protocol's revision that supports interserver secret.
        /// </summary>
        public const int MinRevisionWithInterserverSecret = 54441;

        /// <summary>
        /// The number of the protocol's revision with settings serialized as strings.
        /// </summary>
        public const int MinRevisionWithSettingsSerializedAsStrings = 54429;

        /// <summary>
        /// The minimal number of the revision supported by the client.
        /// </summary>
        public const int MinSupportedRevision = 54423;
    }
}
