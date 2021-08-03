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

namespace Octonica.ClickHouseClient.Types
{
    /// <summary>
    /// Represents basic information about the ClickHouse type that depends on the server's settings.
    /// Provides access to factory methods for creating column readers and writers.
    /// </summary>
    public interface IClickHouseConfigurableTypeInfo : IClickHouseColumnTypeInfo
    {
        /// <summary>
        /// Returns the <see cref="IClickHouseColumnTypeInfo"/> that represents the type with the specified settings.
        /// </summary>
        /// <param name="serverInfo">Information about the server.</param>
        /// <returns>The <see cref="IClickHouseColumnTypeInfo"/> that represents the type with the specified settings.</returns>
        IClickHouseColumnTypeInfo Configure(ClickHouseServerInfo serverInfo);
    }
}
