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

namespace Octonica.ClickHouseClient.Types
{
    /// <summary>
    /// The base interface for an object that provides information about supported types.
    /// </summary>
    public interface IClickHouseTypeInfoProvider
    {
        /// <summary>
        /// Gets the type by its name.
        /// </summary>
        /// <param name="typeName">The name of the type.</param>
        /// <returns>The <see cref="IClickHouseColumnTypeInfo"/> that provides information about the type.</returns>
        IClickHouseColumnTypeInfo GetTypeInfo(string typeName);

        /// <inheritdoc cref="GetTypeInfo(string)"/>
        IClickHouseColumnTypeInfo GetTypeInfo(ReadOnlyMemory<char> typeName);

        /// <summary>
        /// Gets the type based on the <see cref="IClickHouseColumnDescriptor"/>.
        /// </summary>
        /// <param name="columnDescriptor">The descriptor of a column.</param>
        /// <returns>The <see cref="IClickHouseColumnTypeInfo"/> that provides information about the type.</returns>
        IClickHouseColumnTypeInfo GetTypeInfo(IClickHouseColumnDescriptor columnDescriptor);

        /// <summary>
        /// Returns the <see cref="IClickHouseColumnTypeInfo"/> that provides access to types configured with the specified settings.
        /// </summary>
        /// <param name="serverInfo">Information about the server.</param>
        /// <returns>The <see cref="IClickHouseColumnTypeInfo"/> that provides access to types configured with the specified settings.</returns>
        IClickHouseTypeInfoProvider Configure(ClickHouseServerInfo serverInfo);        
    }
}
