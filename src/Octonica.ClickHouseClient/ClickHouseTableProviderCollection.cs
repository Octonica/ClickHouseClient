#region License Apache 2.0
/* Copyright 2021 Octonica
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

using Octonica.ClickHouseClient.Utils;
using System;

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// Represents a collection of table providers associated with a <see cref="ClickHouseCommand"/>. This class cannot be inherited.
    /// </summary>
    public sealed class ClickHouseTableProviderCollection : IndexedCollectionBase<string, IClickHouseTableProvider>
    {
        /// <summary>
        /// Initializes a new instance of <see cref="ClickHouseTableProviderCollection"/> with the default capacity.
        /// </summary>
        public ClickHouseTableProviderCollection()
            : base(StringComparer.OrdinalIgnoreCase)
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="ClickHouseTableProviderCollection"/> with the specified capacity capacity.
        /// </summary>
        /// <param name="capacity">The initial number of elements that the collection can contain.</param>
        public ClickHouseTableProviderCollection(int capacity)
            : base(capacity, StringComparer.OrdinalIgnoreCase)
        {
        }

        /// <inheritdoc/>
        protected sealed override string GetKey(IClickHouseTableProvider item)
        {
            return item.TableName;
        }
    }
}
