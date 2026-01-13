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

using System.Collections;
using System.Collections.Generic;

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// The basic interface for an object that provides access to a table along with metadata.
    /// </summary>
    public interface IClickHouseTableProvider
    {
        /// <summary>
        /// Gets the name of the table.
        /// </summary>
        string TableName { get; }

        /// <summary>
        /// Gets the number of columns in the table.
        /// </summary>
        int ColumnCount { get; }

        /// <summary>
        /// Gets the number of rows in the table.
        /// </summary>
        int RowCount { get; }

        /// <summary>
        /// Gets the descriptor of the column at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index of the column.</param>
        /// <returns>The descriptor of the column.</returns>
        IClickHouseColumnDescriptor GetColumnDescriptor(int index);

        /// <summary>
        /// Returns the object that represents the column at the specified index. It must implement one of interfaces:
        /// <see cref="IReadOnlyList{T}"/>,
        /// <see cref="IList{T}"/>,
        /// <see cref="IAsyncEnumerable{T}"/>,
        /// <see cref="IEnumerable{T}"/> or
        /// <see cref="IEnumerable"/>.
        /// </summary>
        /// <param name="index">The zero-based index of the column.</param>
        /// <returns>The object that represents the column at the specified index.</returns>
        object GetColumn(int index);
    }
}
