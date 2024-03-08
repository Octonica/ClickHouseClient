#region License Apache 2.0
/* Copyright 2019-2021, 2024 Octonica
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

namespace Octonica.ClickHouseClient.Protocol
{
    /// <summary>
    /// The base interface for an object capable of sequential writing column's data to a binary buffer.
    /// </summary>
    public interface IClickHouseColumnWriter
    {
        /// <summary>
        /// The name of the column to write data to.
        /// </summary>
        string ColumnName { get; }

        /// <summary>
        /// The full name of the ClickHouse type of the column.
        /// </summary>
        string ColumnType { get; }

        /// <summary>
        /// Writes next block of data to the target span.
        /// </summary>
        /// <param name="writeTo">The buffer to write data to.</param>
        /// <returns>The length of written data or <see cref="SequenceSize.Empty"/> if the provided buffer is too small.</returns>
        SequenceSize WriteNext(Span<byte> writeTo);

        /// <summary>
        /// Writes prefix specific to the column's type.
        /// </summary>
        /// <param name="writeTo">The buffer to write data to.</param>
        /// <returns>
        /// The length of written data or <see cref="SequenceSize.Empty"/> if the provided buffer is too small.
        /// The prefix is counted for a single element, so the number of elements (<see cref="SequenceSize.Elements"/>)
        /// can be either 0 or 1.
        /// </returns>
        SequenceSize WritePrefix(Span<byte> writeTo) => new SequenceSize(0, 1);
    }
}
