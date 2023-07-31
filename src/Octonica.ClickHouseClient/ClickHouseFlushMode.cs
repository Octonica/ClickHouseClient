#region License Apache 2.0
/* Copyright 2023 Octonica
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

using System.Threading;

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// Describes a set of strategies used by <see cref="ClickHouseColumnWriter"/> for commiting a transaction.
    /// </summary>
    public enum ClickHouseTransactionMode
    {
        /// <summary>
        /// The default strategy. This strategy is used if no strategy is specified. The same as <see cref="Auto"/>.
        /// </summary>
        Default = 0,

        /// <summary>
        /// <see cref="ClickHouseColumnWriter"/> will send a table and finish the query (commits transaction).
        /// Next table (if any) will be sent with the new INSERT query (in a new transaction).
        /// </summary>
        Auto = 1,

        /// <summary>
        /// <see cref="ClickHouseColumnWriter"/> will <b>not</b> send confirmation after writing a table.
        /// Next table (if any) will be sent with the current INSERT query (in the same transaction).
        /// </summary>
        /// <remarks>Call the method <see cref="ClickHouseColumnWriter.Commit"/> (or <see cref="ClickHouseColumnWriter.CommitAsync(CancellationToken)"/>) after writing tables.</remarks>
        Manual = 2,

        /// <summary>
        /// <see cref="ClickHouseColumnWriter"/> will send each block of data in a separate query (one transaction per block).
        /// </summary>
        Block = 3
    }
}
