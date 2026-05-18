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

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// Represents information about a query execution progress.
    /// </summary>
    public readonly struct ClickHouseQueryExecutionProgress
    {
        /// <summary>
        /// The number of rows processed by a query.
        /// </summary>
        public ulong Rows { get; }

        /// <summary>
        /// The number of bytes processed by a query.
        /// </summary>
        public ulong Bytes { get; }

        /// <summary>
        /// The total number of rows processed by a query.
        /// </summary>
        public ulong TotalRows { get; }

        /// <summary>
        /// The total number of rows processed by a query.
        /// </summary>
        public ulong TotalBytes { get; }

        /// <summary>
        /// The number of rows written by a query.
        /// </summary>
        public ulong WrittenRows { get; }

        /// <summary>
        /// The number of bytes processed by a query.
        /// </summary>
        public ulong WrittenBytes { get; }

        /// <summary>
        /// The time elapsed from the start of a query execution in nanoseconds.
        /// </summary>
        public ulong ElapsedNanoseconds { get; }

        /// <summary>
        /// Creates an instance of a query execution process.
        /// </summary>
        public ClickHouseQueryExecutionProgress(ulong rows, ulong bytes, ulong totalRows, ulong totalBytes, ulong writtenRows, ulong writtenBytes, ulong elapsedNanoseconds)
        {
            Rows = rows;
            Bytes = bytes;
            TotalRows = totalRows;
            TotalBytes = totalBytes;
            WrittenRows = writtenRows;
            WrittenBytes = writtenBytes;
            ElapsedNanoseconds = elapsedNanoseconds;
        }
    }
}
