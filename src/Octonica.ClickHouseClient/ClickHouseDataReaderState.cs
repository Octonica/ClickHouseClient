#region License Apache 2.0
/* Copyright 2020-2021 Octonica
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
    /// Describes the current state of the <see cref="ClickHouseDataReader"/>.
    /// </summary>
    public enum ClickHouseDataReaderState
    {
        /// <summary>
        /// The reader was broken.
        /// This usualy indicates that there were a network error while fetching data.
        /// </summary>
        Broken = 0,

        /// <summary>
        /// The reader is closed. Which means that the end of the data stream was reached.
        /// </summary>
        Closed = 1,

        /// <summary>
        /// The reader is currently reading the main result set.
        /// </summary>
        Data = 2,

        /// <summary>
        /// The reader reached to the end of the current result set and there possible are the next result set.
        /// <br/>
        /// Use one of methods <see cref="ClickHouseDataReader.NextResult()"/> or <see cref="ClickHouseDataReader.NextResultAsync(System.Threading.CancellationToken)"/>
        /// to check if there are the next result set.
        /// </summary>
        NextResultPending = 3,

        /// <summary>
        /// The reader is currently reading the TOTALS result set.
        /// </summary>
        Totals = 4,

        /// <summary>
        /// The reader is currently reading the EXTREMES result set.
        /// </summary>
        Extremes = 5,

        /// <summary>
        /// The reader can no longer read the data and waiting for closing.
        /// <br/>
        /// Use one of methods <see cref="ClickHouseDataReader.Close()"/> <see cref="ClickHouseDataReader.CloseAsync()"/>
        /// to close the reader.
        /// </summary>
        /// <remarks>
        /// This state usualy indicates that the reader was created with a <see cref="System.Data.CommandBehavior"/> that
        /// forbids further reading even if there are more rows in the current result set or there are more result sets.
        /// </remarks>
        ClosePending = 6,

        /// <summary>
        /// The reader is currently reading profile events.
        /// </summary>
        ProfileEvents = 7
    }
}
