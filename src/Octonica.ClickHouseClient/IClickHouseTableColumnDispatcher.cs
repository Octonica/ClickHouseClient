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

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// The interface for objects performing an arbitrary operation over the generic column.
    /// </summary>
    /// <typeparam name="TRes">The type of a value returned by the dispatcher.</typeparam>
    public interface IClickHouseTableColumnDispatcher<out TRes>
    {
        /// <summary>
        /// When implemented in a derived class performs an arbitrary operation over the column.
        /// </summary>
        /// <typeparam name="T">The type of the column's values.</typeparam>
        /// <param name="column">The column.</param>
        /// <returns>The result of the operation.</returns>
        TRes Dispatch<T>(IClickHouseTableColumn<T> column);
    }
}
