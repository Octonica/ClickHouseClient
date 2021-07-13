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

using System;

namespace Octonica.ClickHouseClient
{
    public interface IClickHouseArrayTableColumn<TElement> : IClickHouseTableColumn
    {
        /// <summary>
        /// Copies elements from the array to the specified buffer.
        /// </summary>
        /// <param name="index">The zero-based index of the row.</param>
        /// <param name="buffer">The buffer into which to copy data.</param>
        /// <param name="dataOffset">The index within the row from which to begin the copy operation.</param>
        /// <returns>The actual number of copied elements.</returns>
        int CopyTo(int index, Span<TElement> buffer, int dataOffset);
    }
}
