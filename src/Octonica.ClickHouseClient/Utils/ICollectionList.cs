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

using System.Collections.Generic;

namespace Octonica.ClickHouseClient.Utils
{
    internal interface ICollectionList<out T>
    {
        int Count { get; }
        T this[int listIndex, int index] { get; }
        IEnumerable<T> GetItems();
        IEnumerable<int> GetListLengths();
        int GetLength(int listIndex);
    }
}