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
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

namespace Octonica.ClickHouseClient.Utils
{
    internal sealed class ReadOnlyMemoryCollectionList<T> : ICollectionList<T>
    {
        private readonly IReadOnlyList<ReadOnlyMemory<T>> _list;

        public ReadOnlyMemoryCollectionList(IReadOnlyList<ReadOnlyMemory<T>> list)
            => _list = list;

        public int Count => _list.Count;

        public T this[int listIndex, int index]
            => _list[listIndex].Span[index];

        public IEnumerable<T> GetItems()
            => _list.SelectMany(MemoryMarshal.ToEnumerable);

        public IEnumerable<int> GetListLengths()
            => _list.Select(item => item.Length);

        public int GetLength(int listIndex)
            => _list[listIndex].Length;
    }
}