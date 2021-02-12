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
using System.Collections;
using System.Collections.Generic;

namespace Octonica.ClickHouseClient.Utils
{
    internal sealed class ReadOnlyMemoryList<T> : IReadOnlyList<T>
    {
        private readonly ReadOnlyMemory<T> _memory;

        public int Count => _memory.Length;

        public ReadOnlyMemoryList(ReadOnlyMemory<T> memory)
        {
            _memory = memory;
        }

        public ReadOnlyMemoryList(Memory<T> memory)
        {
            _memory = memory;
        }

        public IEnumerator<T> GetEnumerator()
        {
            for (int i = 0; i < _memory.Length; i++)
                yield return _memory.Span[i];
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public T this[int index] => _memory.Span[index];
    }
}
