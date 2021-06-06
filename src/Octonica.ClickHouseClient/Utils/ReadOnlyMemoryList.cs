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
    internal sealed class ReadOnlyMemoryList<T> : IReadOnlyListExt<T>
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

        public IReadOnlyListExt<T> Slice(int start, int length)
        {
            if (start < 0 || start > Count)
                throw new ArgumentOutOfRangeException(nameof(start));
            if (length < 0 || start + length > Count)
                throw new ArgumentOutOfRangeException(nameof(length));

            return new ReadOnlyMemoryList<T>(_memory.Slice(start, length));
        }

        public IReadOnlyListExt<TOut> Map<TOut>(Func<T, TOut> map)
        {
            return MappedReadOnlyList<T, TOut>.Map(_memory, map);
        }

        public int CopyTo(Span<T> span, int start)
        {
            if (start < 0 || start > _memory.Length)
                throw new ArgumentOutOfRangeException(nameof(start));

            var length = Math.Min(_memory.Length - start, span.Length);
            _memory.Slice(start, length).Span.CopyTo(span);
            return length;            
        }

        public T this[int index] => _memory.Span[index];
    }
}
