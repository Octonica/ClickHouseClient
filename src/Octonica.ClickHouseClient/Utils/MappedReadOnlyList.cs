#region License Apache 2.0
/* Copyright 2019-2021 Octonica
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
using System.Linq;

namespace Octonica.ClickHouseClient.Utils
{
    internal sealed class MappedReadOnlyList<TIn, TOut> : IReadOnlyListExt<TOut>
    {
        private readonly IReadOnlyList<TIn> _innerList;
        private readonly Func<TIn, TOut> _map;

        public int Count => _innerList.Count;

        private MappedReadOnlyList(IReadOnlyList<TIn> innerList, Func<TIn, TOut> map)
        {
            _innerList = innerList ?? throw new ArgumentNullException(nameof(innerList));
            _map = map ?? throw new ArgumentNullException(nameof(map));
        }

        public IEnumerator<TOut> GetEnumerator()
        {
            return _innerList.Select(_map).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public IReadOnlyListExt<TOut> Slice(int start, int length)
        {
            return MappedReadOnlyListSpan<TIn, TOut>.Create(_innerList, _map, start, length);
        }

        public IReadOnlyListExt<T> Map<T>(Func<TOut, T> map)
        {
            if (map == null)
                throw new ArgumentNullException(nameof(map));

            return new MappedReadOnlyList<TIn, T>(_innerList, Combine(_map, map));

            static Func<TIn, T> Combine(Func<TIn, TOut> f1, Func<TOut, T> f2)
            {
                return v => f2(f1(v));
            }
        }

        public int CopyTo(Span<TOut> span, int start)
        {
            if (start < 0 || start > Count)
                throw new ArgumentOutOfRangeException(nameof(start));

            var length = Math.Min(Count - start, span.Length);
            var end = start + length;
            for (int i = start, j = 0; i < end; i++, j++)
                span[j] = _map(_innerList[i]);

            return length;
        }

        public TOut this[int index] => _map(_innerList[index]);

        public static IReadOnlyListExt<TOut> Map(IReadOnlyList<TIn> list, Func<TIn, TOut> map)
        {
            if (list is IReadOnlyListExt<TIn> readOnlyListExt)
                return readOnlyListExt.Map(map);

            return new MappedReadOnlyList<TIn, TOut>(list, map);
        }

        public static IReadOnlyListExt<TOut> Map(ReadOnlyMemory<TIn> memory, Func<TIn, TOut> map)
        {
            return new MappedReadOnlyList<TIn, TOut>(new ReadOnlyMemoryList<TIn>(memory), map);
        }
    }
}
