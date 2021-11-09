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
using System.Linq;

namespace Octonica.ClickHouseClient.Utils
{
    internal sealed class MappedReadOnlyListSpan<TIn, TOut> : IReadOnlyListExt<TOut>
    {
        private readonly IReadOnlyList<TIn> _innerList;
        private readonly Func<TIn, TOut> _map;
        private readonly int _offset;

        public int Count { get; }

        private MappedReadOnlyListSpan(IReadOnlyList<TIn> innerList, Func<TIn, TOut> map, int offset, int count)
        {
            if (innerList == null)
                throw new ArgumentNullException(nameof(innerList));
            if (map == null)
                throw new ArgumentNullException(nameof(map));
            if (offset < 0 || offset > innerList.Count)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (count < 0 || offset + count > innerList.Count)
                throw new ArgumentOutOfRangeException(nameof(count));

            _innerList = innerList;
            _map = map;
            _offset = offset;
            Count = count;
        }

        public IEnumerator<TOut> GetEnumerator()
        {
            return _innerList.Skip(_offset).Take(Count).Select(_map).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public IReadOnlyListExt<TOut> Slice(int start, int length)
        {
            if (start < 0 || start > Count)
                throw new ArgumentOutOfRangeException(nameof(start));
            if (length < 0 || start + length > Count)
                throw new ArgumentOutOfRangeException(nameof(length));

            if (_innerList is IReadOnlyListExt<TIn> readOnlyListExt)
                return readOnlyListExt.Slice(_offset + start, length).Map(_map);

            return new MappedReadOnlyListSpan<TIn, TOut>(_innerList, _map, _offset + start, length);
        }

        public IReadOnlyListExt<T> Map<T>(Func<TOut, T> map)
        {
            if (map == null)
                throw new ArgumentNullException(nameof(map));

            return new MappedReadOnlyListSpan<TIn, T>(_innerList, Combine(_map, map), _offset, Count);            

            static Func<TIn,T> Combine(Func<TIn,TOut> f1, Func<TOut,T> f2)
            {
                return v => f2(f1(v));
            }
        }

        public int CopyTo(Span<TOut> span, int start)
        {
            if (start < 0 || start > Count)
                throw new ArgumentOutOfRangeException(nameof(start));

            var length = Math.Min(Count - start, span.Length);
            var end = _offset + start + length;
            for (int i = _offset + start, j = 0; i < end; i++, j++)
                span[j] = _map(_innerList[i]);

            return length;
        }

        public TOut this[int index]
        {
            get
            {
                if (index < 0 || index >= Count)
                    throw new IndexOutOfRangeException();

                return _map(_innerList[index + _offset]);
            }
        }

        public static IReadOnlyListExt<TOut> Create(IReadOnlyList<TIn> list, Func<TIn, TOut> map, int offset, int count)
        {
            if (list is IReadOnlyListExt<TIn> readOnlyListExt)
                return readOnlyListExt.Slice(offset, count).Map(map);

            if (list is TIn[] array)
                return new ReadOnlyMemoryList<TIn>(array).Slice(offset, count).Map(map);

            return new MappedReadOnlyListSpan<TIn, TOut>(list, map, offset, count);
        }
    }
}
