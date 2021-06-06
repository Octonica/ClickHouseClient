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
    internal sealed class ListSpan<T> : IReadOnlyListExt<T>
    {
        private readonly IList<T> _innerList;
        private readonly int _offset;

        public int Count { get; }

        private ListSpan(IList<T> innerList, int offset, int count)
        {
            if (innerList == null)
                throw new ArgumentNullException(nameof(innerList));
            if (offset < 0 || offset > innerList.Count)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (count < 0 || offset + count > innerList.Count)
                throw new ArgumentOutOfRangeException(nameof(count));

            _innerList = innerList;
            _offset = offset;
            Count = count;
        }

        public IEnumerator<T> GetEnumerator()
        {
            return _innerList.Skip(_offset).Take(Count).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public IReadOnlyListExt<T> Slice(int start, int length)
        {
            if (start < 0 || start > Count)
                throw new ArgumentOutOfRangeException(nameof(start));
            if (start + length > Count)
                throw new ArgumentOutOfRangeException(nameof(length));

            return new ListSpan<T>(_innerList, start + _offset, length);
        }

        public IReadOnlyListExt<TOut> Map<TOut>(Func<T, TOut> map)
        {
            return MappedListSpan<T, TOut>.Create(_innerList, map, _offset, Count);
        }

        public int CopyTo(Span<T> span, int start)
        {
            if (start < 0 || start > Count)
                throw new ArgumentOutOfRangeException(nameof(start));
            
            var length = Math.Min(Count - start, span.Length);
            if (_innerList is T[] array)
            {
                new ReadOnlySpan<T>(array, _offset + start, length).CopyTo(span);
            }
            else
            {
                var end = _offset + start + length;
                for (int i = _offset + start, j = 0; i < end; i++, j++)
                    span[j] = _innerList[i];
            }

            return length;            
        }

        public T this[int index]
        {
            get
            {
                if (index < 0 || index >= Count)
                    throw new IndexOutOfRangeException();

                return _innerList[index + _offset];
            }
        }

        public static IReadOnlyListExt<T> Slice(IList<T> list, int offset, int count)
        {
            if (list is IReadOnlyListExt<T> readOnlyListExt)
                return readOnlyListExt.Slice(offset, count);

            return new ListSpan<T>(list, offset, count);
        }
    }
}
