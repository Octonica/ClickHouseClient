#region License Apache 2.0
/* Copyright 2019-2020 Octonica
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
    internal sealed class ReadOnlyListSpan<T> : IReadOnlyList<T>
    {
        private readonly IReadOnlyList<T> _innerList;
        private readonly int _offset;

        public int Count { get; }

        public ReadOnlyListSpan(IReadOnlyList<T> innerList, int offset)
        {
            if (innerList == null)
                throw new ArgumentNullException(nameof(innerList));
            if (offset < 0 || offset > innerList.Count)
                throw new ArgumentOutOfRangeException(nameof(offset));

            Count = innerList.Count - offset;
            if (innerList is ReadOnlyListSpan<T> innerSpan)
            {
                _innerList = innerSpan._innerList;
                _offset = innerSpan._offset + offset;
            }
            else
            {
                _innerList = innerList;
                _offset = offset;
            }
        }

        public ReadOnlyListSpan(IReadOnlyList<T> innerList, int offset, int count)
        {
            if (innerList == null)
                throw new ArgumentNullException(nameof(innerList));
            if (offset < 0 || offset > innerList.Count)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (count < 0 || offset + count > innerList.Count)
                throw new ArgumentOutOfRangeException(nameof(count));

            Count = count;
            if (innerList is ReadOnlyListSpan<T> innerSpan)
            {
                _innerList = innerSpan._innerList;
                _offset = innerSpan._offset + offset;
            }
            else
            {
                _innerList = innerList;
                _offset = offset;
            }
        }

        public IEnumerator<T> GetEnumerator()
        {
            return _innerList.Skip(_offset).Take(Count).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
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
    }
}
