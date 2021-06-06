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
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace Octonica.ClickHouseClient.Utils
{
    internal class ConstantReadOnlyList<T> : IReadOnlyListExt<T>
    {
        [AllowNull]
        private readonly T _value;

        public int Count { get; }

        public ConstantReadOnlyList([AllowNull] T value, int count)
        {
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));

            Count = count;
            _value = value;
        }

        public IEnumerator<T> GetEnumerator()
        {
            return Enumerable.Repeat(_value, Count).GetEnumerator();
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

            return new ConstantReadOnlyList<T>(_value, length);
        }

        public IReadOnlyListExt<TOut> Map<TOut>(Func<T, TOut> map)
        {
            if (map == null)
                throw new ArgumentNullException(nameof(map));

            return new ConstantReadOnlyList<TOut>(map(_value), Count);            
        }

        public int CopyTo(Span<T> span, int offset)
        {
            if (offset < 0 || offset > Count)
                throw new ArgumentOutOfRangeException(nameof(offset));

            var length = Math.Min(span.Length, Count - offset);
            span.Slice(0, length).Fill(_value);
            return length;
        }

        public T this[int index]
        {
            get
            {
                if (index >= 0 && index < Count)
                    return _value;

                throw new IndexOutOfRangeException();
            }
        }
    }
}
