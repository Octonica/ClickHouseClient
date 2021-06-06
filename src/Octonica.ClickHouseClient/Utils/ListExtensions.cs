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

namespace Octonica.ClickHouseClient.Utils
{
    internal static class ListExtensions
    {
        public static IReadOnlyList<TOut> Map<TIn, TOut>(this IReadOnlyList<TIn> list, Func<TIn, TOut> map)
        {
            return MappedReadOnlyList<TIn, TOut>.Map(list, map);
        }

        public static IReadOnlyList<TOut> Map<TIn, TOut>(this IList<TIn> list, Func<TIn, TOut> map)
        {
            if (list == null)
                throw new ArgumentNullException(nameof(list));

            return list.Slice(0).Map(map);
        }

        public static IReadOnlyList<T> Slice<T>(this IReadOnlyList<T> list, int start)
        {
            if (list == null)
                throw new ArgumentNullException(nameof(list));

            if (start == 0)
                return list;

            return list.Slice(start, list.Count - start);            
        }

        public static IReadOnlyList<T> Slice<T>(this IReadOnlyList<T> list, int start, int length)
        {
            if (list == null)
                throw new ArgumentNullException(nameof(list));

            if (start == 0 && length == list.Count)
                return list;

            if (list is T[] array)
                return new ReadOnlyMemoryList<T>(array).Slice(start, length);

            return ReadOnlyListSpan<T>.Slice(list, start, length);
        }

        public static IReadOnlyList<T> Slice<T>(this IList<T> list, int start)
        {
            if (list == null)
                throw new ArgumentNullException(nameof(list));

            if (start == 0 && list is IReadOnlyList<T> readOnlyList)
                return readOnlyList;

            return list.Slice(start, list.Count - start);            
        }

        public static IReadOnlyList<T> Slice<T>(this IList<T> list, int start, int length)
        {
            if (list == null)
                throw new ArgumentNullException(nameof(list));

            if (start == 0 && list is IReadOnlyList<T> readOnlyList && length == readOnlyList.Count)
                return readOnlyList;

            if (list is T[] array)
                return new ReadOnlyMemoryList<T>(array).Slice(start, length);

            return ListSpan<T>.Slice(list, start, length);
        }

        public static int CopyTo<T>(this IReadOnlyList<T> list, Span<T> span, int start)
        {
            if (list == null)
                throw new ArgumentNullException(nameof(list));

            if (list is IReadOnlyListExt<T> readOnlyListExt)
                return readOnlyListExt.CopyTo(span, start);

            if (start < 0 || start > list.Count)
                throw new ArgumentOutOfRangeException(nameof(start));

            var length = Math.Min(list.Count - start, span.Length);
            if (list is T[] array)
            {
                new ReadOnlySpan<T>(array, start, length).CopyTo(span);
            }
            else
            {
                var end = start + length;
                for (int i = start, j = 0; i < end; i++, j++)
                    span[j] = list[i];
            }

            return length;
        }
    }
}
