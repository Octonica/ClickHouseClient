#region License Apache 2.0
/* Copyright 2020 Octonica
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
    internal static class MultiDimensionalArrayReadOnlyListAdapter
    {
        public static (Func<Array, object> createList, Type listElementType) Dispatch(Type arrayElementType, int arrayRank)
        {
            return TypeDispatcher.Dispatch(arrayElementType, new Dispatcher(arrayRank));
        }

        private sealed class Dispatcher : ITypeDispatcher<(Func<Array, object> createList, Type listElementType)>
        {
            private readonly int _arrayRank;

            public Dispatcher(int arrayRank)
            {
                _arrayRank = arrayRank;
            }

            public (Func<Array, object> createList, Type listElementType) Dispatch<T>()
            {
                return Dispatch(
                    _arrayRank - 1,
                    (arr, indices, idx) =>
                    {
                        object? result;
                        if (indices == null)
                        {
                            result = arr.GetValue(idx);
                        }
                        else
                        {
                            int[] idxCopy = new int[indices.Length + 1];
                            Array.Copy(indices, idxCopy, indices.Length);
                            idxCopy[^1] = idx;
                            result = arr.GetValue(idxCopy);
                        }

                        // Actually the result can be null if T is nullable
                        return (T)result!;
                    });
            }

            private static (Func<Array, object> createList, Type listElementType) Dispatch<T>(int depth, Func<Array, int[]?, int, T> selector)
            {
                return depth == 0
                    ? ((Func<Array, object> createList, Type listElementType))(array => new Adapter<T>(array, null, selector), typeof(T))
                    : Dispatch(
                    depth - 1,
                    (arr, indices, idx) =>
                    {
                        int[] idxCopy;
                        if (indices == null)
                        {
                            idxCopy = new[] { idx };
                        }
                        else
                        {
                            idxCopy = new int[indices.Length + 1];
                            Array.Copy(indices, idxCopy, indices.Length);
                            idxCopy[^1] = idx;
                        }

                        return new Adapter<T>(arr, idxCopy, selector);
                    });
            }
        }

        private sealed class Adapter<T> : IReadOnlyList<T>
        {
            private readonly Array _array;
            private readonly int[]? _indices;
            private readonly Func<Array, int[]?, int, T> _selector;

            public int Count => _array.GetLength(_indices?.Length ?? 0);

            public Adapter(Array array, int[]? indices, Func<Array, int[]?, int, T> selector)
            {
                _array = array;
                _indices = indices;
                _selector = selector;
            }

            public T this[int index] => _selector(_array, _indices, index);

            public IEnumerator<T> GetEnumerator()
            {
                int count = _array.GetLength(_indices?.Length ?? 0);
                for (int i = 0; i < count; i++)
                {
                    yield return _selector(_array, _indices, i);
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }
    }
}
