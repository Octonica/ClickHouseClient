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

using System.Collections;
using System.Collections.Generic;

namespace Octonica.ClickHouseClient.Tests
{
    // Wrappers for List<T>. Each wrapper implements only one of interfaces implemented by List<T>.

    internal abstract class ListWrapperBase<T>
    {
        public List<T> List { get; }

        public int Count => List.Count;

        public bool IsReadOnly => ((IList<T>) List).IsReadOnly;

        protected ListWrapperBase(List<T> list)
        {
            List = list;
        }

        public IEnumerator<T> GetEnumerator()
        {
            return List.GetEnumerator();
        }

        public void Add(T item)
        {
            List.Add(item);
        }

        public void Clear()
        {
            List.Clear();
        }

        public bool Contains(T item)
        {
            return List.Contains(item);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            List.CopyTo(array, arrayIndex);
        }

        public bool Remove(T item)
        {
            return List.Remove(item);
        }

        public int IndexOf(T item)
        {
            return List.IndexOf(item);
        }

        public void Insert(int index, T item)
        {
            List.Insert(index, item);
        }

        public void RemoveAt(int index)
        {
            List.RemoveAt(index);
        }

        public T this[int index]
        {
            get => List[index];
            set => List[index] = value;
        }
    }

    internal sealed class EnumerableListWrapper<T> : ListWrapperBase<T>, IEnumerable
    {
        public EnumerableListWrapper(List<T> list)
            : base(list)
        {
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    internal sealed class GenericEnumerableListWrapper<T> : ListWrapperBase<T>, IEnumerable<T>
    {
        public GenericEnumerableListWrapper(List<T> list)
            : base(list)
        {
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    internal sealed class ListWrapper<T> : ListWrapperBase<T>, IList<T>
    {
        public ListWrapper(List<T> list)
            : base(list)
        {
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
