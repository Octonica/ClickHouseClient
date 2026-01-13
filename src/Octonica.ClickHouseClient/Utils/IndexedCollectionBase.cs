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
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace Octonica.ClickHouseClient.Utils
{
    /// <summary>
    /// Represents a collection of items accessible both by key and index.
    /// </summary>
    /// <typeparam name="TKey">The type of keys.</typeparam>
    /// <typeparam name="TValue">The type of items.</typeparam>
    public abstract class IndexedCollectionBase<TKey, TValue> :
        IList<TValue>,
        IReadOnlyList<TValue>,
        IReadOnlyDictionary<TKey, TValue>,
        ICollection
        where TKey : notnull
        where TValue : notnull
    {
        private readonly Dictionary<TKey, TValue> _items;
        private readonly List<TKey> _keys;

        /// <inheritdoc/>
        public int Count => _items.Count;

        bool ICollection<TValue>.IsReadOnly => false;

        IEnumerable<TValue> IReadOnlyDictionary<TKey, TValue>.Values => this;

        IEnumerable<TKey> IReadOnlyDictionary<TKey, TValue>.Keys => _keys.AsReadOnly();

        bool ICollection.IsSynchronized => false;

        object ICollection.SyncRoot => ((ICollection)_items).SyncRoot;

        /// <summary>
        /// Initializes a new instance of the <see cref="IndexedCollectionBase{TKey, TValue}"/> class that is empty, has the
        /// default initial capacity, and uses the specified <see cref="IEqualityComparer{T}"/>.
        /// </summary>
        /// <param name="comparer">
        /// The <see cref="IEqualityComparer{T}"/> implementation to use when comparing keys, or <see langword="null"/> to use the
        /// default <see cref="EqualityComparer{T}"/> for the type of the key.
        /// </param>
        protected IndexedCollectionBase(IEqualityComparer<TKey>? comparer)
        {
            _items = new Dictionary<TKey, TValue>(comparer);
            _keys = [];
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IndexedCollectionBase{TKey, TValue}"/> class that is empty, has the
        /// specified initial capacity, and uses the specified <see cref="IEqualityComparer{T}"/>.
        /// </summary>
        /// <param name="capacity">The initial number of elements that the collection can contain.</param>
        /// <param name="comparer">
        /// The <see cref="IEqualityComparer{T}"/> implementation to use when comparing keys, or <see langword="null"/> to use the
        /// default <see cref="EqualityComparer{T}"/> for the type of the key.
        /// </param>
        protected IndexedCollectionBase(int capacity, IEqualityComparer<TKey>? comparer)
        {
            _items = new Dictionary<TKey, TValue>(capacity, comparer);
            _keys = new List<TKey>(capacity);
        }

        /// <summary>
        /// Extracts and returns the key of the item.
        /// </summary>
        /// <param name="item">The item from which the key should be extracted.</param>
        /// <returns>The key of the item.</returns>
        protected abstract TKey GetKey(TValue item);

        /// <inheritdoc/>
        public bool ContainsKey(TKey key)
        {
            return _items.ContainsKey(key);
        }

        /// <inheritdoc/>
        public bool TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value)
        {
            return _items.TryGetValue(key, out value);
        }

        /// <summary>
        /// Determines the index of the item with the specific key.
        /// </summary>
        /// <param name="key">The key of the item in the collection.</param>
        /// <returns>The index of the item found in the collection; otherwise -1.</returns>
        public int IndexOf(TKey key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (!_items.ContainsKey(key))
            {
                return -1;
            }

            IEqualityComparer<TKey> comparer = _items.Comparer;
            return _keys.FindIndex(item => comparer.Equals(item, key));
        }

        /// <inheritdoc/>
        public int IndexOf(TValue item)
        {
            if (item == null)
            {
                throw new ArgumentNullException(nameof(item));
            }

            TKey key = GetKey(item);
            if (!_items.TryGetValue(key, out TValue? existingValue) || !existingValue.Equals(item))
            {
                return -1;
            }

            IEqualityComparer<TKey> comparer = _items.Comparer;
            return _keys.FindIndex(item => comparer.Equals(item, key));
        }

        /// <inheritdoc/>
        public void Insert(int index, TValue item)
        {
            if (item == null)
            {
                throw new ArgumentNullException(nameof(item));
            }

            TKey key = GetKey(item);
            _items.Add(key, item);
            try
            {
                _keys.Insert(index, key);
            }
            catch
            {
                _ = _items.Remove(key);
                throw;
            }
        }

        /// <inheritdoc/>
        public bool Remove(TValue item)
        {
            if (item == null)
            {
                return false;
            }

            TKey key = GetKey(item);
            if (!_items.TryGetValue(key, out TValue? existingValue) || !existingValue.Equals(item))
            {
                return false;
            }

            IEqualityComparer<TKey> comparer = _items.Comparer;
            int idx = _keys.FindIndex(item => comparer.Equals(item, key));
            Debug.Assert(idx >= 0);

            RemoveAt(idx);
            return true;
        }

        /// <summary>
        /// Removes the item with the specific key from the collection.
        /// </summary>
        /// <param name="key">The key of the item in the collection.</param>
        /// <returns><see langword="true"/> if the item was successfully removed from the collection. <see langword="false"/> if an item was not found in the collection.</returns>
        public bool Remove(TKey key)
        {
            if (key == null)
            {
                return false;
            }

            if (!_items.TryGetValue(key, out TValue? existingValue))
            {
                return false;
            }

            IEqualityComparer<TKey> comparer = _items.Comparer;
            int idx = _keys.FindIndex(item => comparer.Equals(item, key));
            Debug.Assert(idx >= 0);

            RemoveAt(idx);
            return true;
        }

        /// <inheritdoc/>
        public void RemoveAt(int index)
        {
            TKey key = _keys[index];
            _keys.RemoveAt(index);
            _ = _items.Remove(key);
        }

        /// <inheritdoc/>
        public void Add(TValue item)
        {
            if (item == null)
            {
                throw new ArgumentNullException(nameof(item));
            }

            TKey key = GetKey(item);
            _items.Add(key, item);
            _keys.Add(key);
        }

        /// <inheritdoc/>
        public void Clear()
        {
            _keys.Clear();
            _items.Clear();
        }

        /// <inheritdoc/>
        public bool Contains(TValue item)
        {
            if (item == null)
            {
                throw new ArgumentNullException(nameof(item));
            }

            TKey key = GetKey(item);
            return _items.TryGetValue(key, out TValue? existingItem) && existingItem.Equals(item);
        }

        /// <inheritdoc/>
        public void CopyTo(TValue[] array, int arrayIndex)
        {
            int sourceIdx = 0, targetIdx = arrayIndex;
            while (sourceIdx < _keys.Count)
            {
                array[targetIdx++] = _items[_keys[sourceIdx++]];
            }
        }

        void ICollection.CopyTo(Array array, int index)
        {
            int sourceIdx = 0, targetIdx = index;
            while (sourceIdx < _keys.Count)
            {
                array.SetValue(_items[_keys[sourceIdx++]], targetIdx++);
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
        {
            foreach (TKey name in _keys)
            {
                yield return new KeyValuePair<TKey, TValue>(name, _items[name]);
            }
        }

        /// <inheritdoc/>
        public IEnumerator<TValue> GetEnumerator()
        {
            foreach (TKey name in _keys)
            {
                yield return _items[name];
            }
        }

        /// <inheritdoc/>
        public TValue this[TKey key] => _items[key];

        /// <inheritdoc/>
        public TValue this[int index]
        {
            get => _items[_keys[index]];
            set
            {
                if (value == null)
                {
                    throw new ArgumentNullException(nameof(value));
                }

                TKey valueKey = GetKey(value);
                TValue item = _items[_keys[index]];
                TKey itemKey = GetKey(item);
                if (_items.Comparer.Equals(itemKey, valueKey))
                {
                    _items[valueKey] = value;
                    Debug.Assert(_keys.Count == _items.Count);
                }
                else
                {
                    _items.Add(valueKey, value);
                    _ = _items.Remove(itemKey);
                }

                _keys[index] = valueKey;
            }
        }
    }
}
