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

        public int Count => _items.Count;

        bool ICollection<TValue>.IsReadOnly => false;

        IEnumerable<TValue> IReadOnlyDictionary<TKey, TValue>.Values => this;

        IEnumerable<TKey> IReadOnlyDictionary<TKey, TValue>.Keys => _keys.AsReadOnly();

        bool ICollection.IsSynchronized => false;

        object ICollection.SyncRoot => ((ICollection)_items).SyncRoot;

        protected IndexedCollectionBase(IEqualityComparer<TKey>? comparer)
        {
            _items = new Dictionary<TKey, TValue>(comparer);
            _keys = new List<TKey>();
        }

        protected IndexedCollectionBase(int capacity, IEqualityComparer<TKey>? comparer)
        {
            _items = new Dictionary<TKey, TValue>(capacity, comparer);
            _keys = new List<TKey>(capacity);
        }

        protected abstract TKey GetKey(TValue item);

        public bool ContainsKey(TKey key)
        {
            return _items.ContainsKey(key);
        }

        public bool TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value)
        {
            return _items.TryGetValue(key, out value);
        }

        public int IndexOf(TKey key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (!_items.ContainsKey(key))
                return -1;

            var comparer = _items.Comparer;
            return _keys.FindIndex(item => comparer.Equals(item, key));
        }

        public int IndexOf(TValue item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));

            var key = GetKey(item);
            if (!_items.TryGetValue(key, out var existingValue) || !existingValue.Equals(item))
                return -1;

            var comparer = _items.Comparer;
            return _keys.FindIndex(item => comparer.Equals(item, key));
        }

        public void Insert(int index, TValue item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));

            var key = GetKey(item);
            _items.Add(key, item);
            try
            {
                _keys.Insert(index, key);
            }
            catch
            {
                _items.Remove(key);
                throw;
            }
        }

        public bool Remove(TValue item)
        {
            if (item == null)
                return false;

            var key = GetKey(item);
            if (!_items.TryGetValue(key, out var existingValue) || !existingValue.Equals(item))
                return false;

            var comparer = _items.Comparer;
            var idx = _keys.FindIndex(item => comparer.Equals(item, key));
            Debug.Assert(idx >= 0);

            RemoveAt(idx);
            return true;
        }

        public bool Remove(TKey key)
        {
            if (key == null)
                return false;

            if (!_items.TryGetValue(key, out var existingValue))
                return false;

            var comparer = _items.Comparer;
            var idx = _keys.FindIndex(item => comparer.Equals(item, key));
            Debug.Assert(idx >= 0);

            RemoveAt(idx);
            return true;
        }

        public void RemoveAt(int index)
        {
            var key = _keys[index];
            _keys.RemoveAt(index);
            _items.Remove(key);
        }

        public void Add(TValue item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));

            var key = GetKey(item);
            _items.Add(key, item);
            _keys.Add(key);
        }

        public void Clear()
        {
            _keys.Clear();
            _items.Clear();
        }

        public bool Contains(TValue item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));

            var key = GetKey(item);
            if (!_items.TryGetValue(key, out var existingItem))
                return false;

            return existingItem.Equals(item);
        }

        public void CopyTo(TValue[] array, int arrayIndex)
        {
            int sourceIdx = 0, targetIdx = arrayIndex;
            while (sourceIdx < _keys.Count)
                array[targetIdx++] = _items[_keys[sourceIdx++]];
        }

        void ICollection.CopyTo(Array array, int index)
        {
            int sourceIdx = 0, targetIdx = index;
            while (sourceIdx < _keys.Count)
                array.SetValue(_items[_keys[sourceIdx++]], targetIdx++);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
        {
            foreach (var name in _keys)
                yield return new KeyValuePair<TKey, TValue>(name, _items[name]);
        }

        public IEnumerator<TValue> GetEnumerator()
        {
            foreach (var name in _keys)
                yield return _items[name];
        }

        public TValue this[TKey key] => _items[key];

        public TValue this[int index]
        {
            get => _items[_keys[index]];
            set
            {
                if (value == null)
                    throw new ArgumentNullException(nameof(value));

                var valueKey = GetKey(value);
                var item = _items[_keys[index]];
                var itemKey = GetKey(item);
                if (_items.Comparer.Equals(itemKey, valueKey))
                {
                    _items[valueKey] = value;
                    Debug.Assert(_keys.Count == _items.Count);
                }
                else
                {
                    _items.Add(valueKey, value);
                    _items.Remove(itemKey);
                }

                _keys[index] = valueKey;
            }
        }
    }
}
