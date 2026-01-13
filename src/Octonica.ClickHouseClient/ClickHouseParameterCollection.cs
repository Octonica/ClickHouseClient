#region License Apache 2.0
/* Copyright 2019-2021, 2024 Octonica
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
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// Represents a collection of parameters associated with a <see cref="ClickHouseCommand"/>. This class cannot be inherited.
    /// </summary>
    public sealed class ClickHouseParameterCollection : DbParameterCollection, IList<ClickHouseParameter>, IReadOnlyList<ClickHouseParameter>
    {
        private readonly List<string> _parameterNames = [];

        private readonly Dictionary<string, ClickHouseParameter> _parameters = new(StringComparer.OrdinalIgnoreCase);

        /// <inheritdoc/>
        public override int Count => _parameters.Count;

        /// <inheritdoc/>
        public override object SyncRoot => ((ICollection)_parameters).SyncRoot;

        /// <inheritdoc/>
        public override int Add(object value)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            ClickHouseParameter parameter = (ClickHouseParameter)value;
            return Add(parameter);
        }

        /// <summary>
        /// Creates, adds to the collection and returns a new parameter with specified name and value.
        /// </summary>
        /// <param name="parameterName">The name of the parameter.</param>
        /// <param name="value">The value of the paramter.</param>
        /// <returns>A new <see cref="ClickHouseParameter"/> added to the collection.</returns>
        public ClickHouseParameter AddWithValue(string parameterName, object? value)
        {
            ClickHouseParameter parameter = new(parameterName) { Value = value };
            _ = Add(parameter);
            return parameter;
        }

        /// <summary>
        /// Creates, adds to the collection and returns a new parameter with specified name, value and type.
        /// </summary>
        /// <param name="parameterName">The name of the parameter.</param>
        /// <param name="value">The value of the paramter.</param>
        /// <param name="dbType">The type of the paramter</param>
        /// <returns>A new <see cref="ClickHouseParameter"/> added to the collection.</returns>
        public ClickHouseParameter AddWithValue(string parameterName, object? value, DbType dbType)
        {
            return AddWithValue(parameterName, value, (ClickHouseDbType)dbType);
        }

        /// <summary>
        /// Creates, adds to the collection and returns a new parameter with specified name, value and type.
        /// </summary>
        /// <param name="parameterName">The name of the parameter.</param>
        /// <param name="value">The value of the paramter.</param>
        /// <param name="dbType">The type of the paramter</param>
        /// <returns>A new <see cref="ClickHouseParameter"/> added to the collection.</returns>
        public ClickHouseParameter AddWithValue(string parameterName, object? value, ClickHouseDbType dbType)
        {
            ClickHouseParameter parameter = new(parameterName) { Value = value, ClickHouseDbType = dbType };
            _ = Add(parameter);
            return parameter;
        }

        void ICollection<ClickHouseParameter>.Add(ClickHouseParameter item)
        {
            _ = Add(item);
        }

        /// <summary>
        /// Adds an existing parameter to the collection.
        /// </summary>
        /// <param name="item">The parameter.</param>
        /// <returns>The zero-based index of the parameter in the collection.</returns>
        public int Add(ClickHouseParameter item)
        {
            if (item == null)
            {
                throw new ArgumentNullException(nameof(item));
            }

            if (item.Collection != null)
            {
                string errorText = ReferenceEquals(item.Collection, this)
                    ? $"The parameter \"{item.ParameterName}\" already belongs to the collection. It can't be added to the same connection twice."
                    : $"The parameter \"{item.ParameterName}\" already belongs to a collection. It can't be added to different collections.";

                throw new ArgumentException(errorText, nameof(item));
            }

            if (_parameters.ContainsKey(item.Id))
            {
                throw new ArgumentException($"A parameter with the name \"{item.ParameterName}\" already exists in the collection.", nameof(item));
            }

            _parameters.Add(item.Id, item);
            int result = _parameterNames.Count;
            _parameterNames.Add(item.Id);
            item.Collection = this;

            return result;
        }

        /// <inheritdoc/>
        public override void Clear()
        {
            foreach (ClickHouseParameter parameter in _parameters.Values)
            {
                parameter.Collection = null;
            }

            _parameters.Clear();
            _parameterNames.Clear();
        }

        /// <inheritdoc/>
        public bool Contains(ClickHouseParameter item)
        {
            return item != null && _parameters.TryGetValue(item.Id, out ClickHouseParameter? parameter) && ReferenceEquals(item, parameter);
        }

        /// <inheritdoc/>
        public void CopyTo(ClickHouseParameter[] array, int arrayIndex)
        {
            int i = arrayIndex;
            foreach (string key in _parameterNames)
            {
                array[i++] = _parameters[key];
            }
        }

        /// <inheritdoc/>
        public override bool Contains(object value)
        {
            return value is ClickHouseParameter parameter && Contains(parameter);
        }

        /// <inheritdoc/>
        public override int IndexOf(object value)
        {
            return value is not ClickHouseParameter parameter ? -1 : IndexOf(parameter);
        }

        /// <inheritdoc/>
        public override void Insert(int index, object value)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            ClickHouseParameter parameter = (ClickHouseParameter)value;
            Insert(index, parameter);
        }

        /// <inheritdoc/>
        public bool Remove(ClickHouseParameter item)
        {
            if (item == null)
            {
                throw new ArgumentNullException(nameof(item));
            }

            if (!_parameters.TryGetValue(item.Id, out ClickHouseParameter? existingParameter) || !ReferenceEquals(item, existingParameter))
            {
                return false;
            }

            IEqualityComparer<string> comparer = _parameters.Comparer;
            string name = item.Id;
            int index = _parameterNames.FindIndex(n => comparer.Equals(n, name));

            _parameterNames.RemoveAt(index);
            bool result = _parameters.Remove(name);
            item.Collection = null;

            Debug.Assert(result);
            return true;
        }

        /// <summary>
        /// Removes the parameter with the specified name from the collection.
        /// </summary>
        /// <param name="parameterName">The name of the parameter.</param>
        /// <returns><see langword="true"/> if the parameter was removed; <see langword="false"/> if a parameter with the specified name is not present in the collection.</returns>
        public bool Remove(string parameterName)
        {
            return Remove(parameterName, out _);
        }

        /// <summary>
        /// Removes the parameter with the specified name from the collection.
        /// </summary>
        /// <param name="parameterName">The name of the parameter.</param>
        /// <param name="parameter">When this method returns, contains the removed parameter or <see langword="null"/> if a parameter was not removed.</param>
        /// <returns> if the parameter was removed; <see langword="false"/> if a parameter with the specified name is not present in the collection.</returns>
        public bool Remove(string parameterName, [MaybeNullWhen(false)] out ClickHouseParameter parameter)
        {
            if (parameterName == null)
            {
                throw new ArgumentNullException(nameof(parameterName));
            }

            string name = ClickHouseParameter.TrimParameterName(parameterName);
            if (!_parameters.Remove(name, out parameter))
            {
                return false;
            }

            parameter.Collection = null;
            IEqualityComparer<string> comparer = _parameters.Comparer;
            int index = _parameterNames.FindIndex(n => comparer.Equals(n, name));
            _parameterNames.RemoveAt(index);
            return true;
        }

        /// <inheritdoc/>
        public override void Remove(object value)
        {
            if (value is not ClickHouseParameter parameter)
            {
                return;
            }

            _ = Remove(parameter);
        }

        /// <inheritdoc/>
        public int IndexOf(ClickHouseParameter item)
        {
            if (item == null)
            {
                return -1;
            }

            if (!_parameters.TryGetValue(item.Id, out ClickHouseParameter? existingParameter) || !ReferenceEquals(item, existingParameter))
            {
                return -1;
            }

            IEqualityComparer<string> comparer = _parameters.Comparer;
            string name = item.Id;
            int index = _parameterNames.FindIndex(n => comparer.Equals(n, name));

            return index;
        }

        /// <inheritdoc/>
        public void Insert(int index, ClickHouseParameter item)
        {
            if (item == null)
            {
                throw new ArgumentNullException(nameof(item));
            }

            if (item.Collection != null)
            {
                string errorText = ReferenceEquals(item.Collection, this)
                    ? $"The parameter \"{item.ParameterName}\" already belongs to the collection. It can't be added to the same connection twice."
                    : $"The parameter \"{item.ParameterName}\" already belongs to a collection. It can't be added to different collections.";

                throw new ArgumentException(errorText, nameof(item));
            }

            if (_parameters.ContainsKey(item.Id))
            {
                throw new ArgumentException($"A parameter with the name \"{item.ParameterName}\" already exists in the collection.", nameof(item));
            }

            _parameterNames.Insert(index, item.Id);
            _parameters.Add(item.Id, item);
            item.Collection = this;
        }

        /// <inheritdoc/>
        public override void RemoveAt(int index)
        {
            string name = _parameterNames[index];
            if (_parameters.Remove(name, out ClickHouseParameter? parameter))
            {
                parameter.Collection = null;
            }

            _parameterNames.RemoveAt(index);
        }

        /// <inheritdoc/>
        public override void RemoveAt(string parameterName)
        {
            _ = Remove(parameterName, out _);
        }

        /// <inheritdoc/>
        protected override void SetParameter(int index, DbParameter value)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            string name = _parameterNames[index];
            ClickHouseParameter parameter = (ClickHouseParameter)value;

            IEqualityComparer<string> comparer = _parameters.Comparer;
            if (comparer.Equals(name, parameter.Id))
            {
                ClickHouseParameter existingParameter = _parameters[name];
                if (!ReferenceEquals(parameter, existingParameter))
                {
                    if (parameter.Collection != null)
                    {
                        string errorText = ReferenceEquals(parameter.Collection, this)
                            ? $"The parameter \"{parameter.ParameterName}\" already belongs to the collection. It can't be added to the same connection twice."
                            : $"The parameter \"{parameter.ParameterName}\" already belongs to a collection. It can't be added to different collections.";

                        throw new ArgumentException(errorText, nameof(value));
                    }

                    _parameters[name] = parameter;
                    existingParameter.Collection = null;
                    parameter.Collection = this;
                }
            }
            else
            {
                if (parameter.Collection != null)
                {
                    string errorText = ReferenceEquals(parameter.Collection, this)
                        ? $"The parameter \"{parameter.ParameterName}\" already belongs to the collection. It can't be added to the same connection twice."
                        : $"The parameter \"{parameter.ParameterName}\" already belongs to a collection. It can't be added to different collections.";

                    throw new ArgumentException(errorText, nameof(value));
                }

                if (_parameters.ContainsKey(parameter.Id))
                {
                    throw new ArgumentException($"A parameter with the name \"{parameter.ParameterName}\" already exists in the collection.", nameof(value));
                }

                if (_parameters.Remove(name, out ClickHouseParameter? existingParameter))
                {
                    existingParameter.Collection = null;
                }

                _parameters.Add(parameter.Id, parameter);
                _parameterNames[index] = parameter.Id;
                parameter.Collection = this;
            }
        }

        /// <inheritdoc/>
        protected override void SetParameter(string parameterName, DbParameter value)
        {
            if (parameterName == null)
            {
                throw new ArgumentNullException(nameof(parameterName));
            }

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            string name = ClickHouseParameter.TrimParameterName(parameterName);
            ClickHouseParameter parameter = (ClickHouseParameter)value;

            IEqualityComparer<string> comparer = _parameters.Comparer;
            if (_parameters.TryGetValue(name, out ClickHouseParameter? existingParameter))
            {
                if (!ReferenceEquals(parameter, existingParameter))
                {
                    if (parameter.Collection != null)
                    {
                        string errorText = ReferenceEquals(parameter.Collection, this)
                            ? $"The parameter \"{parameter.ParameterName}\" already belongs to the collection. It can't be added to the same connection twice."
                            : $"The parameter \"{parameter.ParameterName}\" already belongs to a collection. It can't be added to different collections.";

                        throw new ArgumentException(errorText, nameof(value));
                    }
                }

                if (comparer.Equals(name, parameter.Id))
                {
                    _parameters[name] = parameter;
                }
                else
                {
                    if (_parameters.ContainsKey(parameter.Id))
                    {
                        throw new ArgumentException($"A parameter with the name \"{parameter.ParameterName}\" already exists in the collection.", nameof(value));
                    }

                    int index = _parameterNames.FindIndex(n => comparer.Equals(n, name));
                    _parameterNames[index] = parameter.Id;

                    _ = _parameters.Remove(name);
                    _parameters.Add(parameter.Id, parameter);
                }

                if (!ReferenceEquals(parameter, existingParameter))
                {
                    existingParameter.Collection = null;
                    parameter.Collection = this;
                }
            }
            else
            {
                _ = comparer.Equals(name, parameter.Id)
                    ? Add(parameter)
                    : throw new ArgumentException(
                    $"A parameter with the name \"{parameterName}\" is not present in the collection. It can't be replaced with the parameter \"{parameter.ParameterName}\".",
                    nameof(parameterName));
            }
        }

        /// <inheritdoc/>
        public override int IndexOf(string parameterName)
        {
            if (parameterName == null)
            {
                throw new ArgumentNullException(nameof(parameterName));
            }

            string name = ClickHouseParameter.TrimParameterName(parameterName);
            IEqualityComparer<string> comparer = _parameters.Comparer;

            return _parameterNames.FindIndex(n => comparer.Equals(n, name));
        }

        /// <inheritdoc/>
        public override bool Contains(string value)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            string parameterName = ClickHouseParameter.TrimParameterName(value);
            return _parameters.ContainsKey(parameterName);
        }

        /// <inheritdoc/>
        public override void CopyTo(Array array, int index)
        {
            if (array == null)
            {
                throw new ArgumentNullException(nameof(array));
            }

            int i = index;
            foreach (string name in _parameterNames)
            {
                array.SetValue(_parameters[name], i++);
            }
        }

        IEnumerator<ClickHouseParameter> IEnumerable<ClickHouseParameter>.GetEnumerator()
        {
            return _parameterNames.Select(n => _parameters[n]).GetEnumerator();
        }

        /// <inheritdoc/>
        public override IEnumerator GetEnumerator()
        {
            return _parameterNames.Select(n => _parameters[n]).GetEnumerator();
        }

        /// <inheritdoc/>
        protected override DbParameter GetParameter(int index)
        {
            return _parameters[_parameterNames[index]];
        }

        /// <inheritdoc/>
        protected override DbParameter GetParameter(string parameterName)
        {
            return !TryGetValue(parameterName, out ClickHouseParameter? parameter)
                ? throw new ArgumentException($"Parameter \"{parameterName}\" not found.", nameof(parameterName))
                : (DbParameter)parameter;
        }

        /// <inheritdoc/>
        public override void AddRange(Array values)
        {
            if (values == null)
            {
                throw new ArgumentNullException(nameof(values));
            }

            foreach (ClickHouseParameter parameter in values.Cast<ClickHouseParameter>())
            {
                _ = Add(parameter);
            }
        }

        /// <summary>
        /// Adds the specified parameters to the collection.
        /// </summary>
        /// <param name="parameters">The set of parameters that should be added to this collection.</param>
        /// <remarks>This operation is not atomic, it calls <see cref="Add(ClickHouseParameter)"/> for each parameter in <paramref name="parameters"/>.</remarks>
        public void AddRange(IEnumerable<ClickHouseParameter> parameters)
        {
            if (parameters == null)
            {
                throw new ArgumentNullException(nameof(parameters));
            }

            foreach (ClickHouseParameter parameter in parameters)
            {
                _ = Add(parameter);
            }
        }

        /// <summary>
        /// Gets the <see cref="ClickHouseParameter"/> with the specified name.
        /// </summary>
        /// <param name="parameterName">The name of the parameter.</param>
        /// <param name="parameter">
        /// When this method returns, contains the <see cref="ClickHouseParameter"/> with the specified name or
        /// <see langword="null"/> if a parameter is not present in the collection.
        /// </param>
        /// <returns><see langword="true"/> if the parameter with the specified name was found in the collection; otherwise <see langword="false"/></returns>
        public bool TryGetValue(string parameterName, [NotNullWhen(true)] out ClickHouseParameter? parameter)
        {
            if (parameterName == null)
            {
                throw new ArgumentNullException(nameof(parameterName));
            }

            string name = ClickHouseParameter.TrimParameterName(parameterName);
            return _parameters.TryGetValue(name, out parameter);
        }

        internal void OnParameterIdChanged(string originalId, ClickHouseParameter parameter)
        {
            Debug.Assert(ReferenceEquals(parameter.Collection, this));
            if (_parameters.Comparer.Equals(originalId, parameter.Id))
            {
                return;
            }

            SetParameter(originalId, parameter);
        }

        /// <inheritdoc/>
        public new ClickHouseParameter this[int index]
        {
            get => (ClickHouseParameter)base[index];
            set => base[index] = value;
        }

        /// <summary>Gets or sets the <see cref="ClickHouseParameter"/> with the specified name.</summary>
        /// <param name="parameterName">The name of the <see cref="ClickHouseParameter"/> in the collection.</param>
        /// <returns>The <see cref="ClickHouseParameter"/> with the specified name.</returns>
        public new ClickHouseParameter this[string parameterName]
        {
            get => (ClickHouseParameter)base[parameterName];
            set => base[parameterName] = value;
        }
    }
}
