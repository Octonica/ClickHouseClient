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
        private readonly List<string> _parameterNames = new List<string>();

        private readonly Dictionary<string, ClickHouseParameter> _parameters = new Dictionary<string, ClickHouseParameter>(StringComparer.OrdinalIgnoreCase);

        /// <inheritdoc/>
        public override int Count => _parameters.Count;

        /// <inheritdoc/>
        public override object SyncRoot => ((ICollection) _parameters).SyncRoot;

        /// <inheritdoc/>
        public override int Add(object value)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            var parameter = (ClickHouseParameter) value;
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
            var parameter = new ClickHouseParameter(parameterName) {Value = value};
            Add(parameter);
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
            var parameter = new ClickHouseParameter(parameterName) { Value = value, ClickHouseDbType = dbType };
            Add(parameter);
            return parameter;
        }

        void ICollection<ClickHouseParameter>.Add(ClickHouseParameter item)
        {
            Add(item);
        }

        /// <summary>
        /// Adds an existing parameter to the collection.
        /// </summary>
        /// <param name="item">The parameter.</param>
        /// <returns>The zero-based index of the parameter in the collection.</returns>
        public int Add(ClickHouseParameter item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));

            if (item.Collection != null)
            {
                var errorText = ReferenceEquals(item.Collection, this)
                    ? $"The parameter \"{item.ParameterName}\" already belongs to the collection. It can't be added to the same connection twice."
                    : $"The parameter \"{item.ParameterName}\" already belongs to a collection. It can't be added to different collections.";

                throw new ArgumentException(errorText, nameof(item));
            }

            if (_parameters.ContainsKey(item.Id))
                throw new ArgumentException($"A parameter with the name \"{item.ParameterName}\" already exists in the collection.", nameof(item));

            _parameters.Add(item.Id, item);
            var result = _parameterNames.Count;
            _parameterNames.Add(item.Id);
            item.Collection = this;

            return result;
        }

        /// <inheritdoc/>
        public override void Clear()
        {
            foreach (var parameter in _parameters.Values)
                parameter.Collection = null;

            _parameters.Clear();
            _parameterNames.Clear();
        }

        /// <inheritdoc/>
        public bool Contains(ClickHouseParameter item)
        {
            return item != null && _parameters.TryGetValue(item.Id, out var parameter) && ReferenceEquals(item, parameter);
        }

        /// <inheritdoc/>
        public void CopyTo(ClickHouseParameter[] array, int arrayIndex)
        {
            int i = arrayIndex;
            foreach (var key in _parameterNames)
                array[i++] = _parameters[key];
        }

        /// <inheritdoc/>
        public override bool Contains(object value)
        {
            if (!(value is ClickHouseParameter parameter))
                return false;

            return Contains(parameter);
        }

        /// <inheritdoc/>
        public override int IndexOf(object value)
        {
            if (!(value is ClickHouseParameter parameter))
                return -1;

            return IndexOf(parameter);
        }

        /// <inheritdoc/>
        public override void Insert(int index, object value)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            var parameter = (ClickHouseParameter) value;
            Insert(index, parameter);
        }

        /// <inheritdoc/>
        public bool Remove(ClickHouseParameter item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));

            if (!_parameters.TryGetValue(item.Id, out var existingParameter) || !ReferenceEquals(item, existingParameter))
                return false;

            var comparer = _parameters.Comparer;
            var name = item.Id;
            var index = _parameterNames.FindIndex(n => comparer.Equals(n, name));

            _parameterNames.RemoveAt(index);
            var result = _parameters.Remove(name);
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
                throw new ArgumentNullException(nameof(parameterName));

            var name = ClickHouseParameter.TrimParameterName(parameterName);
            if (!_parameters.Remove(name, out parameter))
                return false;

            parameter.Collection = null;
            var comparer = _parameters.Comparer;
            var index = _parameterNames.FindIndex(n => comparer.Equals(n, name));
            _parameterNames.RemoveAt(index);
            return true;
        }

        /// <inheritdoc/>
        public override void Remove(object value)
        {
            if (!(value is ClickHouseParameter parameter))
                return;

            Remove(parameter);
        }

        /// <inheritdoc/>
        public int IndexOf(ClickHouseParameter item)
        {
            if (item == null)
                return -1;

            if (!_parameters.TryGetValue(item.Id, out var existingParameter) || !ReferenceEquals(item, existingParameter))
                return -1;

            var comparer = _parameters.Comparer;
            var name = item.Id;
            var index = _parameterNames.FindIndex(n => comparer.Equals(n, name));

            return index;
        }

        /// <inheritdoc/>
        public void Insert(int index, ClickHouseParameter item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));

            if (item.Collection != null)
            {
                var errorText = ReferenceEquals(item.Collection, this)
                    ? $"The parameter \"{item.ParameterName}\" already belongs to the collection. It can't be added to the same connection twice."
                    : $"The parameter \"{item.ParameterName}\" already belongs to a collection. It can't be added to different collections.";

                throw new ArgumentException(errorText, nameof(item));
            }

            if (_parameters.ContainsKey(item.Id))
                throw new ArgumentException($"A parameter with the name \"{item.ParameterName}\" already exists in the collection.", nameof(item));

            _parameterNames.Insert(index, item.Id);
            _parameters.Add(item.Id, item);
            item.Collection = this;
        }

        /// <inheritdoc/>
        public override void RemoveAt(int index)
        {
            var name = _parameterNames[index];
            if (_parameters.Remove(name, out var parameter))
                parameter.Collection = null;

            _parameterNames.RemoveAt(index);
        }

        /// <inheritdoc/>
        public override void RemoveAt(string parameterName)
        {
            Remove(parameterName, out _);
        }

        /// <inheritdoc/>
        protected override void SetParameter(int index, DbParameter value)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            var name = _parameterNames[index];
            var parameter = (ClickHouseParameter) value;

            var comparer = _parameters.Comparer;
            if (comparer.Equals(name, parameter.Id))
            {
                var existingParameter = _parameters[name];
                if (!ReferenceEquals(parameter, existingParameter))
                {
                    if (parameter.Collection != null)
                    {
                        var errorText = ReferenceEquals(parameter.Collection, this)
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
                    var errorText = ReferenceEquals(parameter.Collection, this)
                        ? $"The parameter \"{parameter.ParameterName}\" already belongs to the collection. It can't be added to the same connection twice."
                        : $"The parameter \"{parameter.ParameterName}\" already belongs to a collection. It can't be added to different collections.";

                    throw new ArgumentException(errorText, nameof(value));
                }

                if (_parameters.ContainsKey(parameter.Id))
                    throw new ArgumentException($"A parameter with the name \"{parameter.ParameterName}\" already exists in the collection.", nameof(value));

                if(_parameters.Remove(name, out var existingParameter))
                    existingParameter.Collection = null;

                _parameters.Add(parameter.Id, parameter);
                _parameterNames[index] = parameter.Id;
                parameter.Collection = this;
            }
        }

        /// <inheritdoc/>
        protected override void SetParameter(string parameterName, DbParameter value)
        {
            if (parameterName == null)
                throw new ArgumentNullException(nameof(parameterName));
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            var name = ClickHouseParameter.TrimParameterName(parameterName);
            var parameter = (ClickHouseParameter) value;

            var comparer = _parameters.Comparer;
            if (_parameters.TryGetValue(name, out var existingParameter))
            {
                if (!ReferenceEquals(parameter, existingParameter))
                {
                    if (parameter.Collection != null)
                    {
                        var errorText = ReferenceEquals(parameter.Collection, this)
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
                        throw new ArgumentException($"A parameter with the name \"{parameter.ParameterName}\" already exists in the collection.", nameof(value));

                    var index = _parameterNames.FindIndex(n => comparer.Equals(n, name));
                    _parameterNames[index] = parameter.Id;

                    _parameters.Remove(name);
                    _parameters.Add(parameter.Id, parameter);
                }

                if (!ReferenceEquals(parameter, existingParameter))
                {
                    existingParameter.Collection = null;
                    parameter.Collection = this;
                }
            }
            else if (comparer.Equals(name, parameter.Id))
            {
                Add(parameter);
            }
            else
            {
                throw new ArgumentException(
                    $"A parameter with the name \"{parameterName}\" is not present in the collection. It can't be replaced with the parameter \"{parameter.ParameterName}\".",
                    nameof(parameterName));
            }
        }

        /// <inheritdoc/>
        public override int IndexOf(string parameterName)
        {
            if (parameterName == null)
                throw new ArgumentNullException(nameof(parameterName));

            var name = ClickHouseParameter.TrimParameterName(parameterName);
            var comparer = _parameters.Comparer;

            return _parameterNames.FindIndex(n => comparer.Equals(n, name));
        }

        /// <inheritdoc/>
        public override bool Contains(string value)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            var parameterName = ClickHouseParameter.TrimParameterName(value);
            return _parameters.ContainsKey(parameterName);
        }

        /// <inheritdoc/>
        public override void CopyTo(Array array, int index)
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));

            var i = index;
            foreach (var name in _parameterNames)
                array.SetValue(_parameters[name], i++);
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
            if (!TryGetValue(parameterName, out var parameter))
                throw new ArgumentException($"Parameter \"{parameterName}\" not found.", nameof(parameterName));

            return parameter;
        }

        /// <inheritdoc/>
        public override void AddRange(Array values)
        {
            if (values == null)
                throw new ArgumentNullException(nameof(values));

            foreach (var parameter in values.Cast<ClickHouseParameter>())
                Add(parameter);
        }

        /// <summary>
        /// Adds the specified parameters to the collection.
        /// </summary>
        /// <param name="parameters">The set of parameters that should be added to this collection.</param>
        /// <remarks>This operation is not atomic, it calls <see cref="Add(ClickHouseParameter)"/> for each parameter in <paramref name="parameters"/>.</remarks>
        public void AddRange(IEnumerable<ClickHouseParameter> parameters)
        {
            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            foreach (var parameter in parameters)
                Add(parameter);
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
                throw new ArgumentNullException(nameof(parameterName));

            var name = ClickHouseParameter.TrimParameterName(parameterName);
            return _parameters.TryGetValue(name, out parameter);
        }

        internal void OnParameterIdChanged(string originalId, ClickHouseParameter parameter)
        {
            Debug.Assert(ReferenceEquals(parameter.Collection, this));
            if (_parameters.Comparer.Equals(originalId, parameter.Id))
                return;

            SetParameter(originalId, parameter);
        }

        /// <inheritdoc/>
        public new ClickHouseParameter this[int index]
        {
            get => (ClickHouseParameter) base[index];
            set => base[index] = value;
        }

        /// <summary>Gets or sets the <see cref="ClickHouseParameter"/> with the specified name.</summary>
        /// <param name="parameterName">The name of the <see cref="ClickHouseParameter"/> in the collection.</param>
        /// <returns>The <see cref="ClickHouseParameter"/> with the specified name.</returns>
        public new ClickHouseParameter this[string parameterName]
        {
            get => (ClickHouseParameter) base[parameterName];
            set => base[parameterName] = value;
        }
    }
}
