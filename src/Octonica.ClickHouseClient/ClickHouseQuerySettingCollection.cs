#region License Apache 2.0
/* Copyright 2026 Octonica
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

using Octonica.ClickHouseClient.Utils;
using System;

namespace Octonica.ClickHouseClient;

/// <summary>
/// Represents a collection of query settings associated with a <see cref="ClickHouseCommand"/>. This class cannot be inherited.
/// </summary>
public sealed class ClickHouseQuerySettingCollection : IndexedCollectionBase<string, ClickHouseQuerySetting>
{
    /// <summary>
    /// Initializes a new instance of <see cref="ClickHouseQuerySettingCollection"/> with the default capacity.
    /// </summary>
    public ClickHouseQuerySettingCollection()
        : base(StringComparer.Ordinal)
    {
    }

    /// <summary>
    /// Initializes a new instance of <see cref="ClickHouseQuerySettingCollection"/> with the specified capacity.
    /// </summary>
    /// <param name="capacity">The initial number of elements that the collection can contain.</param>
    public ClickHouseQuerySettingCollection(int capacity)
        : base(capacity, StringComparer.Ordinal)
    {
    }

    /// <inheritdoc/>
    protected sealed override string GetKey(ClickHouseQuerySetting item)
    {
        return item.Name;
    }

    /// <summary>
    /// Creates, adds to the collection and returns a new setting with the specified name.
    /// </summary>
    /// <param name="name">The name of the setting.</param>
    /// <returns>A new <see cref="ClickHouseQuerySetting"/> added to the collection.</returns>
    public ClickHouseQuerySetting Add(string name)
    {
        var setting = new ClickHouseQuerySetting(name);
        Add(setting);
        return setting;
    }

    /// <summary>
    /// Creates, adds to the collection and returns a new setting with the specified name and value.
    /// </summary>
    /// <param name="name">The name of the setting.</param>
    /// <param name="value">The value of the setting.</param>
    /// <returns>A new <see cref="ClickHouseQuerySetting"/> added to the collection.</returns>
    public ClickHouseQuerySetting Add(string name, string value)
    {
        return AddAndSetValue(name, s => s.SetValue(value));
    }

    /// <summary>
    /// Creates, adds to the collection and returns a new setting with the specified name and value.
    /// </summary>
    /// <param name="name">The name of the setting.</param>
    /// <param name="value">The value of the setting.</param>
    /// <returns>A new <see cref="ClickHouseQuerySetting"/> added to the collection.</returns>
    public ClickHouseQuerySetting Add(string name, bool value)
    {
        return AddAndSetValue(name, s => s.SetValue(value));
    }

    /// <summary>
    /// Creates, adds to the collection and returns a new setting with the specified name and value.
    /// </summary>
    /// <param name="name">The name of the setting.</param>
    /// <param name="value">The value of the setting.</param>
    /// <returns>A new <see cref="ClickHouseQuerySetting"/> added to the collection.</returns>
    public ClickHouseQuerySetting Add(string name, byte value)
    {
        return AddAndSetValue(name, s => s.SetValue(value));
    }

    /// <summary>
    /// Creates, adds to the collection and returns a new setting with the specified name and value.
    /// </summary>
    /// <param name="name">The name of the setting.</param>
    /// <param name="value">The value of the setting.</param>
    /// <returns>A new <see cref="ClickHouseQuerySetting"/> added to the collection.</returns>
    public ClickHouseQuerySetting Add(string name, sbyte value)
    {
        return AddAndSetValue(name, s => s.SetValue(value));
    }

    /// <summary>
    /// Creates, adds to the collection and returns a new setting with the specified name and value.
    /// </summary>
    /// <param name="name">The name of the setting.</param>
    /// <param name="value">The value of the setting.</param>
    /// <returns>A new <see cref="ClickHouseQuerySetting"/> added to the collection.</returns>
    public ClickHouseQuerySetting Add(string name, short value)
    {
        return AddAndSetValue(name, s => s.SetValue(value));
    }

    /// <summary>
    /// Creates, adds to the collection and returns a new setting with the specified name and value.
    /// </summary>
    /// <param name="name">The name of the setting.</param>
    /// <param name="value">The value of the setting.</param>
    /// <returns>A new <see cref="ClickHouseQuerySetting"/> added to the collection.</returns>
    public ClickHouseQuerySetting Add(string name, ushort value)
    {
        return AddAndSetValue(name, s => s.SetValue(value));
    }

    /// <summary>
    /// Creates, adds to the collection and returns a new setting with the specified name and value.
    /// </summary>
    /// <param name="name">The name of the setting.</param>
    /// <param name="value">The value of the setting.</param>
    /// <returns>A new <see cref="ClickHouseQuerySetting"/> added to the collection.</returns>
    public ClickHouseQuerySetting Add(string name, int value)
    {
        return AddAndSetValue(name, s => s.SetValue(value));
    }

    /// <summary>
    /// Creates, adds to the collection and returns a new setting with the specified name and value.
    /// </summary>
    /// <param name="name">The name of the setting.</param>
    /// <param name="value">The value of the setting.</param>
    /// <returns>A new <see cref="ClickHouseQuerySetting"/> added to the collection.</returns>
    public ClickHouseQuerySetting Add(string name, uint value)
    {
        return AddAndSetValue(name, s => s.SetValue(value));
    }

    /// <summary>
    /// Creates, adds to the collection and returns a new setting with the specified name and value.
    /// </summary>
    /// <param name="name">The name of the setting.</param>
    /// <param name="value">The value of the setting.</param>
    /// <returns>A new <see cref="ClickHouseQuerySetting"/> added to the collection.</returns>
    public ClickHouseQuerySetting Add(string name, long value)
    {
        return AddAndSetValue(name, s => s.SetValue(value));
    }

    /// <summary>
    /// Creates, adds to the collection and returns a new setting with the specified name and value.
    /// </summary>
    /// <param name="name">The name of the setting.</param>
    /// <param name="value">The value of the setting.</param>
    /// <returns>A new <see cref="ClickHouseQuerySetting"/> added to the collection.</returns>
    public ClickHouseQuerySetting Add(string name, ulong value)
    {
        return AddAndSetValue(name, s => s.SetValue(value));
    }

    /// <summary>
    /// Creates, adds to the collection and returns a new setting with the specified name and value.
    /// </summary>
    /// <param name="name">The name of the setting.</param>
    /// <param name="value">The value of the setting.</param>
    /// <returns>A new <see cref="ClickHouseQuerySetting"/> added to the collection.</returns>
    public ClickHouseQuerySetting Add(string name, float value)
    {
        return AddAndSetValue(name, s => s.SetValue(value));
    }

    /// <summary>
    /// Creates, adds to the collection and returns a new setting with the specified name and value.
    /// </summary>
    /// <param name="name">The name of the setting.</param>
    /// <param name="value">The value of the setting.</param>
    /// <returns>A new <see cref="ClickHouseQuerySetting"/> added to the collection.</returns>
    public ClickHouseQuerySetting Add(string name, double value)
    {
        return AddAndSetValue(name, s => s.SetValue(value));
    }

    /// <summary>
    /// Creates, adds to the collection and returns a new setting with the specified name and value.
    /// </summary>
    /// <param name="name">The name of the setting.</param>
    /// <param name="value">The value of the setting.</param>
    /// <returns>A new <see cref="ClickHouseQuerySetting"/> added to the collection.</returns>
    public ClickHouseQuerySetting Add(string name, decimal value)
    {
        return AddAndSetValue(name, s => s.SetValue(value));
    }

    private ClickHouseQuerySetting AddAndSetValue(string name, Action<ClickHouseQuerySetting> setValue)
    {
        var setting = new ClickHouseQuerySetting(name);
        setValue(setting);
        Add(setting);
        return setting;
    }
}
