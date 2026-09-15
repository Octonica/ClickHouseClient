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

using System;
using System.Globalization;

namespace Octonica.ClickHouseClient;

/// <summary>
/// Represents a ClickHouse setting sent with a query over the native protocol.
/// </summary>
/// <remarks>
/// The original value assigned through <see cref="SetValue(string)"/> (or an overload) is available via
/// <see cref="Value"/>. The client formats that value as an unquoted string for the server's
/// <c>parseFromString</c> (numbers without quotes, strings without SQL quotes, maps as <c>{'k':'v'}</c>).
/// </remarks>
public sealed class ClickHouseQuerySetting
{
    /// <summary>
    /// Gets the name of the setting.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the original value assigned to the setting.
    /// </summary>
    /// <returns>The value passed to <see cref="SetValue(string)"/> or an overload. The default is <see langword="null"/> until a value is set.</returns>
    public object? Value { get; private set; }

    /// <summary>
    /// Gets or sets a value indicating whether the setting is important.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> if an unknown setting name should be treated as an error by the server;
    /// otherwise <see langword="false"/>. The default is <see langword="true"/>.
    /// </returns>
    public bool Important { get; set; } = true;

    /// <summary>
    /// Gets or sets a value indicating whether the setting is a user-defined custom setting.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> if the setting is custom; otherwise <see langword="false"/>. The default is <see langword="false"/>.
    /// </returns>
    public bool Custom { get; set; }

    /// <summary>
    /// The value formatted for the native protocol (the text consumed by the server's <c>parseFromString</c>).
    /// </summary>
    internal string? FormattedValue { get; private set; }

    /// <summary>
    /// Initializes a new instance of <see cref="ClickHouseQuerySetting"/> with the specified name.
    /// </summary>
    /// <param name="name">The name of the setting. Must be a non-empty string.</param>
    /// <exception cref="ArgumentException"><paramref name="name"/> is <see langword="null"/> or empty.</exception>
    public ClickHouseQuerySetting(string name)
    {
        if (string.IsNullOrEmpty(name))
            throw new ArgumentException("The name of a query setting must be a non-empty string.", nameof(name));

        Name = name;
    }

    /// <summary>
    /// Sets the value of the setting to the specified string. The string is sent as-is, without extra quotes.
    /// </summary>
    /// <param name="value">The value of the setting.</param>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is <see langword="null"/>.</exception>
    public void SetValue(string value)
    {
        if (value == null)
            throw new ArgumentNullException(nameof(value));

        SetValueInternal(value, value);
    }

    /// <summary>
    /// Sets the value of the setting to the specified boolean. The value is sent as <c>1</c> or <c>0</c>.
    /// </summary>
    /// <param name="value">The value of the setting.</param>
    public void SetValue(bool value)
    {
        SetValueInternal(value, value ? "1" : "0");
    }

    /// <summary>
    /// Sets the value of the setting to the specified 8-bit unsigned integer.
    /// </summary>
    /// <param name="value">The value of the setting.</param>
    public void SetValue(byte value)
    {
        SetNumberValue(value);
    }

    /// <summary>
    /// Sets the value of the setting to the specified 8-bit signed integer.
    /// </summary>
    /// <param name="value">The value of the setting.</param>
    public void SetValue(sbyte value)
    {
        SetNumberValue(value);
    }

    /// <summary>
    /// Sets the value of the setting to the specified 16-bit signed integer.
    /// </summary>
    /// <param name="value">The value of the setting.</param>
    public void SetValue(short value)
    {
        SetNumberValue(value);
    }

    /// <summary>
    /// Sets the value of the setting to the specified 16-bit unsigned integer.
    /// </summary>
    /// <param name="value">The value of the setting.</param>
    public void SetValue(ushort value)
    {
        SetNumberValue(value);
    }

    /// <summary>
    /// Sets the value of the setting to the specified 32-bit signed integer.
    /// </summary>
    /// <param name="value">The value of the setting.</param>
    public void SetValue(int value)
    {
        SetNumberValue(value);
    }

    /// <summary>
    /// Sets the value of the setting to the specified 32-bit unsigned integer.
    /// </summary>
    /// <param name="value">The value of the setting.</param>
    public void SetValue(uint value)
    {
        SetNumberValue(value);
    }

    /// <summary>
    /// Sets the value of the setting to the specified 64-bit signed integer.
    /// </summary>
    /// <param name="value">The value of the setting.</param>
    public void SetValue(long value)
    {
        SetNumberValue(value);
    }

    /// <summary>
    /// Sets the value of the setting to the specified 64-bit unsigned integer.
    /// </summary>
    /// <param name="value">The value of the setting.</param>
    public void SetValue(ulong value)
    {
        SetNumberValue(value);
    }

    /// <summary>
    /// Sets the value of the setting to the specified single-precision floating-point number.
    /// </summary>
    /// <param name="value">The value of the setting.</param>
    public void SetValue(float value)
    {
        SetNumberValue(value);
    }

    /// <summary>
    /// Sets the value of the setting to the specified double-precision floating-point number.
    /// </summary>
    /// <param name="value">The value of the setting.</param>
    public void SetValue(double value)
    {
        SetNumberValue(value);
    }

    /// <summary>
    /// Sets the value of the setting to the specified decimal number.
    /// </summary>
    /// <param name="value">The value of the setting.</param>
    public void SetValue(decimal value)
    {
        SetNumberValue(value);
    }

    internal ClickHouseServerSettingFlags GetFlags()
    {
        var flags = ClickHouseServerSettingFlags.None;
        if (Important)
            flags |= ClickHouseServerSettingFlags.Important;
        if (Custom)
            flags |= ClickHouseServerSettingFlags.Custom;

        return flags;
    }

    private void SetNumberValue<T>(T value)
        where T : struct, IFormattable
    {
        SetValueInternal(value, value.ToString(null, CultureInfo.InvariantCulture));
    }

    private void SetValueInternal(object value, string formattedValue)
    {
        Value = value;
        FormattedValue = formattedValue;
    }
}
