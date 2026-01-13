#region License Apache 2.0
/* Copyright 2020-2021 Octonica
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

namespace Octonica.ClickHouseClient.Types
{
    /// <summary>
    /// The class represents a converter that can convert values between a ClickHouse's enum and <typeparamref name="TEnum"/>.
    /// The values of enums are matched by their codes (integers).
    /// </summary>
    /// <typeparam name="TEnum">The type of the enum.</typeparam>
    public sealed class ClickHouseEnumConverter<TEnum> : IClickHouseEnumConverter<TEnum>
        where TEnum : Enum
    {
        private readonly Dictionary<int, TEnum> _values;

        /// <summary>
        /// Initializes a new instance of the converter. <typeparamref name="TEnum"/>'s values are acquired via reflection.
        /// </summary>
        public ClickHouseEnumConverter()
        {
            Array enumValues = Enum.GetValues(typeof(TEnum));
            _values = new Dictionary<int, TEnum>(enumValues.Length);
            foreach (object? enumValue in enumValues)
            {
                int intValue = Convert.ToInt32(enumValue);
                _values[intValue] = (TEnum)enumValue!;
            }
        }

        T IClickHouseEnumConverter.Dispatch<T>(IClickHouseEnumConverterDispatcher<T> dispatcher)
        {
            return dispatcher.Dispatch(this);
        }

        /// <summary>
        /// Searches for a enum's value corresponding to a numeric value of the ClickHouse enum.
        /// </summary>
        /// <param name="value">The numeric value of the ClickHouse enum.</param>
        /// <param name="stringValue">The string value of the ClickHouse enum. This converter ignores the value of this parameter.</param>
        /// <param name="enumValue">When this method returns, contains the value of enum or the default value of <typeparamref name="TEnum"/> when returns <see langword="false"/>.</param>
        /// <returns><see langword="true"/> if there are <typeparamref name="TEnum"/>'s value corresponding to the specified ClickHouse enum; otherwise <see langword="false"/>.</returns>
        public bool TryMap(int value, string stringValue, out TEnum enumValue)
        {
            return _values.TryGetValue(value, out enumValue!);
        }
    }
}
