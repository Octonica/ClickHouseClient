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
using System.Collections.Generic;

namespace Octonica.ClickHouseClient.Types
{
    public sealed class ClickHouseEnumConverter<TEnum> : IClickHouseEnumConverter<TEnum>
        where TEnum : Enum
    {
        private readonly Dictionary<int, TEnum> _values;

        public ClickHouseEnumConverter()
        {
            var enumValues = Enum.GetValues(typeof(TEnum));
            _values = new Dictionary<int, TEnum>(enumValues.Length);
            foreach (var enumValue in enumValues)
            {
                var intValue = Convert.ToInt32(enumValue);
                _values[intValue] = (TEnum) enumValue!;
            }
        }

        T IClickHouseEnumConverter.Dispatch<T>(IClickHouseEnumConverterDispatcher<T> dispatcher)
        {
            return dispatcher.Dispatch(this);
        }

        public bool TryMap(int value, string stringValue, out TEnum enumValue)
        {
            return _values.TryGetValue(value, out enumValue!);
        }
    }
}
