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

#if NETCOREAPP3_1_OR_GREATER && !NET6_0_OR_GREATER

using System;

namespace Octonica.ClickHouseClient.Types
{
    partial class Date32TableColumn : IClickHouseTableColumn<DateTime>
    {
        public DateTime GetValue(int index)
        {
            var value = _buffer.Span[index];
            if (value == DefaultValue)
                return default;

            return DateTime.UnixEpoch.AddDays(value);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            if (typeof(T) == typeof(DateTime?))
                return (IClickHouseTableColumn<T>)(object)new NullableStructTableColumn<DateTime>(null, this);

            return null;
        }
    }
}

#endif