#region License Apache 2.0
/* Copyright 2019-2020 Octonica
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

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class Int16TableColumn : StructureTableColumn<short>
    {
        public Int16TableColumn(ReadOnlyMemory<short> buffer)
            : base(buffer)
        {
        }

        public override IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            if (typeof(T) == typeof(int))
            {
                return (IClickHouseTableColumn<T>)(object)new ReinterpretedTableColumn<short, int>(this, v => v);
            }

            return typeof(T) == typeof(long)
                ? (IClickHouseTableColumn<T>)(object)new ReinterpretedTableColumn<short, long>(this, v => v)
                : typeof(T) == typeof(int?)
                ? (IClickHouseTableColumn<T>)(object)new NullableStructTableColumn<int>(null, new ReinterpretedTableColumn<short, int>(this, v => v))
                : typeof(T) == typeof(long?)
                ? (IClickHouseTableColumn<T>)(object)new NullableStructTableColumn<long>(null, new ReinterpretedTableColumn<short, long>(this, v => v))
                : base.TryReinterpret<T>();
        }
    }
}
