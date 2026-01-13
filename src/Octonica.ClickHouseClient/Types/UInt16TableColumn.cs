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
    internal sealed class UInt16TableColumn : StructureTableColumn<ushort>
    {
        public UInt16TableColumn(ReadOnlyMemory<ushort> buffer)
            : base(buffer)
        {
        }

        public override IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            if (typeof(T) == typeof(int))
            {
                return (IClickHouseTableColumn<T>)(object)new ReinterpretedTableColumn<ushort, int>(this, v => v);
            }

            if (typeof(T) == typeof(uint))
            {
                return (IClickHouseTableColumn<T>)(object)new ReinterpretedTableColumn<ushort, uint>(this, v => v);
            }

            if (typeof(T) == typeof(long))
            {
                return (IClickHouseTableColumn<T>)(object)new ReinterpretedTableColumn<ushort, long>(this, v => v);
            }

            if (typeof(T) == typeof(ulong))
            {
                return (IClickHouseTableColumn<T>)(object)new ReinterpretedTableColumn<ushort, ulong>(this, v => v);
            }

            if (typeof(T) == typeof(int?))
            {
                return (IClickHouseTableColumn<T>)(object)new NullableStructTableColumn<int>(null, new ReinterpretedTableColumn<ushort, int>(this, v => v));
            }

            return typeof(T) == typeof(uint?)
                ? (IClickHouseTableColumn<T>)(object)new NullableStructTableColumn<uint>(null, new ReinterpretedTableColumn<ushort, uint>(this, v => v))
                : typeof(T) == typeof(long?)
                ? (IClickHouseTableColumn<T>)(object)new NullableStructTableColumn<long>(null, new ReinterpretedTableColumn<ushort, long>(this, v => v))
                : typeof(T) == typeof(ulong?)
                ? (IClickHouseTableColumn<T>)(object)new NullableStructTableColumn<ulong>(null, new ReinterpretedTableColumn<ushort, ulong>(this, v => v))
                : base.TryReinterpret<T>();
        }
    }
}
