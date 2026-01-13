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

#if NET6_0_OR_GREATER

using System;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class DateOnlyTableColumn : StructureTableColumn<DateOnly>
    {
        public DateOnlyTableColumn(ReadOnlyMemory<DateOnly> buffer)
            : base(buffer)
        {
        }

        public override IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            return typeof(T) == typeof(DateTime)
                ? (IClickHouseTableColumn<T>)(object)new ReinterpretedTableColumn<DateOnly, DateTime>(this, dateOnly => dateOnly.ToDateTime(TimeOnly.MinValue))
                : typeof(T) == typeof(DateTime?)
                ? (IClickHouseTableColumn<T>)(object)new NullableStructTableColumn<DateTime>(null, new ReinterpretedTableColumn<DateOnly, DateTime>(this, dateOnly => dateOnly.ToDateTime(TimeOnly.MinValue)))
                : base.TryReinterpret<T>();
        }
    }
}

#endif
