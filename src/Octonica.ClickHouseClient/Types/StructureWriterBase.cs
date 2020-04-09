#region License Apache 2.0
/* Copyright 2019-2020 Octonica LLC
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
using Octonica.ClickHouseClient.Protocol;

namespace Octonica.ClickHouseClient.Types
{
    public abstract class StructureWriterBase<T> : IClickHouseColumnWriter
        where T : struct
    {
        private readonly IReadOnlyList<T> _rows;

        private int _position;

        protected int ElementSize { get; }

        public string ColumnName { get; }

        public string ColumnType { get; }

        protected StructureWriterBase(string columnName, string columnType, int elementSize, IReadOnlyList<T> rows)
        {
            ElementSize = elementSize;
            _rows = rows;
            ColumnName = columnName;
            ColumnType = columnType;
        }

        public virtual SequenceSize WriteNext(Span<byte> writeTo)
        {
            var elementsCount = Math.Min(_rows.Count - _position, writeTo.Length / ElementSize);

            for (int i = 0; i < elementsCount; i++, _position++)
            {
                WriteElement(writeTo.Slice(i * ElementSize), _rows[_position]);
            }

            return new SequenceSize(elementsCount * ElementSize, elementsCount);
        }

        protected abstract void WriteElement(Span<byte> writeTo, in T value);
    }
}
