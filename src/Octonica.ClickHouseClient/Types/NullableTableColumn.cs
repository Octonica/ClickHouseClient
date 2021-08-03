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
using System.Diagnostics;
using System.Linq;
using Octonica.ClickHouseClient.Exceptions;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class NullableTableColumn : IClickHouseTableColumn
    {
        private readonly BitArray? _nullFlags;
        private readonly IClickHouseTableColumn _baseColumn;

        public int RowCount => _baseColumn.RowCount;

        private NullableTableColumn(BitArray? nullFlags, IClickHouseTableColumn baseColumn)
        {
            _nullFlags = nullFlags;
            _baseColumn = baseColumn;
        }

        public bool IsNull(int index)
        {
            if (_nullFlags != null && _nullFlags[index])
                return true;

            return _baseColumn.IsNull(index);
        }

        public object GetValue(int index)
        {
            if (_nullFlags != null && _nullFlags[index])
                return DBNull.Value;

            return _baseColumn.GetValue(index);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            return TryMakeNullableColumn<T>(_nullFlags, _baseColumn);
        }

        public static IClickHouseTableColumn MakeNullableColumn(BitArray? nullFlags, IClickHouseTableColumn baseColumn)
        {
            var columnType = baseColumn.GetType();
            Type? recognizedElementType = null;
            foreach (var itf in columnType.GetInterfaces().Where(i => i.IsGenericType))
            {
                var typeDef = itf.GetGenericTypeDefinition();
                if (typeDef != typeof(IClickHouseTableColumn<>))
                    continue;

                if (recognizedElementType == null)
                {
                    recognizedElementType = itf.GenericTypeArguments[0];
                }
                else
                {
                    recognizedElementType = null;
                    break;
                }
            }

            IClickHouseTableColumn? result = null;
            if (recognizedElementType != null)
            {
                if (recognizedElementType.IsValueType && !(recognizedElementType.IsGenericType && recognizedElementType.GetGenericTypeDefinition() == typeof(Nullable<>)))
                    recognizedElementType = typeof(Nullable<>).MakeGenericType(recognizedElementType);

                result = TryMakeNullableColumn(recognizedElementType, nullFlags, baseColumn);
            }

            return result ?? new NullableTableColumn(nullFlags, baseColumn);
        }

        public static IClickHouseTableColumn<T>? TryMakeNullableColumn<T>(BitArray? nullFlags, IClickHouseTableColumn notNullableColumn)
        {
            return (IClickHouseTableColumn<T>?) TryMakeNullableColumn(typeof(T), nullFlags, notNullableColumn);
        }

        IClickHouseArrayTableColumn<T>? IClickHouseTableColumn.TryReinterpretAsArray<T>()
        {
            return TryMakeNullableArrayColumn<T>(this, _nullFlags, _baseColumn);
        }

        public static IClickHouseArrayTableColumn<T>? TryMakeNullableArrayColumn<T>(IClickHouseTableColumn reinterpretationRoot, BitArray? nullFlags, IClickHouseTableColumn notNullableColumn)
        {
            var notNullableArrayColumn = notNullableColumn as IClickHouseArrayTableColumn<T> ?? notNullableColumn.TryReinterpretAsArray<T>();
            if (notNullableArrayColumn == null)
                return null;

            return new NullableArrayTableColumn<T>(reinterpretationRoot, nullFlags, notNullableArrayColumn);
        }

        private static IClickHouseTableColumn? TryMakeNullableColumn(Type underlyingType, BitArray? nullFlags, IClickHouseTableColumn notNullableColumn)
        {
            Type dispatcherType;
            bool reinterpretAsNotNullable = false;
            if (underlyingType.IsValueType)
            {
                Type columnType;
                if (!underlyingType.IsGenericType || underlyingType.GetGenericTypeDefinition() != typeof(Nullable<>))
                {
                    reinterpretAsNotNullable = true;
                    columnType = underlyingType;
                }
                else
                {
                    columnType = Nullable.GetUnderlyingType(underlyingType) ?? underlyingType;
                }

                dispatcherType = typeof(NullableStructTableColumnDispatcher<>).MakeGenericType(columnType);
            }
            else if (underlyingType.IsClass)
            {
                dispatcherType = typeof(NullableObjTableColumnDispatcher<>).MakeGenericType(underlyingType);
            }
            else
            {
                return null;
            }

            var dispatcher = (INullableColumnDispatcher) Activator.CreateInstance(dispatcherType)!;

            return dispatcher.Dispatch(nullFlags, notNullableColumn, reinterpretAsNotNullable);
        }

        private interface INullableColumnDispatcher
        {
            IClickHouseTableColumn? Dispatch(BitArray? nullFlags, IClickHouseTableColumn notNullableColumn, bool reinterpretAsNotNullable);
        }

        private sealed class NullableStructTableColumnDispatcher<TStruct> : INullableColumnDispatcher
            where TStruct : struct
        {
            public IClickHouseTableColumn? Dispatch(BitArray? nullFlags, IClickHouseTableColumn notNullableColumn, bool reinterpretAsNotNullable)
            {
                var reinterpretedColumn = notNullableColumn as IClickHouseTableColumn<TStruct> ?? notNullableColumn.TryReinterpret<TStruct>();
                if (reinterpretedColumn == null)
                    return null;

                var result = new NullableStructTableColumn<TStruct>(nullFlags, reinterpretedColumn);
                if (!reinterpretAsNotNullable)
                    return result;

                return result.AsNotNullable();
            }
        }

        private sealed class NullableObjTableColumnDispatcher<TObj> : INullableColumnDispatcher
            where TObj : class
        {
            public IClickHouseTableColumn? Dispatch(BitArray? nullFlags, IClickHouseTableColumn notNullableColumn, bool reinterpretAsNotNullable)
            {
                Debug.Assert(!reinterpretAsNotNullable);

                var reinterpretedColumn = notNullableColumn as IClickHouseTableColumn<TObj> ?? notNullableColumn.TryReinterpret<TObj>();
                if (reinterpretedColumn == null)
                    return null;

                if (nullFlags == null)
                    return reinterpretedColumn;
                
                return new NullableObjTableColumn<TObj>(nullFlags, reinterpretedColumn);
            }
        }
    }

    internal sealed class NullableStructTableColumn<TStruct> : IClickHouseTableColumn<TStruct?>
        where TStruct : struct
    {
        private readonly BitArray? _nullFlags;
        private readonly IClickHouseTableColumn<TStruct> _baseColumn;

        public int RowCount => _baseColumn.RowCount;

        public NullableStructTableColumn(BitArray? nullFlags, IClickHouseTableColumn<TStruct> baseColumn)
        {
            _nullFlags = nullFlags;
            _baseColumn = baseColumn;
        }

        public bool IsNull(int index)
        {
            if (_nullFlags != null && _nullFlags[index])
                return true;

            return _baseColumn.IsNull(index);
        }

        public TStruct? GetValue(int index)
        {
            if (_nullFlags != null && _nullFlags[index])
                return null;

            return _baseColumn.GetValue(index);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            return NullableTableColumn.TryMakeNullableColumn<T>(_nullFlags, _baseColumn);
        }

        IClickHouseArrayTableColumn<T>? IClickHouseTableColumn.TryReinterpretAsArray<T>()
        {
            return NullableTableColumn.TryMakeNullableArrayColumn<T>(this, _nullFlags, _baseColumn);
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            return (object?) GetValue(index) ?? DBNull.Value;
        }

        public IClickHouseTableColumn<TStruct> AsNotNullable()
        {
            if (_nullFlags == null)
                return _baseColumn;

            return new NullableStructTableColumnNotNullableAdapter<TStruct>(_nullFlags, _baseColumn);
        }
    }

    internal sealed class NullableStructTableColumnNotNullableAdapter<TStruct> : IClickHouseTableColumn<TStruct>
        where TStruct : struct
    {
        private readonly BitArray _nullFlags;
        private readonly IClickHouseTableColumn<TStruct> _baseColumn;

        public int RowCount => _baseColumn.RowCount;

        public NullableStructTableColumnNotNullableAdapter(BitArray nullFlags, IClickHouseTableColumn<TStruct> baseColumn)
        {
            _nullFlags = nullFlags;
            _baseColumn = baseColumn;
        }

        public bool IsNull(int index)
        {
            return _nullFlags[index] || _baseColumn.IsNull(index);
        }

        public TStruct GetValue(int index)
        {
            if (_nullFlags[index])
                throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, $"Can't convert NULL to \"{typeof(TStruct)}\".");

            return _baseColumn.GetValue(index);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            return NullableTableColumn.TryMakeNullableColumn<T>(_nullFlags, _baseColumn);
        }

        IClickHouseArrayTableColumn<T>? IClickHouseTableColumn.TryReinterpretAsArray<T>()
        {
            return NullableTableColumn.TryMakeNullableArrayColumn<T>(this, _nullFlags, _baseColumn);
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            if (_nullFlags != null && _nullFlags[index])
                return DBNull.Value;

            return _baseColumn.GetValue(index);
        }
    }

    internal sealed class NullableObjTableColumn<TObj> : IClickHouseTableColumn<TObj?>
        where TObj : class
    {
        private readonly BitArray _nullFlags;
        private readonly IClickHouseTableColumn<TObj> _baseColumn;

        public int RowCount => _baseColumn.RowCount;

        public NullableObjTableColumn(BitArray nullFlags, IClickHouseTableColumn<TObj> baseColumn)
        {
            _nullFlags = nullFlags;
            _baseColumn = baseColumn;
        }

        public bool IsNull(int index)
        {
            return _nullFlags[index] || _baseColumn.IsNull(index);
        }

        public TObj? GetValue(int index)
        {
            if (_nullFlags[index])
                return null;

            return _baseColumn.GetValue(index);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            return NullableTableColumn.TryMakeNullableColumn<T>(_nullFlags, _baseColumn);
        }

        IClickHouseArrayTableColumn<T>? IClickHouseTableColumn.TryReinterpretAsArray<T>()
        {
            return NullableTableColumn.TryMakeNullableArrayColumn<T>(this, _nullFlags, _baseColumn);
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            return (object?) GetValue(index) ?? DBNull.Value;
        }
    }

    internal sealed class NullableArrayTableColumn<TElement> : IClickHouseArrayTableColumn<TElement>
    {
        private readonly IClickHouseTableColumn _reinterpretationRoot;
        private readonly BitArray? _nullFlags;
        private readonly IClickHouseArrayTableColumn<TElement> _arrayColumn;

        public int RowCount => throw new NotImplementedException();

        public NullableArrayTableColumn(IClickHouseTableColumn reinterpretationRoot, BitArray? nullFlags, IClickHouseArrayTableColumn<TElement> arrayColumn)
        {
            _reinterpretationRoot = reinterpretationRoot;
            _nullFlags = nullFlags;
            _arrayColumn = arrayColumn;
        }

        public object GetValue(int index)
        {
            if (_nullFlags != null && _nullFlags[index])
                return DBNull.Value;

            return _arrayColumn.GetValue(index);
        }

        public bool IsNull(int index)
        {
            return (_nullFlags != null && _nullFlags[index]) || _arrayColumn.IsNull(index);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            return _reinterpretationRoot as IClickHouseTableColumn<T> ?? _reinterpretationRoot.TryReinterpret<T>();
        }

        IClickHouseArrayTableColumn<T>? IClickHouseTableColumn.TryReinterpretAsArray<T>()
        {
            return _reinterpretationRoot as IClickHouseArrayTableColumn<T> ?? _reinterpretationRoot.TryReinterpretAsArray<T>();
        }

        public int CopyTo(int index, Span<TElement> buffer, int dataOffset)
        {
            if (_nullFlags != null && _nullFlags[index])
                throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Can't copy NULL value to the buffer.");

            return _arrayColumn.CopyTo(index, buffer, dataOffset);
        }
    }
}
