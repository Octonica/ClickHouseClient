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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient.Types
{
    internal abstract class TupleTableColumnBase : IClickHouseTableColumn
    {
        public int RowCount { get; }

        protected TupleTableColumnBase(int rowCount)
        {
            RowCount = rowCount;
        }

        public abstract IEnumerable<IClickHouseTableColumn> GetColumns();

        protected abstract object GetTupleValue(int index);

        public bool IsNull(int index)
        {
            return false;
        }

        public object GetValue(int index)
        {
            return GetTupleValue(index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void CheckIndex(int index)
        {
            if (index < 0 || index > RowCount)
                throw new ArgumentOutOfRangeException(nameof(index));
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            var columns = GetColumns();
            var columnList = columns as IReadOnlyList<IClickHouseTableColumn> ?? columns.ToList();

            return (IClickHouseTableColumn<T>?) TryMakeTupleColumn(typeof(T), RowCount, columnList);
        }

        public static TupleTableColumnBase MakeTupleColumn(int rowCount, IReadOnlyList<IClickHouseTableColumn> columns)
        {
            var columnElementTypes = new Type[columns.Count];

            for (var i = 0; i < columns.Count; i++)
            {
                var column = columns[i];
                var columnType = column.GetType();
                Type? elementType = null;
                foreach (var itf in columnType.GetInterfaces().Where(itf => itf.IsGenericType))
                {
                    var itfDef = itf.GetGenericTypeDefinition();
                    if (itfDef != typeof(IClickHouseTableColumn<>))
                        continue;

                    if (elementType == null)
                        elementType = itf.GetGenericArguments()[0];
                    else
                    {
                        elementType = null;
                        break;
                    }
                }

                columnElementTypes[i] = elementType ?? typeof(object);
            }

            var tupleType = TupleTypeInfo.MakeTupleType(columnElementTypes);
            var tupleColumn = TryMakeTupleColumn(tupleType, rowCount, columns);

            if (tupleColumn == null)
                throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. The column of the required type can't be created.");

            return tupleColumn;
        }

        private static TupleTableColumnBase? TryMakeTupleColumn(Type type, int rowCount, IReadOnlyList<IClickHouseTableColumn> columns)
        {
            if (columns == null)
                throw new ArgumentNullException(nameof(columns));
            if (!type.IsGenericType)
                return null;

            var typeDef = type.GetGenericTypeDefinition();
            Type? reinterpreterTypeDef = null;
            switch (columns.Count)
            {
                case 1:
                    if (typeDef == typeof(Tuple<>))
                        reinterpreterTypeDef = typeof(TupleTableColumn<>.Reinterpreter);
                    else if (typeDef == typeof(ValueTuple<>))
                        reinterpreterTypeDef = typeof(ValueTupleTableColumn<>.Reinterpreter);
                    break;
                case 2:
                    if (typeDef == typeof(Tuple<,>))
                        reinterpreterTypeDef = typeof(TupleTableColumn<,>.Reinterpreter);
                    else if (typeDef == typeof(ValueTuple<,>))
                        reinterpreterTypeDef = typeof(ValueTupleTableColumn<,>.Reinterpreter);
                    else if (typeDef == typeof(KeyValuePair<,>))
                        reinterpreterTypeDef = typeof(KeyValuePairTableColumn<,>.Reinterpreter);
                    break;
                case 3:
                    if (typeDef == typeof(Tuple<,,>))
                        reinterpreterTypeDef = typeof(TupleTableColumn<,,>.Reinterpreter);
                    else if (typeDef == typeof(ValueTuple<,,>))
                        reinterpreterTypeDef = typeof(ValueTupleTableColumn<,,>.Reinterpreter);
                    break;
                case 4:
                    if (typeDef == typeof(Tuple<,,,>))
                        reinterpreterTypeDef = typeof(TupleTableColumn<,,,>.Reinterpreter);
                    else if (typeDef == typeof(ValueTuple<,,,>))
                        reinterpreterTypeDef = typeof(ValueTupleTableColumn<,,,>.Reinterpreter);
                    break;
                case 5:
                    if (typeDef == typeof(Tuple<,,,,>))
                        reinterpreterTypeDef = typeof(TupleTableColumn<,,,,>.Reinterpreter);
                    else if (typeDef == typeof(ValueTuple<,,,,>))
                        reinterpreterTypeDef = typeof(ValueTupleTableColumn<,,,,>.Reinterpreter);
                    break;
                case 6:
                    if (typeDef == typeof(Tuple<,,,,,>))
                        reinterpreterTypeDef = typeof(TupleTableColumn<,,,,,>.Reinterpreter);
                    else if (typeDef == typeof(ValueTuple<,,,,,>))
                        reinterpreterTypeDef = typeof(ValueTupleTableColumn<,,,,,>.Reinterpreter);
                    break;
                case 7:
                    if (typeDef == typeof(Tuple<,,,,,,>))
                        reinterpreterTypeDef = typeof(TupleTableColumn<,,,,,,>.Reinterpreter);
                    else if (typeDef == typeof(ValueTuple<,,,,,,>))
                        reinterpreterTypeDef = typeof(ValueTupleTableColumn<,,,,,,>.Reinterpreter);
                    break;
                default:
                    if (columns.Count < 8)
                        break;

                    if (typeDef == typeof(Tuple<,,,,,,,>))
                        reinterpreterTypeDef = typeof(TupleTableColumn<,,,,,,,,>.Reinterpreter);
                    else if (typeDef == typeof(ValueTuple<,,,,,,,>))
                        reinterpreterTypeDef = typeof(ValueTupleTableColumn<,,,,,,,,>.Reinterpreter);
                    else
                        break;

                    var tuple8Args = type.GetGenericArguments();
                    var extraTupleType = tuple8Args[^1];
                    var extraColumn = TryMakeTupleColumn(extraTupleType, rowCount, columns.Slice(7));
                    if (extraColumn == null)
                        return null;

                    var extraColumnType = extraColumn.GetType();
                    if (!typeof(IClickHouseTableColumn<>).MakeGenericType(extraTupleType).IsAssignableFrom(extraColumnType))
                        return null;

                    var tuple8ColumnTypeArgs = new Type[9];
                    tuple8Args.CopyTo(tuple8ColumnTypeArgs, 0);
                    tuple8ColumnTypeArgs[^1] = extraColumnType;

                    var tuple8ColumnInterpreter = (ReinterpreterBase) Activator.CreateInstance(reinterpreterTypeDef.MakeGenericType(tuple8ColumnTypeArgs))!;

                    var tuple8Columns = new List<IClickHouseTableColumn>(8);
                    tuple8Columns.AddRange(columns.Take(7));
                    tuple8Columns.Add(extraColumn);

                    return tuple8ColumnInterpreter.TryReinterpret(rowCount, tuple8Columns);
            }

            if (reinterpreterTypeDef == null)
                return null;

            var tupleArgs = type.GetGenericArguments();
            var reinterpreterType = reinterpreterTypeDef.MakeGenericType(tupleArgs);
            var reinterpreter = (ReinterpreterBase) Activator.CreateInstance(reinterpreterType)!;

            return reinterpreter.TryReinterpret(rowCount, columns);
        }

        internal abstract class ReinterpreterBase
        {
            public abstract TupleTableColumnBase? TryReinterpret(int rowCount, IReadOnlyList<IClickHouseTableColumn> columns);

            protected static IClickHouseTableColumn<T>? TryReinterpret<T>(IClickHouseTableColumn column)
            {
                var result = column as IClickHouseTableColumn<T> ?? column.TryReinterpret<T>();
                if (result == null && typeof(T) == typeof(object))
                    return (IClickHouseTableColumn<T>) (object) new ObjectColumnAdapter(column);

                return result;
            }
        }
    }

    internal sealed class TupleTableColumn<T1> : TupleTableColumnBase, IClickHouseTableColumn<Tuple<T1>>
    {
        private readonly IClickHouseTableColumn<T1> _column1;

        private TupleTableColumn(int rowCount, IClickHouseTableColumn<T1> column1)
            : base(rowCount)
        {
            _column1 = column1;
        }

        public new Tuple<T1> GetValue(int index)
        {
            CheckIndex(index);

            return new Tuple<T1>(_column1.GetValue(index));
        }

        protected override object GetTupleValue(int index)
        {
            return GetValue(index);
        }

        public override IEnumerable<IClickHouseTableColumn> GetColumns()
        {
            yield return _column1;
        }

        internal class Reinterpreter : ReinterpreterBase
        {
            public override TupleTableColumnBase? TryReinterpret(int rowCount, IReadOnlyList<IClickHouseTableColumn> columns)
            {
                Debug.Assert(columns.Count == 1);

                var column1 = TryReinterpret<T1>(columns[0]);
                if (column1 == null)
                    return null;

                return new TupleTableColumn<T1>(rowCount, column1);
            }
        }
    }

    internal sealed class TupleTableColumn<T1, T2> : TupleTableColumnBase, IClickHouseTableColumn<Tuple<T1, T2>>
    {
        private readonly IClickHouseTableColumn<T1> _column1;
        private readonly IClickHouseTableColumn<T2> _column2;

        private TupleTableColumn(int rowCount, IClickHouseTableColumn<T1> column1, IClickHouseTableColumn<T2> column2)
            : base(rowCount)
        {
            _column1 = column1;
            _column2 = column2;
        }

        public new Tuple<T1, T2> GetValue(int index)
        {
            CheckIndex(index);

            return new Tuple<T1, T2>(_column1.GetValue(index), _column2.GetValue(index));
        }

        protected override object GetTupleValue(int index)
        {
            return GetValue(index);
        }

        public override IEnumerable<IClickHouseTableColumn> GetColumns()
        {
            yield return _column1;
            yield return _column2;
        }

        internal class Reinterpreter : ReinterpreterBase
        {
            public override TupleTableColumnBase? TryReinterpret(int rowCount, IReadOnlyList<IClickHouseTableColumn> columns)
            {
                Debug.Assert(columns.Count == 2);

                var column1 = TryReinterpret<T1>(columns[0]);
                if (column1 == null)
                    return null;

                var column2 = TryReinterpret<T2>(columns[1]);
                if (column2 == null)
                    return null;

                return new TupleTableColumn<T1, T2>(rowCount, column1, column2);
            }
        }
    }

    internal sealed class TupleTableColumn<T1, T2, T3> : TupleTableColumnBase, IClickHouseTableColumn<Tuple<T1, T2, T3>>
    {
        private readonly IClickHouseTableColumn<T1> _column1;
        private readonly IClickHouseTableColumn<T2> _column2;
        private readonly IClickHouseTableColumn<T3> _column3;

        private TupleTableColumn(int rowCount, IClickHouseTableColumn<T1> column1, IClickHouseTableColumn<T2> column2, IClickHouseTableColumn<T3> column3)
            : base(rowCount)
        {
            _column1 = column1;
            _column2 = column2;
            _column3 = column3;
        }

        public new Tuple<T1, T2, T3> GetValue(int index)
        {
            CheckIndex(index);

            return new Tuple<T1, T2, T3>(_column1.GetValue(index), _column2.GetValue(index), _column3.GetValue(index));
        }

        protected override object GetTupleValue(int index)
        {
            return GetValue(index);
        }

        public override IEnumerable<IClickHouseTableColumn> GetColumns()
        {
            yield return _column1;
            yield return _column2;
            yield return _column3;
        }

        internal class Reinterpreter : ReinterpreterBase
        {
            public override TupleTableColumnBase? TryReinterpret(int rowCount, IReadOnlyList<IClickHouseTableColumn> columns)
            {
                Debug.Assert(columns.Count == 3);

                var column1 = TryReinterpret<T1>(columns[0]);
                if (column1 == null)
                    return null;

                var column2 = TryReinterpret<T2>(columns[1]);
                if (column2 == null)
                    return null;

                var column3 = TryReinterpret<T3>(columns[2]);
                if (column3 == null)
                    return null;

                return new TupleTableColumn<T1, T2, T3>(rowCount, column1, column2, column3);
            }
        }
    }

    internal sealed class TupleTableColumn<T1, T2, T3, T4> : TupleTableColumnBase, IClickHouseTableColumn<Tuple<T1, T2, T3, T4>>
    {
        private readonly IClickHouseTableColumn<T1> _column1;
        private readonly IClickHouseTableColumn<T2> _column2;
        private readonly IClickHouseTableColumn<T3> _column3;
        private readonly IClickHouseTableColumn<T4> _column4;

        private TupleTableColumn(int rowCount, IClickHouseTableColumn<T1> column1, IClickHouseTableColumn<T2> column2, IClickHouseTableColumn<T3> column3, IClickHouseTableColumn<T4> column4)
            : base(rowCount)
        {
            _column1 = column1;
            _column2 = column2;
            _column3 = column3;
            _column4 = column4;
        }

        public new Tuple<T1, T2, T3, T4> GetValue(int index)
        {
            CheckIndex(index);

            return new Tuple<T1, T2, T3, T4>(_column1.GetValue(index), _column2.GetValue(index), _column3.GetValue(index), _column4.GetValue(index));
        }

        protected override object GetTupleValue(int index)
        {
            return GetValue(index);
        }

        public override IEnumerable<IClickHouseTableColumn> GetColumns()
        {
            yield return _column1;
            yield return _column2;
            yield return _column3;
            yield return _column4;
        }

        internal class Reinterpreter : ReinterpreterBase
        {
            public override TupleTableColumnBase? TryReinterpret(int rowCount, IReadOnlyList<IClickHouseTableColumn> columns)
            {
                Debug.Assert(columns.Count == 4);

                var column1 = TryReinterpret<T1>(columns[0]);
                if (column1 == null)
                    return null;

                var column2 = TryReinterpret<T2>(columns[1]);
                if (column2 == null)
                    return null;

                var column3 = TryReinterpret<T3>(columns[2]);
                if (column3 == null)
                    return null;

                var column4 = TryReinterpret<T4>(columns[3]);
                if (column4 == null)
                    return null;

                return new TupleTableColumn<T1, T2, T3, T4>(rowCount, column1, column2, column3, column4);
            }
        }
    }

    internal sealed class TupleTableColumn<T1, T2, T3, T4, T5> : TupleTableColumnBase, IClickHouseTableColumn<Tuple<T1, T2, T3, T4, T5>>
    {
        private readonly IClickHouseTableColumn<T1> _column1;
        private readonly IClickHouseTableColumn<T2> _column2;
        private readonly IClickHouseTableColumn<T3> _column3;
        private readonly IClickHouseTableColumn<T4> _column4;
        private readonly IClickHouseTableColumn<T5> _column5;

        private TupleTableColumn(
            int rowCount,
            IClickHouseTableColumn<T1> column1,
            IClickHouseTableColumn<T2> column2,
            IClickHouseTableColumn<T3> column3,
            IClickHouseTableColumn<T4> column4,
            IClickHouseTableColumn<T5> column5)
            : base(rowCount)
        {
            _column1 = column1;
            _column2 = column2;
            _column3 = column3;
            _column4 = column4;
            _column5 = column5;
        }

        public new Tuple<T1, T2, T3, T4, T5> GetValue(int index)
        {
            CheckIndex(index);

            return new Tuple<T1, T2, T3, T4, T5>(_column1.GetValue(index), _column2.GetValue(index), _column3.GetValue(index), _column4.GetValue(index), _column5.GetValue(index));
        }

        protected override object GetTupleValue(int index)
        {
            return GetValue(index);
        }

        public override IEnumerable<IClickHouseTableColumn> GetColumns()
        {
            yield return _column1;
            yield return _column2;
            yield return _column3;
            yield return _column4;
            yield return _column5;
        }

        internal class Reinterpreter : ReinterpreterBase
        {
            public override TupleTableColumnBase? TryReinterpret(int rowCount, IReadOnlyList<IClickHouseTableColumn> columns)
            {
                Debug.Assert(columns.Count == 5);

                var column1 = TryReinterpret<T1>(columns[0]);
                if (column1 == null)
                    return null;

                var column2 = TryReinterpret<T2>(columns[1]);
                if (column2 == null)
                    return null;

                var column3 = TryReinterpret<T3>(columns[2]);
                if (column3 == null)
                    return null;

                var column4 = TryReinterpret<T4>(columns[3]);
                if (column4 == null)
                    return null;

                var column5 = TryReinterpret<T5>(columns[4]);
                if (column5 == null)
                    return null;

                return new TupleTableColumn<T1, T2, T3, T4, T5>(rowCount, column1, column2, column3, column4, column5);
            }
        }
    }

    internal sealed class TupleTableColumn<T1, T2, T3, T4, T5, T6> : TupleTableColumnBase, IClickHouseTableColumn<Tuple<T1, T2, T3, T4, T5, T6>>
    {
        private readonly IClickHouseTableColumn<T1> _column1;
        private readonly IClickHouseTableColumn<T2> _column2;
        private readonly IClickHouseTableColumn<T3> _column3;
        private readonly IClickHouseTableColumn<T4> _column4;
        private readonly IClickHouseTableColumn<T5> _column5;
        private readonly IClickHouseTableColumn<T6> _column6;

        private TupleTableColumn(
            int rowCount,
            IClickHouseTableColumn<T1> column1,
            IClickHouseTableColumn<T2> column2,
            IClickHouseTableColumn<T3> column3,
            IClickHouseTableColumn<T4> column4,
            IClickHouseTableColumn<T5> column5,
            IClickHouseTableColumn<T6> column6)
            : base(rowCount)
        {
            _column1 = column1;
            _column2 = column2;
            _column3 = column3;
            _column4 = column4;
            _column5 = column5;
            _column6 = column6;
        }

        public new Tuple<T1, T2, T3, T4, T5, T6> GetValue(int index)
        {
            CheckIndex(index);

            return new Tuple<T1, T2, T3, T4, T5, T6>(
                _column1.GetValue(index),
                _column2.GetValue(index),
                _column3.GetValue(index),
                _column4.GetValue(index),
                _column5.GetValue(index),
                _column6.GetValue(index));
        }

        protected override object GetTupleValue(int index)
        {
            return GetValue(index);
        }

        public override IEnumerable<IClickHouseTableColumn> GetColumns()
        {
            yield return _column1;
            yield return _column2;
            yield return _column3;
            yield return _column4;
            yield return _column5;
            yield return _column6;
        }

        internal class Reinterpreter : ReinterpreterBase
        {
            public override TupleTableColumnBase? TryReinterpret(int rowCount, IReadOnlyList<IClickHouseTableColumn> columns)
            {
                Debug.Assert(columns.Count == 6);

                var column1 = TryReinterpret<T1>(columns[0]);
                if (column1 == null)
                    return null;

                var column2 = TryReinterpret<T2>(columns[1]);
                if (column2 == null)
                    return null;

                var column3 = TryReinterpret<T3>(columns[2]);
                if (column3 == null)
                    return null;

                var column4 = TryReinterpret<T4>(columns[3]);
                if (column4 == null)
                    return null;

                var column5 = TryReinterpret<T5>(columns[4]);
                if (column5 == null)
                    return null;

                var column6 = TryReinterpret<T6>(columns[5]);
                if (column6 == null)
                    return null;

                return new TupleTableColumn<T1, T2, T3, T4, T5, T6>(rowCount, column1, column2, column3, column4, column5, column6);
            }
        }
    }

    internal sealed class TupleTableColumn<T1, T2, T3, T4, T5, T6, T7> : TupleTableColumnBase, IClickHouseTableColumn<Tuple<T1, T2, T3, T4, T5, T6, T7>>
    {
        private readonly IClickHouseTableColumn<T1> _column1;
        private readonly IClickHouseTableColumn<T2> _column2;
        private readonly IClickHouseTableColumn<T3> _column3;
        private readonly IClickHouseTableColumn<T4> _column4;
        private readonly IClickHouseTableColumn<T5> _column5;
        private readonly IClickHouseTableColumn<T6> _column6;
        private readonly IClickHouseTableColumn<T7> _column7;

        private TupleTableColumn(
            int rowCount,
            IClickHouseTableColumn<T1> column1,
            IClickHouseTableColumn<T2> column2,
            IClickHouseTableColumn<T3> column3,
            IClickHouseTableColumn<T4> column4,
            IClickHouseTableColumn<T5> column5,
            IClickHouseTableColumn<T6> column6,
            IClickHouseTableColumn<T7> column7)
            : base(rowCount)
        {
            _column1 = column1;
            _column2 = column2;
            _column3 = column3;
            _column4 = column4;
            _column5 = column5;
            _column6 = column6;
            _column7 = column7;
        }

        public new Tuple<T1, T2, T3, T4, T5, T6, T7> GetValue(int index)
        {
            CheckIndex(index);

            return new Tuple<T1, T2, T3, T4, T5, T6, T7>(
                _column1.GetValue(index),
                _column2.GetValue(index),
                _column3.GetValue(index),
                _column4.GetValue(index),
                _column5.GetValue(index),
                _column6.GetValue(index),
                _column7.GetValue(index));
        }

        protected override object GetTupleValue(int index)
        {
            return GetValue(index);
        }

        public override IEnumerable<IClickHouseTableColumn> GetColumns()
        {
            yield return _column1;
            yield return _column2;
            yield return _column3;
            yield return _column4;
            yield return _column5;
            yield return _column6;
            yield return _column7;
        }

        internal class Reinterpreter : ReinterpreterBase
        {
            public override TupleTableColumnBase? TryReinterpret(int rowCount, IReadOnlyList<IClickHouseTableColumn> columns)
            {
                Debug.Assert(columns.Count == 7);

                var column1 = TryReinterpret<T1>(columns[0]);
                if (column1 == null)
                    return null;

                var column2 = TryReinterpret<T2>(columns[1]);
                if (column2 == null)
                    return null;

                var column3 = TryReinterpret<T3>(columns[2]);
                if (column3 == null)
                    return null;

                var column4 = TryReinterpret<T4>(columns[3]);
                if (column4 == null)
                    return null;

                var column5 = TryReinterpret<T5>(columns[4]);
                if (column5 == null)
                    return null;

                var column6 = TryReinterpret<T6>(columns[5]);
                if (column6 == null)
                    return null;

                var column7 = TryReinterpret<T7>(columns[6]);
                if (column7 == null)
                    return null;

                return new TupleTableColumn<T1, T2, T3, T4, T5, T6, T7>(rowCount, column1, column2, column3, column4, column5, column6, column7);
            }
        }
    }

#pragma warning disable CS8714 // The type cannot be used as type parameter in the generic type or method. Nullability of type argument doesn't match 'notnull' constraint.
    internal sealed class TupleTableColumn<T1, T2, T3, T4, T5, T6, T7, TRest, TColumnRest> : TupleTableColumnBase, IClickHouseTableColumn<Tuple<T1, T2, T3, T4, T5, T6, T7, TRest>>
        where TColumnRest : TupleTableColumnBase, IClickHouseTableColumn<TRest>
    {
        private readonly IClickHouseTableColumn<T1> _column1;
        private readonly IClickHouseTableColumn<T2> _column2;
        private readonly IClickHouseTableColumn<T3> _column3;
        private readonly IClickHouseTableColumn<T4> _column4;
        private readonly IClickHouseTableColumn<T5> _column5;
        private readonly IClickHouseTableColumn<T6> _column6;
        private readonly IClickHouseTableColumn<T7> _column7;
        private readonly TColumnRest _columnRest;

        private TupleTableColumn(
            int rowCount,
            IClickHouseTableColumn<T1> column1,
            IClickHouseTableColumn<T2> column2,
            IClickHouseTableColumn<T3> column3,
            IClickHouseTableColumn<T4> column4,
            IClickHouseTableColumn<T5> column5,
            IClickHouseTableColumn<T6> column6,
            IClickHouseTableColumn<T7> column7,
            TColumnRest columnRest)
            : base(rowCount)
        {
            _column1 = column1;
            _column2 = column2;
            _column3 = column3;
            _column4 = column4;
            _column5 = column5;
            _column6 = column6;
            _column7 = column7;
            _columnRest = columnRest;
        }

        public new Tuple<T1, T2, T3, T4, T5, T6, T7, TRest> GetValue(int index)
        {
            CheckIndex(index);

            return new Tuple<T1, T2, T3, T4, T5, T6, T7, TRest>(
                _column1.GetValue(index),
                _column2.GetValue(index),
                _column3.GetValue(index),
                _column4.GetValue(index),
                _column5.GetValue(index),
                _column6.GetValue(index),
                _column7.GetValue(index),
                ((IClickHouseTableColumn<TRest>) _columnRest).GetValue(index));
        }

        protected override object GetTupleValue(int index)
        {
            return GetValue(index);
        }

        public override IEnumerable<IClickHouseTableColumn> GetColumns()
        {
            yield return _column1;
            yield return _column2;
            yield return _column3;
            yield return _column4;
            yield return _column5;
            yield return _column6;
            yield return _column7;

            foreach (var extraColumn in _columnRest.GetColumns())
                yield return extraColumn;
        }

        internal class Reinterpreter : ReinterpreterBase
        {
            public override TupleTableColumnBase? TryReinterpret(int rowCount, IReadOnlyList<IClickHouseTableColumn> columns)
            {
                Debug.Assert(columns.Count == 8);

                var column1 = TryReinterpret<T1>(columns[0]);
                if (column1 == null)
                    return null;

                var column2 = TryReinterpret<T2>(columns[1]);
                if (column2 == null)
                    return null;

                var column3 = TryReinterpret<T3>(columns[2]);
                if (column3 == null)
                    return null;

                var column4 = TryReinterpret<T4>(columns[3]);
                if (column4 == null)
                    return null;

                var column5 = TryReinterpret<T5>(columns[4]);
                if (column5 == null)
                    return null;

                var column6 = TryReinterpret<T6>(columns[5]);
                if (column6 == null)
                    return null;

                var column7 = TryReinterpret<T7>(columns[6]);
                if (column7 == null)
                    return null;

                var column8 = TryReinterpret<TRest>(columns[7]);
                if (!(column8 is TColumnRest columnRest))
                    return null;

                return new TupleTableColumn<T1, T2, T3, T4, T5, T6, T7, TRest, TColumnRest>(rowCount, column1, column2, column3, column4, column5, column6, column7, columnRest);
            }
        }
    }
#pragma warning restore CS8714

    internal sealed class ValueTupleTableColumn<T1> : TupleTableColumnBase, IClickHouseTableColumn<ValueTuple<T1>>
    {
        private readonly IClickHouseTableColumn<T1> _column1;

        private ValueTupleTableColumn(int rowCount, IClickHouseTableColumn<T1> column1)
            : base(rowCount)
        {
            _column1 = column1;
        }

        public new ValueTuple<T1> GetValue(int index)
        {
            CheckIndex(index);

            return new ValueTuple<T1>(_column1.GetValue(index));
        }

        protected override object GetTupleValue(int index)
        {
            return GetValue(index);
        }

        public override IEnumerable<IClickHouseTableColumn> GetColumns()
        {
            yield return _column1;
        }

        internal class Reinterpreter : ReinterpreterBase
        {
            public override TupleTableColumnBase? TryReinterpret(int rowCount, IReadOnlyList<IClickHouseTableColumn> columns)
            {
                Debug.Assert(columns.Count == 1);

                var column1 = TryReinterpret<T1>(columns[0]);
                if (column1 == null)
                    return null;

                return new ValueTupleTableColumn<T1>(rowCount, column1);
            }
        }
    }

    internal sealed class ValueTupleTableColumn<T1, T2> : TupleTableColumnBase, IClickHouseTableColumn<ValueTuple<T1, T2>>
    {
        private readonly IClickHouseTableColumn<T1> _column1;
        private readonly IClickHouseTableColumn<T2> _column2;

        private ValueTupleTableColumn(int rowCount, IClickHouseTableColumn<T1> column1, IClickHouseTableColumn<T2> column2)
            : base(rowCount)
        {
            _column1 = column1;
            _column2 = column2;
        }

        public new ValueTuple<T1, T2> GetValue(int index)
        {
            CheckIndex(index);

            return new ValueTuple<T1, T2>(_column1.GetValue(index), _column2.GetValue(index));
        }

        protected override object GetTupleValue(int index)
        {
            return GetValue(index);
        }

        public override IEnumerable<IClickHouseTableColumn> GetColumns()
        {
            yield return _column1;
            yield return _column2;
        }

        internal class Reinterpreter : ReinterpreterBase
        {
            public override TupleTableColumnBase? TryReinterpret(int rowCount, IReadOnlyList<IClickHouseTableColumn> columns)
            {
                Debug.Assert(columns.Count == 2);

                var column1 = TryReinterpret<T1>(columns[0]);
                if (column1 == null)
                    return null;

                var column2 = TryReinterpret<T2>(columns[1]);
                if (column2 == null)
                    return null;

                return new ValueTupleTableColumn<T1, T2>(rowCount, column1, column2);
            }
        }
    }

    internal sealed class ValueTupleTableColumn<T1, T2, T3> : TupleTableColumnBase, IClickHouseTableColumn<ValueTuple<T1, T2, T3>>
    {
        private readonly IClickHouseTableColumn<T1> _column1;
        private readonly IClickHouseTableColumn<T2> _column2;
        private readonly IClickHouseTableColumn<T3> _column3;

        private ValueTupleTableColumn(int rowCount, IClickHouseTableColumn<T1> column1, IClickHouseTableColumn<T2> column2, IClickHouseTableColumn<T3> column3)
            : base(rowCount)
        {
            _column1 = column1;
            _column2 = column2;
            _column3 = column3;
        }

        public new ValueTuple<T1, T2, T3> GetValue(int index)
        {
            CheckIndex(index);

            return new ValueTuple<T1, T2, T3>(_column1.GetValue(index), _column2.GetValue(index), _column3.GetValue(index));
        }

        protected override object GetTupleValue(int index)
        {
            return GetValue(index);
        }

        public override IEnumerable<IClickHouseTableColumn> GetColumns()
        {
            yield return _column1;
            yield return _column2;
            yield return _column3;
        }

        internal class Reinterpreter : ReinterpreterBase
        {
            public override TupleTableColumnBase? TryReinterpret(int rowCount, IReadOnlyList<IClickHouseTableColumn> columns)
            {
                Debug.Assert(columns.Count == 3);

                var column1 = TryReinterpret<T1>(columns[0]);
                if (column1 == null)
                    return null;

                var column2 = TryReinterpret<T2>(columns[1]);
                if (column2 == null)
                    return null;

                var column3 = TryReinterpret<T3>(columns[2]);
                if (column3 == null)
                    return null;

                return new ValueTupleTableColumn<T1, T2, T3>(rowCount, column1, column2, column3);
            }
        }
    }

    internal sealed class ValueTupleTableColumn<T1, T2, T3, T4> : TupleTableColumnBase, IClickHouseTableColumn<ValueTuple<T1, T2, T3, T4>>
    {
        private readonly IClickHouseTableColumn<T1> _column1;
        private readonly IClickHouseTableColumn<T2> _column2;
        private readonly IClickHouseTableColumn<T3> _column3;
        private readonly IClickHouseTableColumn<T4> _column4;

        private ValueTupleTableColumn(int rowCount, IClickHouseTableColumn<T1> column1, IClickHouseTableColumn<T2> column2, IClickHouseTableColumn<T3> column3, IClickHouseTableColumn<T4> column4)
            : base(rowCount)
        {
            _column1 = column1;
            _column2 = column2;
            _column3 = column3;
            _column4 = column4;
        }

        public new ValueTuple<T1, T2, T3, T4> GetValue(int index)
        {
            CheckIndex(index);

            return new ValueTuple<T1, T2, T3, T4>(_column1.GetValue(index), _column2.GetValue(index), _column3.GetValue(index), _column4.GetValue(index));
        }

        protected override object GetTupleValue(int index)
        {
            return GetValue(index);
        }

        public override IEnumerable<IClickHouseTableColumn> GetColumns()
        {
            yield return _column1;
            yield return _column2;
            yield return _column3;
            yield return _column4;
        }

        internal class Reinterpreter : ReinterpreterBase
        {
            public override TupleTableColumnBase? TryReinterpret(int rowCount, IReadOnlyList<IClickHouseTableColumn> columns)
            {
                Debug.Assert(columns.Count == 4);

                var column1 = TryReinterpret<T1>(columns[0]);
                if (column1 == null)
                    return null;

                var column2 = TryReinterpret<T2>(columns[1]);
                if (column2 == null)
                    return null;

                var column3 = TryReinterpret<T3>(columns[2]);
                if (column3 == null)
                    return null;

                var column4 = TryReinterpret<T4>(columns[3]);
                if (column4 == null)
                    return null;

                return new ValueTupleTableColumn<T1, T2, T3, T4>(rowCount, column1, column2, column3, column4);
            }
        }
    }

    internal sealed class ValueTupleTableColumn<T1, T2, T3, T4, T5> : TupleTableColumnBase, IClickHouseTableColumn<ValueTuple<T1, T2, T3, T4, T5>>
    {
        private readonly IClickHouseTableColumn<T1> _column1;
        private readonly IClickHouseTableColumn<T2> _column2;
        private readonly IClickHouseTableColumn<T3> _column3;
        private readonly IClickHouseTableColumn<T4> _column4;
        private readonly IClickHouseTableColumn<T5> _column5;

        private ValueTupleTableColumn(
            int rowCount,
            IClickHouseTableColumn<T1> column1,
            IClickHouseTableColumn<T2> column2,
            IClickHouseTableColumn<T3> column3,
            IClickHouseTableColumn<T4> column4,
            IClickHouseTableColumn<T5> column5)
            : base(rowCount)
        {
            _column1 = column1;
            _column2 = column2;
            _column3 = column3;
            _column4 = column4;
            _column5 = column5;
        }

        public new ValueTuple<T1, T2, T3, T4, T5> GetValue(int index)
        {
            CheckIndex(index);

            return new ValueTuple<T1, T2, T3, T4, T5>(_column1.GetValue(index), _column2.GetValue(index), _column3.GetValue(index), _column4.GetValue(index), _column5.GetValue(index));
        }

        protected override object GetTupleValue(int index)
        {
            return GetValue(index);
        }

        public override IEnumerable<IClickHouseTableColumn> GetColumns()
        {
            yield return _column1;
            yield return _column2;
            yield return _column3;
            yield return _column4;
            yield return _column5;
        }

        internal class Reinterpreter : ReinterpreterBase
        {
            public override TupleTableColumnBase? TryReinterpret(int rowCount, IReadOnlyList<IClickHouseTableColumn> columns)
            {
                Debug.Assert(columns.Count == 5);

                var column1 = TryReinterpret<T1>(columns[0]);
                if (column1 == null)
                    return null;

                var column2 = TryReinterpret<T2>(columns[1]);
                if (column2 == null)
                    return null;

                var column3 = TryReinterpret<T3>(columns[2]);
                if (column3 == null)
                    return null;

                var column4 = TryReinterpret<T4>(columns[3]);
                if (column4 == null)
                    return null;

                var column5 = TryReinterpret<T5>(columns[4]);
                if (column5 == null)
                    return null;

                return new ValueTupleTableColumn<T1, T2, T3, T4, T5>(rowCount, column1, column2, column3, column4, column5);
            }
        }
    }

    internal sealed class ValueTupleTableColumn<T1, T2, T3, T4, T5, T6> : TupleTableColumnBase, IClickHouseTableColumn<ValueTuple<T1, T2, T3, T4, T5, T6>>
    {
        private readonly IClickHouseTableColumn<T1> _column1;
        private readonly IClickHouseTableColumn<T2> _column2;
        private readonly IClickHouseTableColumn<T3> _column3;
        private readonly IClickHouseTableColumn<T4> _column4;
        private readonly IClickHouseTableColumn<T5> _column5;
        private readonly IClickHouseTableColumn<T6> _column6;

        private ValueTupleTableColumn(
            int rowCount,
            IClickHouseTableColumn<T1> column1,
            IClickHouseTableColumn<T2> column2,
            IClickHouseTableColumn<T3> column3,
            IClickHouseTableColumn<T4> column4,
            IClickHouseTableColumn<T5> column5,
            IClickHouseTableColumn<T6> column6)
            : base(rowCount)
        {
            _column1 = column1;
            _column2 = column2;
            _column3 = column3;
            _column4 = column4;
            _column5 = column5;
            _column6 = column6;
        }

        public new ValueTuple<T1, T2, T3, T4, T5, T6> GetValue(int index)
        {
            CheckIndex(index);

            return new ValueTuple<T1, T2, T3, T4, T5, T6>(
                _column1.GetValue(index),
                _column2.GetValue(index),
                _column3.GetValue(index),
                _column4.GetValue(index),
                _column5.GetValue(index),
                _column6.GetValue(index));
        }

        protected override object GetTupleValue(int index)
        {
            return GetValue(index);
        }

        public override IEnumerable<IClickHouseTableColumn> GetColumns()
        {
            yield return _column1;
            yield return _column2;
            yield return _column3;
            yield return _column4;
            yield return _column5;
            yield return _column6;
        }

        internal class Reinterpreter : ReinterpreterBase
        {
            public override TupleTableColumnBase? TryReinterpret(int rowCount, IReadOnlyList<IClickHouseTableColumn> columns)
            {
                Debug.Assert(columns.Count == 6);

                var column1 = TryReinterpret<T1>(columns[0]);
                if (column1 == null)
                    return null;

                var column2 = TryReinterpret<T2>(columns[1]);
                if (column2 == null)
                    return null;

                var column3 = TryReinterpret<T3>(columns[2]);
                if (column3 == null)
                    return null;

                var column4 = TryReinterpret<T4>(columns[3]);
                if (column4 == null)
                    return null;

                var column5 = TryReinterpret<T5>(columns[4]);
                if (column5 == null)
                    return null;

                var column6 = TryReinterpret<T6>(columns[5]);
                if (column6 == null)
                    return null;

                return new ValueTupleTableColumn<T1, T2, T3, T4, T5, T6>(rowCount, column1, column2, column3, column4, column5, column6);
            }
        }
    }

    internal sealed class ValueTupleTableColumn<T1, T2, T3, T4, T5, T6, T7> : TupleTableColumnBase, IClickHouseTableColumn<ValueTuple<T1, T2, T3, T4, T5, T6, T7>>
    {
        private readonly IClickHouseTableColumn<T1> _column1;
        private readonly IClickHouseTableColumn<T2> _column2;
        private readonly IClickHouseTableColumn<T3> _column3;
        private readonly IClickHouseTableColumn<T4> _column4;
        private readonly IClickHouseTableColumn<T5> _column5;
        private readonly IClickHouseTableColumn<T6> _column6;
        private readonly IClickHouseTableColumn<T7> _column7;

        private ValueTupleTableColumn(
            int rowCount,
            IClickHouseTableColumn<T1> column1,
            IClickHouseTableColumn<T2> column2,
            IClickHouseTableColumn<T3> column3,
            IClickHouseTableColumn<T4> column4,
            IClickHouseTableColumn<T5> column5,
            IClickHouseTableColumn<T6> column6,
            IClickHouseTableColumn<T7> column7)
            : base(rowCount)
        {
            _column1 = column1;
            _column2 = column2;
            _column3 = column3;
            _column4 = column4;
            _column5 = column5;
            _column6 = column6;
            _column7 = column7;
        }

        public new ValueTuple<T1, T2, T3, T4, T5, T6, T7> GetValue(int index)
        {
            CheckIndex(index);

            return new ValueTuple<T1, T2, T3, T4, T5, T6, T7>(
                _column1.GetValue(index),
                _column2.GetValue(index),
                _column3.GetValue(index),
                _column4.GetValue(index),
                _column5.GetValue(index),
                _column6.GetValue(index),
                _column7.GetValue(index));
        }

        protected override object GetTupleValue(int index)
        {
            return GetValue(index);
        }

        public override IEnumerable<IClickHouseTableColumn> GetColumns()
        {
            yield return _column1;
            yield return _column2;
            yield return _column3;
            yield return _column4;
            yield return _column5;
            yield return _column6;
            yield return _column7;
        }

        internal class Reinterpreter : ReinterpreterBase
        {
            public override TupleTableColumnBase? TryReinterpret(int rowCount, IReadOnlyList<IClickHouseTableColumn> columns)
            {
                Debug.Assert(columns.Count == 7);

                var column1 = TryReinterpret<T1>(columns[0]);
                if (column1 == null)
                    return null;

                var column2 = TryReinterpret<T2>(columns[1]);
                if (column2 == null)
                    return null;

                var column3 = TryReinterpret<T3>(columns[2]);
                if (column3 == null)
                    return null;

                var column4 = TryReinterpret<T4>(columns[3]);
                if (column4 == null)
                    return null;

                var column5 = TryReinterpret<T5>(columns[4]);
                if (column5 == null)
                    return null;

                var column6 = TryReinterpret<T6>(columns[5]);
                if (column6 == null)
                    return null;

                var column7 = TryReinterpret<T7>(columns[6]);
                if (column7 == null)
                    return null;

                return new ValueTupleTableColumn<T1, T2, T3, T4, T5, T6, T7>(rowCount, column1, column2, column3, column4, column5, column6, column7);
            }
        }
    }

    internal sealed class ValueTupleTableColumn<T1, T2, T3, T4, T5, T6, T7, TRest, TColumnRest> : TupleTableColumnBase, IClickHouseTableColumn<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>>
        where TRest : struct
        where TColumnRest : TupleTableColumnBase, IClickHouseTableColumn<TRest>
    {
        private readonly IClickHouseTableColumn<T1> _column1;
        private readonly IClickHouseTableColumn<T2> _column2;
        private readonly IClickHouseTableColumn<T3> _column3;
        private readonly IClickHouseTableColumn<T4> _column4;
        private readonly IClickHouseTableColumn<T5> _column5;
        private readonly IClickHouseTableColumn<T6> _column6;
        private readonly IClickHouseTableColumn<T7> _column7;
        private readonly TColumnRest _columnRest;

        private ValueTupleTableColumn(
            int rowCount,
            IClickHouseTableColumn<T1> column1,
            IClickHouseTableColumn<T2> column2,
            IClickHouseTableColumn<T3> column3,
            IClickHouseTableColumn<T4> column4,
            IClickHouseTableColumn<T5> column5,
            IClickHouseTableColumn<T6> column6,
            IClickHouseTableColumn<T7> column7,
            TColumnRest columnRest)
            : base(rowCount)
        {
            _column1 = column1;
            _column2 = column2;
            _column3 = column3;
            _column4 = column4;
            _column5 = column5;
            _column6 = column6;
            _column7 = column7;
            _columnRest = columnRest;
        }

        public new ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest> GetValue(int index)
        {
            CheckIndex(index);

            return new ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>(
                _column1.GetValue(index),
                _column2.GetValue(index),
                _column3.GetValue(index),
                _column4.GetValue(index),
                _column5.GetValue(index),
                _column6.GetValue(index),
                _column7.GetValue(index),
                ((IClickHouseTableColumn<TRest>) _columnRest).GetValue(index));
        }

        protected override object GetTupleValue(int index)
        {
            return GetValue(index);
        }

        public override IEnumerable<IClickHouseTableColumn> GetColumns()
        {
            yield return _column1;
            yield return _column2;
            yield return _column3;
            yield return _column4;
            yield return _column5;
            yield return _column6;
            yield return _column7;

            foreach (var extraColumn in _columnRest.GetColumns())
                yield return extraColumn;
        }

        internal class Reinterpreter : ReinterpreterBase
        {
            public override TupleTableColumnBase? TryReinterpret(int rowCount, IReadOnlyList<IClickHouseTableColumn> columns)
            {
                Debug.Assert(columns.Count == 8);

                var column1 = TryReinterpret<T1>(columns[0]);
                if (column1 == null)
                    return null;

                var column2 = TryReinterpret<T2>(columns[1]);
                if (column2 == null)
                    return null;

                var column3 = TryReinterpret<T3>(columns[2]);
                if (column3 == null)
                    return null;

                var column4 = TryReinterpret<T4>(columns[3]);
                if (column4 == null)
                    return null;

                var column5 = TryReinterpret<T5>(columns[4]);
                if (column5 == null)
                    return null;

                var column6 = TryReinterpret<T6>(columns[5]);
                if (column6 == null)
                    return null;

                var column7 = TryReinterpret<T7>(columns[6]);
                if (column7 == null)
                    return null;

                var column8 = TryReinterpret<TRest>(columns[7]);
                if (!(column8 is TColumnRest columnRest))
                    return null;

                return new ValueTupleTableColumn<T1, T2, T3, T4, T5, T6, T7, TRest, TColumnRest>(rowCount, column1, column2, column3, column4, column5, column6, column7, columnRest);
            }
        }
    }
}
