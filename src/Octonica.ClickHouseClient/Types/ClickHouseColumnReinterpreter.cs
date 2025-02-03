#region License Apache 2.0
/* Copyright 2024 Octonica
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

using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Utils;
using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class ClickHouseColumnReinterpreter : ITypeDispatcher<IClickHouseColumnReinterpreter>
    {
        private static readonly ClickHouseColumnReinterpreter Instance = new ClickHouseColumnReinterpreter();

        private ClickHouseColumnReinterpreter()
        {
        }

        IClickHouseColumnReinterpreter ITypeDispatcher<IClickHouseColumnReinterpreter>.Dispatch<T>()
        {
            return ClickHouseColumnReinterpreter<T>.Instance;
        }

        public static IClickHouseColumnReinterpreter Create(Type type)
        {
            return TypeDispatcher.Create(type).Dispatch(Instance);
        }

        public static IClickHouseColumnReinterpreter Create<T, TRes>(Func<T, TRes> convert)
        {
            var srcType = typeof(T);
            var resType = typeof(TRes);
            Type implTypeDef;
            if (srcType.IsValueType)
            {
                srcType = Nullable.GetUnderlyingType(srcType) ?? srcType;
                if (resType.IsValueType)
                {
                    resType = Nullable.GetUnderlyingType(resType) ?? resType;
                    implTypeDef = typeof(ClickHouseColumnStructToStructReinterpreter<,>);
                }
                else
                {
                    implTypeDef = typeof(ClickHouseColumnStructToObjReinterpreter<,>);
                }
            }
            else
            {
                if (resType.IsValueType)
                {
                    resType = Nullable.GetUnderlyingType(resType) ?? resType;
                    implTypeDef = typeof(ClickHouseColumnObjToStructReinterpreter<,>);
                }
                else
                {
                    implTypeDef = typeof(ClickHouseColumnObjToObjReinterpreter<,>);
                }
            }

            var implType = implTypeDef.MakeGenericType(srcType, resType);
            var impl = Activator.CreateInstance(implType, convert);
            Debug.Assert(impl != null);

            return (IClickHouseColumnReinterpreter)impl;
        }

        public static TValue GuardNull<TValue>(TValue? value)
            where TValue : struct
        {
            return value
                ?? throw new ClickHouseException(
                    ClickHouseErrorCodes.CallbackError,
                    "A callback function provided by the caller returned a null reference." + Environment.NewLine +
                    "When configuring a data reader with a custom column reader, make sure that the callback function will not return null when its argument is not null.");
        }

        [return: NotNull]
        public static TValue GuardNull<TValue>(TValue? value)
            where TValue : class
        {
            return value
                ?? throw new ClickHouseException(
                    ClickHouseErrorCodes.CallbackError,
                    "A callback function provided by the caller returned a null reference." + Environment.NewLine +
                    "When configuring a data reader with a custom column reader, make sure that the callback function will not return null when its argument is not null.");
        }

        public static IClickHouseTableColumn<TRes> Reinterpret<T, TRes>(IClickHouseTableColumn? root, IClickHouseTableColumn<T> column, Func<T, TRes> convert)
        {
            if (column is IClickHouseReinterpretedTableColumn<T> rc)
                return rc.Chain(convert);

            return new ReinterpretedTableColumn<T, TRes>(root, column, convert);
        }
    }

    internal sealed class ClickHouseColumnReinterpreter<T> : IClickHouseColumnReinterpreter
    {
        public static ClickHouseColumnReinterpreter<T> Instance = new ClickHouseColumnReinterpreter<T>();

        public Type BuiltInConvertToType => typeof(T);

        public Type? ExternalConvertToType => null;

        private ClickHouseColumnReinterpreter()
        {
        }

        public IClickHouseTableColumn? TryReinterpret(IClickHouseTableColumn column)
        {
            return (column as IClickHouseTableColumn<T>) ?? column.TryReinterpret<T>();
        }
    }

    internal sealed class ClickHouseObjectColumnReinterpreter<T> : IClickHouseColumnReinterpreter
    {
        private readonly Func<object, T> _convert;

        public Type? BuiltInConvertToType => null;

        public Type ExternalConvertToType => typeof(T);

        public ClickHouseObjectColumnReinterpreter(Func<object, T> convert)
        {
            _convert = Guard(convert);
        }

        public IClickHouseTableColumn? TryReinterpret(IClickHouseTableColumn column)
        {
            if (column is IClickHouseReinterpretedTableColumn<object> rc)
                return rc.Chain(_convert);

            return new ReinterpretedObjectTableColumn<T>(column, _convert);
        }

        private static Func<object, T> Guard(Func<object, T> convert)
        {
            return v => convert(v ?? DBNull.Value) ?? (T)(object)DBNull.Value;
        }
    }

    internal abstract class ClickHouseColumnReinterpreter<T, TRes> : IClickHouseColumnReinterpreter
    {
        public Type BuiltInConvertToType => typeof(T);

        public Type ExternalConvertToType => typeof(TRes);

        public abstract IClickHouseTableColumn? TryReinterpret(IClickHouseTableColumn column);
    }

    internal sealed class ClickHouseColumnObjToObjReinterpreter<T, TRes> : ClickHouseColumnReinterpreter<T, TRes>
        where T : class
        where TRes : class
    {
        private readonly Func<T, TRes> _convert;

        public ClickHouseColumnObjToObjReinterpreter(Func<T, TRes> convert)
        {
            _convert = convert;
        }

        public override IClickHouseTableColumn? TryReinterpret(IClickHouseTableColumn column)
        {
            if (column is NullableObjTableColumn<T> nc)
                return nc.ReinterpretAsObj(Guard(_convert));

            if (column is IClickHouseTableColumn<T> c)
                return ClickHouseColumnReinterpreter.Reinterpret(null, c, GuardNullable(_convert));

            var reinterpretedColumn = column.TryReinterpret<T>();
            if (reinterpretedColumn == null)
                return null;

            if (reinterpretedColumn is NullableObjTableColumn<T> nrc)
                nrc.ReinterpretAsObj(Guard(_convert));

            return ClickHouseColumnReinterpreter.Reinterpret(column, reinterpretedColumn, GuardNullable(_convert));
        }

        private static Func<T, TRes> Guard(Func<T, TRes> convert)
        {
            return v => ClickHouseColumnReinterpreter.GuardNull(convert(v));
        }

        private static Func<T, TRes> GuardNullable(Func<T, TRes> convert)
        {
            return v => v == null ? null! : ClickHouseColumnReinterpreter.GuardNull(convert(v));
        }
    }

    internal sealed class ClickHouseColumnObjToStructReinterpreter<T, TRes> : ClickHouseColumnReinterpreter<T, TRes>
        where T : class
        where TRes : struct
    {
        private readonly Func<T, TRes?>? _convert0;
        private readonly Func<T, TRes>? _convert1;

        public ClickHouseColumnObjToStructReinterpreter(Func<T, TRes> convert)
        {
            _convert1 = convert;
        }

        public ClickHouseColumnObjToStructReinterpreter(Func<T, TRes?> convert)
        {
            _convert0 = convert;
        }

        public override IClickHouseTableColumn? TryReinterpret(IClickHouseTableColumn column)
        {
            if (column is NullableObjTableColumn<T> nullableColumn)
                return nullableColumn.ReinterpretAsStruct(GetConvert());

            IClickHouseTableColumn? rootColumn = null;
            var reinterpretedColumn = column as IClickHouseTableColumn<T>;
            if (reinterpretedColumn == null)
                rootColumn = column;

            reinterpretedColumn = column.TryReinterpret<T>();
            if (reinterpretedColumn == null)
                return null;

            if (reinterpretedColumn is NullableObjTableColumn<T> nullableReinterpretedColumn)
                return nullableReinterpretedColumn.ReinterpretAsStruct(GetConvert());

            if (_convert0 != null)
            {
                // The caller explixitly requested the conversion to a nullable struct
                return ClickHouseColumnReinterpreter.Reinterpret(rootColumn, reinterpretedColumn, GuardNullable(_convert0));
            }

            // It's safe to assume that the column is non-nullable
            return ClickHouseColumnReinterpreter.Reinterpret(rootColumn, reinterpretedColumn, GetConvert());
        }

        private Func<T, TRes> GetConvert()
        {
            if (_convert1 != null)
                return _convert1;
            if (_convert0 != null)
                return Guard(_convert0);

            throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Ivalid object state. If you see this message, please, report a bug.");
        }

        private static Func<T, TRes> Guard(Func<T, TRes?> convert)
        {
            return v => ClickHouseColumnReinterpreter.GuardNull(convert(v));
        }

        private static Func<T, TRes?> GuardNullable(Func<T, TRes?> convert)
        {
            return v => v == null ? (TRes?)null : ClickHouseColumnReinterpreter.GuardNull(convert(v));
        }
    }

    internal sealed class ClickHouseColumnStructToObjReinterpreter<T, TRes> : ClickHouseColumnReinterpreter<T, TRes>
        where T : struct
        where TRes : class
    {
        private readonly Func<T?, TRes>? _convert0;
        private readonly Func<T, TRes>? _convert1;

        public ClickHouseColumnStructToObjReinterpreter(Func<T, TRes> convert)
        {
            _convert1 = convert;
        }

        public ClickHouseColumnStructToObjReinterpreter(Func<T?, TRes> convert)
        {
            _convert0 = convert;
        }

        public override IClickHouseTableColumn? TryReinterpret(IClickHouseTableColumn column)
        {
            if (column is IClickHouseTableColumn<T> structColumn)
                return ClickHouseColumnReinterpreter.Reinterpret(null, structColumn, GetConvert());
            if (column is IClickHouseTableColumn<T?> nullableStructColumn)
                return ClickHouseColumnReinterpreter.Reinterpret(null, nullableStructColumn, GetConvertNullable());

            IClickHouseTableColumn<T?>? reinterpretedNullableColumn;
            var reinterpretedStructColumn = column.TryReinterpret<T>();
            if (reinterpretedStructColumn is NullableStructTableColumnNotNullableAdapter<T> adapter)
            {
                reinterpretedNullableColumn = adapter.Unguard();
            }
            else if (reinterpretedStructColumn != null)
            {
                return ClickHouseColumnReinterpreter.Reinterpret(column, reinterpretedStructColumn, GetConvert());
            }
            else
            {
                reinterpretedNullableColumn = column.TryReinterpret<T?>();
            }

            if (reinterpretedNullableColumn != null)
            {
                return ClickHouseColumnReinterpreter.Reinterpret(column, reinterpretedNullableColumn, GetConvertNullable());
            }

            return null;
        }

        private Func<T, TRes> GetConvert()
        {
            if (_convert1 != null)
                return Guard(_convert1);
            if (_convert0 != null)
                return Guard(_convert0);

            throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Ivalid object state. If you see this message, please, report a bug.");
        }

        private Func<T?, TRes> GetConvertNullable()
        {
            if (_convert1 != null)
                return GuardNullable(_convert1);
            if (_convert0 != null)
                return GuardNullable(_convert0);

            throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Ivalid object state. If you see this message, please, report a bug.");
        }

        private static Func<T, TRes> Guard(Func<T, TRes> convert)
        {
            return v => ClickHouseColumnReinterpreter.GuardNull(convert(v));
        }

        private static Func<T, TRes> Guard(Func<T?, TRes> convert)
        {
            return v => ClickHouseColumnReinterpreter.GuardNull(convert(v));
        }

        private static Func<T?, TRes> GuardNullable(Func<T, TRes> convert)
        {
            return v => v == null ? null! : ClickHouseColumnReinterpreter.GuardNull(convert(v.Value));
        }

        private static Func<T?, TRes> GuardNullable(Func<T?, TRes> convert)
        {
            return v => v == null ? null! : ClickHouseColumnReinterpreter.GuardNull(convert(v));
        }
    }

    internal sealed class ClickHouseColumnStructToStructReinterpreter<T, TRes> : ClickHouseColumnReinterpreter<T, TRes>
        where T : struct
        where TRes : struct
    {
        private readonly Func<T?, TRes?>? _convert00;
        private readonly Func<T?, TRes>? _convert01;
        private readonly Func<T, TRes?>? _convert10;
        private readonly Func<T, TRes>? _convert11;

        public ClickHouseColumnStructToStructReinterpreter(Func<T?, TRes?> convert)
        {
            _convert00 = convert;
        }

        public ClickHouseColumnStructToStructReinterpreter(Func<T?, TRes> convert)
        {
            _convert01 = convert;
        }

        public ClickHouseColumnStructToStructReinterpreter(Func<T, TRes?> convert)
        {
            _convert10 = convert;
        }

        public ClickHouseColumnStructToStructReinterpreter(Func<T, TRes> convert)
        {
            _convert11 = convert;
        }

        public override IClickHouseTableColumn? TryReinterpret(IClickHouseTableColumn column)
        {
            if (column is IClickHouseTableColumn<T> structColumn)
                return ClickHouseColumnReinterpreter.Reinterpret(null, structColumn, GetConvert());
            if (column is IClickHouseTableColumn<T?> nullableStructColumn)
                return ClickHouseColumnReinterpreter.Reinterpret(null, nullableStructColumn, GetConvertNullable());

            IClickHouseTableColumn<T?>? reinterpretedNullableColumn;
            var reinterpretedStructColumn = column.TryReinterpret<T>();
            if (reinterpretedStructColumn is NullableStructTableColumnNotNullableAdapter<T> adapter)
            {
                reinterpretedNullableColumn = adapter.Unguard();
            }
            else if (reinterpretedStructColumn != null)
            {
                return ClickHouseColumnReinterpreter.Reinterpret(column, reinterpretedStructColumn, GetConvert());
            }
            else
            {
                reinterpretedNullableColumn = column.TryReinterpret<T?>();
            }

            if (reinterpretedNullableColumn != null)
            {
                return ClickHouseColumnReinterpreter.Reinterpret(column, reinterpretedNullableColumn, GetConvertNullable());
            }

            return null;
        }

        private Func<T, TRes> GetConvert()
        {
            if (_convert11 != null)
                return _convert11;
            if (_convert00 != null)
                return Guard(_convert00);
            if (_convert01 != null)
                return Guard(_convert01);
            if (_convert10 != null)
                return Guard(_convert10);

            throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Ivalid object state. If you see this message, please, report a bug.");
        }

        private Func<T?, TRes?> GetConvertNullable()
        {
            if (_convert11 != null)
                return GuardNullable(_convert11);
            if (_convert00 != null)
                return GuardNullable(_convert00);
            if (_convert01 != null)
                return GuardNullable(_convert01);
            if (_convert10 != null)
                return GuardNullable(_convert10);

            throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Ivalid object state. If you see this message, please, report a bug.");
        }

        private static Func<T, TRes> Guard(Func<T?, TRes?> convert)
        {
            return v => ClickHouseColumnReinterpreter.GuardNull(convert(v));
        }

        private static Func<T, TRes> Guard(Func<T, TRes?> convert)
        {
            return v => ClickHouseColumnReinterpreter.GuardNull(convert(v));
        }

        private static Func<T, TRes> Guard(Func<T?, TRes> convert)
        {
            return v => convert(v);
        }

        public static Func<T?, TRes?> GuardNullable(Func<T?, TRes?> convert)
        {
            return v => v == null ? (TRes?)null : ClickHouseColumnReinterpreter.GuardNull(convert(v));
        }

        public static Func<T?, TRes?> GuardNullable(Func<T?, TRes> convert)
        {
            return v => v == null ? (TRes?)null : convert(v);
        }

        public static Func<T?, TRes?> GuardNullable(Func<T, TRes?> convert)
        {
            return v => v == null ? (TRes?)null : ClickHouseColumnReinterpreter.GuardNull(convert(v.Value));
        }

        public static Func<T?, TRes?> GuardNullable(Func<T, TRes> convert)
        {
            return v => v == null ? (TRes?)null : convert(v.Value);
        }
    }
}
