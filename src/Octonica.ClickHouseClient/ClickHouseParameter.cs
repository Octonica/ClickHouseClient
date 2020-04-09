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
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Types;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient
{
    public class ClickHouseParameter : DbParameter
    {
        // https://github.com/ClickHouse/ClickHouse/blob/master/docs/en/query_language/syntax.md
        private static readonly Regex ParameterNameRegex = new Regex("^[a-zA-Z_][0-9a-zA-Z_]*$");

        private readonly string _parameterName;

        private bool? _forcedNullable;
        private DbType? _forcedType;
        private byte? _forcedScale;
        private byte? _forcedPrecision;

        internal string Id { get; }

        public override DbType DbType
        {
            get => _forcedType ?? GetTypeFromValue().dbType;
            set => _forcedType = value;
        }

        public override ParameterDirection Direction
        {
            get => ParameterDirection.Input;
            set
            {
                if (value != ParameterDirection.Input)
                    throw new NotSupportedException("Only input parameters are supported.");
            }
        }

        public override bool IsNullable
        {
            get => _forcedNullable ?? Value == null || Value == DBNull.Value;
            set => _forcedNullable = value;
        }

        public sealed override string ParameterName
        {
            get => _parameterName;
            set => throw new NotSupportedException("The name of the parameter is read-only.");
        }

        public override string? SourceColumn { get; set; }

        public override object? Value { get; set; }

        public override bool SourceColumnNullMapping { get; set; }

        public override int Size { get; set; }

        public override byte Precision
        {
            get => _forcedPrecision ?? DecimalTypeInfoBase.DefaultPrecision;
            set => _forcedPrecision = value;
        }

        public override byte Scale
        {
            get => _forcedScale ?? DecimalTypeInfoBase.DefaultScale;
            set => _forcedScale = value;
        }

        public Encoding? StringEncoding { get; set; }

        public ClickHouseParameter(string parameterName)
        {
            if (parameterName == null)
                throw new ArgumentNullException(nameof(parameterName));

            if (parameterName.Length > 0 && parameterName[0] == '{' && parameterName[^1] == '}')
                Id = parameterName[1..^1];
            else
                Id = parameterName;

            if (!ParameterNameRegex.IsMatch(Id))
                throw new ArgumentException("The name of the parameter must be a valid ClickHouse identifier.", nameof(parameterName));
            
            _parameterName = parameterName;
        }

        public override void ResetDbType()
        {
            _forcedType = null;
            _forcedNullable = null;
            _forcedPrecision = null;
            _forcedScale = null;
            Size = 0;
            StringEncoding = null;
        }

        internal IClickHouseColumnWriter CreateParameterColumnWriter(IClickHouseTypeInfoProvider typeInfoProvider)
        {
            string typeName;
            Type clrType;
            object? preparedValue = null;
            switch (_forcedType)
            {
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                    throw new NotSupportedException($"Parameter \"{ParameterName}\". The type \"{_forcedType}\" is not supported. String encoding can be specified with the property \"{nameof(Encoding)}\".");
                case DbType.Time:
                case DbType.Xml:
                    throw new NotSupportedException($"Parameter \"{ParameterName}\". The type \"{_forcedType}\" is not supported.");

                case DbType.Binary:
                    typeName = Size <= 0 ? "Array(UInt8)" : string.Format(CultureInfo.InvariantCulture, "FixedString({0})", Size);
                    clrType = typeof(byte[]);
                    break;
                case DbType.Byte:
                    typeName = "UInt8";
                    clrType = typeof(byte);
                    break;
                case DbType.Boolean:
                    typeName = "UInt8";
                    clrType = typeof(byte);
                    break;
                case DbType.Currency:
                    typeName = "Decimal(18, 4)";
                    clrType = typeof(decimal);
                    break;
                case DbType.Date:
                    typeName = "Date";
                    clrType = typeof(DateTime);
                    break;
                case DbType.DateTime:
                    typeName = "DateTime";
                    clrType = typeof(DateTime);
                    break;
                case DbType.Decimal:
                    typeName = string.Format(CultureInfo.InvariantCulture, "Decimal({0}, {1})", DecimalTypeInfoBase.DefaultPrecision, DecimalTypeInfoBase.DefaultScale);
                    clrType = typeof(decimal);
                    break;
                case DbType.Double:
                    typeName = "Float64";
                    clrType = typeof(double);
                    break;
                case DbType.Guid:
                    typeName = "UUID";
                    clrType = typeof(Guid);
                    break;
                case DbType.Int16:
                    typeName = "Int16";
                    clrType = typeof(short);
                    break;
                case DbType.Int32:
                    typeName = "Int32";
                    clrType = typeof(int);
                    break;
                case DbType.Int64:
                    typeName = "Int64";
                    clrType = typeof(long);
                    break;
                case DbType.Object:
                    if (Value != null)
                        throw new NotSupportedException();

                    typeName = "Nothing";
                    clrType = typeof(DBNull);
                    break;
                case DbType.SByte:
                    typeName = "Int8";
                    clrType = typeof(sbyte);
                    break;
                case DbType.Single:
                    typeName = "Float32";
                    clrType = typeof(float);
                    break;
                case DbType.String:
                    typeName = "String";
                    clrType = typeof(string);
                    break;
                case DbType.UInt16:
                    typeName = "UInt16";
                    clrType = typeof(ushort);
                    break;
                case DbType.UInt32:
                    typeName = "UInt32";
                    clrType = typeof(uint);
                    break;
                case DbType.UInt64:
                    typeName = "UInt32";
                    clrType = typeof(ulong);
                    break;
                case DbType.VarNumeric:
                    typeName = string.Format(CultureInfo.InvariantCulture, "Decimal({0}, {1})", Precision, Scale);
                    clrType = typeof(decimal);
                    break;
                case DbType.StringFixedLength:
                {
                    if (Size <= 0)
                        throw new ClickHouseException(ClickHouseErrorCodes.InvalidQueryParameterConfiguration, $"Parameter \"{ParameterName}\". The size of the fixed string must be a positive number.");

                    typeName = string.Format(CultureInfo.InvariantCulture, "FixedString({0})", Size);
                    if (Value is string strValue)
                    {
                        var encoding = StringEncoding ?? Encoding.UTF8;
                        var bytes = encoding.GetBytes(strValue);
                        if (bytes.Length > Size)
                        {
                            throw new ClickHouseException(
                                ClickHouseErrorCodes.InvalidQueryParameterConfiguration,
                                $"Parameter \"{ParameterName}\". The length of the string in bytes with encoding \"{encoding.EncodingName}\" is greater than the size of the parameter.");
                        }

                        preparedValue = bytes;
                    }

                    clrType = typeof(byte[]);
                    break;
                }
                case DbType.DateTime2:
                    typeName = "DateTime";
                    clrType = typeof(DateTime);
                    break;
                case DbType.DateTimeOffset:
                    typeName = "DateTime";
                    clrType = typeof(DateTime);
                    break;
                case null:
                    typeName = GetTypeFromValue().clickHouseType;
                    clrType = Value?.GetType() ?? typeof(DBNull);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            bool isNull = Value == DBNull.Value || Value == null;
            if (isNull && _forcedNullable == false)
                throw new ClickHouseException(ClickHouseErrorCodes.InvalidQueryParameterConfiguration, $"The parameter \"{ParameterName}\" is declared as non-nullable but its value is null.");

            if (_forcedNullable == true || isNull)
            {
                typeName = $"Nullable({typeName})";
            }

            var typeInfo = typeInfoProvider.GetTypeInfo(typeName);

            if (!isNull)
                clrType = (preparedValue ?? Value)!.GetType();
            else if (clrType.IsValueType)
                clrType = typeof(Nullable<>).MakeGenericType(clrType);

            var columnSettings = StringEncoding == null ? null : new ClickHouseColumnSettings(StringEncoding);
            var columnBuilder = new ParameterColumnWriterBuilder(Id, isNull ? null : preparedValue ?? Value, columnSettings, typeInfo);
            var column = TypeDispatcher.Dispatch(clrType, columnBuilder);

            return column;
        }

        private (DbType dbType, string clickHouseType) GetTypeFromValue()
        {
            switch (Value)
            {
                case string _:
                    return (DbType.String, "String");
                case byte[] _:
                    return (DbType.Binary, "Array(UInt8)");
                case byte _:
                    return (DbType.Byte, "UInt8");
                case bool _:
                    return (DbType.Boolean, "UInt8");
                case decimal _:
                    return (DbType.Decimal, "Decimal");
                case DateTime _:
                case DateTimeOffset _:
                    return (DbType.DateTime, "DateTime");
                case double _:
                    return (DbType.Double, "Float64");
                case Guid _:
                    return (DbType.Guid, "UUID");
                case short _:
                    return (DbType.Int16, "Int16");
                case int _:
                    return (DbType.Int32, "Int32");
                case long _:
                    return (DbType.Int64, "Int64");
                case sbyte _:
                    return (DbType.SByte, "Int8");
                case float _:
                    return (DbType.Single, "Float32");
                case ushort _:
                    return (DbType.UInt16, "UInt16");
                case uint _:
                    return (DbType.UInt32, "UInt32");
                case ulong _:
                    return (DbType.UInt64, "UInt32");
                case DBNull _:
                case null:
                    return (DbType.Object, "Nothing");
                default:
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidQueryParameterConfiguration, $"The type \"{Value.GetType()}\" of the parameter \"{ParameterName}\" is not supported.");
            }
        }

        private class ParameterColumnWriterBuilder : ITypeDispatcher<IClickHouseColumnWriter>
        {
            private readonly string _parameterId;
            private readonly object? _value;
            private readonly ClickHouseColumnSettings? _columnSettings;
            private readonly IClickHouseTypeInfo _typeInfo;

            public ParameterColumnWriterBuilder(string parameterId, object? value, ClickHouseColumnSettings? columnSettings, IClickHouseTypeInfo typeInfo)
            {
                _parameterId = parameterId;
                _value = value;
                _columnSettings = columnSettings;
                _typeInfo = typeInfo ?? throw new ArgumentNullException(nameof(typeInfo));
            }

            
            public IClickHouseColumnWriter Dispatch<T>()
            {

                var singleElementColumn = new ConstantReadOnlyList<T>((T)_value, 1);
                return _typeInfo.CreateColumnWriter(_parameterId, singleElementColumn, _columnSettings);
            }
        }
    }
}
