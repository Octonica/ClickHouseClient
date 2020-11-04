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
using System.Net;
using System.Net.Sockets;
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
        private ClickHouseDbType? _forcedType;
        private byte? _forcedScale;
        private byte? _forcedPrecision;

        internal string Id { get; }

        public ClickHouseDbType ClickHouseDbType
        {
            get=> _forcedType ?? GetTypeFromValue().dbType;
            set => _forcedType = value;
        }

        public override DbType DbType
        {
            get
            {
                var chType = ClickHouseDbType;
                if (chType > ClickHouseDbType.ClickHouseSpecificTypeDelimiterCode)
                    return DbType.Object;

                return (DbType) chType;
            }
            set => ClickHouseDbType = (ClickHouseDbType) value;
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
            get => _forcedPrecision ?? (ClickHouseDbType == ClickHouseDbType.DateTime64 ? (byte) DateTime64TypeInfo.DefaultPrecision : DecimalTypeInfoBase.DefaultPrecision);
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
            object? preparedValue = null;
            switch (_forcedType)
            {
                case ClickHouseDbType.AnsiString:
                case ClickHouseDbType.AnsiStringFixedLength:
                    throw new NotSupportedException($"Parameter \"{ParameterName}\". The type \"{_forcedType}\" is not supported. String encoding can be specified with the property \"{nameof(Encoding)}\".");
                case ClickHouseDbType.Time:
                case ClickHouseDbType.Xml:
                    throw new NotSupportedException($"Parameter \"{ParameterName}\". The type \"{_forcedType}\" is not supported.");

                case ClickHouseDbType.Binary:
                    typeName = Size <= 0 ? "Array(UInt8)" : string.Format(CultureInfo.InvariantCulture, "FixedString({0})", Size);
                    break;
                case ClickHouseDbType.Byte:
                    typeName = "UInt8";
                    break;
                case ClickHouseDbType.Boolean:
                    typeName = "UInt8";
                    break;
                case ClickHouseDbType.Currency:
                    typeName = "Decimal(18, 4)";
                    break;
                case ClickHouseDbType.Date:
                    typeName = "Date";
                    break;
                case ClickHouseDbType.DateTime:
                    typeName = "DateTime";
                    break;
                case ClickHouseDbType.Decimal:
                    typeName = string.Format(CultureInfo.InvariantCulture, "Decimal({0}, {1})", DecimalTypeInfoBase.DefaultPrecision, DecimalTypeInfoBase.DefaultScale);
                    break;
                case ClickHouseDbType.Double:
                    typeName = "Float64";
                    break;
                case ClickHouseDbType.Guid:
                    typeName = "UUID";
                    break;
                case ClickHouseDbType.Int16:
                    typeName = "Int16";
                    break;
                case ClickHouseDbType.Int32:
                    typeName = "Int32";
                    break;
                case ClickHouseDbType.Int64:
                    typeName = "Int64";
                    break;
                case ClickHouseDbType.Object:
                    if (Value != null)
                        throw new NotSupportedException();

                    typeName = "Nothing";
                    break;
                case ClickHouseDbType.SByte:
                    typeName = "Int8";
                    break;
                case ClickHouseDbType.Single:
                    typeName = "Float32";
                    break;
                case ClickHouseDbType.String:
                    typeName = "String";
                    break;
                case ClickHouseDbType.UInt16:
                    typeName = "UInt16";
                    break;
                case ClickHouseDbType.UInt32:
                    typeName = "UInt32";
                    break;
                case ClickHouseDbType.UInt64:
                    typeName = "UInt32";
                    break;
                case ClickHouseDbType.VarNumeric:
                    typeName = string.Format(CultureInfo.InvariantCulture, "Decimal({0}, {1})", Precision, Scale);
                    break;
                case ClickHouseDbType.StringFixedLength:
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

                    break;
                }
                case ClickHouseDbType.DateTime2:
                    typeName = "DateTime64(7)";
                    break;
                case ClickHouseDbType.DateTime64:
                    typeName = string.Format(CultureInfo.InvariantCulture, "DateTime64({0})", Precision);
                    break;
                case ClickHouseDbType.DateTimeOffset:
                    typeName = "DateTime";
                    break;
                case ClickHouseDbType.IpV4:
                    typeName = "IPv4";
                    break;
                case ClickHouseDbType.IpV6:
                    typeName = "IPv6";
                    break;
                case ClickHouseDbType.ClickHouseSpecificTypeDelimiterCode:
                    goto default;
                case null:
                    typeName = GetTypeFromValue().clickHouseType;
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
            var clrType = isNull ? typeInfo.GetFieldType() : (preparedValue ?? Value)!.GetType();

            var columnSettings = StringEncoding == null ? null : new ClickHouseColumnSettings(StringEncoding);
            var columnBuilder = new ParameterColumnWriterBuilder(Id, isNull ? null : preparedValue ?? Value, columnSettings, typeInfo);
            var column = TypeDispatcher.Dispatch(clrType, columnBuilder);

            return column;
        }

        private (ClickHouseDbType dbType, string clickHouseType) GetTypeFromValue()
        {
            switch (Value)
            {
                case string _:
                    return (ClickHouseDbType.String, "String");
                case byte[] _:
                    return (ClickHouseDbType.Binary, "Array(UInt8)");
                case byte _:
                    return (ClickHouseDbType.Byte, "UInt8");
                case bool _:
                    return (ClickHouseDbType.Boolean, "UInt8");
                case decimal _:
                    return (ClickHouseDbType.Decimal, string.Format(CultureInfo.InvariantCulture, "Decimal({0}, {1})", DecimalTypeInfoBase.DefaultPrecision, DecimalTypeInfoBase.DefaultScale));
                case DateTime _:
                case DateTimeOffset _:
                    return (ClickHouseDbType.DateTime, "DateTime");
                case double _:
                    return (ClickHouseDbType.Double, "Float64");
                case Guid _:
                    return (ClickHouseDbType.Guid, "UUID");
                case short _:
                    return (ClickHouseDbType.Int16, "Int16");
                case int _:
                    return (ClickHouseDbType.Int32, "Int32");
                case long _:
                    return (ClickHouseDbType.Int64, "Int64");
                case sbyte _:
                    return (ClickHouseDbType.SByte, "Int8");
                case float _:
                    return (ClickHouseDbType.Single, "Float32");
                case ushort _:
                    return (ClickHouseDbType.UInt16, "UInt16");
                case uint _:
                    return (ClickHouseDbType.UInt32, "UInt32");
                case ulong _:
                    return (ClickHouseDbType.UInt64, "UInt64");

                case IPAddress ipAddress:
                    if (ipAddress.AddressFamily == AddressFamily.InterNetwork)
                        return (ClickHouseDbType.IpV4, "IPv4");
                    if (ipAddress.AddressFamily == AddressFamily.InterNetworkV6)
                        return (ClickHouseDbType.IpV6, "IPv6");
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidQueryParameterConfiguration, $"Parameter \"{ParameterName}\". The type \"{ipAddress.AddressFamily}\" of the network address is not supported.");

                case DBNull _:
                case null:
                    return (ClickHouseDbType.Object, "Nothing");
                default:
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidQueryParameterConfiguration, $"The type \"{Value.GetType()}\" of the parameter \"{ParameterName}\" is not supported.");
            }
        }

        private class ParameterColumnWriterBuilder : ITypeDispatcher<IClickHouseColumnWriter>
        {
            private readonly string _parameterId;
            private readonly object? _value;
            private readonly ClickHouseColumnSettings? _columnSettings;
            private readonly IClickHouseColumnTypeInfo _typeInfo;

            public ParameterColumnWriterBuilder(string parameterId, object? value, ClickHouseColumnSettings? columnSettings, IClickHouseColumnTypeInfo typeInfo)
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
