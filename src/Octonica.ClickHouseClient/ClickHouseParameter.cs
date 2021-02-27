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
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Types;
using Octonica.ClickHouseClient.Utils;
using TimeZoneConverter;

namespace Octonica.ClickHouseClient
{
    public class ClickHouseParameter : DbParameter, ICloneable
    {
        // https://github.com/ClickHouse/ClickHouse/blob/master/docs/en/query_language/syntax.md
        private static readonly Regex ParameterNameRegex = new Regex("^[a-zA-Z_][0-9a-zA-Z_]*$");

        private string _parameterName;

        private object? _value;
        private int _size;
        private TimeZoneInfo? _timeZone;

        private bool? _forcedNullable;
        private ClickHouseDbType? _forcedType;
        private byte? _forcedScale;
        private byte? _forcedPrecision;
        private int? _forcedArrayRank;

        private ValueTypeInfo? _valueTypeInfo;

        private string? _sourceColumn;

        internal string Id { get; private set; }

        public ClickHouseParameterCollection? Collection { get; internal set; }

        public ClickHouseDbType ClickHouseDbType
        {
            get => _forcedType ?? GetTypeFromValue().DbType;
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
            get => _forcedNullable ?? GetTypeFromValue().IsNullable;
            set => _forcedNullable = value;
        }

        [AllowNull]
        public sealed override string ParameterName
        {
            get => _parameterName;
            set
            {
                var id = GetId(value);
                Debug.Assert(value != null);

                if (StringComparer.Ordinal.Equals(id, Id))
                {
                    _parameterName = value;
                    return;
                }

                if (Collection == null)
                {
                    Id = id;
                    _parameterName = value;
                    return;
                }

                var oldId = Id;
                var oldParameterName = _parameterName;
                Id = id;
                _parameterName = value;

                try
                {
                    Collection.OnParameterIdChanged(oldId, this);
                }
                catch
                {
                    Id = oldId;
                    _parameterName = oldParameterName;
                    throw;
                }
            }
        }

        [AllowNull]
        public override string SourceColumn
        {
            get => _sourceColumn ?? string.Empty;
            set => _sourceColumn = value;
        }

        public override object? Value
        {
            get => _value;
            set
            {
                _value = value;
                _valueTypeInfo = null;
            }
        }

        public override bool SourceColumnNullMapping { get; set; }

        public override int Size
        {
            get => _size;
            set
            {
                _size = value;
                _valueTypeInfo = null;
            }
        }

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

        /// <summary>
        /// This property allows to specify the timezone for datetime types (<see cref="ClickHouseClient.ClickHouseDbType.DateTime"/>,
        /// <see cref="ClickHouseClient.ClickHouseDbType.DateTimeOffset"/>, <see cref="ClickHouseClient.ClickHouseDbType.DateTime2"/>
        /// and <see cref="ClickHouseClient.ClickHouseDbType.DateTime64"/>).
        /// </summary>
        public TimeZoneInfo? TimeZone
        {
            get => _timeZone;
            set
            {
                _timeZone = value;
                _valueTypeInfo = null;
            }
        }

        public bool IsArray
        {
            get => ArrayRank > 0;
            set
            {
                if (value)
                {
                    if (_forcedArrayRank == null || ArrayRank == 0)
                        ArrayRank = 1;
                }
                else
                {
                    if (_forcedArrayRank == null || ArrayRank > 0)
                        ArrayRank = 0;
                }
            }
        }

        public int ArrayRank
        {
            get
            {
                if (_forcedArrayRank != null)
                    return _forcedArrayRank.Value;

                var typeInfo = GetTypeFromValue();
                if (typeInfo.ArrayRank > 0 && typeInfo.DbType == ClickHouseDbType.Byte && (_forcedType == ClickHouseDbType.String || _forcedType == ClickHouseDbType.StringFixedLength))
                    return typeInfo.ArrayRank - 1;

                return typeInfo.ArrayRank;
            }
            set
            {
                if (value < 0)
                    throw new ArgumentException("The rank of an array must be a non-negative number.", nameof(value));

                _forcedArrayRank = value;
            }
        }

        public ClickHouseParameter()
            : this("parameter")
        {
        }

        public ClickHouseParameter(string parameterName)
        {
            if (parameterName == null)
                throw new ArgumentNullException(nameof(parameterName));

            Id = GetId(parameterName);
            _parameterName = parameterName;
        }

        public override void ResetDbType()
        {
            _forcedType = null;
            _forcedNullable = null;
            _forcedPrecision = null;
            _forcedScale = null;
            _forcedArrayRank = null;

            _size = 0;
            StringEncoding = null;
            _timeZone = null;
            _valueTypeInfo = null;
        }

        object ICloneable.Clone()
        {
            return Clone();
        }

        public virtual ClickHouseParameter Clone()
        {
            var result = new ClickHouseParameter(ParameterName);
            CopyTo(result);
            return result;
        }

        /// <summary>
        /// Copy all properties except <see cref="ParameterName"/> of this parameter to the target parameter.
        /// </summary>
        public void CopyTo(ClickHouseParameter parameter)
        {
            parameter._forcedType = _forcedType;
            parameter._forcedArrayRank = _forcedArrayRank;
            parameter._forcedNullable = _forcedNullable;
            parameter._forcedPrecision = _forcedPrecision;
            parameter._forcedScale = _forcedScale;
            parameter._forcedArrayRank = _forcedArrayRank;

            parameter._size = _size;
            parameter.StringEncoding = StringEncoding;
            parameter._timeZone = _timeZone;
            parameter._value = _value;

            parameter.SourceColumn = SourceColumn;
            parameter.SourceColumnNullMapping = SourceColumnNullMapping;
            parameter.SourceVersion = SourceVersion;

            parameter._valueTypeInfo = null;
        }

        internal IClickHouseColumnWriter CreateParameterColumnWriter(IClickHouseTypeInfoProvider typeInfoProvider)
        {
            string typeName;
            object? preparedValue = null;
            string? tzCode;
            switch (_forcedType)
            {
                case ClickHouseDbType.AnsiString:
                case ClickHouseDbType.AnsiStringFixedLength:
                    throw new NotSupportedException($"Parameter \"{ParameterName}\". The type \"{_forcedType}\" is not supported. String encoding can be specified with the property \"{nameof(StringEncoding)}\".");
                case ClickHouseDbType.Array:
                    throw new NotSupportedException($"Parameter \"{ParameterName}\". The type \"{_forcedType}\" is not supported. An array could be declared with properties \"{nameof(ArrayRank)}\" or \"{nameof(IsArray)}\".");
                case ClickHouseDbType.Enum:
                case ClickHouseDbType.Nothing:
                case ClickHouseDbType.Time:
                case ClickHouseDbType.Tuple:
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
                    typeName = "UInt64";
                    break;
                case ClickHouseDbType.VarNumeric:
                    typeName = string.Format(CultureInfo.InvariantCulture, "Decimal({0}, {1})", Precision, Scale);
                    break;
                case ClickHouseDbType.StringFixedLength:
                {
                    if (Size <= 0)
                    {
                        throw new ClickHouseException(
                            ClickHouseErrorCodes.InvalidQueryParameterConfiguration,
                            $"Parameter \"{ParameterName}\". The size of the fixed string must be a positive number.");
                    }

                    typeName = string.Format(CultureInfo.InvariantCulture, "FixedString({0})", Size);
                    
                    ReadOnlySpan<char> strSpan = default;
                    bool isStr = false;
                    if (Value is string strValue)
                    {
                        strSpan = strValue;
                        isStr = true;
                    }
                    else if (Value is Memory<char> mem)
                    {
                        strSpan = mem.Span;
                        isStr = true;
                    }
                    else if (Value is ReadOnlyMemory<char> roMem)
                    {
                        strSpan = roMem.Span;
                        isStr = true;
                    }

                    if (isStr)
                    {
                        var encoding = StringEncoding ?? Encoding.UTF8;
                        var size = encoding.GetByteCount(strSpan);
                        if (size > Size)
                        {
                            throw new ClickHouseException(
                                ClickHouseErrorCodes.InvalidQueryParameterConfiguration,
                                $"Parameter \"{ParameterName}\". The length of the string in bytes with encoding \"{encoding.EncodingName}\" is greater than the size of the parameter.");
                        }

                        var bytes = new byte[size];
                        encoding.GetBytes(strSpan, bytes);
                        preparedValue = bytes;
                    }

                    break;
                }
                case ClickHouseDbType.DateTime2:
                    tzCode = GetTimeZoneCode();
                    typeName = tzCode == null ? "DateTime64(7)" : $"DateTime64(7, '{tzCode}')";
                    break;
                case ClickHouseDbType.DateTime64:
                    tzCode = GetTimeZoneCode();
                    typeName = tzCode == null
                        ? string.Format(CultureInfo.InvariantCulture, "DateTime64({0})", Precision)
                        : string.Format(CultureInfo.InvariantCulture, "DateTime64({0}, '{1}')", Precision, tzCode);

                    break;
                case ClickHouseDbType.DateTime:
                case ClickHouseDbType.DateTimeOffset:
                    tzCode = GetTimeZoneCode();
                    typeName = tzCode == null ? "DateTime" : $"DateTime('{tzCode}')";
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
                    typeName = GetTypeFromValue().ClickHouseType;
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            bool isNull = Value == DBNull.Value || Value == null;
            if (isNull && _forcedNullable == false)
                throw new ClickHouseException(ClickHouseErrorCodes.InvalidQueryParameterConfiguration, $"The parameter \"{ParameterName}\" is declared as non-nullable but its value is null.");

            bool isNullable;
            if (_forcedNullable != null)
                isNullable = _forcedNullable.Value;
            else if (isNull)
                isNullable = true;
            else
                isNullable = GetTypeFromValue().IsNullable;

            if (isNullable)
                typeName = $"Nullable({typeName})";

            int arrayRank = ArrayRank;
            for (int i = 0; i < arrayRank; i++)
                typeName = $"Array({typeName})";

            var typeInfo = typeInfoProvider.GetTypeInfo(typeName);
            var clrType = isNull ? typeInfo.GetFieldType() : (preparedValue ?? Value)!.GetType();
            var columnSettings = StringEncoding == null ? null : new ClickHouseColumnSettings(StringEncoding);
            var columnBuilder = new ParameterColumnWriterBuilder(Id, isNull ? null : preparedValue ?? Value, columnSettings, typeInfo);
            var column = TypeDispatcher.Dispatch(clrType, columnBuilder);
            return column;
        }

        private ValueTypeInfo GetTypeFromValue()
        {
            if (_valueTypeInfo != null)
                return _valueTypeInfo.Value;

            ValueTypeInfo result;
            if (Value == null)
            {
                result = new ValueTypeInfo(ClickHouseDbType.Nothing, "Nothing", true, 0);
            }
            else if (Value is IPAddress ipAddress)
            {
                if (ipAddress.AddressFamily == AddressFamily.InterNetwork)
                {
                    result = new ValueTypeInfo(ClickHouseDbType.IpV4, "IPv4", false, 0);
                }
                else if (ipAddress.AddressFamily == AddressFamily.InterNetworkV6)
                {
                    result = new ValueTypeInfo(ClickHouseDbType.IpV6, "IPv6", false, 0);
                }
                else
                {
                    throw new ClickHouseException(
                        ClickHouseErrorCodes.InvalidQueryParameterConfiguration,
                        $"Parameter \"{ParameterName}\". The type \"{ipAddress.AddressFamily}\" of the network address is not supported.");
                }
            }
            else
            {
                result = GetTypeFromValue(Value.GetType(), false);
            }

            _valueTypeInfo = result;
            return result;
        }

        private ValueTypeInfo GetTypeFromValue(Type valueType, bool valueCanBeNull)
        {
            if (valueType == typeof(string))
                return new ValueTypeInfo(ClickHouseDbType.String, "String", valueCanBeNull, 0);
            if (valueType == typeof(byte))
                return new ValueTypeInfo(ClickHouseDbType.Byte, "UInt8", false, 0);
            if (valueType == typeof(bool))
                return new ValueTypeInfo(ClickHouseDbType.Boolean, "UInt8", false, 0);
            if (valueType == typeof(decimal))
                return new ValueTypeInfo(ClickHouseDbType.Decimal, string.Format(CultureInfo.InvariantCulture, "Decimal({0}, {1})", DecimalTypeInfoBase.DefaultPrecision, DecimalTypeInfoBase.DefaultScale), false, 0);
            if (valueType == typeof(double))
                return new ValueTypeInfo(ClickHouseDbType.Double, "Float64", false, 0);
            if (valueType == typeof(Guid))
                return new ValueTypeInfo(ClickHouseDbType.Guid, "UUID", false, 0);
            if (valueType == typeof(short))
                return new ValueTypeInfo(ClickHouseDbType.Int16, "Int16", false, 0);
            if (valueType == typeof(int))
                return new ValueTypeInfo(ClickHouseDbType.Int32, "Int32", false, 0);
            if (valueType == typeof(long))
                return new ValueTypeInfo(ClickHouseDbType.Int64, "Int64", false, 0);
            if (valueType == typeof(sbyte))
                return new ValueTypeInfo(ClickHouseDbType.SByte, "Int8", false, 0);
            if (valueType == typeof(float))
                return new ValueTypeInfo(ClickHouseDbType.Single, "Float32", false, 0);
            if (valueType == typeof(ushort))
                return new ValueTypeInfo(ClickHouseDbType.UInt16, "UInt16", false, 0);
            if (valueType == typeof(uint))
                return new ValueTypeInfo(ClickHouseDbType.UInt32, "UInt32", false, 0);
            if (valueType == typeof(ulong))
                return new ValueTypeInfo(ClickHouseDbType.UInt64, "UInt64", false, 0);
            if (valueType == typeof(IPAddress))
                return new ValueTypeInfo(ClickHouseDbType.IpV6, "IPv6", valueCanBeNull, 0);
            if (valueType == typeof(DBNull))
                return new ValueTypeInfo(ClickHouseDbType.Nothing, "Nothing", true, 0);

            if (valueType == typeof(DateTime) || valueType == typeof(DateTimeOffset))
            {
                var tzCode = GetTimeZoneCode();
                return new ValueTypeInfo(ClickHouseDbType.DateTime, tzCode == null ? "DateTime" : $"DateTime('{tzCode}')", false, 0);
            }

            int arrayRank = 1;
            Type? elementType = null;
            if (valueType.IsArray)
            {
                arrayRank = valueType.GetArrayRank();
                elementType = valueType.GetElementType();

                if (elementType == typeof(char))
                {
                    elementType = typeof(string);
                    --arrayRank;
                }
            }
            else
            {
                foreach (var itf in valueType.GetInterfaces())
                {
                    if (!itf.IsGenericType)
                        continue;

                    if (itf.GetGenericTypeDefinition() != typeof(IReadOnlyList<>))
                        continue;

                    var listElementType = itf.GetGenericArguments()[0];
                    if (elementType != null)
                    {
                        throw new ClickHouseException(
                            ClickHouseErrorCodes.InvalidQueryParameterConfiguration,
                            $"The type \"{valueType}\" of the parameter \"{ParameterName}\" implements \"{typeof(IReadOnlyList<>)}\" at least twice with generic arguments \"{elementType}\" and \"{listElementType}\".");
                    }

                    elementType = listElementType;
                }
            }

            if (elementType == null && valueType.IsGenericType)
            {
                var valueTypeDef = valueType.GetGenericTypeDefinition();
                if (valueTypeDef == typeof(Memory<>) || valueTypeDef == typeof(ReadOnlyMemory<>))
                {
                    elementType = valueType.GetGenericArguments()[0];

                    // Memory<char> or ReadOnlyMemory<char> should be interpreted as string
                    if (elementType == typeof(char))
                        return GetTypeFromValue(typeof(string), false);
                }
            }

            if (elementType != null)
            {
                try
                {
                    var elementInfo = GetTypeFromValue(elementType, arrayRank > 0 || valueCanBeNull);
                    return new ValueTypeInfo(elementInfo.DbType, elementInfo.ClickHouseType, elementInfo.IsNullable, elementInfo.ArrayRank + arrayRank);
                }
                catch (ClickHouseException ex)
                {
                    if (ex.ErrorCode != ClickHouseErrorCodes.InvalidQueryParameterConfiguration)
                        throw;

                    throw new ClickHouseException(
                        ClickHouseErrorCodes.InvalidQueryParameterConfiguration,
                        $"The type \"{valueType}\" of the parameter \"{ParameterName}\" is not supported. See the inner exception for details.",
                        ex);
                }
            }

            if (valueType.IsGenericType && valueType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                elementType = valueType.GetGenericArguments()[0];
                try
                {
                    var elementInfo = GetTypeFromValue(elementType, false);
                    return new ValueTypeInfo(elementInfo.DbType, elementInfo.ClickHouseType, true, elementInfo.ArrayRank);
                }
                catch (ClickHouseException ex)
                {
                    if (ex.ErrorCode != ClickHouseErrorCodes.InvalidQueryParameterConfiguration)
                        throw;

                    throw new ClickHouseException(
                        ClickHouseErrorCodes.InvalidQueryParameterConfiguration,
                        $"The type \"{valueType}\" of the parameter \"{ParameterName}\" is not supported. See the inner exception for details.",
                        ex);
                }
            }

            throw new ClickHouseException(ClickHouseErrorCodes.InvalidQueryParameterConfiguration, $"The type \"{valueType}\" of the parameter \"{ParameterName}\" is not supported.");
        }

        private string? GetTimeZoneCode()
        {
            var tzCode = TimeZone?.Id;
            if (tzCode != null && RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                tzCode = TZConvert.WindowsToIana(tzCode);

            return tzCode;
        }

        public static bool IsValidParameterName(string? parameterName)
        {
            return ValidateParameterName(parameterName, out _);
        }

        private static string GetId(string? parameterName)
        {
            if (!ValidateParameterName(parameterName, out var id))
                throw new ArgumentException("The name of the parameter must be a valid ClickHouse identifier.", nameof(parameterName));

            return id;
        }

        private static bool ValidateParameterName(string? parameterName, [MaybeNullWhen(false)] out string id)
        {
            if (string.IsNullOrWhiteSpace(parameterName))
            {
                id = null;
                return false;
            }

            id = TrimParameterName(parameterName);
            return ParameterNameRegex.IsMatch(id);
        }

        internal static string TrimParameterName(string parameterName)
        {
            if (parameterName.Length > 0)
            {
                if (parameterName[0] == '{' && parameterName[^1] == '}')
                    return parameterName[1..^1];

                // MSSQL-style parameter name
                if (parameterName[0] == '@')
                    return parameterName[1..];
            }

            return parameterName;
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
                var singleElementColumn = new ConstantReadOnlyList<T>((T) _value, 1);
                return _typeInfo.CreateColumnWriter(_parameterId, singleElementColumn, _columnSettings);
            }
        }

        private readonly struct ValueTypeInfo
        {
            public ClickHouseDbType DbType { get; }
            public string ClickHouseType { get; }
            public bool IsNullable { get; }
            public int ArrayRank { get; }

            public ValueTypeInfo(ClickHouseDbType dbType, string clickHouseType, bool isNullable, int arrayRank)
            {
                DbType = dbType;
                ClickHouseType = clickHouseType;
                IsNullable = isNullable;
                ArrayRank = arrayRank;
            }
        }
    }
}
