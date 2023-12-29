#region License Apache 2.0
/* Copyright 2019-2023 Octonica
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
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
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
    /// <summary>
    /// Represents a parameter to a <see cref="ClickHouseCommand"/>. This class cannot be inherited.
    /// </summary>
    public sealed class ClickHouseParameter : DbParameter, ICloneable
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

        private IntermediateClickHouseTypeInfo? _valueTypeInfo;

        private string? _sourceColumn;

        internal string Id { get; private set; }

        /// <summary>
        /// Gets the collection to which this parameter is attached.
        /// </summary>
        public ClickHouseParameterCollection? Collection { get; internal set; }

        /// <summary>
        /// Gets or sets the <see cref="ClickHouseDbType"/> of the parameter.
        /// </summary>
        /// <returns>One of the <see cref="ClickHouseDbType"/> values. The default value is defined based on the type of the parameter's value.</returns>
        public ClickHouseDbType ClickHouseDbType
        {
            get => _forcedType ?? GetTypeFromValue().DbType;
            set => _forcedType = value;
        }

        /// <summary>
        /// Gets or sets the <see cref="DbType"/> of the parameter.
        /// </summary>
        /// <returns>
        /// One of the <see cref="DbType"/> values. The default value is defined based on the type of the parameter's value.
        /// For ClickHouse-specific types returns <see cref="DbType.Object"/>.
        /// </returns>        
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

        /// <summary>
        /// Gets the direction of the parameter. Always returns <see cref="ParameterDirection.Input"/>.
        /// </summary>
        /// <returns>Always returns <see cref="ParameterDirection.Input"/>.</returns>
        /// <exception cref="NotSupportedException">Throws <see cref="NotSupportedException"/> on attempt to set the value different from <see cref="ParameterDirection.Input"/>.</exception>
        public override ParameterDirection Direction
        {
            get => ParameterDirection.Input;
            set
            {
                if (value != ParameterDirection.Input)
                    throw new NotSupportedException("Only input parameters are supported.");
            }
        }

        /// <summary>
        /// Gets or sets the value indicating whether the type of the parameter is nullable.
        /// </summary>
        /// <returns><see langword="true"/> if the parameter's value can be NULL; otherwise <see langword="false"/>. The default value is defined based on the type of the parameter's value.</returns>
        public override bool IsNullable
        {
            get => _forcedNullable ?? GetTypeFromValue().IsNullable;
            set => _forcedNullable = value;
        }

        /// <summary>
        /// Gets or sets the name of the parameter.
        /// </summary>
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

        /// <inheritdoc/>
        /// <remarks>This property is ignored by ClickHouseClient.</remarks>
        [AllowNull]
        public override string SourceColumn
        {
            get => _sourceColumn ?? string.Empty;
            set => _sourceColumn = value;
        }

        /// <inheritdoc/>
        public override object? Value
        {
            get => _value;
            set
            {
                _value = value;
                _valueTypeInfo = null;
            }
        }

        /// <inheritdoc/>
        /// <remarks>This property is ignored by ClickHouseClient.</remarks>
        public override bool SourceColumnNullMapping { get; set; }

        /// <summary>
        /// Gets or sets the size. This value is applied to the ClickHouse type FixedString.
        /// </summary>
        public override int Size
        {
            get => _size;
            set
            {
                _size = value;
                _valueTypeInfo = null;
            }
        }

        /// <summary>
        /// Gets or sets the precision. This value is applied to ClickHouse types Decimal and DateTime64.
        /// </summary>
        public override byte Precision
        {
            get => _forcedPrecision ?? (ClickHouseDbType == ClickHouseDbType.DateTime64 ? (byte) DateTime64TypeInfo.DefaultPrecision : DecimalTypeInfoBase.DefaultPrecision);
            set => _forcedPrecision = value;
        }

        /// <summary>
        /// Gets or sets the scale. This value is applied to the ClickHouse type Decimal.
        /// </summary>
        public override byte Scale
        {
            get => _forcedScale ?? DecimalTypeInfoBase.DefaultScale;
            set => _forcedScale = value;
        }

        /// <summary>
        /// Gets or sets the encoding that will be used when writing a string value to the database.
        /// </summary>
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

        /// <summary>
        /// Gets or sets the value indicating whether the type is an array.
        /// </summary>
        /// <returns><see langword="true"/> if the value is an array; otherwise <see langword="false"/>. The default value is defined based on the <see cref="ArrayRank"/>.</returns>
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

        /// <summary>
        /// Gets or sets the rank (a number of dimensions) of an array.
        /// </summary>
        /// <returns>The rank of an array. 0 if the type is not an array. The default value is defined based on the type of the parameter's value.</returns>
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

        /// <summary>
        /// Gets or sets the mode of passing this parameter to the query. The value of this property overrides <see cref="ClickHouseCommand.ParametersMode"/>.
        /// </summary>
        /// <returns>The mode of passing this parameter to the query. The default value is <see cref="ClickHouseParameterMode.Inherit"/>.</returns>
        public ClickHouseParameterMode ParameterMode { get; set; } = ClickHouseParameterMode.Inherit;

        /// <summary>
        /// Initializes a new instance of <see cref="ClickHouseParameter"/> with the default name.
        /// </summary>
        public ClickHouseParameter()
            : this("parameter")
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="ClickHouseParameter"/> with the specified name.
        /// </summary>
        /// <param name="parameterName">The name of the parameter.</param>
        public ClickHouseParameter(string parameterName)
        {
            if (parameterName == null)
                throw new ArgumentNullException(nameof(parameterName));

            Id = GetId(parameterName);
            _parameterName = parameterName;
        }

        /// <inheritdoc/>
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

        /// <summary>
        /// Creates a new <see cref="ClickHouseParameter"/> that is a copy of this instance. The new parameter is not attached to any <see cref="ClickHouseParameterCollection"/>.
        /// </summary>
        /// <returns>A new <see cref="ClickHouseParameter"/> that is a copy of this instance.</returns>
        public ClickHouseParameter Clone()
        {
            var result = new ClickHouseParameter(ParameterName);
            CopyTo(result);
            return result;
        }

        /// <summary>
        /// Copies all properties except <see cref="ParameterName"/> of this parameter to the target parameter.
        /// </summary>
        /// <param name="parameter">The parameter to which the properties of this parameters should be copied.</param>
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
            parameter.ParameterMode = ParameterMode;
        }

        internal ClickHouseParameterWriter CreateParameterWriter(IClickHouseTypeInfoProvider typeInfoProvider)
        {
            return CreateParameterWriter(typeInfoProvider, Create);

            static ClickHouseParameterWriter Create(string id, object? value, ClickHouseColumnSettings? columnSettings, IClickHouseColumnTypeInfo typeInfo, Type clrType)
            {
                return ClickHouseParameterWriter.Dispatch(typeInfo, value);
            }
        }

        internal IClickHouseColumnWriter CreateParameterColumnWriter(IClickHouseTypeInfoProvider typeInfoProvider)
        {
            return CreateParameterWriter(typeInfoProvider, Create);

            static IClickHouseColumnWriter Create(string id, object? value, ClickHouseColumnSettings? columnSettings, IClickHouseColumnTypeInfo typeInfo, Type clrType)
            {
                var columnBuilder = new ParameterColumnWriterBuilder(id, value, columnSettings, typeInfo);
                var column = TypeDispatcher.Dispatch(clrType, columnBuilder);
                return column;
            }
        }

        private T CreateParameterWriter<T>(IClickHouseTypeInfoProvider typeInfoProvider, Func<string, object?, ClickHouseColumnSettings?, IClickHouseColumnTypeInfo, Type, T> createWriter)
        {
            bool isNull = Value == DBNull.Value || Value == null;
            if (isNull && _forcedNullable == false)
                throw new ClickHouseException(ClickHouseErrorCodes.InvalidQueryParameterConfiguration, $"The parameter \"{ParameterName}\" is declared as non-nullable but it's value is null.");

            var typeInfo = GetTypeInfo(typeInfoProvider);
            object? preparedValue = null;
            if (_forcedType == ClickHouseDbType.StringFixedLength)
            {
                Debug.Assert(typeInfo.TypeName == "FixedString");

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
            }

            var clrType = isNull ? typeInfo.GetFieldType() : (preparedValue ?? Value)!.GetType();
            var columnSettings = StringEncoding == null ? null : new ClickHouseColumnSettings(StringEncoding);
            return createWriter(Id, isNull ? null : preparedValue ?? Value, columnSettings, typeInfo, clrType);
        }

        internal IClickHouseColumnTypeInfo GetTypeInfo(IClickHouseTypeInfoProvider typeInfoProvider)
        {
            var adapter = new ParameterColumnTypeDescriptorAdapter(this);
            try
            {
                return typeInfoProvider.GetTypeInfo(adapter);
            }
            catch (ClickHouseException ex)
            {
                throw new ClickHouseException(ex.ErrorCode, $"Parameter \"{ParameterName}\". {ex.Message}", ex);
            }
        }

        private IntermediateClickHouseTypeInfo GetTypeFromValue()
        {
            if (_valueTypeInfo != null)
                return _valueTypeInfo.Value;

            var result = GetValueDependentType();

            if (result == null)
            {
                try
                {
                    result = ClickHouseTypeInfoProvider.GetTypeFromValue(Value?.GetType() ?? typeof(DBNull), Value == null, TimeZone);
                }
                catch (ClickHouseException ex)
                {
                    throw new ClickHouseException(ex.ErrorCode, $"Parameter \"{ParameterName}\". {ex.Message}", ex);
                }
            }

            _valueTypeInfo = result;
            return result.Value;
        }

        private IntermediateClickHouseTypeInfo? GetValueDependentType()
        {
            if (Value is IPAddress ipAddress)
            {
                if (ipAddress.AddressFamily == AddressFamily.InterNetwork)
                    return new IntermediateClickHouseTypeInfo(ClickHouseDbType.IpV4, "IPv4", false, 0);

                if (ipAddress.AddressFamily == AddressFamily.InterNetworkV6)
                    return new IntermediateClickHouseTypeInfo(ClickHouseDbType.IpV6, "IPv6", false, 0);

                throw new ClickHouseException(
                        ClickHouseErrorCodes.InvalidQueryParameterConfiguration,
                        $"Parameter \"{ParameterName}\". The type \"{ipAddress.AddressFamily}\" of the network address is not supported.");
            }

            return null;
        }

        /// <summary>
        /// Checks if the provided string is a valid name for a parameter.
        /// </summary>
        /// <param name="parameterName">The string to check.</param>
        /// <returns><see langword="true"/> if the string is a valid parameter name; otherwise <see langword="false"/>.</returns>
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

        internal ClickHouseParameterMode GetParameterMode(ClickHouseParameterMode inheritParameterMode)
        {
            var mode = ParameterMode;
            if (mode == ClickHouseParameterMode.Inherit)
                return inheritParameterMode;

            return mode;
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
            [AllowNull] private readonly object _value;
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

        private class ParameterColumnTypeDescriptorAdapter : IClickHouseColumnTypeDescriptor
        {
            private readonly ClickHouseParameter _parameter;

            public ClickHouseDbType? ClickHouseDbType => _parameter._forcedType ?? _parameter.GetValueDependentType()?.DbType;

            public Type ValueType => _parameter?.Value?.GetType() ?? typeof(DBNull);

            public bool? IsNullable => _parameter._forcedNullable;

            public int Size => _parameter._size;

            public byte? Precision => _parameter._forcedPrecision;

            public byte? Scale => _parameter._forcedScale;

            public TimeZoneInfo? TimeZone => _parameter.TimeZone;

            public int? ArrayRank => _parameter._forcedArrayRank;

            public ParameterColumnTypeDescriptorAdapter(ClickHouseParameter parameter)
            {
                _parameter = parameter;
            }
        }
    }
}
