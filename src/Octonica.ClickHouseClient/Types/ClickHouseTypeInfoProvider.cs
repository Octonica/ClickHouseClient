#region License Apache 2.0
/* Copyright 2019-2022 Octonica
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
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Numerics;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;
using NodaTime;

namespace Octonica.ClickHouseClient.Types
{
    #region DefaultTypeInfoProvider (obsolete)

    /// <summary>
    /// This class is obsolete and will be deleted. Use <see cref="ClickHouseTypeInfoProvider"/> instead of <see cref="DefaultTypeInfoProvider"/>.
    /// </summary>
    [Obsolete(nameof(DefaultTypeInfoProvider) + " was renamed to " + nameof(ClickHouseTypeInfoProvider) + ".")]
    public class DefaultTypeInfoProvider : ClickHouseTypeInfoProvider
    {
        /// <summary>
        /// The instance of <see cref="DefaultTypeInfoProvider"/> provides access to all types supported by ClickHouseClient.
        /// </summary>
        /// <remarks>
        /// The class <see cref="DefaultTypeInfoProvider"/> is obsolete.
        /// Use <see cref="ClickHouseTypeInfoProvider.Instance"/> instead of <see cref="Instance"/>.
        /// </remarks>
        [Obsolete(nameof(DefaultTypeInfoProvider) + " was renamed to " + nameof(ClickHouseTypeInfoProvider) + ".")]
        public static readonly new DefaultTypeInfoProvider Instance = new DefaultTypeInfoProvider();

        private DefaultTypeInfoProvider()
            : this(GetDefaultTypes())
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="DefaultTypeInfoProvider"/> with a collection of supported types.
        /// </summary>
        /// <param name="types">The collection of supported types.</param>
        /// <remarks>
        /// The class <see cref="DefaultTypeInfoProvider"/> is obsolete.
        /// Use the base class <see cref="ClickHouseTypeInfoProvider"/> instead of <see cref="DefaultTypeInfoProvider"/>.
        /// </remarks>
        [Obsolete(nameof(DefaultTypeInfoProvider) + " was renamed to " + nameof(ClickHouseTypeInfoProvider) + ".")]
        protected DefaultTypeInfoProvider(IEnumerable<IClickHouseColumnTypeInfo> types)
            : base(types)
        {
        }
    }

    #endregion DefaultTypeInfoProvider (obsolete)

    /// <summary>
    /// The default implementation of the interface <see cref="IClickHouseTypeInfoProvider"/>. This class provides access to
    /// all types supported by ClickHouseClient.
    /// </summary>
    public class ClickHouseTypeInfoProvider : IClickHouseTypeInfoProvider
    {
        /// <summary>
        /// The instance of <see cref="ClickHouseTypeInfoProvider"/> provides access to all types supported by ClickHouseClient.
        /// </summary>
        public static readonly ClickHouseTypeInfoProvider Instance = new ClickHouseTypeInfoProvider();

        private readonly Dictionary<string, IClickHouseColumnTypeInfo> _types;

        private ClickHouseTypeInfoProvider()
            : this(GetDefaultTypes())
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="ClickHouseTypeInfoProvider"/> with a collection of supported types.
        /// </summary>
        /// <param name="types">The collection of supported types.</param>
        /// <remarks>It is possible to get types supported by default with the method <see cref="GetDefaultTypes()"/>.</remarks>
        protected ClickHouseTypeInfoProvider(IEnumerable<IClickHouseColumnTypeInfo> types)
        {
            if (types == null)
                throw new ArgumentNullException(nameof(types));

            _types = types.ToDictionary(t => t.TypeName);
        }

        /// <inheritdoc/>
        public IClickHouseColumnTypeInfo GetTypeInfo(string typeName)
        {
            var typeNameMem = typeName.AsMemory();
            var (baseTypeName, options) = ParseTypeName(typeNameMem);
            
            var result = typeNameMem.Span == baseTypeName.Span ? GetTypeInfo(typeName, options) : GetTypeInfo(baseTypeName.ToString(), options);

            return result ?? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeName}\" is not supported.");
        }

        /// <inheritdoc/>
        public IClickHouseColumnTypeInfo GetTypeInfo(ReadOnlyMemory<char> typeName)
        {
            var (baseTypeName, options) = ParseTypeName(typeName);
            var result = GetTypeInfo(baseTypeName.ToString(), options);

            return result ?? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeName.ToString()}\" is not supported.");
        }

        private IClickHouseColumnTypeInfo? GetTypeInfo(string baseTypeName, List<ReadOnlyMemory<char>>? options)
        {
            if (!_types.TryGetValue(baseTypeName, out var typeInfo))
                return null;

            if (options != null && options.Count > 0)
                typeInfo = typeInfo.GetDetailedTypeInfo(options, this);

            return typeInfo;
        }

        private static (ReadOnlyMemory<char> baseTypeName, List<ReadOnlyMemory<char>>? options) ParseTypeName(ReadOnlyMemory<char> typeName)
        {
            var typeNameSpan = typeName.Span;

            var pOpenIdx = typeNameSpan.IndexOf('(');
            if (pOpenIdx == 0)
                throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The name of the type (\"{typeNameSpan.ToString()}\") can't start with \"(\".");

            ReadOnlyMemory<char> baseTypeName;
            List<ReadOnlyMemory<char>>? options = null;
            if (pOpenIdx < 0)
            {
                baseTypeName = typeName.Trim();
            }
            else
            {
                baseTypeName = typeName.Slice(0, pOpenIdx).Trim();

                int count = 1;
                int currentIdx = pOpenIdx;
                int optionStartIdx = pOpenIdx + 1;
                ReadOnlySpan<char> significantChars = "(,)'`";
                do
                {
                    if (typeNameSpan.Length - 1 == currentIdx)
                        break;

                    var pNextIdx = typeNameSpan.Slice(currentIdx + 1).IndexOfAny(significantChars);
                    if (pNextIdx < 0)
                        break;

                    pNextIdx += currentIdx + 1;
                    currentIdx = pNextIdx;
                    if ("'`".Contains(typeNameSpan[currentIdx]))
                    {
                        var len = ClickHouseSyntaxHelper.GetQuotedTokenLength(typeNameSpan.Slice(currentIdx), typeNameSpan[currentIdx]);
                        if (len < 0)
                            break;

                        Debug.Assert(len > 0);
                        currentIdx += len - 1;
                    }
                    else if (typeNameSpan[currentIdx] == '(')
                    {
                        ++count;
                    }
                    else if (typeNameSpan[currentIdx] == ')')
                    {
                        --count;
                        if (count == 0)
                            break;
                    }
                    else if (count == 1)
                    {
                        var currentOption = typeName.Slice(optionStartIdx, currentIdx - optionStartIdx).Trim();
                        optionStartIdx = currentIdx + 1;

                        if (options != null)
                            options.Add(currentOption);
                        else
                            options = new List<ReadOnlyMemory<char>>(2) {currentOption};
                    }

                } while (true);

                if (count != 0)
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The number of open parentheses doesn't match to the number of close parentheses in the type name \"{typeNameSpan.ToString()}\".");

                if (currentIdx != typeNameSpan.Length - 1)
                {
                    var unexpectedString = typeNameSpan.Slice(currentIdx + 1);
                    if (!unexpectedString.Trim().IsEmpty)
                    {
                        throw new ClickHouseException(
                            ClickHouseErrorCodes.InvalidTypeName,
                            $"There are unexpected characters (\"{unexpectedString.ToString()}\") in the type name \"{typeNameSpan.ToString()}\" after closing parenthesis.");
                    }
                }

                var lastOption = typeName.Slice(optionStartIdx, currentIdx - optionStartIdx).Trim();
                if (options != null)
                    options.Add(lastOption);
                else
                    options = new List<ReadOnlyMemory<char>>(1) {lastOption};
            }

            return (baseTypeName, options);
        }

        /// <inheritdoc/>
        public IClickHouseColumnTypeInfo GetTypeInfo(IClickHouseColumnTypeDescriptor typeDescriptor)
        {
            string? tzCode;
            string typeName;
            IntermediateClickHouseTypeInfo typeInfo;
            switch (typeDescriptor.ClickHouseDbType)
            {
                case ClickHouseDbType.AnsiString:
                case ClickHouseDbType.AnsiStringFixedLength:
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeDescriptor.ClickHouseDbType}\" is not supported. String encoding can be specified with the property \"{nameof(ClickHouseParameter.StringEncoding)}\".");
                case ClickHouseDbType.Array:
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeDescriptor.ClickHouseDbType}\" is not supported. An array could be declared with properties \"{nameof(IClickHouseColumnDescriptor.ArrayRank)}\" or \"{nameof(ClickHouseParameter.IsArray)}\".");
                case ClickHouseDbType.Enum:
                case ClickHouseDbType.Nothing:
                case ClickHouseDbType.Time:
                case ClickHouseDbType.Tuple:
                case ClickHouseDbType.Xml:
                case ClickHouseDbType.Map:
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeDescriptor.ClickHouseDbType}\" is not supported.");

                case ClickHouseDbType.Binary:
                    if (typeDescriptor.Size <= 0)
                    {
                        typeInfo = new IntermediateClickHouseTypeInfo(ClickHouseDbType.Byte, "UInt8", false, 1);
                        goto AFTER_TYPE_INFO_DEFINED;
                    }

                    typeName = string.Format(CultureInfo.InvariantCulture, "FixedString({0})", typeDescriptor.Size);
                    break;
                case ClickHouseDbType.Byte:
                    typeName = "UInt8";
                    break;
                case ClickHouseDbType.Boolean:
                    typeName = "Bool";
                    break;
                case ClickHouseDbType.Currency:
                    typeName = "Decimal(18, 4)";
                    break;
                case ClickHouseDbType.Date:
                    typeName = "Date";
                    break;
                case ClickHouseDbType.Date32:
                    typeName = "Date32";
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
                case ClickHouseDbType.Int128:
                    typeName = "Int128";
                    break;
                case ClickHouseDbType.Int256:
                    typeName = "Int256";
                    break;
                case ClickHouseDbType.Object:
                    if (typeDescriptor.ValueType != typeof(DBNull))
                        throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeDescriptor.ClickHouseDbType}\" is not supported.");

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
                case ClickHouseDbType.UInt128:
                    typeName = "UInt128";
                    break;
                case ClickHouseDbType.UInt256:
                    typeName = "UInt256";
                    break;
                case ClickHouseDbType.VarNumeric:
                    typeName = string.Format(
                        CultureInfo.InvariantCulture,
                        "Decimal({0}, {1})",
                        typeDescriptor.Precision ?? DecimalTypeInfoBase.DefaultPrecision,
                        typeDescriptor.Scale ?? DecimalTypeInfoBase.DefaultScale);

                    break;
                case ClickHouseDbType.StringFixedLength:
                    if (typeDescriptor.Size <= 0)
                        throw new ClickHouseException(ClickHouseErrorCodes.InvalidQueryParameterConfiguration, $"The size of the fixed string must be a positive number.");
                    
                    typeName = string.Format(CultureInfo.InvariantCulture, "FixedString({0})", typeDescriptor.Size);
                    break;
                case ClickHouseDbType.DateTime2:
                    tzCode = GetTimeZoneCode(typeDescriptor.TimeZone);
                    typeName = tzCode == null ? "DateTime64(7)" : $"DateTime64(7, '{tzCode}')";
                    break;
                case ClickHouseDbType.DateTime64:
                    tzCode = GetTimeZoneCode(typeDescriptor.TimeZone);
                    typeName = tzCode == null
                        ? string.Format(CultureInfo.InvariantCulture, "DateTime64({0})", typeDescriptor.Precision ?? DateTime64TypeInfo.DefaultPrecision)
                        : string.Format(CultureInfo.InvariantCulture, "DateTime64({0}, '{1}')", typeDescriptor.Precision ?? DateTime64TypeInfo.DefaultPrecision, tzCode);

                    break;
                case ClickHouseDbType.DateTime:
                case ClickHouseDbType.DateTimeOffset:
                    tzCode = GetTimeZoneCode(typeDescriptor.TimeZone);
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
                    typeInfo = GetTypeFromValue(typeDescriptor.ValueType, typeDescriptor.IsNullable ?? false, typeDescriptor.TimeZone);
                    goto AFTER_TYPE_INFO_DEFINED;
                default:
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"There is no type associated with the value \"{typeDescriptor.ClickHouseDbType}\".");
            }

            if (typeDescriptor.IsNullable != null)
            {
                typeInfo = new IntermediateClickHouseTypeInfo(typeDescriptor.ClickHouseDbType.Value, typeName, typeDescriptor.IsNullable.Value, typeDescriptor.ArrayRank ?? 0);
            }
            else
            {
                // Derive nullability from the value's type. It's important to know whether the value is nullable or not because
                // nullability is a part of ClickHouse type
                var autoType = GetTypeFromValue(typeDescriptor.ValueType, typeDescriptor.IsNullable ?? false, typeDescriptor.TimeZone);
                typeInfo = new IntermediateClickHouseTypeInfo(typeDescriptor.ClickHouseDbType.Value, typeName, autoType.IsNullable, typeDescriptor.ArrayRank ?? 0);
            }

        // This label is an alternative exit point for switch
        // It's a shortcut for several cases when typeInfo is fully defined
        AFTER_TYPE_INFO_DEFINED:

            bool isNull = typeDescriptor.ValueType == typeof(DBNull);
            if (isNull && typeDescriptor.IsNullable == false)
                throw new ClickHouseException(ClickHouseErrorCodes.InvalidQueryParameterConfiguration, $"The value of the type \"{typeDescriptor.ValueType}\" can't be declared as non-nullable.");

            bool isNullable;
            if (typeDescriptor.IsNullable != null)
                isNullable = typeDescriptor.IsNullable.Value;
            else if (isNull)
                isNullable = true;
            else
                isNullable = typeInfo.IsNullable;

            typeName = typeInfo.ClickHouseType;
            if (isNullable)
                typeName = $"Nullable({typeName})";

            var arrayRank = typeDescriptor?.ArrayRank ?? typeInfo.ArrayRank;
            for (int i = 0; i < arrayRank; i++)
                typeName = $"Array({typeName})";

            return GetTypeInfo(typeName);
        }

        internal static IntermediateClickHouseTypeInfo GetTypeFromValue(Type valueType, bool valueCanBeNull, DateTimeZone? timeZone)
        {
            if (valueType == typeof(string))
                return new IntermediateClickHouseTypeInfo(ClickHouseDbType.String, "String", valueCanBeNull, 0);
            if (valueType == typeof(byte))
                return new IntermediateClickHouseTypeInfo(ClickHouseDbType.Byte, "UInt8", false, 0);
            if (valueType == typeof(bool))
                return new IntermediateClickHouseTypeInfo(ClickHouseDbType.Boolean, "Bool", false, 0);
            if (valueType == typeof(decimal))
                return new IntermediateClickHouseTypeInfo(ClickHouseDbType.Decimal, string.Format(CultureInfo.InvariantCulture, "Decimal({0}, {1})", DecimalTypeInfoBase.DefaultPrecision, DecimalTypeInfoBase.DefaultScale), false, 0);
            if (valueType == typeof(double))
                return new IntermediateClickHouseTypeInfo(ClickHouseDbType.Double, "Float64", false, 0);
            if (valueType == typeof(Guid))
                return new IntermediateClickHouseTypeInfo(ClickHouseDbType.Guid, "UUID", false, 0);
            if (valueType == typeof(short))
                return new IntermediateClickHouseTypeInfo(ClickHouseDbType.Int16, "Int16", false, 0);
            if (valueType == typeof(int))
                return new IntermediateClickHouseTypeInfo(ClickHouseDbType.Int32, "Int32", false, 0);
            if (valueType == typeof(long))
                return new IntermediateClickHouseTypeInfo(ClickHouseDbType.Int64, "Int64", false, 0);
            if (valueType == typeof(sbyte))
                return new IntermediateClickHouseTypeInfo(ClickHouseDbType.SByte, "Int8", false, 0);
            if (valueType == typeof(BigInteger))
                return new IntermediateClickHouseTypeInfo(ClickHouseDbType.Int256, "Int256", false, 0);
            if (valueType == typeof(float))
                return new IntermediateClickHouseTypeInfo(ClickHouseDbType.Single, "Float32", false, 0);
            if (valueType == typeof(ushort))
                return new IntermediateClickHouseTypeInfo(ClickHouseDbType.UInt16, "UInt16", false, 0);
            if (valueType == typeof(uint))
                return new IntermediateClickHouseTypeInfo(ClickHouseDbType.UInt32, "UInt32", false, 0);
            if (valueType == typeof(ulong))
                return new IntermediateClickHouseTypeInfo(ClickHouseDbType.UInt64, "UInt64", false, 0);
            if (valueType == typeof(IPAddress))
                return new IntermediateClickHouseTypeInfo(ClickHouseDbType.IpV6, "IPv6", valueCanBeNull, 0);
            if (valueType == typeof(DBNull))
                return new IntermediateClickHouseTypeInfo(ClickHouseDbType.Nothing, "Nothing", true, 0);

#if NET6_0_OR_GREATER
            if (valueType == typeof(DateOnly))
                return new IntermediateClickHouseTypeInfo(ClickHouseDbType.Date, "Date", false, 0);
#endif

            if (valueType == typeof(DateTime) || valueType == typeof(DateTimeOffset))
            {
                var tzCode = GetTimeZoneCode(timeZone);
                return new IntermediateClickHouseTypeInfo(ClickHouseDbType.DateTime, tzCode == null ? "DateTime" : $"DateTime('{tzCode}')", false, 0);
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
                            ClickHouseErrorCodes.TypeNotSupported,
                            $"The type \"{valueType}\" implements \"{typeof(IReadOnlyList<>)}\" at least twice with generic arguments \"{elementType}\" and \"{listElementType}\".");
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
                        return GetTypeFromValue(typeof(string), false, timeZone);
                }
            }

            if (elementType != null)
            {
                try
                {
                    var elementInfo = GetTypeFromValue(elementType, arrayRank > 0 || valueCanBeNull, timeZone);
                    return new IntermediateClickHouseTypeInfo(elementInfo.DbType, elementInfo.ClickHouseType, elementInfo.IsNullable, elementInfo.ArrayRank + arrayRank);
                }
                catch (ClickHouseException ex)
                {
                    if (ex.ErrorCode != ClickHouseErrorCodes.TypeNotSupported)
                        throw;

                    throw new ClickHouseException(
                        ClickHouseErrorCodes.TypeNotSupported,
                        $"The type \"{valueType}\" is not supported. See the inner exception for details.",
                        ex);
                }
            }

            if (valueType.IsGenericType && valueType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                elementType = valueType.GetGenericArguments()[0];
                try
                {
                    var elementInfo = GetTypeFromValue(elementType, false, timeZone);
                    return new IntermediateClickHouseTypeInfo(elementInfo.DbType, elementInfo.ClickHouseType, true, elementInfo.ArrayRank);
                }
                catch (ClickHouseException ex)
                {
                    if (ex.ErrorCode != ClickHouseErrorCodes.TypeNotSupported)
                        throw;

                    throw new ClickHouseException(
                        ClickHouseErrorCodes.TypeNotSupported,
                        $"The type \"{valueType}\" is not supported. See the inner exception for details.",
                        ex);
                }
            }

            throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{valueType}\" is not supported.");
        }

        [return: NotNullIfNotNull("timeZone")]
        private static string? GetTimeZoneCode(DateTimeZone? timeZone)
        {
            if (timeZone == null)
                return null;

            return TimeZoneHelper.GetTimeZoneId(timeZone);
        }

        /// <inheritdoc/>
        public IClickHouseTypeInfoProvider Configure(ClickHouseServerInfo serverInfo)
        {
            if (serverInfo == null)
                throw new ArgumentNullException(nameof(serverInfo));

            return new ClickHouseTypeInfoProvider(_types.Values.Select(t => (t as IClickHouseConfigurableTypeInfo)?.Configure(serverInfo) ?? t));
        }

        /// <summary>
        /// Returns all types supported by the ClickHouseClient.
        /// </summary>
        /// <returns>All types supported by the ClickHouseClient.</returns>
        protected static IEnumerable<IClickHouseColumnTypeInfo> GetDefaultTypes()
        {
            return new IClickHouseColumnTypeInfo[]
            {
                new ArrayTypeInfo(),
                new LowCardinalityTypeInfo(),
                new TupleTypeInfo(),

                new BoolTypeInfo(),

                new DateTypeInfo(),
                new Date32TypeInfo(),
                new DateTimeTypeInfo(),
                new DateTime64TypeInfo(),

                new DecimalTypeInfo(),
                new Decimal32TypeInfo(),
                new Decimal64TypeInfo(),
                new Decimal128TypeInfo(),

                new Float32TypeInfo(),
                new Float64TypeInfo(),

                new Int8TypeInfo(),
                new Int16TypeInfo(),
                new Int32TypeInfo(),
                new Int64TypeInfo(),
                new Int128TypeInfo(),
                new Int256TypeInfo(),

                new UInt8TypeInfo(),
                new UInt16TypeInfo(),
                new UInt32TypeInfo(),
                new UInt64TypeInfo(),
                new UInt128TypeInfo(),
                new UInt256TypeInfo(),

                new StringTypeInfo(),
                new FixedStringTypeInfo(),

                new UuidTypeInfo(),

                new NothingTypeInfo(),
                new NullableTypeInfo(),

                new IpV4TypeInfo(),
                new IpV6TypeInfo(),

                new Enum8TypeInfo(),
                new Enum16TypeInfo(),

                new MapTypeInfo(),
            };
        }
    }
}
