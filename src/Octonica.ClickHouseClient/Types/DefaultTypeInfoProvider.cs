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
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;

namespace Octonica.ClickHouseClient.Types
{
    public class DefaultTypeInfoProvider : IClickHouseTypeInfoProvider
    {
        public static readonly DefaultTypeInfoProvider Instance = new DefaultTypeInfoProvider();

        private readonly Dictionary<string, IClickHouseColumnTypeInfo> _types;

        private DefaultTypeInfoProvider()
            : this(GetDefaultTypes())
        {
        }

        protected DefaultTypeInfoProvider(IEnumerable<IClickHouseColumnTypeInfo> types)
        {
            if (types == null)
                throw new ArgumentNullException(nameof(types));

            _types = types.ToDictionary(t => t.TypeName);
        }

        public IClickHouseColumnTypeInfo GetTypeInfo(string typeName)
        {
            var typeNameMem = typeName.AsMemory();
            var (baseTypeName, options) = ParseTypeName(typeNameMem);
            
            var result = typeNameMem.Span == baseTypeName.Span ? GetTypeInfo(typeName, options) : GetTypeInfo(baseTypeName.ToString(), options);

            return result ?? throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeName}\" is not supported.");
        }

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

        public IClickHouseTypeInfoProvider Configure(ClickHouseServerInfo serverInfo)
        {
            if (serverInfo == null)
                throw new ArgumentNullException(nameof(serverInfo));

            return new DefaultTypeInfoProvider(_types.Values.Select(t => (t as IClickHouseConfigurableTypeInfo)?.Configure(serverInfo) ?? t));
        }

        protected static IEnumerable<IClickHouseColumnTypeInfo> GetDefaultTypes()
        {
            return new IClickHouseColumnTypeInfo[]
            {
                new ArrayTypeInfo(),
                new LowCardinalityTypeInfo(),
                new TupleTypeInfo(),

                new DateTypeInfo(),
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

                new UInt8TypeInfo(),
                new UInt16TypeInfo(),
                new UInt32TypeInfo(),
                new UInt64TypeInfo(),

                new StringTypeInfo(),
                new FixedStringTypeInfo(),

                new UuidTypeInfo(),

                new NothingTypeInfo(),
                new NullableTypeInfo(),

                new IpV4TypeInfo(),
                new IpV6TypeInfo(),

                new Enum8TypeInfo(),
                new Enum16TypeInfo(),
            };
        }
    }
}
