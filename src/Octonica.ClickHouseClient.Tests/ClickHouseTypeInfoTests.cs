#region License Apache 2.0
/* Copyright 2021 Octonica
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
using System.Linq;
using Octonica.ClickHouseClient.Types;
using Xunit;

namespace Octonica.ClickHouseClient.Tests
{
    public class ClickHouseTypeInfoTests
    {
        [Theory]
        [InlineData("Nullable(Nothing)", "Nothing")]
        [InlineData("Nullable( String )", "String")]
        [InlineData("Nullable ( DateTime( 'Asia/Yekaterinburg' ) )", "DateTime('Asia/Yekaterinburg')")]
        public void NullableGenericArguments(string typeName, string baseTypeName)
        {
            var typeInfo = DefaultTypeInfoProvider.Instance.GetTypeInfo(typeName);
            
            Assert.Equal(1, typeInfo.GenericArgumentsCount);
            Assert.Equal(1, typeInfo.TypeArgumentsCount);

            Assert.Equal(baseTypeName, typeInfo.GetGenericArgument(0).ComplexTypeName);
            Assert.Equal(baseTypeName, Assert.IsAssignableFrom<IClickHouseTypeInfo>(typeInfo.GetTypeArgument(0)).ComplexTypeName);
        }

        [Theory]
        [InlineData("LowCardinality(String)", "String")]
        [InlineData("LowCardinality( Decimal ( 28, 10 ))", "Decimal(28, 10)")]
        public void LowCardinalityGenericArguments(string typeName, string baseTypeName)
        {
            var typeInfo = DefaultTypeInfoProvider.Instance.GetTypeInfo(typeName);

            Assert.Equal(1, typeInfo.GenericArgumentsCount);
            Assert.Equal(1, typeInfo.TypeArgumentsCount);

            Assert.Equal(baseTypeName, typeInfo.GetGenericArgument(0).ComplexTypeName);
            Assert.Equal(baseTypeName, Assert.IsAssignableFrom<IClickHouseTypeInfo>(typeInfo.GetTypeArgument(0)).ComplexTypeName);
        }

        [Fact]
        public void TupleGenericArguments()
        {
            var typeNames = new[] {"Decimal(19, 6)", "String", "Nullable(String)", "DateTime64(5, 'Europe/Prague')", "UInt8", "Int32", "Float32", "Enum8('a'=10, 'b'=20)", "UInt64"};

            for (int i = 1; i <= typeNames.Length; i++)
            {
                var typeName = "Tuple(" + string.Join(',', typeNames.Take(i)) + ')';
                var typeInfo = DefaultTypeInfoProvider.Instance.GetTypeInfo(typeName);

                Assert.Equal(i, typeInfo.GenericArgumentsCount);
                Assert.Equal(i, typeInfo.TypeArgumentsCount);
                for (int j = 0; j < i; j++)
                {
                    IClickHouseTypeInfo baseType = typeInfo.GetGenericArgument(j);
                    Assert.Equal(typeNames[j], baseType.ComplexTypeName);
                    
                    var typeArgument = typeInfo.GetTypeArgument(j);
                    baseType = Assert.IsAssignableFrom<IClickHouseTypeInfo>(typeArgument);
                    Assert.Equal(typeNames[j], baseType.ComplexTypeName);
                }
            }
        }

        [Theory]
        [InlineData("Array(Int32)", "Int32", null)]
        [InlineData("Array(Nullable(Int32))", "Nullable(Int32)", "Int32")]
        [InlineData("Array(Array(Nothing))", "Array(Nothing)", "Nothing")]
        public void ArrayGenericArguments(string typeName, string baseTypeName, string? baseBaseTypeName)
        {
            var typeInfo = DefaultTypeInfoProvider.Instance.GetTypeInfo(typeName);

            Assert.Equal(1, typeInfo.GenericArgumentsCount);
            Assert.Equal(1, typeInfo.TypeArgumentsCount);

            var baseType = typeInfo.GetGenericArgument(0);

            Assert.Equal(baseTypeName, baseType.ComplexTypeName);
            Assert.Equal(baseTypeName, Assert.IsAssignableFrom<IClickHouseTypeInfo>(typeInfo.GetTypeArgument(0)).ComplexTypeName);

            if (baseBaseTypeName == null)
            {
                Assert.Equal(0, baseType.GenericArgumentsCount);
                return;
            }

            Assert.Equal(1, baseType.GenericArgumentsCount);
            Assert.Equal(1, baseType.TypeArgumentsCount);

            Assert.Equal(baseBaseTypeName, baseType.GetGenericArgument(0).ComplexTypeName);
            Assert.Equal(baseBaseTypeName, Assert.IsAssignableFrom<IClickHouseTypeInfo>(baseType.GetTypeArgument(0)).ComplexTypeName);
        }

        [Theory]
        [InlineData("Decimal32(5)", "Decimal32", 5, null)]
        [InlineData("Decimal64(1)", "Decimal64", 1, null)]
        [InlineData("Decimal128(9)", "Decimal128", 9, null)]
        [InlineData("Decimal(35, 10)", "Decimal", 35, 10)]
        public void DecimalTypeArguments(string typeName, string expectedTypeName, int firstArgument, int? secondArgument)
        {
            var typeInfo = DefaultTypeInfoProvider.Instance.GetTypeInfo(typeName);

            int expectedCount = 1 + (secondArgument == null ? 0 : 1);
            Assert.Equal(0, typeInfo.GenericArgumentsCount);
            Assert.Equal(expectedTypeName, typeInfo.TypeName);
            Assert.Equal(expectedCount, typeInfo.TypeArgumentsCount);

            Assert.Equal(firstArgument, typeInfo.GetTypeArgument(0));

            if (secondArgument != null)
                Assert.Equal(secondArgument, typeInfo.GetTypeArgument(1));
        }

        [Fact]
        public void DateTimeTypeArguments()
        {
            var typeInfo = DefaultTypeInfoProvider.Instance.GetTypeInfo("DateTime('Asia/Macau')");

            Assert.Equal("DateTime", typeInfo.TypeName);
            Assert.Equal(0, typeInfo.GenericArgumentsCount);
            Assert.Equal(1, typeInfo.TypeArgumentsCount);

            Assert.Equal("Asia/Macau", typeInfo.GetTypeArgument(0));

            typeInfo = DefaultTypeInfoProvider.Instance.GetTypeInfo("DateTime");

            Assert.Equal("DateTime", typeInfo.TypeName);
            Assert.Equal(0, typeInfo.GenericArgumentsCount);
            Assert.Equal(0, typeInfo.TypeArgumentsCount);
        }

        [Fact]
        public void DateTime64TypeArguments()
        {
            var typeInfo = DefaultTypeInfoProvider.Instance.GetTypeInfo("DateTime64(3, 'Africa/Addis_Ababa')");

            Assert.Equal("DateTime64", typeInfo.TypeName);
            Assert.Equal(0, typeInfo.GenericArgumentsCount);
            Assert.Equal(2, typeInfo.TypeArgumentsCount);

            Assert.Equal(3, typeInfo.GetTypeArgument(0));
            Assert.Equal("Africa/Addis_Ababa", typeInfo.GetTypeArgument(1));

            typeInfo = DefaultTypeInfoProvider.Instance.GetTypeInfo("DateTime64(5)");

            Assert.Equal("DateTime64", typeInfo.TypeName);
            Assert.Equal(0, typeInfo.GenericArgumentsCount);
            Assert.Equal(1, typeInfo.TypeArgumentsCount);

            Assert.Equal(5, typeInfo.GetTypeArgument(0));
        }

        [Theory]
        [InlineData("Enum8('a' = 42)", new[] { "a" }, new sbyte[] { 42 })]
        [InlineData("Enum8('a' = 2, 'C' = -3,'b'=1)", new[] { "a", "C", "b" }, new sbyte[] { 2, -3, 1 })]
        public void Enum8TypeArguments(string typeName, string[] expectedKeys, sbyte[] expectedValues)
        {
            Assert.Equal(expectedKeys.Length, expectedValues.Length);

            var typeInfo = DefaultTypeInfoProvider.Instance.GetTypeInfo(typeName);

            Assert.Equal("Enum8", typeInfo.TypeName);
            Assert.Equal(0, typeInfo.GenericArgumentsCount);
            Assert.Equal(expectedKeys.Length, typeInfo.TypeArgumentsCount);

            for (int i = 0; i < expectedKeys.Length; i++)
            {
                var typeArgumentObj = typeInfo.GetTypeArgument(i);
                var typeArgument = Assert.IsType<KeyValuePair<string, sbyte>>(typeArgumentObj);
                Assert.Equal(typeArgument.Key, expectedKeys[i]);
                Assert.Equal(typeArgument.Value, expectedValues[i]);
            }
        }

        [Theory]
        [InlineData("Enum16('a' = 1024)", new[] { "a" }, new short[] { 1024 })]
        [InlineData("Enum16('a' = 8965, 'C' = 5,'b'=-3256)", new[] { "a", "C", "b" }, new short[] { 8965, 5, -3256 })]
        public void Enum16TypeArguments(string typeName, string[] expectedKeys, short[] expectedValues)
        {
            Assert.Equal(expectedKeys.Length, expectedValues.Length);

            var typeInfo = DefaultTypeInfoProvider.Instance.GetTypeInfo(typeName);

            Assert.Equal("Enum16", typeInfo.TypeName);
            Assert.Equal(0, typeInfo.GenericArgumentsCount);
            Assert.Equal(expectedKeys.Length, typeInfo.TypeArgumentsCount);

            for (int i = 0; i < expectedKeys.Length; i++)
            {
                var typeArgumentObj = typeInfo.GetTypeArgument(i);
                var typeArgument = Assert.IsType<KeyValuePair<string, short>>(typeArgumentObj);
                Assert.Equal(typeArgument.Key, expectedKeys[i]);
                Assert.Equal(typeArgument.Value, expectedValues[i]);
            }            
        }

        [Fact]
        public void FixedStringTypeArguments()
        {
            var typeInfo = DefaultTypeInfoProvider.Instance.GetTypeInfo("FixedString(42)");

            Assert.Equal("FixedString", typeInfo.TypeName);
            Assert.Equal(0, typeInfo.GenericArgumentsCount);
            Assert.Equal(1, typeInfo.TypeArgumentsCount);

            Assert.Equal(42, typeInfo.GetTypeArgument(0));
        }
    }
}
