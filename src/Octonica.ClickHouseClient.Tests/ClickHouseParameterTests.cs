#region License Apache 2.0
/* Copyright 2021-2022 Octonica
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
using System.Collections.ObjectModel;
using System.Data;
using System.Linq.Expressions;
using System.Net;
using System.Reflection;
using System.Text;
using NodaTime;
using Octonica.ClickHouseClient.Utils;
using Xunit;

namespace Octonica.ClickHouseClient.Tests
{
    public class ClickHouseParameterTests
    {
        private static readonly ReadOnlyDictionary<string, Action<ClickHouseParameter, ClickHouseParameter>> ParameterPublicProperties;

        static ClickHouseParameterTests()
        {
            var properties = new Dictionary<string, Action<ClickHouseParameter, ClickHouseParameter>>();
            foreach (var property in typeof(ClickHouseParameter).GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (property.GetMethod == null || property.SetMethod == null || !property.GetMethod.IsPublic || !property.SetMethod.IsPublic)
                    continue;

                var comparer = TypeDispatcher.Dispatch(property.PropertyType, new PropertyComparerDispatcher(property));
                properties.Add(property.Name, comparer);
            }

            ParameterPublicProperties = new ReadOnlyDictionary<string, Action<ClickHouseParameter, ClickHouseParameter>>(properties);
        }

        [Fact]
        public void Clone()
        {
            var p1 = new ClickHouseParameter("p1") { Value = new[] { 42 }, ClickHouseDbType = ClickHouseDbType.VarNumeric, Precision = 19, Scale = 7, ParameterMode = ClickHouseParameterMode.Interpolate };

            var p2 = p1.Clone();

            Assert.NotSame(p1, p2);
            AssertParametersEqual(p1, p2);
            Assert.Equal("p1", p1.ParameterName);
            Assert.Equal(p1.Value, p2.Value);
            Assert.Equal(ClickHouseDbType.VarNumeric, p2.ClickHouseDbType);
            Assert.Equal(19, p2.Precision);
            Assert.Equal(7, p2.Scale);
            Assert.Equal(ClickHouseParameterMode.Interpolate, p2.ParameterMode);

            p2.TimeZone = DateTimeZoneProviders.Tzdb.GetSystemDefault();
            p2.Size = 35;
            p2.ArrayRank = 3;
            p2.StringEncoding = Encoding.ASCII;
            p2.IsNullable = true;

            var collection = new ClickHouseParameterCollection { p2 };

            var p3 = p2.Clone();
            Assert.NotSame(p2, p3);
            Assert.Null(p3.Collection);
            Assert.Same(collection, p2.Collection);

            AssertParametersEqual(p2, p3);
            Assert.Equal(DateTimeZoneProviders.Tzdb.GetSystemDefault(), p3.TimeZone);
            Assert.Equal(35, p3.Size);
            Assert.Equal(3, p3.ArrayRank);
            Assert.Equal(Encoding.ASCII, p3.StringEncoding);
            Assert.True(p3.IsNullable);

            p2.SourceColumn = "some value";
            p2.SourceColumnNullMapping = true;

            var p4 = p2.Clone();

            Assert.NotSame(p2, p4);
            Assert.Null(p4.Collection);

            AssertParametersEqual(p2, p4);
            Assert.Equal("some value", p4.SourceColumn);
            Assert.True(p2.SourceColumnNullMapping);
        }

        [Fact]
        public void CopyTo()
        {
            var p1 = new ClickHouseParameter("p1")
            {
                ArrayRank = 150,
                ClickHouseDbType = ClickHouseDbType.IpV6,
                IsArray = true,
                IsNullable = true,
                Precision = 24,
                Scale = 255,
                Size = 123456,
                SourceColumn = "aaaaa",
                SourceColumnNullMapping = true,
                StringEncoding = Encoding.BigEndianUnicode,
                TimeZone = DateTimeZone.Utc,
                Value = 123.456m
            };

            var collection = new ClickHouseParameterCollection { p1, new ClickHouseParameter("p2") };
            var p2 = collection["p2"];
            p1.CopyTo(p2);

            AssertParametersEqual(p1, p2, false);
        }

        [Fact]
        public void NotSupportedProperties()
        {
            var p = new ClickHouseParameter();

            Assert.Throws<NotSupportedException>(() => p.Direction = ParameterDirection.InputOutput);
            Assert.Throws<NotSupportedException>(() => p.Direction = ParameterDirection.Output);
            Assert.Throws<NotSupportedException>(() => p.Direction = ParameterDirection.ReturnValue);

            // Inherited behaviour
            p.SourceVersion = DataRowVersion.Current;
            Assert.Equal(DataRowVersion.Default, p.SourceVersion);

            p.SourceVersion = DataRowVersion.Original;
            Assert.Equal(DataRowVersion.Default, p.SourceVersion);

            p.SourceVersion = DataRowVersion.Proposed;
            Assert.Equal(DataRowVersion.Default, p.SourceVersion);
        }

        [Fact]
        public void TypeDetection()
        {
            var testData = new (object? value, ClickHouseDbType expectedType, bool expectedNullable, int expectedArrayRank)[]
            {
                (null, ClickHouseDbType.Nothing, true, 0),
                (DBNull.Value, ClickHouseDbType.Nothing, true, 0),
                (new[] {DBNull.Value}, ClickHouseDbType.Nothing, true, 1),
                
                (true, ClickHouseDbType.Boolean, false, 0),

                ((byte) 1, ClickHouseDbType.Byte, false, 0),
                ((sbyte) 1, ClickHouseDbType.SByte, false, 0),
                ((short) 1, ClickHouseDbType.Int16, false, 0),
                ((ushort) 1, ClickHouseDbType.UInt16, false, 0),
                ((int) 1, ClickHouseDbType.Int32, false, 0),
                ((uint) 1, ClickHouseDbType.UInt32, false, 0),
                ((long) 1, ClickHouseDbType.Int64, false, 0),
                ((ulong) 1, ClickHouseDbType.UInt64, false, 0),

                ((float) 1, ClickHouseDbType.Single, false, 0),
                ((double) 1, ClickHouseDbType.Double, false, 0),
                ((decimal) 1, ClickHouseDbType.Decimal, false, 0),

                (DateTime.Now, ClickHouseDbType.DateTime, false, 0),
                (DateTimeOffset.Now, ClickHouseDbType.DateTime, false, 0),

                (Guid.Empty, ClickHouseDbType.Guid, false, 0),

                (new IPAddress(new byte[] {127, 0, 0, 1}), ClickHouseDbType.IpV4, false, 0),
                (IPAddress.Parse("2001:0db8:11a3:09d7:1f34:8a2e:07a0:765d"), ClickHouseDbType.IpV6, false, 0),

                (string.Empty, ClickHouseDbType.String, false, 0),
                (new[] {'!'}, ClickHouseDbType.String, false, 0),
                (string.Empty.AsMemory(), ClickHouseDbType.String, false, 0),
                (new[] {'!'}.AsMemory(), ClickHouseDbType.String, false, 0),

                (new byte[0], ClickHouseDbType.Byte, false, 1),
                (new byte[0].AsMemory(), ClickHouseDbType.Byte, false, 1),
                ((ReadOnlyMemory<byte>) new byte[0].AsMemory(), ClickHouseDbType.Byte, false, 1),

                (new Guid?[0, 0, 0], ClickHouseDbType.Guid, true, 3),
            };

            var p = new ClickHouseParameter("p");
            foreach(var testCase in testData)
            {
                p.Value = testCase.value;

                Assert.Equal(testCase.expectedType, p.ClickHouseDbType);
                Assert.Equal(testCase.expectedArrayRank, p.ArrayRank);
                Assert.Equal(testCase.expectedArrayRank > 0, p.IsArray);
                Assert.Equal(testCase.expectedNullable, p.IsNullable);
            }
        }

        private static void AssertParametersEqual(ClickHouseParameter expected, ClickHouseParameter actual, bool compareName = true)
        {
            foreach (var pair in ParameterPublicProperties)
            {
                if (pair.Key == nameof(ClickHouseParameter.ParameterName) && !compareName)
                    continue;

                pair.Value(expected, actual);
            }
        }

        private class PropertyComparerDispatcher : ITypeDispatcher<Action<ClickHouseParameter, ClickHouseParameter>>
        {
            private readonly PropertyInfo _propertyInfo;

            public PropertyComparerDispatcher(PropertyInfo propertyInfo)
            {
                _propertyInfo = propertyInfo;
            }

            public Action<ClickHouseParameter, ClickHouseParameter> Dispatch<T>()
            {
                var expParam = Expression.Variable(typeof(ClickHouseParameter), "p");
                var expGetValue = Expression.Property(expParam, _propertyInfo);
                var expLambda = Expression.Lambda<Func<ClickHouseParameter, T>>(expGetValue, expParam);

                var getValue = expLambda.Compile();
                return (expected, actual) =>
                {
                    var expectedValue = getValue(expected);
                    var actualValue = getValue(actual);
                    Assert.Equal(expectedValue, actualValue);
                };
            }
        }
    }
}
