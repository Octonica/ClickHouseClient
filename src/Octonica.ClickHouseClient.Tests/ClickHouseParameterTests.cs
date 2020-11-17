﻿#region License Apache 2.0
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
using System.Collections.ObjectModel;
using System.Data;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
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
            var p1 = new ClickHouseParameter("p1") {Value = new[] {42}, ClickHouseDbType = ClickHouseDbType.VarNumeric, Precision = 19, Scale = 7};
            
            var p2 = p1.Clone();

            Assert.NotSame(p1, p2);
            AssertParametersEqual(p1, p2);
            Assert.Equal("p1", p1.ParameterName);
            Assert.Equal(p1.Value, p2.Value);
            Assert.Equal(ClickHouseDbType.VarNumeric, p2.ClickHouseDbType);
            Assert.Equal(19, p2.Precision);
            Assert.Equal(7, p2.Scale);

            p2.TimeZone = TimeZoneInfo.Local;
            p2.Size = 35;
            p2.ArrayRank = 3;
            p2.StringEncoding = Encoding.ASCII;
            p2.IsNullable = true;

            var collection = new ClickHouseParameterCollection {p2};

            var p3 = p2.Clone();
            Assert.NotSame(p2, p3);
            Assert.Null(p3.Collection);
            Assert.Same(collection, p2.Collection);

            AssertParametersEqual(p2, p3);
            Assert.Equal(TimeZoneInfo.Local, p3.TimeZone);
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
                TimeZone = TimeZoneInfo.Utc,
                Value = 123.456m
            };

            var collection = new ClickHouseParameterCollection {p1, new ClickHouseParameter("p2")};
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
