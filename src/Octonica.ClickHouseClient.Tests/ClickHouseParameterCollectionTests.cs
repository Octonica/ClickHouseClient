#region License Apache 2.0
/* Copyright 2020 Octonica
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
using System.Linq;
using Xunit;

namespace Octonica.ClickHouseClient.Tests
{
    public class ClickHouseParameterCollectionTests
    {
        [Fact]
        public void AddParameter()
        {
            var parameterNames = new[] {"abc", "{abc}", "@abc"};

            foreach (var name in parameterNames)
            {
                var collection = new ClickHouseParameterCollection();
                var parameter = new ClickHouseParameter(name);
                collection.Add(parameter);

                Assert.Single(collection);
                Assert.Same(parameter, collection[0]);

                foreach (var altName in parameterNames)
                {
                    Assert.True(collection.Contains(altName));
                    Assert.True(collection.TryGetValue(altName, out var collectionParameter));
                    Assert.Same(parameter, collectionParameter);
                    Assert.Same(collection, parameter.Collection);
                }
            }
        }

        [Fact]
        public void ChangeParameterName()
        {
            var collection = new ClickHouseParameterCollection();
            var parameter = new ClickHouseParameter("p1");

            Assert.Null(parameter.Collection);
            collection.Add(parameter);
            Assert.Same(collection, parameter.Collection);

            parameter.ParameterName = "{p1}";
            Assert.Same(collection, parameter.Collection);
            Assert.True(collection.TryGetValue(parameter.ParameterName, out var collectionParameter));
            Assert.Same(parameter, collectionParameter);

            parameter.ParameterName = "param123";
            Assert.Same(collection, parameter.Collection);
            Assert.True(collection.TryGetValue(parameter.ParameterName, out collectionParameter));
            Assert.Same(parameter, collectionParameter);

            parameter.ParameterName = "p1";
            Assert.Same(collection, parameter.Collection);
            Assert.True(collection.TryGetValue(parameter.ParameterName, out collectionParameter));
            Assert.Same(parameter, collectionParameter);

            parameter.ParameterName = "P1";
            Assert.Same(collection, parameter.Collection);
            Assert.True(collection.TryGetValue(parameter.ParameterName, out collectionParameter));
            Assert.Same(parameter, collectionParameter);
        }

        [Fact]
        public void RemoveParameter()
        {
            var collection = new ClickHouseParameterCollection();
            var parameters = new[] {new ClickHouseParameter("p1"), new ClickHouseParameter("p2"), new ClickHouseParameter("p3")};

            collection.AddRange(parameters);
            Assert.Equal(parameters.Length, collection.Count);

            Assert.False(collection.Remove("p4", out var removedParameter));
            Assert.Null(removedParameter);
            Assert.Equal(parameters.Length, collection.Count);
            
            Assert.False(collection.Remove("p4"));
            Assert.Equal(parameters.Length, collection.Count);

            collection.RemoveAt("p4");
            Assert.Equal(parameters.Length, collection.Count);

            Assert.True(collection.Remove("p1"));
            Assert.Equal(2, collection.Count);
            Assert.Null(parameters[0].Collection);
            Assert.Same(parameters[1], collection[0]);
            Assert.Same(parameters[2], collection[1]);

            Assert.True(collection.Remove("p3", out removedParameter));
            Assert.Single(collection);
            Assert.Null(parameters[2].Collection);
            Assert.Same(parameters[2], removedParameter);
            Assert.Same(collection[0], parameters[1]);

            collection.RemoveAt("p2");
            Assert.Empty(collection);
            Assert.Null(parameters[1].Collection);

            collection.AddRange(parameters.Reverse().ToArray());
            Assert.Equal(parameters.Length, collection.Count);

            collection.RemoveAt(0);
            Assert.Equal(2, collection.Count);
            Assert.Null(parameters[2].Collection);
            Assert.Same(parameters[1], collection[0]);
            Assert.Same(parameters[0], collection[1]);

            collection.Remove((object)parameters[2]);
            Assert.Equal(2, collection.Count);

            Assert.False(collection.Remove(parameters[2]));
            Assert.Equal(2, collection.Count);

            Assert.True(collection.Remove(parameters[1]));
            Assert.Single(collection);
            Assert.Null(parameters[1].Collection);
            Assert.Same(collection[0], parameters[0]);
        }

        [Fact]
        public void InsertParameter()
        {
            var collection = new ClickHouseParameterCollection();

            collection.Insert(0, (object) new ClickHouseParameter("p1"));
            Assert.Single(collection);
            Assert.Equal("p1", collection[0].ParameterName);
            Assert.Same(collection, collection[0].Collection);

            collection.Insert(0, new ClickHouseParameter("{p2}"));
            Assert.Equal(2, collection.Count);
            Assert.Equal("{p2}", collection[0].ParameterName);
            Assert.Same(collection, collection[0].Collection);
            Assert.Equal("p1", collection[1].ParameterName);
            Assert.Same(collection, collection[1].Collection);

            collection.Insert(2, (object) new ClickHouseParameter("@p3"));
            Assert.Equal(3, collection.Count);
            Assert.Equal("{p2}", collection[0].ParameterName);
            Assert.Same(collection, collection[0].Collection);
            Assert.Equal("p1", collection[1].ParameterName);
            Assert.Same(collection, collection[1].Collection);
            Assert.Equal("@p3", collection[2].ParameterName);
            Assert.Same(collection, collection[2].Collection);

            collection.Insert(1, new ClickHouseParameter("p4"));
            Assert.Equal(4, collection.Count);
            Assert.Equal("{p2}", collection[0].ParameterName);
            Assert.Same(collection, collection[0].Collection);
            Assert.Equal("p4", collection[1].ParameterName);
            Assert.Same(collection, collection[1].Collection);
            Assert.Equal("p1", collection[2].ParameterName);
            Assert.Same(collection, collection[2].Collection);
            Assert.Equal("@p3", collection[3].ParameterName);
            Assert.Same(collection, collection[3].Collection);
        }

        [Fact]
        public void ReplaceParameter()
        {
            var collection = new ClickHouseParameterCollection();

            var parameters = new[] {"p1", "p2", "p3", "p4"}.Select(name => collection.AddWithValue(name, null, DbType.String)).ToArray();
            Assert.Equal(4, collection.Count);

            var newParam = new ClickHouseParameter("p5");
            collection[2] = newParam;
            Assert.Equal(4, collection.Count);
            Assert.Equal("p1", collection[0].ParameterName);
            Assert.Same(collection, collection[0].Collection);
            Assert.Equal("p2", collection[1].ParameterName);
            Assert.Same(collection, collection[1].Collection);
            Assert.Same(newParam, collection[2]);
            Assert.Same(collection, collection[2].Collection);
            Assert.Equal("p4", collection[3].ParameterName);
            Assert.Same(collection, collection[3].Collection);
            Assert.Null(parameters[2].Collection);

            collection["{p5}"] = parameters[2];
            Assert.Equal(4, collection.Count);
            for (int i = 0; i < parameters.Length; i++)
            {
                Assert.Same(parameters[i], collection[i]);
                Assert.Same(collection, parameters[i].Collection);
            }

            Assert.Null(newParam.Collection);

            collection["p1"] = parameters[0];
            Assert.Equal(4, collection.Count);
            for (int i = 0; i < parameters.Length; i++)
            {
                Assert.Same(parameters[i], collection[i]);
                Assert.Same(collection, parameters[i].Collection);
            }

            collection["{p5}"] = newParam;
            Assert.Equal(5, collection.Count);
            for (int i = 0; i < parameters.Length; i++)
            {
                Assert.Same(parameters[i], collection[i]);
                Assert.Same(collection, parameters[i].Collection);
            }

            Assert.Same(newParam, collection[4]);
            Assert.Same(collection, newParam.Collection);

            var ex = Assert.Throws<ArgumentException>(() => collection["p6"] = new ClickHouseParameter("p7"));
            Assert.Equal(5, collection.Count);
            Assert.Equal("parameterName", ex.ParamName);
        }
    }
}
