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
using Xunit;

namespace Octonica.ClickHouseClient.Tests
{
    public class IndexedCollectionTests
    {
        [Fact]
        public void Add()
        {
            var list = new[] {
                new KeyValuePair<uint, string>(1, "one"),
                new KeyValuePair<uint, string>(2, "two"),
                new KeyValuePair<uint, string>(3, "three"),
                new KeyValuePair<uint, string>(4, "four"),
                new KeyValuePair<uint, string>(5, "five"),
                new KeyValuePair<uint, string>(6, "six"),
                new KeyValuePair<uint, string>(7, "seven"),
                new KeyValuePair<uint, string>(8, "eight"),
                new KeyValuePair<uint, string>(9, "nine"),
                new KeyValuePair<uint, string>(0, "zero")
            };

            var collection = new TestIndexedCollection<uint, string>();
            Assert.Empty(collection);
            for (int i = 0; i < list.Length; i++)
            {
                collection.Clear();
                Assert.Empty(collection);

                for (int j = 0; j <= i; j++)
                    collection.Add(list[j]);

                Assert.Equal(i + 1, collection.Count);
                
                // Test IEnumerable
                Assert.Equal(list.Take(i + 1), collection);

                // Test indexer
                for (int j = 0; j <= i; j++)
                    Assert.Equal(list[j], collection[j]);

                // Test key indexer
                for (int j = 0; j <= i; j++)
                    Assert.Equal(list[j], collection[list[j].Key]);

                for (int j = 0; j < list.Length; j++)
                {
                    bool shouldContain = j <= i;

                    Assert.Equal(shouldContain, collection.Contains(list[j]));
                    Assert.Equal(shouldContain, collection.ContainsKey(list[j].Key));
                    Assert.Equal(shouldContain, collection.TryGetValue(list[j].Key, out var actualValue));

                    if (shouldContain)
                        Assert.Equal(list[j], actualValue);

                    var expectedIndex = shouldContain ? j : -1;
                    Assert.Equal(expectedIndex, collection.IndexOf(list[j]));
                    Assert.Equal(expectedIndex, collection.IndexOf(list[j].Key));                   
                }
            }
        }

        [Fact]
        public void Insert()
        {
            var list = new List<KeyValuePair<string, int>>
            {
                new KeyValuePair<string, int>("one", 1),
                new KeyValuePair<string, int>("two", 2),
                new KeyValuePair<string, int>("three", 3),
                new KeyValuePair<string, int>("four", 4),
                new KeyValuePair<string, int>("five", 5)                
            };

            var collection = new TestIndexedCollection<string, int>(StringComparer.Ordinal);
            Assert.Empty(collection);

            for (int i = 0; i <= list.Count; i++)
            {
                collection.Clear();
                Assert.Empty(collection);

                for (int j = list.Count - 1; j >= 0; j--)
                    collection.Insert(0, list[j]);

                Assert.Equal(list.Count, collection.Count);

                var expectedList = list.ToList();
                collection.Insert(i, new KeyValuePair<string, int>("many", 42));
                expectedList.Insert(i, new KeyValuePair<string, int>("many", 42));

                // Test IEnumerable
                Assert.Equal(expectedList, collection);

                // Test indexer
                for (int j = 0; j < expectedList.Count; j++)
                    Assert.Equal(expectedList[j], collection[j]);

                // Test key indexer
                for (int j = 0; j < expectedList.Count; j++)
                    Assert.Equal(expectedList[j], collection[expectedList[j].Key]);

                for (int j = 0; j < expectedList.Count; j++)
                {
                    Assert.Contains(expectedList[j], collection);
                    Assert.True(collection.ContainsKey(expectedList[j].Key));
                    Assert.True(collection.TryGetValue(expectedList[j].Key, out var actualValue));
                    Assert.Equal(expectedList[j], actualValue);

                    Assert.Equal(j, collection.IndexOf(expectedList[j]));
                    Assert.Equal(j, collection.IndexOf(expectedList[j].Key));
                }
            }
        }

        [Theory]
        [InlineData("RemoveAt")]
        [InlineData("RemoveKey")]
        [InlineData("RemoveItem")]
        public void Remove(string removeMethod)
        {
            var list = new List<KeyValuePair<string, int>>
            {
                new KeyValuePair<string, int>("a", 2),
                new KeyValuePair<string, int>("b", 4),
                new KeyValuePair<string, int>("c", 8),
                new KeyValuePair<string, int>("d", 16),
                new KeyValuePair<string, int>("e", 32),
                new KeyValuePair<string, int>("f", 64),
            };

            var collection = new TestIndexedCollection<string, int>(StringComparer.Ordinal);
            Assert.Empty(collection);
            for (int range = 1; range <= list.Count; range++)
            {
                for (int start = 0; start <= list.Count - range; start++)
                {
                    collection.Clear();
                    Assert.Empty(collection);

                    var expectedList = list.ToList();
                    for (int i = 0; i < list.Count; i++)
                        collection.Add(list[i]);

                    for (int i = range - 1; i >= 0; i--)
                    {
                        switch (removeMethod)
                        {
                            case "RemoveKey":
                                var key = expectedList[start + i].Key;
                                Assert.True(collection.Remove(key));
                                Assert.False(collection.Remove(key));
                                break;

                            case "RemoveItem":
                                var item = expectedList[start + i];
                                Assert.True(collection.Remove(item));
                                Assert.False(collection.Remove(item));
                                break;

                            case "RemoveAt":
                                collection.RemoveAt(start + i);
                                break;

                            default:
                                Assert.Fail($"Unknown method: {removeMethod}");
                                break;
                        }

                        expectedList.RemoveAt(start + i);
                    }

                    // Test IEnumerable
                    Assert.Equal(expectedList, collection);

                    // Test indexer
                    for (int i = 0; i < expectedList.Count; i++)
                        Assert.Equal(expectedList[i], collection[i]);

                    // Test key indexer
                    for (int i = 0; i < expectedList.Count; i++)
                        Assert.Equal(expectedList[i], collection[expectedList[i].Key]);

                    for (int i = 0; i < list.Count; i++)
                    {
                        bool shouldContain = i < start || i >= start + range;

                        Assert.Equal(shouldContain, collection.Contains(list[i]));
                        Assert.Equal(shouldContain, collection.ContainsKey(list[i].Key));
                        Assert.Equal(shouldContain, collection.TryGetValue(list[i].Key, out var actualValue));

                        if (shouldContain)
                            Assert.Equal(list[i], actualValue);

                        var expectedIndex = shouldContain ? (i < start ? i : i - range) : -1;
                        Assert.Equal(expectedIndex, collection.IndexOf(list[i]));
                        Assert.Equal(expectedIndex, collection.IndexOf(list[i].Key));
                    }
                }
            }                
        }
    }
}
