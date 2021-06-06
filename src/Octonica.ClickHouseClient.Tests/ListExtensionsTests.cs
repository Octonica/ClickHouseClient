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

using Octonica.ClickHouseClient.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Octonica.ClickHouseClient.Tests
{
    public class ListExtensionsTests
    {
        [Fact]
        public void SliceArray()
        {
            var arrayAsList = (IList<int>)Enumerable.Range(50, 200).ToArray();

            TestSlice(Enumerable.Range(50, 100).ToList(), arrayAsList.Slice(0, 100));
            TestSlice(Enumerable.Range(200, 50).ToList(), arrayAsList.Slice(150));
            TestSlice(Enumerable.Range(100, 100).ToList(), arrayAsList.Slice(50, 100));

            var expectedList = Enumerable.Range(102, 7).ToList();
            TestSlice(expectedList, arrayAsList.Slice(50).Slice(0, 100).Slice(2, 7));            
            TestSlice(expectedList, arrayAsList.Slice(50).Slice(2, 7));
            TestSlice(expectedList, arrayAsList.Slice(0, 100).Slice(50).Slice(2, 7));
            TestSlice(Enumerable.Range(52, 7).ToList(), arrayAsList.Slice(0, 100).Slice(2, 7));

            Assert.Throws<ArgumentOutOfRangeException>(() => arrayAsList.Slice(50).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => arrayAsList.Slice(2, 7).Slice(50));
            Assert.Throws<ArgumentOutOfRangeException>(() => arrayAsList.Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => arrayAsList.Slice(0, 100).Slice(2, 7).Slice(50));
            Assert.Throws<ArgumentOutOfRangeException>(() => arrayAsList.Slice(50).Slice(2, 7).Slice(0, 100));

            var arrayAsRoList = (IReadOnlyList<int>)arrayAsList;

            TestSlice(Enumerable.Range(50, 100).ToList(), arrayAsRoList.Slice(0, 100));
            TestSlice(Enumerable.Range(200, 50).ToList(), arrayAsRoList.Slice(150));
            TestSlice(Enumerable.Range(100, 100).ToList(), arrayAsRoList.Slice(50, 100));

            TestSlice(expectedList, arrayAsRoList.Slice(50).Slice(0, 100).Slice(2, 7));
            TestSlice(expectedList, arrayAsRoList.Slice(50).Slice(2, 7));
            TestSlice(expectedList, arrayAsRoList.Slice(0, 100).Slice(50).Slice(2, 7));
            TestSlice(Enumerable.Range(52, 7).ToList(), arrayAsRoList.Slice(0, 100).Slice(2, 7));

            Assert.Throws<ArgumentOutOfRangeException>(() => arrayAsRoList.Slice(50).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => arrayAsRoList.Slice(2, 7).Slice(50));
            Assert.Throws<ArgumentOutOfRangeException>(() => arrayAsRoList.Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => arrayAsRoList.Slice(0, 100).Slice(2, 7).Slice(50));
            Assert.Throws<ArgumentOutOfRangeException>(() => arrayAsRoList.Slice(50).Slice(2, 7).Slice(0, 100));
        }

        [Fact]
        public void SliceReadOnlyList()
        {
            var list = (IReadOnlyList<int>)Enumerable.Range(50, 200).ToList();

            TestSlice(Enumerable.Range(50, 100).ToList(), list.Slice(0, 100));
            TestSlice(Enumerable.Range(200, 50).ToList(), list.Slice(150));
            TestSlice(Enumerable.Range(100, 100).ToList(), list.Slice(50, 100));

            var expectedList = Enumerable.Range(102, 7).ToList();
            TestSlice(expectedList, list.Slice(50).Slice(0, 100).Slice(2, 7));
            TestSlice(expectedList, list.Slice(50).Slice(2, 7));
            TestSlice(expectedList, list.Slice(0, 100).Slice(50).Slice(2, 7));
            TestSlice(Enumerable.Range(52, 7).ToList(), list.Slice(0, 100).Slice(2, 7));

            Assert.Throws<ArgumentOutOfRangeException>(() => list.Slice(50).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => list.Slice(2, 7).Slice(50));
            Assert.Throws<ArgumentOutOfRangeException>(() => list.Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => list.Slice(0, 100).Slice(2, 7).Slice(50));
            Assert.Throws<ArgumentOutOfRangeException>(() => list.Slice(50).Slice(2, 7).Slice(0, 100));
        }

        [Fact]
        public void SliceList()
        {
            var list = new ListWrapper<int>(Enumerable.Range(50, 200).ToList());

            TestSlice(Enumerable.Range(50, 100).ToList(), list.Slice(0, 100));
            TestSlice(Enumerable.Range(200, 50).ToList(), list.Slice(150));
            TestSlice(Enumerable.Range(100, 100).ToList(), list.Slice(50, 100));

            var expectedList = Enumerable.Range(102, 7).ToList();
            TestSlice(expectedList, list.Slice(50).Slice(0, 100).Slice(2, 7));
            TestSlice(expectedList, list.Slice(50).Slice(2, 7));
            TestSlice(expectedList, list.Slice(0, 100).Slice(50).Slice(2, 7));
            TestSlice(Enumerable.Range(52, 7).ToList(), list.Slice(0, 100).Slice(2, 7));

            Assert.Throws<ArgumentOutOfRangeException>(() => list.Slice(50).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => list.Slice(2, 7).Slice(50));
            Assert.Throws<ArgumentOutOfRangeException>(() => list.Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => list.Slice(0, 100).Slice(2, 7).Slice(50));
            Assert.Throws<ArgumentOutOfRangeException>(() => list.Slice(50).Slice(2, 7).Slice(0, 100));
        }

        [Fact]
        public void MapArray()
        {
            var mappedArray = ((IReadOnlyList<int>)Enumerable.Range(-50, 100).ToArray()).Map(v => v + 10);
            TestSlice(Enumerable.Range(-40, 100).ToList(), mappedArray);
            TestSlice(Enumerable.Range(-40, 100).Select(v => v * 2).ToList(), mappedArray.Map(v => v * 2));

            mappedArray = ((IList<int>)Enumerable.Range(-50, 100).ToArray()).Map(v => v - 10);
            TestSlice(Enumerable.Range(-60, 100).ToList(), mappedArray);
            TestSlice(Enumerable.Range(-60, 100).Select(v => v * 2).ToList(), mappedArray.Map(v => v * 2));
        }

        [Fact]
        public void MapReadOnlyList()
        {
            var list = ((IReadOnlyList<int>)Enumerable.Range(-50, 100).ToList()).Map(v => v + 10);
            TestSlice(Enumerable.Range(-40, 100).ToList(), list);
            TestSlice(Enumerable.Range(-40, 100).Select(v => v * 2).ToList(), list.Map(v => v * 2));
        }

        [Fact]
        public void MapList()
        {
            var list = new ListWrapper<int>(Enumerable.Range(-50, 100).ToList()).Map(v => v - 10);
            TestSlice(Enumerable.Range(-60, 100).ToList(), list);
            TestSlice(Enumerable.Range(-60, 100).Select(v => v * 2).ToList(), list.Map(v => v * 2));
        }

        [Fact]
        public void SliceMapArray()
        {
            TestMappedArray(((IReadOnlyList<int>)Enumerable.Range(50, 200).ToArray()).Map(v => v + 10));
            TestMappedArray(((IList<int>)Enumerable.Range(50, 200).ToArray()).Map(v => v + 10));

            static void TestMappedArray(IReadOnlyList<int> mappedArray)
            {
                TestSlice(Enumerable.Range(60, 200).ToList(), mappedArray);

                TestSlice(Enumerable.Range(60, 100).ToList(), mappedArray.Slice(0, 100));
                TestSlice(Enumerable.Range(50, 100).ToList(), mappedArray.Map(v => v - 10).Slice(0, 100));
                TestSlice(Enumerable.Range(50, 100).ToList(), mappedArray.Slice(0, 100).Map(v => v - 10));

                TestSlice(Enumerable.Range(210, 50).ToList(), mappedArray.Slice(150));
                TestSlice(Enumerable.Range(200, 50).ToList(), mappedArray.Map(v => v - 10).Slice(150));
                TestSlice(Enumerable.Range(200, 50).ToList(), mappedArray.Slice(150).Map(v => v - 10));

                TestSlice(Enumerable.Range(110, 100).ToList(), mappedArray.Slice(50, 100));
                TestSlice(Enumerable.Range(100, 100).ToList(), mappedArray.Map(v => v - 10).Slice(50, 100));
                TestSlice(Enumerable.Range(100, 100).ToList(), mappedArray.Slice(50, 100).Map(v => v - 10));

                var expectedList = Enumerable.Range(102, 7).ToList();
                TestSlice(expectedList.Select(v => v + 10).ToList(), mappedArray.Slice(50).Slice(0, 100).Slice(2, 7));
                TestSlice(expectedList, mappedArray.Map(v => v - 10).Slice(50).Slice(0, 100).Slice(2, 7));
                TestSlice(expectedList, mappedArray.Slice(50).Map(v => v - 10).Slice(0, 100).Slice(2, 7));
                TestSlice(expectedList, mappedArray.Slice(50).Slice(0, 100).Map(v => v - 10).Slice(2, 7));
                TestSlice(expectedList, mappedArray.Slice(50).Slice(0, 100).Slice(2, 7).Map(v => v - 10));

                TestSlice(expectedList.Select(v => v + 10).ToList(), mappedArray.Slice(50).Slice(2, 7));
                TestSlice(expectedList, mappedArray.Map(v => v - 10).Slice(50).Slice(2, 7));
                TestSlice(expectedList, mappedArray.Slice(50).Map(v => v - 10).Slice(2, 7));
                TestSlice(expectedList, mappedArray.Slice(50).Slice(2, 7).Map(v => v - 10));

                TestSlice(expectedList.Select(v => v + 10).ToList(), mappedArray.Slice(0, 100).Slice(50).Slice(2, 7));
                TestSlice(expectedList, mappedArray.Map(v => v - 10).Slice(0, 100).Slice(50).Slice(2, 7));
                TestSlice(expectedList, mappedArray.Slice(0, 100).Map(v => v - 10).Slice(50).Slice(2, 7));
                TestSlice(expectedList, mappedArray.Slice(0, 100).Slice(50).Map(v => v - 10).Slice(2, 7));
                TestSlice(expectedList, mappedArray.Slice(0, 100).Slice(50).Slice(2, 7).Map(v => v - 10));

                TestSlice(Enumerable.Range(62, 7).ToList(), mappedArray.Slice(0, 100).Slice(2, 7));

                Assert.Throws<ArgumentOutOfRangeException>(() => mappedArray.Slice(50).Slice(2, 7).Slice(0, 100));
                Assert.Throws<ArgumentOutOfRangeException>(() => mappedArray.Map(v => v - 10).Slice(50).Slice(2, 7).Slice(0, 100));
                Assert.Throws<ArgumentOutOfRangeException>(() => mappedArray.Slice(50).Map(v => v - 10).Slice(2, 7).Slice(0, 100));
                Assert.Throws<ArgumentOutOfRangeException>(() => mappedArray.Slice(50).Slice(2, 7).Map(v => v - 10).Slice(0, 100));

                Assert.Throws<ArgumentOutOfRangeException>(() => mappedArray.Slice(2, 7).Slice(50));
                Assert.Throws<ArgumentOutOfRangeException>(() => mappedArray.Map(v => v - 10).Slice(2, 7).Slice(50));
                Assert.Throws<ArgumentOutOfRangeException>(() => mappedArray.Slice(2, 7).Map(v => v - 10).Slice(50));

                Assert.Throws<ArgumentOutOfRangeException>(() => mappedArray.Slice(2, 7).Slice(0, 100));
                Assert.Throws<ArgumentOutOfRangeException>(() => mappedArray.Map(v => v - 10).Slice(2, 7).Slice(0, 100));
                Assert.Throws<ArgumentOutOfRangeException>(() => mappedArray.Slice(2, 7).Map(v => v - 10).Slice(0, 100));

                Assert.Throws<ArgumentOutOfRangeException>(() => mappedArray.Slice(0, 100).Slice(2, 7).Slice(50));
                Assert.Throws<ArgumentOutOfRangeException>(() => mappedArray.Map(v => v - 10).Slice(0, 100).Slice(2, 7).Slice(50));
                Assert.Throws<ArgumentOutOfRangeException>(() => mappedArray.Slice(0, 100).Map(v => v - 10).Slice(2, 7).Slice(50));

                Assert.Throws<ArgumentOutOfRangeException>(() => mappedArray.Slice(50).Slice(2, 7).Slice(0, 100));
                Assert.Throws<ArgumentOutOfRangeException>(() => mappedArray.Map(v => v - 10).Slice(50).Slice(2, 7).Slice(0, 100));
                Assert.Throws<ArgumentOutOfRangeException>(() => mappedArray.Slice(50).Map(v => v - 10).Slice(2, 7).Slice(0, 100));
                Assert.Throws<ArgumentOutOfRangeException>(() => mappedArray.Slice(50).Slice(2, 7).Map(v => v - 10).Slice(0, 100));
            }

            var array = (IReadOnlyList<int>)Enumerable.Range(60, 200).ToArray();

            TestSlice(Enumerable.Range(60, 100).ToList(), array.Slice(0, 100).Map(v => v - 10).Map(v => v + 10));
            TestSlice(Enumerable.Range(50, 100).ToList(), array.Map(v => v - 10).Slice(0, 100));
            TestSlice(Enumerable.Range(50, 100).ToList(), array.Slice(0, 100).Map(v => v - 10));

            TestSlice(Enumerable.Range(210, 50).ToList(), array.Slice(150).Map(v => v - 10).Map(v => v + 10));
            TestSlice(Enumerable.Range(200, 50).ToList(), array.Map(v => v - 10).Slice(150));
            TestSlice(Enumerable.Range(200, 50).ToList(), array.Slice(150).Map(v => v - 10));

            TestSlice(Enumerable.Range(110, 100).ToList(), array.Slice(50, 100).Map(v => v - 10).Map(v => v + 10));
            TestSlice(Enumerable.Range(100, 100).ToList(), array.Map(v => v - 10).Slice(50, 100));
            TestSlice(Enumerable.Range(100, 100).ToList(), array.Slice(50, 100).Map(v => v - 10));

            var expectedList = Enumerable.Range(102, 7).ToList();
            TestSlice(expectedList.Select(v => v + 10).ToList(), array.Slice(50).Map(v => v - 10).Map(v => v + 10).Slice(0, 100).Slice(2, 7));
            TestSlice(expectedList.Select(v => v + 10).ToList(), array.Slice(50).Map(v => v - 10).Slice(0, 100).Map(v => v + 10).Slice(2, 7));
            TestSlice(expectedList.Select(v => v + 10).ToList(), array.Slice(50).Map(v => v - 10).Slice(0, 100).Slice(2, 7).Map(v => v + 10));
            TestSlice(expectedList.Select(v => v + 10).ToList(), array.Slice(50).Slice(0, 100).Map(v => v - 10).Map(v => v + 10).Slice(2, 7));
            TestSlice(expectedList.Select(v => v + 10).ToList(), array.Slice(50).Slice(0, 100).Map(v => v - 10).Slice(2, 7).Map(v => v + 10));
            TestSlice(expectedList.Select(v => v + 10).ToList(), array.Slice(50).Slice(0, 100).Slice(2, 7).Map(v => v - 10).Map(v => v + 10));
            TestSlice(expectedList, array.Map(v => v - 10).Slice(50).Slice(0, 100).Slice(2, 7));
            TestSlice(expectedList, array.Slice(50).Map(v => v - 10).Slice(0, 100).Slice(2, 7));
            TestSlice(expectedList, array.Slice(50).Slice(0, 100).Map(v => v - 10).Slice(2, 7));
            TestSlice(expectedList, array.Slice(50).Slice(0, 100).Slice(2, 7).Map(v => v - 10));

            TestSlice(expectedList, array.Map(v => v - 10).Slice(50).Slice(2, 7));
            TestSlice(expectedList, array.Slice(50).Map(v => v - 10).Slice(2, 7));
            TestSlice(expectedList, array.Slice(50).Slice(2, 7).Map(v => v - 10));

            TestSlice(expectedList, array.Map(v => v - 10).Slice(0, 100).Slice(50).Slice(2, 7));
            TestSlice(expectedList, array.Slice(0, 100).Map(v => v - 10).Slice(50).Slice(2, 7));
            TestSlice(expectedList, array.Slice(0, 100).Slice(50).Map(v => v - 10).Slice(2, 7));
            TestSlice(expectedList, array.Slice(0, 100).Slice(50).Slice(2, 7).Map(v => v - 10));

            Assert.Throws<ArgumentOutOfRangeException>(() => array.Slice(50).Map(v => v - 10).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => array.Slice(50).Slice(2, 7).Map(v => v - 10).Slice(0, 100));

            Assert.Throws<ArgumentOutOfRangeException>(() => array.Slice(2, 7).Map(v => v - 10).Slice(50));

            Assert.Throws<ArgumentOutOfRangeException>(() => array.Slice(2, 7).Map(v => v - 10).Slice(0, 100));

            Assert.Throws<ArgumentOutOfRangeException>(() => array.Slice(0, 100).Map(v => v - 10).Slice(2, 7).Slice(50));

            Assert.Throws<ArgumentOutOfRangeException>(() => array.Slice(50).Map(v => v - 10).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => array.Slice(50).Slice(2, 7).Map(v => v - 10).Slice(0, 100));
        }

        [Fact]
        public void SliceMapReadOnlyList()
        {
            var mappedRoList = ((IReadOnlyList<int>)Enumerable.Range(50, 200).ToList()).Map(v => v + 10);

            TestSlice(Enumerable.Range(60, 200).ToList(), mappedRoList);

            TestSlice(Enumerable.Range(60, 100).ToList(), mappedRoList.Slice(0, 100));
            TestSlice(Enumerable.Range(50, 100).ToList(), mappedRoList.Map(v => v - 10).Slice(0, 100));
            TestSlice(Enumerable.Range(50, 100).ToList(), mappedRoList.Slice(0, 100).Map(v => v - 10));

            TestSlice(Enumerable.Range(210, 50).ToList(), mappedRoList.Slice(150));
            TestSlice(Enumerable.Range(200, 50).ToList(), mappedRoList.Map(v => v - 10).Slice(150));
            TestSlice(Enumerable.Range(200, 50).ToList(), mappedRoList.Slice(150).Map(v => v - 10));

            TestSlice(Enumerable.Range(110, 100).ToList(), mappedRoList.Slice(50, 100));
            TestSlice(Enumerable.Range(100, 100).ToList(), mappedRoList.Map(v => v - 10).Slice(50, 100));
            TestSlice(Enumerable.Range(100, 100).ToList(), mappedRoList.Slice(50, 100).Map(v => v - 10));

            var expectedList = Enumerable.Range(102, 7).ToList();
            TestSlice(expectedList.Select(v => v + 10).ToList(), mappedRoList.Slice(50).Slice(0, 100).Slice(2, 7));
            TestSlice(expectedList, mappedRoList.Map(v => v - 10).Slice(50).Slice(0, 100).Slice(2, 7));
            TestSlice(expectedList, mappedRoList.Slice(50).Map(v => v - 10).Slice(0, 100).Slice(2, 7));
            TestSlice(expectedList, mappedRoList.Slice(50).Slice(0, 100).Map(v => v - 10).Slice(2, 7));
            TestSlice(expectedList, mappedRoList.Slice(50).Slice(0, 100).Slice(2, 7).Map(v => v - 10));

            TestSlice(expectedList.Select(v => v + 10).ToList(), mappedRoList.Slice(50).Slice(2, 7));
            TestSlice(expectedList, mappedRoList.Map(v => v - 10).Slice(50).Slice(2, 7));
            TestSlice(expectedList, mappedRoList.Slice(50).Map(v => v - 10).Slice(2, 7));
            TestSlice(expectedList, mappedRoList.Slice(50).Slice(2, 7).Map(v => v - 10));

            TestSlice(expectedList.Select(v => v + 10).ToList(), mappedRoList.Slice(0, 100).Slice(50).Slice(2, 7));
            TestSlice(expectedList, mappedRoList.Map(v => v - 10).Slice(0, 100).Slice(50).Slice(2, 7));
            TestSlice(expectedList, mappedRoList.Slice(0, 100).Map(v => v - 10).Slice(50).Slice(2, 7));
            TestSlice(expectedList, mappedRoList.Slice(0, 100).Slice(50).Map(v => v - 10).Slice(2, 7));
            TestSlice(expectedList, mappedRoList.Slice(0, 100).Slice(50).Slice(2, 7).Map(v => v - 10));

            TestSlice(Enumerable.Range(62, 7).ToList(), mappedRoList.Slice(0, 100).Slice(2, 7));

            Assert.Throws<ArgumentOutOfRangeException>(() => mappedRoList.Slice(50).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedRoList.Map(v => v - 10).Slice(50).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedRoList.Slice(50).Map(v => v - 10).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedRoList.Slice(50).Slice(2, 7).Map(v => v - 10).Slice(0, 100));

            Assert.Throws<ArgumentOutOfRangeException>(() => mappedRoList.Slice(2, 7).Slice(50));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedRoList.Map(v => v - 10).Slice(2, 7).Slice(50));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedRoList.Slice(2, 7).Map(v => v - 10).Slice(50));

            Assert.Throws<ArgumentOutOfRangeException>(() => mappedRoList.Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedRoList.Map(v => v - 10).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedRoList.Slice(2, 7).Map(v => v - 10).Slice(0, 100));

            Assert.Throws<ArgumentOutOfRangeException>(() => mappedRoList.Slice(0, 100).Slice(2, 7).Slice(50));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedRoList.Map(v => v - 10).Slice(0, 100).Slice(2, 7).Slice(50));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedRoList.Slice(0, 100).Map(v => v - 10).Slice(2, 7).Slice(50));

            Assert.Throws<ArgumentOutOfRangeException>(() => mappedRoList.Slice(50).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedRoList.Map(v => v - 10).Slice(50).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedRoList.Slice(50).Map(v => v - 10).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedRoList.Slice(50).Slice(2, 7).Map(v => v - 10).Slice(0, 100));

            var roList = (IReadOnlyList<int>)Enumerable.Range(60, 200).ToList().AsReadOnly();

            TestSlice(Enumerable.Range(60, 100).ToList(), roList.Slice(0, 100).Map(v => v - 10).Map(v => v + 10));
            TestSlice(Enumerable.Range(50, 100).ToList(), roList.Map(v => v - 10).Slice(0, 100));
            TestSlice(Enumerable.Range(50, 100).ToList(), roList.Slice(0, 100).Map(v => v - 10));

            TestSlice(Enumerable.Range(210, 50).ToList(), roList.Slice(150).Map(v => v - 10).Map(v => v + 10));
            TestSlice(Enumerable.Range(200, 50).ToList(), roList.Map(v => v - 10).Slice(150));
            TestSlice(Enumerable.Range(200, 50).ToList(), roList.Slice(150).Map(v => v - 10));

            TestSlice(Enumerable.Range(110, 100).ToList(), roList.Slice(50, 100).Map(v => v - 10).Map(v => v + 10));
            TestSlice(Enumerable.Range(100, 100).ToList(), roList.Map(v => v - 10).Slice(50, 100));
            TestSlice(Enumerable.Range(100, 100).ToList(), roList.Slice(50, 100).Map(v => v - 10));

            TestSlice(expectedList.Select(v => v + 10).ToList(), roList.Slice(50).Map(v => v - 10).Map(v => v + 10).Slice(0, 100).Slice(2, 7));
            TestSlice(expectedList.Select(v => v + 10).ToList(), roList.Slice(50).Map(v => v - 10).Slice(0, 100).Map(v => v + 10).Slice(2, 7));
            TestSlice(expectedList.Select(v => v + 10).ToList(), roList.Slice(50).Map(v => v - 10).Slice(0, 100).Slice(2, 7).Map(v => v + 10));
            TestSlice(expectedList.Select(v => v + 10).ToList(), roList.Slice(50).Slice(0, 100).Map(v => v - 10).Map(v => v + 10).Slice(2, 7));
            TestSlice(expectedList.Select(v => v + 10).ToList(), roList.Slice(50).Slice(0, 100).Map(v => v - 10).Slice(2, 7).Map(v => v + 10));
            TestSlice(expectedList.Select(v => v + 10).ToList(), roList.Slice(50).Slice(0, 100).Slice(2, 7).Map(v => v - 10).Map(v => v + 10));
            TestSlice(expectedList, roList.Map(v => v - 10).Slice(50).Slice(0, 100).Slice(2, 7));
            TestSlice(expectedList, roList.Slice(50).Map(v => v - 10).Slice(0, 100).Slice(2, 7));
            TestSlice(expectedList, roList.Slice(50).Slice(0, 100).Map(v => v - 10).Slice(2, 7));
            TestSlice(expectedList, roList.Slice(50).Slice(0, 100).Slice(2, 7).Map(v => v - 10));

            TestSlice(expectedList, roList.Map(v => v - 10).Slice(50).Slice(2, 7));
            TestSlice(expectedList, roList.Slice(50).Map(v => v - 10).Slice(2, 7));
            TestSlice(expectedList, roList.Slice(50).Slice(2, 7).Map(v => v - 10));

            TestSlice(expectedList, roList.Map(v => v - 10).Slice(0, 100).Slice(50).Slice(2, 7));
            TestSlice(expectedList, roList.Slice(0, 100).Map(v => v - 10).Slice(50).Slice(2, 7));
            TestSlice(expectedList, roList.Slice(0, 100).Slice(50).Map(v => v - 10).Slice(2, 7));
            TestSlice(expectedList, roList.Slice(0, 100).Slice(50).Slice(2, 7).Map(v => v - 10));

            Assert.Throws<ArgumentOutOfRangeException>(() => roList.Slice(50).Map(v => v - 10).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => roList.Slice(50).Slice(2, 7).Map(v => v - 10).Slice(0, 100));

            Assert.Throws<ArgumentOutOfRangeException>(() => roList.Slice(2, 7).Map(v => v - 10).Slice(50));

            Assert.Throws<ArgumentOutOfRangeException>(() => roList.Slice(2, 7).Map(v => v - 10).Slice(0, 100));

            Assert.Throws<ArgumentOutOfRangeException>(() => roList.Slice(0, 100).Map(v => v - 10).Slice(2, 7).Slice(50));

            Assert.Throws<ArgumentOutOfRangeException>(() => roList.Slice(50).Map(v => v - 10).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => roList.Slice(50).Slice(2, 7).Map(v => v - 10).Slice(0, 100));
        }

        [Fact]
        public void SliceMapList()
        {
            var mappedList = new ListWrapper<int>(Enumerable.Range(50, 200).ToList()).Map(v => v + 10);

            TestSlice(Enumerable.Range(60, 200).ToList(), mappedList);

            TestSlice(Enumerable.Range(60, 100).ToList(), mappedList.Slice(0, 100));
            TestSlice(Enumerable.Range(50, 100).ToList(), mappedList.Map(v => v - 10).Slice(0, 100));
            TestSlice(Enumerable.Range(50, 100).ToList(), mappedList.Slice(0, 100).Map(v => v - 10));

            TestSlice(Enumerable.Range(210, 50).ToList(), mappedList.Slice(150));
            TestSlice(Enumerable.Range(200, 50).ToList(), mappedList.Map(v => v - 10).Slice(150));
            TestSlice(Enumerable.Range(200, 50).ToList(), mappedList.Slice(150).Map(v => v - 10));

            TestSlice(Enumerable.Range(110, 100).ToList(), mappedList.Slice(50, 100));
            TestSlice(Enumerable.Range(100, 100).ToList(), mappedList.Map(v => v - 10).Slice(50, 100));
            TestSlice(Enumerable.Range(100, 100).ToList(), mappedList.Slice(50, 100).Map(v => v - 10));

            var expectedList = Enumerable.Range(102, 7).ToList();
            TestSlice(expectedList.Select(v => v + 10).ToList(), mappedList.Slice(50).Slice(0, 100).Slice(2, 7));
            TestSlice(expectedList, mappedList.Map(v => v - 10).Slice(50).Slice(0, 100).Slice(2, 7));
            TestSlice(expectedList, mappedList.Slice(50).Map(v => v - 10).Slice(0, 100).Slice(2, 7));
            TestSlice(expectedList, mappedList.Slice(50).Slice(0, 100).Map(v => v - 10).Slice(2, 7));
            TestSlice(expectedList, mappedList.Slice(50).Slice(0, 100).Slice(2, 7).Map(v => v - 10));

            TestSlice(expectedList.Select(v => v + 10).ToList(), mappedList.Slice(50).Slice(2, 7));
            TestSlice(expectedList, mappedList.Map(v => v - 10).Slice(50).Slice(2, 7));
            TestSlice(expectedList, mappedList.Slice(50).Map(v => v - 10).Slice(2, 7));
            TestSlice(expectedList, mappedList.Slice(50).Slice(2, 7).Map(v => v - 10));

            TestSlice(expectedList.Select(v => v + 10).ToList(), mappedList.Slice(0, 100).Slice(50).Slice(2, 7));
            TestSlice(expectedList, mappedList.Map(v => v - 10).Slice(0, 100).Slice(50).Slice(2, 7));
            TestSlice(expectedList, mappedList.Slice(0, 100).Map(v => v - 10).Slice(50).Slice(2, 7));
            TestSlice(expectedList, mappedList.Slice(0, 100).Slice(50).Map(v => v - 10).Slice(2, 7));
            TestSlice(expectedList, mappedList.Slice(0, 100).Slice(50).Slice(2, 7).Map(v => v - 10));

            TestSlice(Enumerable.Range(62, 7).ToList(), mappedList.Slice(0, 100).Slice(2, 7));

            Assert.Throws<ArgumentOutOfRangeException>(() => mappedList.Slice(50).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedList.Map(v => v - 10).Slice(50).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedList.Slice(50).Map(v => v - 10).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedList.Slice(50).Slice(2, 7).Map(v => v - 10).Slice(0, 100));

            Assert.Throws<ArgumentOutOfRangeException>(() => mappedList.Slice(2, 7).Slice(50));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedList.Map(v => v - 10).Slice(2, 7).Slice(50));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedList.Slice(2, 7).Map(v => v - 10).Slice(50));

            Assert.Throws<ArgumentOutOfRangeException>(() => mappedList.Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedList.Map(v => v - 10).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedList.Slice(2, 7).Map(v => v - 10).Slice(0, 100));

            Assert.Throws<ArgumentOutOfRangeException>(() => mappedList.Slice(0, 100).Slice(2, 7).Slice(50));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedList.Map(v => v - 10).Slice(0, 100).Slice(2, 7).Slice(50));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedList.Slice(0, 100).Map(v => v - 10).Slice(2, 7).Slice(50));

            Assert.Throws<ArgumentOutOfRangeException>(() => mappedList.Slice(50).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedList.Map(v => v - 10).Slice(50).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedList.Slice(50).Map(v => v - 10).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => mappedList.Slice(50).Slice(2, 7).Map(v => v - 10).Slice(0, 100));

            var roList = new ListWrapper<int>(Enumerable.Range(60, 200).ToList());

            TestSlice(Enumerable.Range(60, 100).ToList(), roList.Slice(0, 100).Map(v => v - 10).Map(v => v + 10));
            TestSlice(Enumerable.Range(50, 100).ToList(), roList.Map(v => v - 10).Slice(0, 100));
            TestSlice(Enumerable.Range(50, 100).ToList(), roList.Slice(0, 100).Map(v => v - 10));

            TestSlice(Enumerable.Range(210, 50).ToList(), roList.Slice(150).Map(v => v - 10).Map(v => v + 10));
            TestSlice(Enumerable.Range(200, 50).ToList(), roList.Map(v => v - 10).Slice(150));
            TestSlice(Enumerable.Range(200, 50).ToList(), roList.Slice(150).Map(v => v - 10));

            TestSlice(Enumerable.Range(110, 100).ToList(), roList.Slice(50, 100).Map(v => v - 10).Map(v => v + 10));
            TestSlice(Enumerable.Range(100, 100).ToList(), roList.Map(v => v - 10).Slice(50, 100));
            TestSlice(Enumerable.Range(100, 100).ToList(), roList.Slice(50, 100).Map(v => v - 10));

            TestSlice(expectedList.Select(v => v + 10).ToList(), roList.Slice(50).Map(v => v - 10).Map(v => v + 10).Slice(0, 100).Slice(2, 7));
            TestSlice(expectedList.Select(v => v + 10).ToList(), roList.Slice(50).Map(v => v - 10).Slice(0, 100).Map(v => v + 10).Slice(2, 7));
            TestSlice(expectedList.Select(v => v + 10).ToList(), roList.Slice(50).Map(v => v - 10).Slice(0, 100).Slice(2, 7).Map(v => v + 10));
            TestSlice(expectedList.Select(v => v + 10).ToList(), roList.Slice(50).Slice(0, 100).Map(v => v - 10).Map(v => v + 10).Slice(2, 7));
            TestSlice(expectedList.Select(v => v + 10).ToList(), roList.Slice(50).Slice(0, 100).Map(v => v - 10).Slice(2, 7).Map(v => v + 10));
            TestSlice(expectedList.Select(v => v + 10).ToList(), roList.Slice(50).Slice(0, 100).Slice(2, 7).Map(v => v - 10).Map(v => v + 10));
            TestSlice(expectedList, roList.Map(v => v - 10).Slice(50).Slice(0, 100).Slice(2, 7));
            TestSlice(expectedList, roList.Slice(50).Map(v => v - 10).Slice(0, 100).Slice(2, 7));
            TestSlice(expectedList, roList.Slice(50).Slice(0, 100).Map(v => v - 10).Slice(2, 7));
            TestSlice(expectedList, roList.Slice(50).Slice(0, 100).Slice(2, 7).Map(v => v - 10));

            TestSlice(expectedList, roList.Map(v => v - 10).Slice(50).Slice(2, 7));
            TestSlice(expectedList, roList.Slice(50).Map(v => v - 10).Slice(2, 7));
            TestSlice(expectedList, roList.Slice(50).Slice(2, 7).Map(v => v - 10));

            TestSlice(expectedList, roList.Map(v => v - 10).Slice(0, 100).Slice(50).Slice(2, 7));
            TestSlice(expectedList, roList.Slice(0, 100).Map(v => v - 10).Slice(50).Slice(2, 7));
            TestSlice(expectedList, roList.Slice(0, 100).Slice(50).Map(v => v - 10).Slice(2, 7));
            TestSlice(expectedList, roList.Slice(0, 100).Slice(50).Slice(2, 7).Map(v => v - 10));

            Assert.Throws<ArgumentOutOfRangeException>(() => roList.Slice(50).Map(v => v - 10).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => roList.Slice(50).Slice(2, 7).Map(v => v - 10).Slice(0, 100));

            Assert.Throws<ArgumentOutOfRangeException>(() => roList.Slice(2, 7).Map(v => v - 10).Slice(50));

            Assert.Throws<ArgumentOutOfRangeException>(() => roList.Slice(2, 7).Map(v => v - 10).Slice(0, 100));

            Assert.Throws<ArgumentOutOfRangeException>(() => roList.Slice(0, 100).Map(v => v - 10).Slice(2, 7).Slice(50));

            Assert.Throws<ArgumentOutOfRangeException>(() => roList.Slice(50).Map(v => v - 10).Slice(2, 7).Slice(0, 100));
            Assert.Throws<ArgumentOutOfRangeException>(() => roList.Slice(50).Slice(2, 7).Map(v => v - 10).Slice(0, 100));
        }

        [Fact]
        public void ArrayCopyTo()
        {
            var array = Enumerable.Range(0, 99).ToArray();
            TestCopyTo(array);         
        }

        [Fact]
        public void ListCopyTo()
        {
            var list = Enumerable.Range(0, 99).ToList();
            TestCopyTo(list);
        }

        private static void TestSlice<T>(IReadOnlyList<T> expected, IReadOnlyList<T> slice)
        {
            Assert.Equal(expected.Count, slice.Count);

            var ex = Assert.ThrowsAny<Exception>(() => slice[-1]);
            if(!(ex is IndexOutOfRangeException))
            {
                var argumentEx = Assert.IsAssignableFrom<ArgumentException>(ex);
                Assert.Equal("index", argumentEx.ParamName);
            }

            ex = Assert.ThrowsAny<Exception>(() => slice[expected.Count]);
            if (!(ex is IndexOutOfRangeException))
            {
                var argumentEx = Assert.IsAssignableFrom<ArgumentException>(ex);
                Assert.Equal("index", argumentEx.ParamName);
            }

            // IEnumerable implementation
            Assert.Equal<T>(expected, slice);

            // IReadOnlyList implementation
            for (int i = 0; i < expected.Count; i++)
                Assert.Equal(expected[i], slice[i]);

            TestCopyTo(slice);
        }

        private static void TestCopyTo<T>(IReadOnlyList<T> list)
        {
            var span = new Span<T>(new T[list.Count + 16]);

            var length = list.CopyTo(span, list.Count);
            Assert.Equal(0, length);

            length = list.CopyTo(default, 0);
            Assert.Equal(0, length);

            Assert.Throws<ArgumentOutOfRangeException>(() => list.CopyTo(new Span<T>(new T[2], 1, 1), -1));
            Assert.Throws<ArgumentOutOfRangeException>(() => list.CopyTo(new Span<T>(new T[1]), list.Count + 1));

            length = list.CopyTo(span, 0);
            Assert.Equal(list.Count, length);
            for (int i = 0; i < length; i++)
                Assert.Equal(list[i], span[i]);

            var halfLength = list.Count / 2;
            length = list.CopyTo(span.Slice(halfLength, halfLength), halfLength / 2);
            Assert.Equal(Math.Min(list.Count - halfLength / 2, halfLength), length);
            for (int i = halfLength / 2, j = halfLength; i < halfLength / 2 + length; i++, j++)
                Assert.Equal(list[i], span[j]);
        }
    }
}
