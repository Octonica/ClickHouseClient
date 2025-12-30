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
using System.Collections.Generic;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Types;
using Octonica.ClickHouseClient.Utils;
using Xunit;

namespace Octonica.ClickHouseClient.Tests
{
    public class CommonUtilsTests
    {
        [Fact]
        public void GetColumnIndex()
        {
            var type = ClickHouseTypeInfoProvider.Instance.GetTypeInfo("Int32");
            var colInfo = new List<ColumnInfo> {new ColumnInfo("COL", type), new ColumnInfo("col", type), new ColumnInfo("another_col", type)}.AsReadOnly();

            var idx = CommonUtils.GetColumnIndex(colInfo, "COL");
            Assert.Equal(0, idx);

            idx = CommonUtils.GetColumnIndex(colInfo, "col");
            Assert.Equal(1, idx);

            idx = CommonUtils.GetColumnIndex(colInfo, "AnOtHeR_cOl");
            Assert.Equal(2, idx);

            Assert.Throws<IndexOutOfRangeException>(() => CommonUtils.GetColumnIndex(colInfo, "there_is_no_column"));
            Assert.Throws<IndexOutOfRangeException>(() => CommonUtils.GetColumnIndex(colInfo, "Col"));
        }
    }
}
