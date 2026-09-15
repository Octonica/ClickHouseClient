#region License Apache 2.0
/* Copyright 2026 Octonica
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
using Xunit;

namespace Octonica.ClickHouseClient.Tests;

public class ClickHouseQuerySettingCollectionTests
{
    [Fact]
    public void AddByName()
    {
        var collection = new ClickHouseQuerySettingCollection();
        var setting = collection.Add("max_threads");

        Assert.Single(collection);
        Assert.Same(setting, collection[0]);
        Assert.Same(setting, collection["max_threads"]);
        Assert.Null(setting.Value);
    }

    [Fact]
    public void AddWithValue()
    {
        var collection = new ClickHouseQuerySettingCollection();
        var numeric = collection.Add("max_threads", 8);
        var flag = collection.Add("extremes", true);
        var text = collection.Add("join_algorithm", "hash");

        Assert.Equal(3, collection.Count);
        Assert.Equal(8, numeric.Value);
        Assert.Equal("8", numeric.FormattedValue);
        Assert.Equal(true, flag.Value);
        Assert.Equal("1", flag.FormattedValue);
        Assert.Equal("hash", text.Value);
    }

    [Fact]
    public void DuplicateNameIsRejected()
    {
        var collection = new ClickHouseQuerySettingCollection();
        collection.Add("max_threads", 8);

        Assert.Throws<ArgumentException>(() => collection.Add("max_threads", 4));
        Assert.Throws<ArgumentException>(() => collection.Add(new ClickHouseQuerySetting("max_threads")));
    }

    [Fact]
    public void NamesAreCaseSensitive()
    {
        var collection = new ClickHouseQuerySettingCollection();
        collection.Add("max_threads", 8);
        collection.Add("Max_threads", 4);

        Assert.Equal(2, collection.Count);
        Assert.Equal(8, collection["max_threads"].Value);
        Assert.Equal(4, collection["Max_threads"].Value);
    }

    [Fact]
    public void EmptyNameIsRejected()
    {
        var collection = new ClickHouseQuerySettingCollection();
        Assert.Throws<ArgumentException>(() => collection.Add(string.Empty));
    }
}
