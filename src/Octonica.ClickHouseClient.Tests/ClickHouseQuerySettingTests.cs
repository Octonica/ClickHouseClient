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

public class ClickHouseQuerySettingTests
{
    [Fact]
    public void EmptyNameIsRejected()
    {
        Assert.Throws<ArgumentException>(() => new ClickHouseQuerySetting(string.Empty));
        Assert.Throws<ArgumentException>(() => new ClickHouseQuerySetting(null!));
    }

    [Fact]
    public void Defaults()
    {
        var setting = new ClickHouseQuerySetting("max_threads");

        Assert.Equal("max_threads", setting.Name);
        Assert.Null(setting.Value);
        Assert.Null(setting.FormattedValue);
        Assert.True(setting.Important);
        Assert.False(setting.Custom);
        Assert.Equal(ClickHouseServerSettingFlags.Important, setting.GetFlags());
    }

    [Fact]
    public void SetValueRoundTripsOriginalAndFormattedValues()
    {
        var setting = new ClickHouseQuerySetting("s");

        setting.SetValue(true);
        Assert.Equal(true, setting.Value);
        Assert.Equal("1", setting.FormattedValue);

        setting.SetValue(false);
        Assert.Equal(false, setting.Value);
        Assert.Equal("0", setting.FormattedValue);

        setting.SetValue(8);
        Assert.Equal(8, setting.Value);
        Assert.IsType<int>(setting.Value);
        Assert.Equal("8", setting.FormattedValue);

        setting.SetValue(8L);
        Assert.IsType<long>(setting.Value);
        Assert.Equal("8", setting.FormattedValue);

        setting.SetValue(1.5d);
        Assert.Equal(1.5d, setting.Value);
        Assert.Equal("1.5", setting.FormattedValue);

        setting.SetValue("hash");
        Assert.Equal("hash", setting.Value);
        Assert.Equal("hash", setting.FormattedValue);
    }

    [Fact]
    public void SetValueRejectsNullString()
    {
        var setting = new ClickHouseQuerySetting("s");
        Assert.Throws<ArgumentNullException>(() => setting.SetValue(null!));
    }

    [Fact]
    public void Flags()
    {
        var setting = new ClickHouseQuerySetting("s");

        setting.Important = false;
        Assert.Equal(ClickHouseServerSettingFlags.None, setting.GetFlags());

        setting.Custom = true;
        Assert.Equal(ClickHouseServerSettingFlags.Custom, setting.GetFlags());

        setting.Important = true;
        Assert.Equal(ClickHouseServerSettingFlags.Important | ClickHouseServerSettingFlags.Custom, setting.GetFlags());
    }
}
