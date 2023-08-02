#region License Apache 2.0
/* Copyright 2019-2023 Octonica
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

namespace Octonica.ClickHouseClient.Tests
{
    public class ClickHouseConnectionStringBuilderTests
    {
        [Fact]
        public void FromString()
        {
            var builder = new ClickHouseConnectionStringBuilder(
                "host=ClickHouse.example.com;port=65500;compress = off;DataBase=\"don't; connect\"\" to me :)\";user=root; password=123456;\r\n bufferSize=1337; ReadWriteTimeout=42; clientname=ClickHouse.NetCore Tests;clientversion=3.2.1");

            var settings = builder.BuildSettings();

            Assert.Equal("ClickHouse.example.com", settings.Host);
            Assert.Equal(65500, settings.Port);
            Assert.Equal("don't; connect\" to me :)", settings.Database);
            Assert.Equal("root", settings.User);
            Assert.Equal("123456", settings.Password);
            Assert.Equal(1337, settings.BufferSize);
            Assert.Equal(42, settings.ReadWriteTimeout);
            Assert.Equal("ClickHouse.NetCore Tests", settings.ClientName);
            Assert.Equal(new ClickHouseVersion(3, 2, 1), settings.ClientVersion);
            Assert.Equal(false, settings.Compress);
        }

        [Fact]
        public void Remove()
        {
            var builder = new ClickHouseConnectionStringBuilder {Host = "some_instance.example.com", Port = ClickHouseConnectionStringBuilder.DefaultPort *2, ClientName = "Test!"};

            Assert.Equal(3, builder.Count);

            Assert.True(builder.Remove("pORT"));
            Assert.Equal(2, builder.Count);
            Assert.Equal(ClickHouseConnectionStringBuilder.DefaultPort, builder.Port);

            Assert.False(builder.Remove("Port"));
            
            Assert.True(builder.Remove("ClientName"));
            Assert.Equal(1, builder.Count);
            Assert.Equal(ClickHouseConnectionStringBuilder.DefaultClientName, builder.ClientName);
        }

        [Fact]
        public void SetConnectionString()
        {
            var builder = new ClickHouseConnectionStringBuilder(
                "host=ClickHouse.example.com;port=65500;DataBase=\"don't; connect\"\" to me :)\";user=root; password=123456;\r\n bufferSize=1337; ReadWriteTimeout=42; clientname=ClickHouse.NetCore Tests;clientversion=3.2.1");

            Assert.Equal(9, builder.Count);

            builder.ConnectionString = "host=localhost; port=31337";
            Assert.Equal(2, builder.Count);
        }

        [Fact]
        public void Default()
        {
            var builder = new ClickHouseConnectionStringBuilder();

            Assert.Equal(string.Empty, builder.ConnectionString);
            Assert.Empty(builder);

            // The host is required
            Assert.Throws<ArgumentException>(() => builder.BuildSettings());

            builder.Host = "localhost";
            Assert.Equal("Host=localhost", builder.ConnectionString);

            var settings = builder.BuildSettings();

            var checkedPropertiesCount = 0;
            Assert.Equal("localhost", settings.Host);
            ++checkedPropertiesCount;

            Assert.Equal(ClickHouseConnectionStringBuilder.DefaultPort, settings.Port);
            ++checkedPropertiesCount;

            Assert.Null(settings.Database);
            ++checkedPropertiesCount;

            Assert.Equal(ClickHouseConnectionStringBuilder.DefaultUser, settings.User);
            ++checkedPropertiesCount;

            Assert.Null(settings.Password);
            ++checkedPropertiesCount;

            Assert.Equal(ClickHouseConnectionStringBuilder.DefaultBufferSize, settings.BufferSize);
            ++checkedPropertiesCount;

            Assert.Equal(ClickHouseConnectionStringBuilder.DefaultReadWriteTimeout, settings.ReadWriteTimeout);
            ++checkedPropertiesCount;

            Assert.Equal(ClickHouseConnectionStringBuilder.DefaultClientName, settings.ClientName);
            ++checkedPropertiesCount;

            Assert.Equal(ClickHouseConnectionStringBuilder.DefaultClientVersion, settings.ClientVersion);
            ++checkedPropertiesCount;

            Assert.Equal(ClickHouseConnectionStringBuilder.DefaultCompress, settings.Compress);
            ++checkedPropertiesCount;

            Assert.Equal(ClickHouseConnectionStringBuilder.DefaultCommandTimeout, settings.CommandTimeout);
            ++checkedPropertiesCount;

            Assert.Equal(ClickHouseConnectionStringBuilder.DefaultTlsMode, settings.TlsMode);
            ++checkedPropertiesCount;

            Assert.Null(settings.RootCertificate);
            ++checkedPropertiesCount;

            Assert.True(settings.ServerCertificateHash.IsEmpty);
            ++checkedPropertiesCount;

            Assert.Equal(ClickHouseConnectionStringBuilder.DefaultParametersMode, settings.ParametersMode);
            ++checkedPropertiesCount;

            Assert.Null(settings.QuotaKey);
            ++checkedPropertiesCount;

            Assert.Equal(checkedPropertiesCount, settings.GetType().GetProperties().Length);
        }

        [Fact]
        public void Clone()
        {
            var builder = new ClickHouseConnectionStringBuilder(
                "host=ClickHouse.example.com;" +
                "port=65500;" +
                "DataBase=\"don't; connect\";" +
                "user=root; " +
                "password=123456;\r\n " +
                "bufferSize=1337; " +
                "ReadWriteTimeout=42; " +
                "clientname=ClickHouse.NetCore Tests;" +
                "clientversion=3.2.1;" +
                $"COMPRESS={!ClickHouseConnectionStringBuilder.DefaultCompress};" +
                "CommandTimeout=123;" +
                "TLSMode=rEqUIrE;" +
                "RootCertificate=/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.pem;" +
                "ServerCertificateHash=1234-5678 9abc-def0;" +
                "ParametersMode=Interpolate;" +
                "QuotaKey='unlimited'");

            var settings = builder.BuildSettings();

            var builderCopy = new ClickHouseConnectionStringBuilder(settings);
            settings = builderCopy.BuildSettings();

            var checkedPropertiesCount = 0;

            Assert.Equal("ClickHouse.example.com", settings.Host);
            ++checkedPropertiesCount;

            Assert.Equal(65500, settings.Port);
            ++checkedPropertiesCount;

            Assert.Equal("don't; connect", settings.Database);
            ++checkedPropertiesCount;

            Assert.Equal("root", settings.User);
            ++checkedPropertiesCount;

            Assert.Equal("123456", settings.Password);
            ++checkedPropertiesCount;

            Assert.Equal(1337, settings.BufferSize);
            ++checkedPropertiesCount;

            Assert.Equal(42, settings.ReadWriteTimeout);
            ++checkedPropertiesCount;

            Assert.Equal("ClickHouse.NetCore Tests", settings.ClientName);
            ++checkedPropertiesCount;

            Assert.Equal(new ClickHouseVersion(3, 2, 1), settings.ClientVersion);
            ++checkedPropertiesCount;

            Assert.Equal(!ClickHouseConnectionStringBuilder.DefaultCompress, settings.Compress);
            ++checkedPropertiesCount;

            Assert.Equal(123, settings.CommandTimeout);
            ++checkedPropertiesCount;

            Assert.Equal(ClickHouseTlsMode.Require, settings.TlsMode);
            ++checkedPropertiesCount;

            Assert.Equal("/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.pem", settings.RootCertificate);
            ++checkedPropertiesCount;

            Assert.Equal(new byte[] { 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0 }, settings.ServerCertificateHash.ToArray());
            ++checkedPropertiesCount;

            Assert.Equal(ClickHouseParameterMode.Interpolate, settings.ParametersMode);
            ++checkedPropertiesCount;

            Assert.Equal("unlimited", settings.QuotaKey);
            ++checkedPropertiesCount;

            Assert.Equal(checkedPropertiesCount, settings.GetType().GetProperties().Length);
        }

        [Fact]
        public void InvalidConnectionString()
        {
            Assert.Throws<ArgumentException>(() => new ClickHouseConnectionStringBuilder("Host=127.0.0.1;User=anonymous;Timeout:1337"));
        }

        [Fact]
        public void ValidConnectionStringWithInvalidProperty()
        {
            var ex = Assert.Throws<ArgumentException>(() => new ClickHouseConnectionStringBuilder("Host=127.0.0.1;User=anonymous;Timeout=1337;Server=clickhouse.example.com"));
            Assert.Equal("keyword", ex.ParamName);
        }
    }
}
