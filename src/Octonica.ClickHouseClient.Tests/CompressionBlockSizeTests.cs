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
using System.Linq;
using Octonica.ClickHouseClient.Protocol;
using Xunit;

namespace Octonica.ClickHouseClient.Tests;

public class CompressionBlockSizeTests
{
    // The default value of the server setting max_compress_block_size
    private const int ServerMaxCompressBlockSize = 1024 * 1024;

    [Fact]
    public void BlockSizeMatchesServerDefault()
    {
        var settings = new ClickHouseConnectionStringBuilder { Host = "localhost" }.BuildSettings();
        Assert.Equal(ServerMaxCompressBlockSize, settings.CompressionBlockSize);
    }

    [Fact]
    public void LargeMessageIsSplitIntoBlocksOfConfiguredSize()
    {
        var settings = new ClickHouseConnectionStringBuilder { Host = "localhost" }.BuildSettings();
        var blockSize = settings.CompressionBlockSize;
        var bufferSize = settings.BufferSize;

        // Three full blocks and a part of the fourth one
        var data = new byte[3 * blockSize + blockSize / 2];
        new Random(42).NextBytes(data.AsSpan(0, data.Length / 2));

        using var encoder = new Lz4CompressionEncoder(bufferSize, blockSize);
        for (int position = 0; position < data.Length; position += bufferSize)
        {
            var length = Math.Min(bufferSize, data.Length - position);
            CompressionTestHelper.Write(encoder, data.AsSpan(position, length), length);
        }

        var blocks = CompressionTestHelper.ReadLz4Blocks(CompressionTestHelper.Complete(encoder));

        Assert.Equal(4, blocks.Count);
        Assert.All(blocks.Take(3), block => Assert.Equal(blockSize, block.Length));
        Assert.Equal(blockSize / 2, blocks[3].Length);
        Assert.Equal(data, blocks.SelectMany(b => b));
    }
}
