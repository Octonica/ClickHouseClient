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

public class Lz4CompressionEncoderTests
{
    public enum DataKind
    {
        Zeros,
        Random,
        Pattern
    }

    [Theory]
    [InlineData(DataKind.Zeros, 8 * 1024, 1)]
    [InlineData(DataKind.Zeros, 8 * 1024, 100_000)]
    [InlineData(DataKind.Random, 8 * 1024, 1)]
    [InlineData(DataKind.Random, 8 * 1024, 100_000)]
    [InlineData(DataKind.Pattern, 8 * 1024, 100_000)]
    [InlineData(DataKind.Zeros, 1024 * 1024, 3 * 1024 * 1024 + 1)]
    [InlineData(DataKind.Random, 1024 * 1024, 3 * 1024 * 1024 + 1)]
    [InlineData(DataKind.Pattern, 1024 * 1024, 3 * 1024 * 1024 + 1)]
    public void RoundTrip(DataKind kind, int blockSize, int dataSize)
    {
        var data = CreateData(kind, dataSize);

        const int bufferSize = 4096;
        using var encoder = new Lz4CompressionEncoder(bufferSize, blockSize);
        for (int position = 0; position < data.Length; position += bufferSize)
        {
            var length = Math.Min(bufferSize, data.Length - position);
            CompressionTestHelper.Write(encoder, data.AsSpan(position, length), length);
        }

        var compressed = CompressionTestHelper.Complete(encoder);
        var blocks = CompressionTestHelper.ReadLz4Blocks(compressed);

        Assert.Equal((dataSize + blockSize - 1) / blockSize, blocks.Count);
        Assert.Equal(data, blocks.SelectMany(b => b));

        if (kind == DataKind.Zeros && dataSize > blockSize)
        {
            // Compressible data must actually be compressed
            Assert.True(compressed.Length < data.Length / 100, $"The size of compressed data is {compressed.Length} bytes, the size of source data is {data.Length} bytes.");
        }
    }

    private static byte[] CreateData(DataKind kind, int size)
    {
        var data = new byte[size];
        switch (kind)
        {
            case DataKind.Zeros:
                break;

            case DataKind.Random:
                new Random(size).NextBytes(data);
                break;

            case DataKind.Pattern:
                // Something like a column of increasing 64-bit integers
                for (int i = 0; i + sizeof(long) <= size; i += sizeof(long))
                    BitConverter.TryWriteBytes(data.AsSpan(i), (long)i / sizeof(long));
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }

        return data;
    }
}
