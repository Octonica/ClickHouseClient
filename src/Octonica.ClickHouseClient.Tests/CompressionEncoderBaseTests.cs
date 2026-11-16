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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Octonica.ClickHouseClient.Protocol;
using Xunit;

namespace Octonica.ClickHouseClient.Tests;

public class CompressionEncoderBaseTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void WriteChunksOfRandomSize(int seed)
    {
        const int bufferSize = 64;
        var rnd = new Random(seed);
        using var encoder = new Lz4CompressionEncoder(bufferSize, 1024);

        // The second message checks that the encoder is reusable after Reset
        for (int message = 0; message < 2; message++)
        {
            encoder.Reset();

            var expected = new List<byte>();
            for (int i = 0; i < 2000; i++)
            {
                // Size hints larger than the free space of the current buffer make the encoder skip buffers
                var sizeHint = rnd.Next(1, 2 * bufferSize);
                var chunk = new byte[rnd.Next(0, sizeHint + 1)];
                rnd.NextBytes(chunk);

                CompressionTestHelper.Write(encoder, chunk, sizeHint);
                expected.AddRange(chunk);
            }

            var blocks = CompressionTestHelper.ReadLz4Blocks(CompressionTestHelper.Complete(encoder));
            Assert.Equal(expected, blocks.SelectMany(b => b));
        }
    }

    [Fact]
    public void WriteManyChunksInLinearTime()
    {
        // Each chunk is larger than the free space left in any filled buffer, so every chunk goes to a new buffer.
        // Searching for free space in all previously filled buffers made writing of a large block quadratic.
        // With the linear search the loop below takes a few milliseconds; with the quadratic one it takes seconds.
        const int bufferSize = 64;
        const int chunkSize = bufferSize / 2 + 1;
        const int chunkCount = 50_000;

        using var encoder = new Lz4CompressionEncoder(bufferSize, 1024 * 1024);
        var chunk = Enumerable.Range(0, chunkSize).Select(i => (byte)i).ToArray();

        var stopwatch = Stopwatch.StartNew();
        for (int i = 0; i < chunkCount; i++)
            CompressionTestHelper.Write(encoder, chunk, chunkSize);
        stopwatch.Stop();

        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(1), $"Writing {chunkCount} chunks took {stopwatch.Elapsed}.");

        var blocks = CompressionTestHelper.ReadLz4Blocks(CompressionTestHelper.Complete(encoder));
        Assert.Equal(chunkSize * chunkCount, blocks.Sum(b => b.Length));
    }
}
