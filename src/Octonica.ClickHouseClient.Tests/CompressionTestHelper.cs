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
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using K4os.Compression.LZ4;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;
using Xunit;

namespace Octonica.ClickHouseClient.Tests;

internal static class CompressionTestHelper
{
    private const int CityHashSize = 16;
    private const int BlockHeaderSize = 1 + 2 * sizeof(int);

    public static void Write(CompressionEncoderBase encoder, ReadOnlySpan<byte> data, int sizeHint)
    {
        var span = encoder.GetSpan(sizeHint);
        data.CopyTo(span);
        encoder.Advance(data.Length);
    }

    public static byte[] Complete(CompressionEncoderBase encoder)
    {
        var output = new ReadWriteBuffer(4096);
        encoder.Complete(output);
        output.Flush();

        var sequence = output.Read();
        var result = sequence.ToArray();
        output.ConfirmRead(result.Length);
        return result;
    }

    /// <summary>
    /// Splits compressed data into LZ4 blocks, verifies the checksum of each block and decompresses it.
    /// The block layout is: CityHash128 checksum (16 bytes), the method byte (0x82 for LZ4),
    /// the size of the block without the checksum (4 bytes), the size of decompressed data (4 bytes), compressed data.
    /// </summary>
    public static List<byte[]> ReadLz4Blocks(byte[] compressed)
    {
        var blocks = new List<byte[]>();
        var position = 0;
        while (position < compressed.Length)
        {
            Assert.Equal(0x82, compressed[position + CityHashSize]);

            var compressedSize = BitConverter.ToInt32(compressed, position + CityHashSize + 1);
            var rawSize = BitConverter.ToInt32(compressed, position + CityHashSize + 1 + sizeof(int));

            var checksum = CityHash.CityHash128(new ReadOnlySequence<byte>(compressed, position + CityHashSize, compressedSize));
            var checksumBytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan(ref checksum, 1));
            Assert.Equal(checksumBytes.ToArray(), compressed.AsSpan(position, CityHashSize).ToArray());

            var block = new byte[rawSize];
            var source = compressed.AsSpan(position + CityHashSize + BlockHeaderSize, compressedSize - BlockHeaderSize);
            Assert.Equal(rawSize, LZ4Codec.Decode(source, block));
            blocks.Add(block);

            position += CityHashSize + compressedSize;
        }

        Assert.Equal(compressed.Length, position);
        return blocks;
    }
}
