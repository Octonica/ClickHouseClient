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
using System.Buffers;
using System.Linq;
using Octonica.ClickHouseClient.Utils;
using Xunit;

namespace Octonica.ClickHouseClient.Tests
{
    public class ReadWriteBufferTests
    {
        [Fact]
        public void WriteBlocks()
        {
            var source = Enumerable.Range(0, 1000).Select(n => unchecked((byte) n)).ToArray();
            for (int i = 0; i < 10; i++)
            {
                var buffer = new ReadWriteBuffer(7);
                var rnd = new Random(i);

                var position = 0;
                while (position < source.Length)
                {
                    var blockSize = Math.Min(rnd.Next(1, 18), source.Length - position);

                    var memory = buffer.GetMemory(blockSize);
                    new ReadOnlySpan<byte>(source, position, blockSize).CopyTo(memory.Span);
                    buffer.ConfirmWrite(blockSize);

                    position += blockSize;
                }

                var empty = buffer.Read();
                Assert.True(empty.IsEmpty);

                buffer.Flush();

                var firstPart = buffer.Read();
                var array = firstPart.Slice(0, 500).ToArray();
                buffer.ConfirmRead(array.Length);

                Assert.Equal(source.Take(500), array);

                var secondPart = buffer.Read();
                secondPart.CopyTo(array);
                buffer.ConfirmRead(array.Length);

                Assert.Equal(source.Skip(500), array);

                empty = buffer.Read();
                Assert.True(empty.IsEmpty);
            }
        }

        [Fact]
        public void WriteExactThreeBlocks()
        {
            var source = Enumerable.Range(0, 7 * 3).Select(n => (byte) n).ToArray();
            var buffer = new ReadWriteBuffer(7);
            for (int i = 0; i < 3; i++)
            {
                for (int j = 0; j < 3; j++)
                {
                    var block = buffer.GetMemory(7);
                    new Span<byte>(source, j * 7, 7).CopyTo(block.Span);
                    buffer.ConfirmWrite(7);
                }

                buffer.Flush();

                var result = buffer.Read().ToArray();
                Assert.Equal(source, result);

                buffer.ConfirmRead(result.Length);
                var empty = buffer.Read();
                Assert.True(empty.IsEmpty);
            }
        }

        [Fact]
        public void ReadWriteSequentialWithoutSegmentOverflow()
        {
            const int segmentSize = 13;

            var source = Enumerable.Range(0, 1000).Select(n => unchecked((byte) n)).ToArray();
            var target = new byte[source.Length];
            var buffer = new ReadWriteBuffer(segmentSize);
            for (int i = 0; i < 100; i++)
            {
                var rnd = new Random(i);
                
                Array.Clear(target, 0, target.Length);
                int sourcePosition = 0;
                int targetPosition = 0;

                int readWriteFactor = i % 3 + 2;
                while (sourcePosition != source.Length)
                {
                    var read = rnd.Next(readWriteFactor) == 0;
                    if (read)
                    {
                        var availableBytes = buffer.Read();
                        if (!availableBytes.IsEmpty)
                        {
                            var availableLen = Math.Min((int) availableBytes.Length, Math.Min(rnd.Next(segmentSize * 3) + 1, target.Length - targetPosition));
                            var targetSlice = new Span<byte>(target).Slice(targetPosition, availableLen);
                            availableBytes.Slice(0, availableLen).CopyTo(targetSlice);

                            targetPosition += availableLen;
                            buffer.ConfirmRead(availableLen);
                            continue;
                        }
                    }

                    var len = Math.Min(source.Length - sourcePosition, rnd.Next(segmentSize * 3) + 1);
                    while (len != 0)
                    {
                        var memory = buffer.GetMemory();
                        var currentLen = Math.Min(len, memory.Length);
                        new ReadOnlySpan<byte>(source).Slice(sourcePosition, currentLen).CopyTo(memory.Span.Slice(0, currentLen));

                        buffer.ConfirmWrite(currentLen);
                        sourcePosition += currentLen;
                        len -= currentLen;
                    }

                    buffer.Flush();
                }

                var lastBlock = buffer.Read();
                Assert.Equal(target.Length - targetPosition, lastBlock.Length);

                lastBlock.CopyTo(new Span<byte>(target, targetPosition, target.Length - targetPosition));
                Assert.Equal(source, target);

                buffer.ConfirmRead((int) lastBlock.Length);
            }
        }

        [Fact]
        public void ReadWriteSequentialWithSegmentOverflow()
        {
            const int segmentSize = 13;

            var source = Enumerable.Range(0, 1000).Select(n => unchecked((byte) n)).ToArray();
            var target = new byte[source.Length];
            var buffer = new ReadWriteBuffer(segmentSize);
            for (int i = 0; i < 100; i++)
            {
                var rnd = new Random(i);

                Array.Clear(target, 0, target.Length);
                int sourcePosition = 0;
                int targetPosition = 0;

                int readWriteFactor = i % 3 + 2;
                while (sourcePosition != source.Length)
                {
                    var read = rnd.Next(readWriteFactor) == 0;
                    if (read)
                    {
                        var availableBytes = buffer.Read();
                        if (!availableBytes.IsEmpty)
                        {
                            var availableLen = Math.Min((int) availableBytes.Length, Math.Min(rnd.Next(segmentSize * 3) + 1, target.Length - targetPosition));
                            var targetSlice = new Span<byte>(target).Slice(targetPosition, availableLen);
                            availableBytes.Slice(0, availableLen).CopyTo(targetSlice);

                            targetPosition += availableLen;
                            buffer.ConfirmRead(availableLen);
                            continue;
                        }
                    }

                    var len = Math.Min(source.Length - sourcePosition, rnd.Next(segmentSize * 3) + 1);
                    var memory = buffer.GetMemory(len);

                    new ReadOnlySpan<byte>(source).Slice(sourcePosition, len).CopyTo(memory.Span.Slice(0, len));

                    buffer.ConfirmWrite(len);
                    sourcePosition += len;

                    buffer.Flush();
                }

                var lastBlock = buffer.Read();
                Assert.Equal(target.Length - targetPosition, lastBlock.Length);

                lastBlock.CopyTo(new Span<byte>(target, targetPosition, target.Length - targetPosition));
                Assert.Equal(source, target);

                buffer.ConfirmRead((int) lastBlock.Length);
            }
        }

        [Fact]
        public void FlushDiscardWithoutSegmentOverflow()
        {
            const int segmentSize = 13;

            var source = Enumerable.Range(0, 1000).Select(n => unchecked((byte)n)).ToArray();
            var target = new byte[source.Length];
            var buffer = new ReadWriteBuffer(segmentSize);
            for (int i = 0; i < 100; i++)
            {
                var rnd = new Random(i);

                Array.Clear(target, 0, target.Length);
                int sourcePosition = 0;
                int targetPosition = 0;
                int discardPosition = 0;

                int readWriteFactor = i % 3 + 2;
                while (discardPosition != source.Length)
                {
                    var read = rnd.Next(readWriteFactor) == 0;
                    if (read)
                    {
                        var availableBytes = buffer.Read();
                        if (!availableBytes.IsEmpty)
                        {
                            var availableLen = Math.Min((int)availableBytes.Length, Math.Min(rnd.Next(segmentSize * 3) + 1, target.Length - targetPosition));
                            var targetSlice = new Span<byte>(target).Slice(targetPosition, availableLen);
                            availableBytes.Slice(0, availableLen).CopyTo(targetSlice);

                            targetPosition += availableLen;
                            buffer.ConfirmRead(availableLen);
                            continue;
                        }
                    }

                    var len = Math.Min(source.Length - sourcePosition, rnd.Next(segmentSize * 3) + 1);
                    while (len != 0)
                    {
                        var memory = buffer.GetMemory();
                        var currentLen = Math.Min(len, memory.Length);
                        new ReadOnlySpan<byte>(source).Slice(sourcePosition, currentLen).CopyTo(memory.Span.Slice(0, currentLen));

                        buffer.ConfirmWrite(currentLen);
                        sourcePosition += currentLen;
                        len -= currentLen;
                    }

                    if (rnd.Next(4) == 0)
                    {
                        buffer.Flush();
                        discardPosition = sourcePosition;
                    }
                    else if (rnd.Next(2) == 0)
                    {
                        buffer.Discard();
                        sourcePosition = discardPosition;
                    }
                }

                var lastBlock = buffer.Read();
                Assert.Equal(target.Length - targetPosition, lastBlock.Length);

                lastBlock.CopyTo(new Span<byte>(target, targetPosition, target.Length - targetPosition));
                Assert.Equal(source, target);

                buffer.ConfirmRead((int)lastBlock.Length);
            }
        }

        [Fact]
        public void FlushDiscardWithSegmentOverflow()
        {
            const int segmentSize = 13;

            var source = Enumerable.Range(0, 1000).Select(n => unchecked((byte)n)).ToArray();
            var target = new byte[source.Length];
            var buffer = new ReadWriteBuffer(segmentSize);
            for (int i = 0; i < 100; i++)
            {
                var rnd = new Random(i);

                Array.Clear(target, 0, target.Length);
                int sourcePosition = 0;
                int targetPosition = 0;
                int discardPosition = 0;

                int readWriteFactor = i % 3 + 2;
                while (discardPosition != source.Length)
                {
                    var read = rnd.Next(readWriteFactor) == 0;
                    if (read)
                    {
                        var availableBytes = buffer.Read();
                        if (!availableBytes.IsEmpty)
                        {
                            var availableLen = Math.Min((int)availableBytes.Length, Math.Min(rnd.Next(segmentSize * 3) + 1, target.Length - targetPosition));
                            var targetSlice = new Span<byte>(target).Slice(targetPosition, availableLen);
                            availableBytes.Slice(0, availableLen).CopyTo(targetSlice);

                            targetPosition += availableLen;
                            buffer.ConfirmRead(availableLen);
                            continue;
                        }
                    }

                    var len = Math.Min(source.Length - sourcePosition, rnd.Next(segmentSize * 3) + 1);
                    var memory = buffer.GetMemory(len);

                    new ReadOnlySpan<byte>(source).Slice(sourcePosition, len).CopyTo(memory.Span.Slice(0, len));

                    buffer.ConfirmWrite(len);
                    sourcePosition += len;

                    if (rnd.Next(4) == 0)
                    {
                        buffer.Flush();
                        discardPosition = sourcePosition;
                    }
                    else if (rnd.Next(2) == 0)
                    {
                        buffer.Discard();
                        sourcePosition = discardPosition;
                    }
                }

                var lastBlock = buffer.Read();
                Assert.Equal(target.Length - targetPosition, lastBlock.Length);

                lastBlock.CopyTo(new Span<byte>(target, targetPosition, target.Length - targetPosition));
                Assert.Equal(source, target);

                buffer.ConfirmRead((int)lastBlock.Length);
            }
        }
    }
}
