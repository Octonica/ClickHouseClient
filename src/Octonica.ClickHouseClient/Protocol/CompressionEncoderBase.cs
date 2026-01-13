#region License Apache 2.0
/* Copyright 2019-2020, 2023 Octonica
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
using System.Diagnostics;
using System.Linq;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Utils;

#if NET8_0_OR_GREATER
using System.Runtime.InteropServices;
#endif

namespace Octonica.ClickHouseClient.Protocol
{
    internal abstract class CompressionEncoderBase : IDisposable
    {
        private readonly int _bufferSize;
        private readonly List<(byte[] buffer, int position)> _buffers = [];
        private readonly List<(int bufferIndex, int offset, int length)> _sequences = [];

        private int _acquiredBufferIndex = -1;

        protected abstract byte AlgorithmIdentifier { get; }

        public abstract CompressionAlgorithm Algorithm { get; }

        protected CompressionEncoderBase(int bufferSize)
        {
            _bufferSize = bufferSize;
        }

        public Span<byte> GetSpan(int sizeHint)
        {
            (byte[]? buffer, int position) = AcquireBuffer(sizeHint);
            return new Span<byte>(buffer, position, buffer.Length - position);
        }

        public Memory<byte> GetMemory(int sizeHint)
        {
            (byte[]? buffer, int position) = AcquireBuffer(sizeHint);
            return new Memory<byte>(buffer, position, buffer.Length - position);
        }

        public Memory<byte> GetMemory()
        {
            return GetMemory(1);
        }

        private (byte[] buffer, int position) AcquireBuffer(int sizeHint)
        {
            if (_acquiredBufferIndex >= 0)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Writing is already in progress.");
            }

            for (int i = _buffers.Count - 1; i >= 0; i--)
            {
                (byte[]? buffer, int position) = _buffers[i];
                if (buffer.Length - position >= sizeHint)
                {
                    _acquiredBufferIndex = i;
                    return (buffer, position);
                }
            }

            _acquiredBufferIndex = _buffers.Count;
            byte[] nextBuffer = new byte[Math.Max(sizeHint, _bufferSize)];
            _buffers.Add((nextBuffer, 0));
            return (nextBuffer, 0);
        }

        public void Advance(int bytes)
        {
            if (bytes < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(bytes));
            }

            if (_acquiredBufferIndex < 0)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Writing is already completer.");
            }

            (byte[]? buffer, int position) = _buffers[_acquiredBufferIndex];

            if (buffer.Length - position < bytes)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Attempt to write after the end of the memory buffer.");
            }

            if (bytes > 0)
            {
                _buffers[_acquiredBufferIndex] = (buffer, position + bytes);
                (int bufferIndex, int offset, int length) lastSequence;
                if (_sequences.Count == 0 || (lastSequence = _sequences[^1]).bufferIndex != _acquiredBufferIndex)
                {
                    _sequences.Add((_acquiredBufferIndex, position, bytes));
                }
                else
                {
                    _sequences[^1] = (_acquiredBufferIndex, lastSequence.offset, lastSequence.length + bytes);
                }
            }

            _acquiredBufferIndex = -1;
        }

        public void Reset()
        {
            if (_acquiredBufferIndex >= 0)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Writing is in progress.");
            }

            _sequences.Clear();

            for (int i = 0; i < _buffers.Count; i++)
            {
                _buffers[i] = (_buffers[i].buffer, 0);
            }
        }

        public void Complete(ReadWriteBuffer pipeWriter)
        {
            if (_acquiredBufferIndex >= 0)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Writing is in progress.");
            }

            /* 
             * Compressed data consist of a sequence of compressed blocks.
             *
             * The structure of the block:
             * 1. CityHash checksum (16 bytes);
             * 2. Algorithm's identifier (1 byte);
             * 3. The size of the block without checksum (4 bytes);
             * 4. The size of data in the block without compression (4 bytes);
             * 5. The block of compressed data.
            */

            List<(int bufferIndex, int offset, int length)> resultSequences = new(_sequences.Count + 1);

            const int cityHashSize = 2 * sizeof(ulong);
            byte[] header = new byte[cityHashSize + 1 + (2 * sizeof(int))];

            Queue<(int bufferIndex, int offset, int length)> freeSequences = new();
            if (_buffers.Count > 0)
            {
                (byte[] buffer, int position) = _buffers[^1];
                if (buffer.Length > position)
                {
                    freeSequences.Enqueue((_buffers.Count - 1, position, buffer.Length - position));
                    _buffers[^1] = (buffer, buffer.Length);
                }
            }

            int readPosition = 0, sequenceIndex = 0;
            bool completed;
            do
            {
                if (resultSequences.Count > 0)
                {
                    foreach ((int bufferIndex, int offset, int length) sequence in resultSequences)
                    {
                        freeSequences.Enqueue(sequence);
                    }

                    resultSequences.Clear();
                }

                completed = true;
                int writePosition = 0, rawSize = 0, encodedSize = 0;
                while (sequenceIndex < _sequences.Count)
                {
                    (int bufferIndex, int offset, int length) readSequence = _sequences[sequenceIndex];
                    if (readPosition == readSequence.length)
                    {
                        sequenceIndex++;
                        readPosition = 0;
                        freeSequences.Enqueue(readSequence);
                        continue;
                    }

                    int count = ConsumeNext(_buffers[readSequence.bufferIndex].buffer, readSequence.offset + readPosition, readSequence.length - readPosition);
                    readPosition += count;
                    rawSize += count;

                    if (readPosition < readSequence.length)
                    {
                        completed = false;
                        break;
                    }
                }

                if (rawSize == 0)
                {
                    break;
                }

                (int bufferIndex, int offset, int length) currentSequence = (-1, 0, 0);
                while (true)
                {
                    if (writePosition == currentSequence.length)
                    {
                        if (currentSequence.length > 0)
                        {
                            resultSequences.Add(currentSequence);
                        }

                        if (freeSequences.Count > 0)
                        {
                            currentSequence = freeSequences.Dequeue();
                        }
                        else
                        {
                            currentSequence = (_buffers.Count, 0, _bufferSize);
                            _buffers.Add((new byte[_bufferSize], _bufferSize));
                        }

                        writePosition = 0;
                    }

                    int result = completed
                        ? EncodeFinal(_buffers[currentSequence.bufferIndex].buffer, currentSequence.offset + writePosition, currentSequence.length - writePosition)
                        : EncodeNext(_buffers[currentSequence.bufferIndex].buffer, currentSequence.offset + writePosition, currentSequence.length - writePosition);

                    encodedSize += result;
                    writePosition += result;

                    if (writePosition < currentSequence.length)
                    {
                        break;
                    }
                }

                if (writePosition > 0)
                {
                    resultSequences.Add((currentSequence.bufferIndex, currentSequence.offset, writePosition));
                }

                Span<byte> headerSpan = header;
                headerSpan[cityHashSize] = AlgorithmIdentifier;
                bool success = BitConverter.TryWriteBytes(headerSpan[(cityHashSize + 1)..], encodedSize + header.Length - cityHashSize);
                Debug.Assert(success);
                success = BitConverter.TryWriteBytes(headerSpan[(cityHashSize + 1 + sizeof(int))..], rawSize);
                Debug.Assert(success);

                List<ReadOnlyMemory<byte>> segments = new(resultSequences.Count + 1) { new(header) };
                segments.AddRange(resultSequences.Select(s => new ReadOnlyMemory<byte>(_buffers[s.bufferIndex].buffer, s.offset, s.length)));
                SimpleReadOnlySequenceSegment<byte> dataSegment = new(segments);

                ReadOnlySequence<byte> dataSequence = new(dataSegment, 0, dataSegment.LastSegment, dataSegment.LastSegment.Memory.Length);
                UInt128 cityHash = CityHash.CityHash128(dataSequence.Slice(cityHashSize));

#if NET8_0_OR_GREATER
                ReadOnlySpan<byte> cityHashBytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan(ref cityHash, 1));
                Debug.Assert(cityHashBytes.Length == 16);
                cityHashBytes.CopyTo(headerSpan);
#else
                success = BitConverter.TryWriteBytes(headerSpan, cityHash.Low);
                Debug.Assert(success);
                success = BitConverter.TryWriteBytes(headerSpan.Slice(sizeof(ulong)), cityHash.High);
                Debug.Assert(success);
#endif

                for (ReadOnlySequenceSegment<byte>? segment = dataSegment; segment != null; segment = segment.Next)
                {
                    ReadOnlyMemory<byte> sourceMem = segment.Memory;
                    while (true)
                    {
                        Memory<byte> targetMem = pipeWriter.GetMemory();
                        if (sourceMem.Length > targetMem.Length)
                        {
                            sourceMem[..targetMem.Length].CopyTo(targetMem);
                            sourceMem = sourceMem[targetMem.Length..];
                            pipeWriter.ConfirmWrite(targetMem.Length);
                        }
                        else
                        {
                            sourceMem.CopyTo(targetMem);
                            pipeWriter.ConfirmWrite(sourceMem.Length);
                            break;
                        }
                    }
                }

                if (writePosition > 0)
                {
                    // This entire sequence should be marked as free
                    resultSequences[^1] = currentSequence;
                }

            } while (!completed);
        }

        protected abstract int ConsumeNext(byte[] source, int offset, int length);

        protected abstract int EncodeNext(byte[] target, int offset, int length);

        protected abstract int EncodeFinal(byte[] target, int offset, int length);

        public abstract void Dispose();
    }
}
