#region License Apache 2.0
/* Copyright 2019-2020 Octonica
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

namespace Octonica.ClickHouseClient.Protocol
{
    internal abstract class CompressionEncoderBase: IDisposable
    {
        private readonly int _bufferSize;
        private readonly List<(byte[] buffer, int position)> _buffers = new List<(byte[] buffer, int position)>();
        private readonly List<(int bufferIndex, int offset, int length)> _sequences = new List<(int bufferIndex, int offset, int length)>();

        private int _acquiredBufferIndex = -1;

        protected abstract byte AlgorithmIdentifier { get; }

        public abstract CompressionAlgorithm Algorithm { get; }

        protected CompressionEncoderBase(int bufferSize)
        {
            _bufferSize = bufferSize;
        }

        public Span<byte> GetSpan(int sizeHint)
        {
            var (buffer, position) = AcquireBuffer(sizeHint);
            return new Span<byte>(buffer, position, buffer.Length - position);
        }

        public Memory<byte> GetMemory(int sizeHint)
        {
            var (buffer, position) = AcquireBuffer(sizeHint);
            return new Memory<byte>(buffer, position, buffer.Length - position);
        }

        public Memory<byte> GetMemory()
        {
            return GetMemory(1);
        }

        private (byte[] buffer, int position) AcquireBuffer(int sizeHint)
        {
            if (_acquiredBufferIndex >= 0)
                throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Writing is already in progress.");

            for (var i = _buffers.Count - 1; i >= 0; i--)
            {
                (var buffer, int position) = _buffers[i];
                if (buffer.Length - position >= sizeHint)
                {
                    _acquiredBufferIndex = i;
                    return (buffer, position);
                }
            }

            _acquiredBufferIndex = _buffers.Count;
            var nextBuffer = new byte[Math.Max(sizeHint, _bufferSize)];
            _buffers.Add((nextBuffer, 0));
            return (nextBuffer, 0);
        }

        public void Advance(int bytes)
        {
            if (bytes < 0)
                throw new ArgumentOutOfRangeException(nameof(bytes));

            if (_acquiredBufferIndex < 0)
                throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Writing is already completer.");

            var (buffer, position) = _buffers[_acquiredBufferIndex];

            if (buffer.Length - position < bytes)
                throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Attempt to write after the end of the memory buffer.");

            if (bytes > 0)
            {
                _buffers[_acquiredBufferIndex] = (buffer, position + bytes);
                (int bufferIndex, int offset, int length) lastSequence;
                if (_sequences.Count == 0 || (lastSequence = _sequences[^1]).bufferIndex != _acquiredBufferIndex)
                    _sequences.Add((_acquiredBufferIndex, position, bytes));
                else
                    _sequences[^1] = (_acquiredBufferIndex, lastSequence.offset, lastSequence.length + bytes);
            }

            _acquiredBufferIndex = -1;
        }

        public void Reset()
        {
            if (_acquiredBufferIndex >= 0)
                throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Writing is in progress.");

            _sequences.Clear();

            for (int i = 0; i < _buffers.Count; i++)
                _buffers[i] = (_buffers[i].buffer, 0);
        }

        public void Complete(ReadWriteBuffer pipeWriter)
        {
            if (_acquiredBufferIndex >= 0)
                throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error. Writing is in progress.");

            var resultSequences = new List<(int bufferIndex, int offset, int length)>(_sequences.Count + 1);

            const int cityHashSize = 2 * sizeof(ulong);
            var header = new byte[cityHashSize + 1 + 2 * sizeof(int)];

            var freeSequences = new Queue<(int bufferIndex, int offset, int length)>();
            if (_buffers.Count > 0)
            {
                var lastBuffer = _buffers[^1];
                if (lastBuffer.buffer.Length > lastBuffer.position)
                {
                    freeSequences.Enqueue((_buffers.Count - 1, lastBuffer.position, lastBuffer.buffer.Length - lastBuffer.position));
                    _buffers[^1] = (lastBuffer.buffer, lastBuffer.buffer.Length);
                }
            }

            (int bufferIndex, int offset, int length) currentSequence = (-1, 0, 0);
            int writePosition = 0, readPosition = 0, sequenceIndex = 0, rawSize = 0, encodedSize = 0;
            while (true)
            {
                bool completed = false;
                while (sequenceIndex < _sequences.Count)
                {
                    var readSequence = _sequences[sequenceIndex];
                    if (readPosition == readSequence.length)
                    {
                        sequenceIndex++;
                        readPosition = 0;
                        freeSequences.Enqueue(readSequence);
                        continue;
                    }

                    var count = ConsumeNext(_buffers[readSequence.bufferIndex].buffer, readSequence.offset + readPosition, readSequence.length - readPosition);
                    readPosition += count;
                    rawSize += count;

                    if (readPosition < readSequence.length)
                    {
                        completed = true;
                        break;
                    }
                }

                if (!completed)
                    break;

                while (true)
                {
                    if (writePosition == currentSequence.length)
                    {
                        if (currentSequence.length > 0)
                            resultSequences.Add(currentSequence);

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

                    var result = EncodeNext(_buffers[currentSequence.bufferIndex].buffer, currentSequence.offset + writePosition, currentSequence.length - writePosition);
                    encodedSize += result;
                    writePosition += result;

                    if (writePosition < currentSequence.length)
                        break;
                }
            }

            while (true)
            {
                if (writePosition == currentSequence.length)
                {
                    if (currentSequence.length > 0)
                        resultSequences.Add(currentSequence);

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

                var result = EncodeFinal(_buffers[currentSequence.bufferIndex].buffer, currentSequence.offset + writePosition, currentSequence.length - writePosition);
                encodedSize += result;
                writePosition += result;

                if (writePosition < currentSequence.length)
                    break;
            }

            if (writePosition > 0)
                resultSequences.Add((currentSequence.bufferIndex, currentSequence.offset, writePosition));

            Span<byte> headerSpan = header;
            headerSpan[cityHashSize] = AlgorithmIdentifier;
            var success = BitConverter.TryWriteBytes(headerSpan.Slice(cityHashSize + 1), encodedSize + header.Length - cityHashSize);
            Debug.Assert(success);
            success = BitConverter.TryWriteBytes(headerSpan.Slice(cityHashSize + 1 + sizeof(int)), rawSize);
            Debug.Assert(success);

            var segments = new List<ReadOnlyMemory<byte>>(resultSequences.Count + 1) {new ReadOnlyMemory<byte>(header)};
            segments.AddRange(resultSequences.Select(s => new ReadOnlyMemory<byte>(_buffers[s.bufferIndex].buffer, s.offset, s.length)));
            var dataSegment = new SimpleReadOnlySequenceSegment<byte>(segments);

            var dataSequence = new ReadOnlySequence<byte>(dataSegment, 0, dataSegment.LastSegment, dataSegment.LastSegment.Memory.Length);
            var cityHash = CityHash.CityHash128(dataSequence.Slice(cityHashSize));

            success = BitConverter.TryWriteBytes(headerSpan, cityHash.Low);
            Debug.Assert(success);
            success = BitConverter.TryWriteBytes(headerSpan.Slice(sizeof(ulong)), cityHash.High);
            Debug.Assert(success);
            
            for (ReadOnlySequenceSegment<byte>? segment = dataSegment; segment != null; segment = segment.Next)
            {
                var span = pipeWriter.GetMemory(segment.Memory.Length);
                segment.Memory.CopyTo(span);
                pipeWriter.ConfirmWrite(segment.Memory.Length);
            }
        }

        protected abstract int ConsumeNext(byte[] source, int offset, int length);

        protected abstract int EncodeNext(byte[] target, int offset, int length);

        protected abstract int EncodeFinal(byte[] target, int offset, int length);

        public abstract void Dispose();
    }
}
