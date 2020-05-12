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
using System.Collections.Generic;
using System.Diagnostics;

namespace Octonica.ClickHouseClient.Utils
{
    internal sealed class ReadWriteBuffer
    {
        private readonly int _segmentSize;

        private readonly List<(byte[] buffer, int length)> _segments;

        private int _readSegmentIdx;
        private int _readSegmentPosition;
        private int _readLength;
        private int _writeSegmentIdx;
        private int _writeSegmentPosition;

        private ReadOnlySequence<byte>? _readCache;

        public ReadWriteBuffer(int segmentSize)
        {
            if (segmentSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(segmentSize));

            _segmentSize = segmentSize;
            _segments = new List<(byte[] buffer, int length)>();
        }

        public ReadOnlySequence<byte> Read()
        {
            if (_readCache != null)
                return _readCache.Value;

            if (_readLength == 0)
                return ReadOnlySequence<byte>.Empty;

            var readSegment = _segments[_readSegmentIdx];
            if (readSegment.length - _readSegmentPosition >= _readLength)
                return new ReadOnlySequence<byte>(readSegment.buffer, _readSegmentPosition, _readLength);

            var memory = new List<ReadOnlyMemory<byte>> {new ReadOnlyMemory<byte>(readSegment.buffer, _readSegmentPosition, readSegment.length - _readSegmentPosition)};
            for (int length = _readLength - memory[0].Length, segmentIdx = (_readSegmentIdx + 1) % _segments.Count; length > 0; segmentIdx = (segmentIdx + 1) % _segments.Count)
            {
                var segment = _segments[segmentIdx];
                var memoryBlock = new ReadOnlyMemory<byte>(segment.buffer, 0, Math.Min(segment.length, length));
                memory.Add(memoryBlock);
                length -= memoryBlock.Length;
            }

            var sequenceSegment = new SimpleReadOnlySequenceSegment<byte>(memory);
            _readCache = new ReadOnlySequence<byte>(sequenceSegment, 0, sequenceSegment.LastSegment, sequenceSegment.LastSegment.Memory.Length);
            return _readCache.Value;
        }

        public void ConfirmRead(int length)
        {
            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length));

            if (length > _readLength)
                throw new ArgumentOutOfRangeException(nameof(length), "Internal error. The length can't be greater than the size of the buffer.");

            if (length == 0)
                return;

            var segment = _segments[_readSegmentIdx];
            if (length < segment.length - _readSegmentPosition)
            {
                _readSegmentPosition += length;
            }
            else
            {
                var currentLength = length - segment.length + _readSegmentPosition;
                var currentIndex = _readSegmentIdx;
                var currentSegment = _segments[currentIndex];

                while (true)
                {
                    if (currentLength == 0 && currentIndex == _writeSegmentIdx && currentSegment.buffer.Length == _writeSegmentPosition)
                    {
                        // Read index is pointing to the beginning of the next segment. Write index is pointing to the end of
                        // the current segment. Adjust write index.
                        _writeSegmentIdx = _readSegmentIdx = (currentIndex + 1) % _segments.Count;
                        _writeSegmentPosition = _readSegmentPosition = 0;

                        currentSegment = _segments[_writeSegmentIdx];
                        _segments[_writeSegmentIdx] = (currentSegment.buffer, currentSegment.buffer.Length);
                        break;
                    }

                    currentIndex = (currentIndex + 1) % _segments.Count;
                    Debug.Assert(currentIndex != _readSegmentIdx);
                    currentSegment = _segments[currentIndex];

                    if (currentLength < currentSegment.length)
                    {
                        _readSegmentIdx = currentIndex;
                        _readSegmentPosition = currentLength;
                        break;
                    }

                    currentLength -= currentSegment.length;
                }
            }

            _readLength -= length;

            if (_readCache != null)
            {
                _readCache = _readCache.Value.Slice(length);
                Debug.Assert(_readCache.Value.Length == _readLength);
            }
        }

        public Memory<byte> GetMemory()
        {
            if (_segments.Count > 0)
            {
                var writeSegment = _segments[_writeSegmentIdx];
                if (writeSegment.buffer.Length > _writeSegmentPosition)
                    return new Memory<byte>(writeSegment.buffer, _writeSegmentPosition, writeSegment.buffer.Length - _writeSegmentPosition);
            }

            return AcquireNextSegment(_segmentSize);
        }

        public Memory<byte> GetMemory(int sizeHint)
        {
            if (sizeHint < 0)
                throw new ArgumentOutOfRangeException(nameof(sizeHint));

            if (_segments.Count > 0)
            {
                var writeSegment = _segments[_writeSegmentIdx];
                if (writeSegment.buffer.Length - _writeSegmentPosition >= sizeHint)
                    return new Memory<byte>(writeSegment.buffer, _writeSegmentPosition, writeSegment.buffer.Length - _writeSegmentPosition);
            }

            var segmentSize = _segmentSize;
            if (sizeHint > _segmentSize)
                segmentSize = (sizeHint / _segmentSize + Math.Min(sizeHint % _segmentSize, 1)) * _segmentSize;

            return AcquireNextSegment(segmentSize);
        }

        private Memory<byte> AcquireNextSegment(int size)
        {
            if (_segments.Count == 0)
            {
                _segments.Add((new byte[size], size));
                return new Memory<byte>(_segments[_writeSegmentIdx].buffer);
            }

            _segments[_writeSegmentIdx] = (_segments[_writeSegmentIdx].buffer, _writeSegmentPosition);

            if (_segments.Count > 1)
            {
                var firstFreeSegmentIdx = (_writeSegmentIdx + 1) % _segments.Count;
                for (int i = firstFreeSegmentIdx; i != _readSegmentIdx; i = (i + 1) % _segments.Count)
                {
                    if (_segments[i].buffer.Length >= size)
                    {
                        var segment = _segments[i];
                        if (i != firstFreeSegmentIdx)
                            _segments[i] = _segments[firstFreeSegmentIdx];
                        
                        _segments[firstFreeSegmentIdx] = (segment.buffer, segment.buffer.Length);
                        _writeSegmentIdx = firstFreeSegmentIdx;
                        _writeSegmentPosition = 0;

                        return new Memory<byte>(_segments[_writeSegmentIdx].buffer);
                    }
                }

                if (firstFreeSegmentIdx != _readSegmentIdx)
                {
                    _segments[firstFreeSegmentIdx] = (new byte[size], size);
                    _writeSegmentIdx = firstFreeSegmentIdx;
                    _writeSegmentPosition = 0;

                    return new Memory<byte>(_segments[_writeSegmentIdx].buffer);
                }
            }

            if (_writeSegmentIdx < _readSegmentIdx)
                ++_readSegmentIdx;

            ++_writeSegmentIdx;
            _writeSegmentPosition = 0;
            _segments.Insert(_writeSegmentIdx, (new byte[size], size));
            return new Memory<byte>(_segments[_writeSegmentIdx].buffer);
        }

        public void ConfirmWrite(int length)
        {
            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length));

            var segment = _segments[_writeSegmentIdx];
            if (length > segment.buffer.Length - _writeSegmentPosition)
                throw new ArgumentOutOfRangeException(nameof(length), "Internal error. The length can't be greater than the size of the buffer.");

            _writeSegmentPosition += length;
            Debug.Assert(_writeSegmentPosition <= segment.length);
        }

        public void Flush()
        {
            if (_segments.Count == 0)
                return;

            int length;
            if (_readSegmentIdx == _writeSegmentIdx)
            {
                length = _writeSegmentPosition - _readSegmentPosition;
                Debug.Assert(length >= 0);
            }
            else
            {
                length = _segments[_readSegmentIdx].length - _readSegmentPosition;
                for (int i = (_readSegmentIdx + 1) % _segments.Count; i != _writeSegmentIdx; i = (i + 1) % _segments.Count)
                    length += _segments[i].length;

                length += _writeSegmentPosition;
            }

            if (length == _readLength)
                return;

            Debug.Assert(length > _readLength);

            _readCache = null;
            _readLength = length;
        }

        public void Discard()
        {
            if (_segments.Count == 0)
                return;

            int length = _readLength + _readSegmentPosition;
            int index = _readSegmentIdx;
            while (true)
            {
                var segment = _segments[index];
                if (length <= segment.length)
                {
                    _writeSegmentIdx = index;
                    _writeSegmentPosition = length;
                    _segments[index] = (segment.buffer, segment.buffer.Length);
                    break;
                }
                
                length -= _segments[index].length;
                index = (index + 1) % _segments.Count;
                Debug.Assert(index != _readSegmentIdx);
            }
        }
    }
}
