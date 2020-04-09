#region License Apache 2.0
/* Copyright 2019-2020 Octonica LLC
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
using System.Diagnostics;
using Octonica.ClickHouseClient.Exceptions;

namespace Octonica.ClickHouseClient.Protocol
{
    internal abstract class CompressionDecoderBase : IDisposable
    {
        private readonly int _bufferSize;

        private byte[]? _compressedBuffer;
        private byte[]? _decompressedBuffer;

        private int _compressedPosition;
        private int _compressedSize;

        private int _decompressedAvailable;
        private int _decompressedSize;

        protected abstract byte AlgorithmIdentifier { get; }

        public abstract CompressionAlgorithm Algorithm { get; }

        public bool IsCompleted => _compressedPosition == _compressedSize;

        protected CompressionDecoderBase(int bufferSize)
        {
            _bufferSize = bufferSize;
        }

        public int ReadHeader(ReadOnlySequence<byte> sequence)
        {
            if (!IsCompleted)
                throw new ClickHouseException(ClickHouseErrorCodes.CompressionDecoderError, "Can't start reading of a new block because reading of the current compressed block is not finished.");

            const int cityHashSize = 16, headerSize = sizeof(byte) + 2 * sizeof(int);
            if (sequence.Length < cityHashSize + headerSize)
                return -1;

            byte algorithmIdentifier = sequence.Slice(cityHashSize).FirstSpan[0];
            if (algorithmIdentifier != AlgorithmIdentifier)
            {
                throw new ClickHouseException(
                    ClickHouseErrorCodes.CompressionDecoderError,
                    $"An unexpected compression algorithm identifier was received. Expected value: 0x{AlgorithmIdentifier:X}. Actual value: 0x{algorithmIdentifier:X}.");
            }

            var slice = sequence.Slice(cityHashSize + sizeof(byte));
            Span<byte> intBuffer = stackalloc byte[sizeof(int)];

            slice.Slice(0, sizeof(int)).CopyTo(intBuffer);
            _compressedSize = BitConverter.ToInt32(intBuffer);
            _compressedSize -= headerSize;

            slice.Slice(sizeof(int), sizeof(int)).CopyTo(intBuffer);
            var decompressedSize = BitConverter.ToInt32(intBuffer);

            _compressedPosition = 0;

            if (_compressedBuffer == null || _compressedBuffer.Length < _compressedSize)
                _compressedBuffer = new byte[GetBufferSize(_compressedSize)];

            if (_decompressedBuffer == null)
            {
                Debug.Assert(_decompressedAvailable == 0);
                _decompressedBuffer = new byte[GetBufferSize(decompressedSize)];
            }
            else if (_decompressedBuffer.Length < decompressedSize + _decompressedAvailable)
            {
                var newBuffer = new byte[GetBufferSize(decompressedSize + _decompressedAvailable)];
                if (_decompressedAvailable > 0)
                    Array.Copy(_decompressedBuffer, _decompressedSize - _decompressedAvailable, newBuffer, 0, _decompressedAvailable);

                _decompressedBuffer = newBuffer;
            }
            else
            {
                for (int i = 0, j = _decompressedSize - _decompressedAvailable; j < _decompressedSize; i++, j++)
                    _decompressedBuffer[i] = _decompressedBuffer[j];
            }

            _decompressedSize = decompressedSize + _decompressedAvailable;
            return cityHashSize + headerSize;
        }

        public ReadOnlySequence<byte> Read()
        {
            if (!IsCompleted || _decompressedBuffer == null || _compressedBuffer == null)
                return ReadOnlySequence<byte>.Empty;

            return new ReadOnlySequence<byte>(_decompressedBuffer, _decompressedSize - _decompressedAvailable, _decompressedAvailable);
        }

        public void AdvanceReader(SequencePosition position)
        {
            if (!ReferenceEquals(position.GetObject(), _decompressedBuffer))
                throw new ArgumentException("The position doesn't belong to the sequence.", nameof(position));

            var arrayIndex = position.GetInteger();
            if (arrayIndex < 0)
                arrayIndex = unchecked(arrayIndex - int.MinValue);

            var relativePosition = arrayIndex - (_decompressedSize - _decompressedAvailable);
            if (relativePosition < 0)
                throw new ArgumentOutOfRangeException(nameof(position), "The position must be a non-negative number.");
            if (relativePosition == 0)
                return;

            if (relativePosition > _decompressedAvailable)
                throw new ArgumentOutOfRangeException(nameof(position), "The position must not be greater then the length.");

            _decompressedAvailable -= relativePosition;
        }

        public int ConsumeNext(ReadOnlySequence<byte> sequence)
        {
            if (_compressedPosition == _compressedSize || _compressedBuffer == null)
                return 0;

            var sequencePart = sequence;
            if (sequence.Length > _compressedSize - _compressedPosition)
                sequencePart = sequence.Slice(0, _compressedSize - _compressedPosition);

            sequencePart.CopyTo(((Span<byte>) _compressedBuffer).Slice(_compressedPosition));
            _compressedPosition += (int) sequencePart.Length;

            if (_compressedPosition == _compressedSize)
            {
                var sourceSpan = new Span<byte>(_compressedBuffer, 0, _compressedSize);
                var targetSpan = new Span<byte>(_decompressedBuffer, _decompressedAvailable, _decompressedSize - _decompressedAvailable);
                _decompressedAvailable += Decode(sourceSpan, targetSpan);

                Debug.Assert(_decompressedAvailable == _decompressedSize);
            }

            return (int) sequencePart.Length;
        }

        public void Reset()
        {
            _compressedPosition = 0;
            _compressedSize = 0;
            _decompressedAvailable = 0;
            _decompressedSize = 0;
        }

        protected abstract int Decode(ReadOnlySpan<byte> source, Span<byte> target);

        public abstract void Dispose();

        private int GetBufferSize(int minRequiredSize)
        {
            int size = _bufferSize;
            while (size < minRequiredSize)
                size *= 2;

            return size;
        }
    }
}
