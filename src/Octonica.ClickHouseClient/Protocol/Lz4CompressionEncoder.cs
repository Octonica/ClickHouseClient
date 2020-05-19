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
using K4os.Compression.LZ4;
using K4os.Compression.LZ4.Encoders;

namespace Octonica.ClickHouseClient.Protocol
{
    internal sealed class Lz4CompressionEncoder : CompressionEncoderBase
    {
        private readonly LZ4BlockEncoder _encoder;
        private readonly byte[] _compressedBuffer;

        private int _compressedSize;
        private int _compressedAvailable = -1;

        protected override byte AlgorithmIdentifier => 0x82;

        public override CompressionAlgorithm Algorithm => CompressionAlgorithm.Lz4;

        public Lz4CompressionEncoder(int bufferSize, int blockSize)
            : base(bufferSize)
        {
            _encoder = new LZ4BlockEncoder(LZ4Level.L10_OPT, blockSize);
            _compressedBuffer = new byte[LZ4Codec.MaximumOutputSize(blockSize)];
        }

        protected override int ConsumeNext(byte[] source, int offset, int length)
        {
            _compressedAvailable = -1;
            return _encoder.Topup(source, offset, length);
        }

        protected override int EncodeNext(byte[] target, int offset, int length)
        {
            if (_compressedAvailable == 0)
                return 0;

            if (_compressedAvailable <0)
            {
                _compressedSize = _encoder.Encode(_compressedBuffer, 0, _compressedBuffer.Length, false);
                _compressedAvailable = _compressedSize;
            }

            var maxLen = Math.Min(length, _compressedAvailable);
            Array.Copy(_compressedBuffer, _compressedSize - _compressedAvailable, target, offset, maxLen);
            _compressedAvailable -= maxLen;

            return maxLen;
        }

        protected override int EncodeFinal(byte[] target, int offset, int length)
        {
            if (_compressedAvailable == 0)
                return 0;

            if (_compressedAvailable < 0)
            {
                _encoder.FlushAndEncode(_compressedBuffer, 0, _compressedBuffer.Length, false, out _compressedSize);
                _compressedAvailable = _compressedSize;
            }

            var maxLen = Math.Min(length, _compressedAvailable);
            Array.Copy(_compressedBuffer, _compressedSize - _compressedAvailable, target, offset, maxLen);
            _compressedAvailable -= maxLen;

            return maxLen;
        }

        public override void Dispose()
        {
            _encoder.Dispose();
        }
    }
}
