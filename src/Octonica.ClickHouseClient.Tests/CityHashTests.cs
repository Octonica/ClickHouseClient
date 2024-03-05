#region License
// Copyright (c) 2011 Google, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
#endregion

using System;
using System.Buffers;
using Octonica.ClickHouseClient.Protocol;
using Xunit;

namespace Octonica.ClickHouseClient.Tests
{
    public partial class CityHashTests
    {
        const UInt64 k0 = 0xc3a5c85c97cb3127UL;
        const UInt64 kSeed0 = 1234567;
        const UInt64 kSeed1 = k0;
        static readonly UInt128 kSeed128 = new UInt128(kSeed1, kSeed0);
        const int kDataSize = 1 << 20;
        const int kTestSize = 300;

        byte[]? data;

        // Initialize data to pseudorandom values.
        void setup()
        {
            data = new byte[kDataSize];
            UInt64 a = 9;
            UInt64 b = 777;
            for (int i = 0; i < kDataSize; i++)
            {
                a += b;
                b += a;
                a = (a ^ (a >> 41)) * k0;
                b = (b ^ (b >> 41)) * k0 + (UInt64) i;
                var u = (byte)(b >> 37);
                data[i] = u; //memcpy(data + i, &u, 1); // uint8 -> char
            }
        }

        //#define C(x) 0x ## x ## ULL
        private static UInt64 C(UInt64 v)
        {
            return v;
        }

        void Test(int index, int offset, int len)
        {
            var seq = new ReadOnlySequence<byte>(data!, offset, len);

            UInt128 u = CityHash.CityHash128(seq);
            UInt128 v = CityHash.CityHash128WithSeed(seq, kSeed128);
#if NET8_0_OR_GREATER
            Assert.Equal(new UInt128(testdata[index, 4], testdata[index, 3]), u);
            Assert.Equal(new UInt128(testdata[index, 6], testdata[index, 5]), v);
#else
            Assert.Equal(testdata[index, 3], u.Low);
            Assert.Equal(testdata[index, 4], u.High);
            Assert.Equal(testdata[index, 5], v.Low);
            Assert.Equal(testdata[index, 6], v.High);
#endif
        }

        [Fact]
        public void main()
        {
            setup();
            int i = 0;
            for (; i < kTestSize - 1; i++)
            {
                Test(i, i * i, i);
            }
            Test(i, 0, kDataSize);
        }
    }
}