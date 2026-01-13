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
//
// CityHash, by Geoff Pike and Jyrki Alakuijala
//
// This file provides CityHash128() and related functions.
//
// It's probably possible to create even faster hash functions by
// writing a program that systematically explores some of the space of
// possible hash functions, by using SIMD instructions, or by
// compromising on hash quality.
#endregion

using System;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace Octonica.ClickHouseClient.Protocol
{
    // CityHash v1.0.2 implementation from ClickHouse
    internal static class CityHash
    {
        private static ulong UNALIGNED_LOAD64(ReadOnlySequence<byte> p)
        {
            if (p.FirstSpan.Length > sizeof(ulong))
            {
                return BitConverter.ToUInt64(p.FirstSpan);
            }

            Span<byte> tmpBuffer = stackalloc byte[sizeof(ulong)];
            p.Slice(0, sizeof(ulong)).CopyTo(tmpBuffer);
            return BitConverter.ToUInt64(tmpBuffer);
        }

        private static uint UNALIGNED_LOAD32(ReadOnlySequence<byte> p)
        {
            if (p.FirstSpan.Length > sizeof(uint))
            {
                return BitConverter.ToUInt32(p.FirstSpan);
            }

            Span<byte> tmpBuffer = stackalloc byte[sizeof(uint)];
            p.Slice(0, sizeof(uint)).CopyTo(tmpBuffer);
            return BitConverter.ToUInt32(tmpBuffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong Fetch64(ReadOnlySequence<byte> p)
        {
            return UNALIGNED_LOAD64(p);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static uint Fetch32(ReadOnlySequence<byte> p)
        {
            return UNALIGNED_LOAD32(p);
        }

        // Some primes between 2^63 and 2^64 for various uses.
        private const ulong k0 = 0xc3a5c85c97cb3127UL;
        private const ulong k1 = 0xb492b66fbe98f273UL;
        private const ulong k2 = 0x9ae16a3b2f90404fUL;
        private const ulong k3 = 0xc949d7c7509e6557UL;

        // Bitwise right rotate.  Normally this will compile to a single
        // instruction, especially if the shift is a manifest constant.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong Rotate(ulong val, int shift)
        {
            // Avoid shifting by 64: doing so yields an undefined result.
            return shift == 0 ? val : ((val >> shift) | (val << (64 - shift)));
        }

        // Equivalent to Rotate(), but requires the second arg to be non-zero.
        // On x86-64, and probably others, it's possible for this to compile
        // to a single instruction if both args are already in registers.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong RotateByAtLeast1(ulong val, int shift)
        {
            return (val >> shift) | (val << (64 - shift));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong ShiftMix(ulong val)
        {
            return val ^ (val >> 47);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong HashLen16(ulong u, ulong v)
        {
            return Hash128to64(new UInt128(v, u));
        }

        // Hash 128 input bits down to 64 bits of output.
        // This is intended to be a reasonably good hash function.
        private static ulong Hash128to64(UInt128 x)
        {
            // Murmur-inspired hashing.
            const ulong kMul = 0x9ddfea08eb382d69UL;

            unchecked
            {
#if NET8_0_OR_GREATER
                ulong low = (ulong)x;
                ulong high = (ulong)(x >> 64);
                ulong a = (low ^ high) * kMul;
                a ^= a >> 47;
                ulong b = (high ^ a) * kMul;
#else
                UInt64 a = (x.Low ^ x.High) * kMul;
                a ^= (a >> 47);
                UInt64 b = (x.High ^ a) * kMul;
#endif
                b ^= b >> 47;
                b *= kMul;
                return b;
            }
        }

        private static ulong HashLen0to16(ReadOnlySequence<byte> s)
        {
            ulong len = (ulong)s.Length;

            unchecked
            {
                if (len > 8)
                {
                    ulong a = Fetch64(s);
                    ulong b = Fetch64(s.Slice((int)len - 8));
                    return HashLen16(a, RotateByAtLeast1(b + len, (int)len)) ^ b;
                }

                if (len >= 4)
                {
                    ulong a = Fetch32(s);
                    return HashLen16(len + (a << 3), Fetch32(s.Slice((int)len - 4)));
                }

                if (len > 0)
                {
                    byte a = s.FirstSpan[0];
                    byte b = s.Slice((int)len >> 1).FirstSpan[0];
                    byte c = s.Slice((int)len - 1).FirstSpan[0];

                    uint y = a + ((uint)b << 8);
                    uint z = (uint)(len + ((uint)c << 2));
                    return ShiftMix((y * k2) ^ (z * k3)) * k2;
                }

                return k2;
            }
        }

        // Return a 16-byte hash for 48 bytes.  Quick and dirty.
        // Callers do best to use "random-looking" values for a and b.
        private static (ulong first, ulong second) WeakHashLen32WithSeeds(
            ulong w,
            ulong x,
            ulong y,
            ulong z,
            ulong a,
            ulong b)
        {
            a += w;
            b = Rotate(b + a + z, 21);
            ulong c = a;
            a += x;
            a += y;
            b += Rotate(a, 44);
            return (a + z, b + c);
        }

        // Return a 16-byte hash for s[0] ... s[31], a, and b.  Quick and dirty.
        private static (ulong first, ulong second) WeakHashLen32WithSeeds(
            ReadOnlySequence<byte> s,
            ulong a,
            ulong b)
        {
            return WeakHashLen32WithSeeds(
                Fetch64(s),
                Fetch64(s.Slice(8)),
                Fetch64(s.Slice(16)),
                Fetch64(s.Slice(24)),
                a,
                b);
        }

        // A subroutine for CityHash128().  Returns a decent 128-bit hash for strings
        // of any length representable in ssize_t.  Based on City and Murmur.
        private static UInt128 CityMurmur(ReadOnlySequence<byte> s, UInt128 seed)
        {
            ulong len = (ulong)s.Length;

            unchecked
            {
#if NET8_0_OR_GREATER
                ulong a = (ulong)seed;
                ulong b = (ulong)(seed >> 64);
#else
                UInt64 a = seed.Low;
                UInt64 b = seed.High;
#endif
                ulong c = 0;
                ulong d = 0;
                int l = (int)len - 16;
                if (l <= 0)
                {
                    // len <= 16
                    a = ShiftMix(a * k1) * k1;
                    c = (b * k1) + HashLen0to16(s);
                    d = ShiftMix(a + (len >= 8 ? Fetch64(s) : c));
                }
                else
                {
                    // len > 16
                    c = HashLen16(Fetch64(s.Slice((int)len - 8)) + k1, a);
                    d = HashLen16(b + len, c + Fetch64(s.Slice((int)len - 16)));
                    a += d;
                    do
                    {
                        a ^= ShiftMix(Fetch64(s) * k1) * k1;
                        a *= k1;
                        b ^= a;
                        c ^= ShiftMix(Fetch64(s.Slice(8)) * k1) * k1;
                        c *= k1;
                        d ^= c;
                        s = s.Slice(16);
                        l -= 16;
                    } while (l > 0);
                }

                a = HashLen16(a, c);
                b = HashLen16(d, b);
                return new UInt128(HashLen16(b, a), a ^ b);
            }
        }

        public static UInt128 CityHash128WithSeed(ReadOnlySequence<byte> s, UInt128 seed)
        {
            ulong len = (ulong)s.Length;

            if (len < 128)
            {
                return CityMurmur(s, seed);
            }

            // We expect len >= 128 to be the common case.  Keep 56 bytes of state:
            // v, w, x, y, and z.
            unchecked
            {
                (ulong first, ulong second) v, w;
#if NET8_0_OR_GREATER
                ulong x = (ulong)seed;
                ulong y = (ulong)(seed >> 64);
#else
                UInt64 x = seed.Low;
                UInt64 y = seed.High;
#endif
                ulong z = len * k1;
                v.first = (Rotate(y ^ k1, 49) * k1) + Fetch64(s);
                v.second = (Rotate(v.first, 42) * k1) + Fetch64(s.Slice(8));
                w.first = (Rotate(y + z, 35) * k1) + x;
                w.second = Rotate(x + Fetch64(s.Slice(88)), 53) * k1;

                // This is the same inner loop as CityHash64(), manually unrolled.
                ReadOnlySequence<byte> sOrig = s;
                do
                {
                    x = Rotate(x + y + v.first + Fetch64(s.Slice(16)), 37) * k1;
                    y = Rotate(y + v.second + Fetch64(s.Slice(48)), 42) * k1;
                    x ^= w.second;
                    y ^= v.first;
                    z = Rotate(z ^ w.first, 33);
                    v = WeakHashLen32WithSeeds(s, v.second * k1, x + w.first);
                    w = WeakHashLen32WithSeeds(s.Slice(32), z + w.second, y);

                    (z, x) = (x, z);
                    s = s.Slice(64);
                    x = Rotate(x + y + v.first + Fetch64(s.Slice(16)), 37) * k1;
                    y = Rotate(y + v.second + Fetch64(s.Slice(48)), 42) * k1;
                    x ^= w.second;
                    y ^= v.first;
                    z = Rotate(z ^ w.first, 33);
                    v = WeakHashLen32WithSeeds(s, v.second * k1, x + w.first);
                    w = WeakHashLen32WithSeeds(s.Slice(32), z + w.second, y);

                    (z, x) = (x, z);
                    s = s.Slice(64);
                    len -= 128;
                } while (len >= 128);

                long offset = sOrig.Length - s.Length;
                y += (Rotate(w.first, 37) * k0) + z;
                x += Rotate(v.first + z, 49) * k0;
                // If 0 < len < 128, hash up to 4 chunks of 32 bytes each from the end of s.
                for (int tail_done = 0; tail_done < (int)len;)
                {
                    tail_done += 32;
                    y = (Rotate(y - x, 42) * k0) + v.second;
                    w.first += Fetch64(sOrig.Slice((int)len - tail_done + 16 + offset));
                    x = (Rotate(x, 49) * k0) + w.first;
                    w.first += v.first;
                    v = WeakHashLen32WithSeeds(sOrig.Slice((int)len - tail_done + offset), v.first, v.second);
                }

                // At this point our 48 bytes of state should contain more than
                // enough information for a strong 128-bit hash.  We use two
                // different 48-byte-to-8-byte hashes to get a 16-byte final result.
                x = HashLen16(x, v.first);
                y = HashLen16(y, w.first);
                return new UInt128(
                    HashLen16(x + w.second, y + v.second),
                    HashLen16(x + v.second, w.second) + y);
            }
        }

        public static UInt128 CityHash128(ReadOnlySequence<byte> s)
        {
            ulong len = (ulong)s.Length;

            return len >= 16
                ? CityHash128WithSeed(
                    s.Slice(16),
                    new UInt128(
                        Fetch64(s.Slice(8)),
                        Fetch64(s) ^ k3))
                : len >= 8
                    ? CityHash128WithSeed(
                                    ReadOnlySequence<byte>.Empty,
                                    new UInt128(
                                        Fetch64(s.Slice((int)len - 8)) ^ k1,
                                        Fetch64(s) ^ unchecked(len * k0)))
                    : CityHash128WithSeed(s, new UInt128(k1, k0));
        }
    }

#if !NET8_0_OR_GREATER
    internal readonly struct UInt128
    {
        public ulong Low { get; }
        public ulong High { get; }

        public UInt128(ulong high, ulong low)
        {
            Low = low;
            High = high;
        }
    }
#endif
}
