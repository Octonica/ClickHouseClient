#region License Apache 2.0
/* Copyright 2019-2021 Octonica
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

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class DecimalTableColumn : IClickHouseTableColumn<decimal>
    {
        private const int MaxDecimalScale = 28;
        private static readonly uint[] Scales = {10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000};

        private readonly ReadOnlyMemory<uint> _buffer;
        private readonly int _elementSize;
        private readonly byte _scale;

        public int RowCount => _buffer.Length / _elementSize;

        public DecimalTableColumn(ReadOnlyMemory<uint> buffer, int elementSize, byte scale)
        {
            if (elementSize != 1 && elementSize != 2 && elementSize != 4)
                throw new ArgumentOutOfRangeException(nameof(elementSize));

            _buffer = buffer;
            _elementSize = elementSize;
            _scale = scale;
        }

        public bool IsNull(int index)
        {
            return false;
        }

        public decimal GetValue(int index)
        {
            var startIndex = index * _elementSize;
            var span = _buffer.Span;
            var lowLow = span[startIndex];
            bool isNegative;
            uint lowHigh, highLow;
            if (_elementSize == 1)
            {
                isNegative = (lowLow & unchecked((uint) int.MinValue)) != 0;
                if (isNegative)
                    lowLow = unchecked(0 - lowLow);

                lowHigh = highLow = 0;
            }
            else if (_elementSize == 2)
            {
                lowHigh = span[startIndex + 1];

                isNegative = (lowHigh & unchecked((uint) int.MinValue)) != 0;
                if (isNegative)
                {
                    if (lowLow == 0)
                    {
                        lowHigh = unchecked(0 - lowHigh);
                    }
                    else
                    {
                        lowLow = unchecked(0 - lowLow);
                        lowHigh ^= uint.MaxValue;
                    }
                }

                highLow = 0;
            }
            else
            {
                lowHigh = span[startIndex + 1];
                highLow = span[startIndex + 2];
                var highHigh = span[startIndex + 3];

                isNegative = (highHigh & unchecked((uint) int.MinValue)) != 0;
                if (isNegative)
                {
                    if (lowLow == 0)
                    {
                        if (lowHigh == 0)
                        {
                            if (highLow == 0)
                            {
                                highHigh = unchecked(0 - highHigh);
                            }
                            else
                            {
                                highLow = unchecked(0 - highLow);
                                highHigh ^= uint.MaxValue;
                            }
                        }
                        else
                        {
                            lowHigh = unchecked(0 - lowHigh);
                            highLow ^= uint.MaxValue;
                            highHigh ^= uint.MaxValue;
                        }
                    }
                    else
                    {
                        lowLow = unchecked(0 - lowLow);
                        lowHigh ^= uint.MaxValue;
                        highLow ^= uint.MaxValue;
                        highHigh ^= uint.MaxValue;
                    }
                }

                if (highHigh != 0 || _scale > MaxDecimalScale)
                {
                    uint mll = lowLow, mlh = lowHigh, mhl = highLow, mhh = highHigh;
                    byte scale = _scale;
                    var deltaScale = _scale > MaxDecimalScale ? _scale - MaxDecimalScale : 0;
                    // Attempt to rescale the value without loss of significant digits. It should work for values written by this DB driver.
                    while (mhh != 0 || deltaScale != 0)
                    {
                        int scaleIndex = 0;
                        if (deltaScale > 0)
                        {
                            scaleIndex = Math.Min(deltaScale, Scales.Length) - 1;
                            deltaScale -= (byte) (scaleIndex + 1);
                        }
                        else
                        {
                            for (; scaleIndex < Scales.Length - 1; scaleIndex++)
                            {
                                if (mhh < Scales[scaleIndex])
                                    break;
                            }
                        }

                        var mul = Scales[scaleIndex];

                        ulong rem = mhh % mul;
                        mhh /= mul;

                        var val = mhl | rem << 32;
                        rem = val % mul;
                        mhl = (uint) (val / mul);

                        val = mlh | rem << 32;
                        rem = val % mul;
                        mlh = (uint) (val / mul);

                        val = mll | rem << 32;
                        rem = val % mul;
                        if (rem != 0 || scaleIndex >= scale)
                            throw new NotSupportedException($"Value 0x{highHigh:x8}_{highLow:x8}_{lowHigh:x8}_{lowLow:x8} is too long for the type \"{typeof(decimal).FullName}\".");

                        mll = (uint) (val / mul);
                        scale = (byte) (scale - scaleIndex - 1);
                    }

                    return new decimal(unchecked((int) mll), unchecked((int) mlh), unchecked((int) mhl), isNegative, scale);
                }
            }

            return new decimal(unchecked((int) lowLow), unchecked((int) lowHigh), unchecked((int) highLow), isNegative, _scale);
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            return GetValue(index);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            if (typeof(T) == typeof(decimal?))
                return (IClickHouseTableColumn<T>)(object)new NullableStructTableColumn<decimal>(null, this);

            return null;
        }

        bool IClickHouseTableColumn.TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, out T dispatchedValue)
        {
            dispatchedValue = dispatcher.Dispatch(this);
            return true;
        }
    }
}
