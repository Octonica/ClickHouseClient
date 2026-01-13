#region License Apache 2.0
/* Copyright 2023 Octonica
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

using Octonica.ClickHouseClient.Protocol;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class StringLiteralValueWriter : IClickHouseParameterValueWriter
    {
        private readonly ReadOnlyMemory<char> _value;
        private readonly bool _includeQuotes;
        private readonly List<int>? _escapeIndices;

        public int Length { get; }

        public StringLiteralValueWriter(ReadOnlyMemory<char> value, bool includeQuotes)
        {
            Encoding encoding = Encoding.UTF8;
            int length = includeQuotes ? 4 : 0, i = 0;
            while (i < value.Length)
            {
                int idx = value.Span[i..].IndexOfAny("\\'\r\n\t");
                if (idx >= 0)
                {
                    _escapeIndices ??= new List<int>(4);

                    length += 4;
                    _escapeIndices.Add(i + idx);
                    ReadOnlySpan<char> slice = value.Slice(i, idx).Span;
                    length += encoding.GetByteCount(slice);
                    i += idx + 1;
                }
                else
                {
                    ReadOnlySpan<char> slice = value[i..].Span;
                    length += encoding.GetByteCount(slice);
                    i = value.Length;
                }
            }

            _value = value;
            _includeQuotes = includeQuotes;
            Length = length;
        }

        public int Write(Memory<byte> buffer)
        {
            Debug.Assert(buffer.Length >= Length);

            Encoding encoding = Encoding.UTF8;
            int i = 0, bytesWritten = 0;
            if (_includeQuotes)
            {
                buffer.Span[bytesWritten++] = (byte)'\\';
                buffer.Span[bytesWritten++] = (byte)'\'';
            }

            if (_escapeIndices != null)
            {
                foreach (int escapeIdx in _escapeIndices)
                {
                    ReadOnlyMemory<char> slice = _value[i..escapeIdx];
                    bytesWritten += encoding.GetBytes(slice.Span, buffer[bytesWritten..].Span);
                    Span<byte> escapeBuffer = buffer[bytesWritten..].Span;

                    escapeBuffer[0] = (byte)'\\';
                    escapeBuffer[1] = (byte)'\\';
                    escapeBuffer[2] = (byte)'\\';
                    escapeBuffer[3] = (byte)(_value.Span[escapeIdx] switch
                    {
                        '\r' => 'r',
                        '\n' => 'n',
                        '\t' => 't',
                        var c => c
                    });

                    bytesWritten += 4;
                    i = escapeIdx + 1;
                }
            }

            if (i < _value.Length)
            {
                bytesWritten += encoding.GetBytes(_value[i..].Span, buffer[bytesWritten..].Span);
            }

            if (_includeQuotes)
            {
                buffer.Span[bytesWritten++] = (byte)'\\';
                buffer.Span[bytesWritten++] = (byte)'\'';
            }

            Debug.Assert(bytesWritten == Length);
            return bytesWritten;
        }
    }
}
