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
            var encoding = Encoding.UTF8;
            int length = includeQuotes ? 2 : 0, i = 0;
            while (i < value.Length)
            {
                var idx = value.Span.Slice(i).IndexOf("\\'\r\n\t");
                if (idx >= 0)
                {
                    _escapeIndices ??= new List<int>(4);

                    length += 2;
                    _escapeIndices.Add(i + idx);
                    var slice = value.Slice(i, idx - i).Span;
                    length += encoding.GetByteCount(slice);
                    i = idx + 1;
                }
                else
                {
                    var slice = value.Slice(i).Span;
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

            var encoding = Encoding.UTF8;
            int i = 0, bytesWritten = 0;
            if (_includeQuotes)
                buffer.Span[bytesWritten++] = (byte)'\'';

            if (_escapeIndices != null)
            {
                foreach (var escapeIdx in _escapeIndices)
                {
                    var slice = _value.Slice(i, escapeIdx - i);
                    bytesWritten += encoding.GetBytes(slice.Span, buffer.Slice(bytesWritten).Span);
                    var escapeBuffer = buffer.Slice(bytesWritten).Span;

                    switch (_value.Span[escapeIdx])
                    {
                        case '\'':
                            escapeBuffer[0] = (byte)'\'';
                            escapeBuffer[1] = (byte)'\'';
                            break;

                        case '\r':
                            escapeBuffer[0] = (byte)'\\';
                            escapeBuffer[1] = (byte)'\r';
                            break;

                        case '\n':
                            escapeBuffer[0] = (byte)'\\';
                            escapeBuffer[1] = (byte)'\n';
                            break;

                        case '\t':
                            escapeBuffer[0] = (byte)'\\';
                            escapeBuffer[1] = (byte)'\t';
                            break;

                        default:
                            escapeBuffer[0] = (byte)'\\';
                            escapeBuffer[1] = (byte)_value.Span[escapeIdx];
                            break;
                    }

                    bytesWritten += 2;
                    i = escapeIdx + 1;
                }
            }

            if (i < _value.Length)
                bytesWritten += encoding.GetBytes(_value.Slice(i).Span, buffer.Slice(bytesWritten).Span);

            if (_includeQuotes)
                buffer.Span[bytesWritten++] = (byte)'\'';

            Debug.Assert(bytesWritten == Length);
            return bytesWritten;
        }
    }
}
