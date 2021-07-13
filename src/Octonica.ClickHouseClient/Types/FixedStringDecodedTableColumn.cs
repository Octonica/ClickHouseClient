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
using System.Text;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class FixedStringDecodedTableColumn : FixedStringTableColumnBase<string>
    {
        public FixedStringDecodedTableColumn(Memory<byte> buffer, int rowSize, Encoding encoding)
            : base(buffer, rowSize, encoding)
        {
        }

        protected override string GetValue(Encoding encoding, ReadOnlySpan<byte> span)
        {
            var charCount = encoding.GetCharCount(span);
            var charSpan = new Span<char>(new char[charCount]);

            encoding.GetChars(span, charSpan);

            int pos;
            for (pos = charSpan.Length - 1; pos >= 0; pos--)
            {
                if (charSpan[pos] != 0)
                    break;
            }

            charSpan = charSpan.Slice(0, pos + 1);
            if (charSpan.IsEmpty)
                return string.Empty;

            return new string(charSpan);
        }
    }
}
