#region License Apache 2.0
/* Copyright 2021, 2024 Octonica
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
    internal sealed class FixedStringDecodedCharArrayTableColumn : FixedStringTableColumnBase<char[]>
    {
        public override char[] DefaultValue => Array.Empty<char>();

        public FixedStringDecodedCharArrayTableColumn(Memory<byte> buffer, int rowSize, Encoding encoding)
            : base(buffer, rowSize, encoding)
        {
        }

        protected override char[] GetValue(Encoding encoding, ReadOnlySpan<byte> span)
        {
            var charCount = encoding.GetCharCount(span);
            var result = new char[charCount];

            encoding.GetChars(span, result);

            int pos;
            for (pos = result.Length - 1; pos >= 0; pos--)
            {
                if (result[pos] != 0)
                    break;
            }

            if (pos == 0)
                return Array.Empty<char>();

            if (pos + 1 < result.Length)
                Array.Resize(ref result, pos + 1);

            return result;
        }
    }
}
