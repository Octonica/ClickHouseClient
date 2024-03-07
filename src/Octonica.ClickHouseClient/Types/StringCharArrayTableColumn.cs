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
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class StringCharArrayTableColumn : StringTableColumnBase<char[]>
    {
        public override char[] DefaultValue => Array.Empty<char>();

        public StringCharArrayTableColumn(Encoding encoding, List<(int segmentIndex, int offset, int length)> layouts, List<Memory<byte>> segments) 
            : base(encoding, layouts, segments)
        {
        }

        protected override char[] GetValue(Encoding encoding, ReadOnlySpan<byte> span)
        {
            if (span.IsEmpty)
                return Array.Empty<char>();

            var charCount = encoding.GetCharCount(span);
            var result = new char[charCount];

            var length = encoding.GetChars(span, result);
            Debug.Assert(length == charCount);
            return result;            
        }
    }
}
