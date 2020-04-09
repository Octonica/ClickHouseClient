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
using System.Text;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class FixedStringTableColumn : IClickHouseTableColumn<byte[]>
    {
        private readonly Memory<byte> _buffer;
        private readonly int _rowSize;
        private readonly Encoding _encoding;

        public int RowCount { get; }

        public FixedStringTableColumn(Memory<byte> buffer, int rowSize, Encoding encoding)
        {
            _buffer = buffer;
            _rowSize = rowSize;
            _encoding = encoding;
            RowCount = _buffer.Length / _rowSize;
        }

        public bool IsNull(int index)
        {
            return false;
        }

        public byte[] GetValue(int index)
        {
            var array = new byte[_rowSize];
            _buffer.Span.Slice(index * _rowSize, _rowSize).CopyTo(new Span<byte>(array));
            return array;
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            return GetValue(index);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            if (typeof(T) == typeof(string))
                return (IClickHouseTableColumn<T>?) (object) new FixedStringDecodedTableColumn(_buffer, _rowSize, _encoding);

            return null;
        }
    }

    internal sealed class FixedStringDecodedTableColumn : IClickHouseTableColumn<string>
    {
        private readonly Memory<byte> _buffer;
        private readonly int _rowSize;
        private readonly Encoding _encoding;

        public int RowCount { get; }

        public FixedStringDecodedTableColumn(Memory<byte> buffer, int rowSize, Encoding encoding)
        {
            _buffer = buffer;
            _rowSize = rowSize;
            _encoding = encoding;
            RowCount = _buffer.Length / _rowSize;
        }

        public bool IsNull(int index)
        {
            return false;
        }

        public string GetValue(int index)
        {
            var rowByteSpan = _buffer.Slice(index * _rowSize, _rowSize).Span;
            
            var charCount = _encoding.GetCharCount(rowByteSpan);
            var charSpan = new Span<char>(new char[charCount]);

            _encoding.GetChars(rowByteSpan, charSpan);

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

        object IClickHouseTableColumn.GetValue(int index)
        {
            return GetValue(index);
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            if (typeof(T) == typeof(byte[]))
                return (IClickHouseTableColumn<T>) (object) new FixedStringTableColumn(_buffer, _rowSize, _encoding);

            return null;
        }
    }
}
