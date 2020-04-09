#region License Apache 2.0
/* Copyright 2019-2020 Octonica LLC
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
using System.Text;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class StringTableColumn : IClickHouseTableColumn<string?>
    {
        private readonly Encoding _encoding;
        private readonly List<(int segmentIndex, int offset, int length)> _layouts;
        private readonly List<Memory<byte>> _segments;

        public int RowCount => _layouts.Count;

        public StringTableColumn(Encoding encoding, List<(int segmentIndex, int offset, int length)> layouts, List<Memory<byte>> segments)
        {
            _encoding = encoding ?? throw new ArgumentNullException(nameof(encoding));
            _layouts = layouts ?? throw new ArgumentNullException(nameof(layouts));
            _segments = segments ?? throw new ArgumentNullException(nameof(segments));
        }

        public bool IsNull(int index)
        {
            return false;
        }

        public string GetValue(int index)
        {
            var (segmentIndex, offset, length) = _layouts[index];
            if (length == 0)
                return string.Empty;

            var span = _segments[segmentIndex].Slice(offset, length).Span;
            var result = _encoding.GetString(span);
            return result;
        }

        object IClickHouseTableColumn.GetValue(int index)
        {
            var (segmentIndex, offset, length) = _layouts[index];
            if (length == 0)
                return string.Empty;

            var span = _segments[segmentIndex].Slice(offset, length).Span;
            var result = _encoding.GetString(span);
            return result;
        }

        public IClickHouseTableColumn<T>? TryReinterpret<T>()
        {
            return null;
        }
    }
}
