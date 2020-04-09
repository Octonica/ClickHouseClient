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

namespace Octonica.ClickHouseClient.Protocol
{
    public readonly struct SequenceSize
    {
        public int Bytes { get; }

        public int Elements { get; }

        public SequenceSize(int bytes, int elements)
        {
            Bytes = bytes;
            Elements = elements;
        }

        public SequenceSize AddBytes(int bytes)
        {
            return new SequenceSize(Bytes + bytes, Elements);
        }

        public SequenceSize AddElements(int elements)
        {
            return new SequenceSize(Bytes, Elements + elements);
        }

        public SequenceSize Add(SequenceSize size)
        {
            return new SequenceSize(Bytes + size.Bytes, Elements + size.Elements);
        }

        public static SequenceSize operator +(SequenceSize x, SequenceSize y)
        {
            return new SequenceSize(x.Bytes + y.Bytes, x.Elements + y.Elements);
        }
    }
}
