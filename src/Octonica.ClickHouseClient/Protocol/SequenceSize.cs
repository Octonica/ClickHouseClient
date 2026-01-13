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

namespace Octonica.ClickHouseClient.Protocol
{
    /// <summary>
    /// Represents the size of a sequence of elements both in the number of bytes and in the number of elements.
    /// </summary>
    public readonly struct SequenceSize
    {
        /// <summary>
        /// The size of an empty sequence that has zero elements and zero bytes.
        /// </summary>
        public static readonly SequenceSize Empty = new(0, 0);

        /// <summary>
        /// The number of bytes in the sequence.
        /// </summary>        
        public int Bytes { get; }

        /// <summary>
        /// The number of elements in the sequence.
        /// </summary>
        /// <remarks>The sequence can contain zero elements with non-zero bytes when it has headers or other metadata.</remarks>
        public int Elements { get; }

        /// <summary>
        /// Initializes a new instance of <see cref="SequenceSize"/> with the specified number of bytes and elements.
        /// </summary>
        /// <param name="bytes">The number of bytes in the sequence.</param>
        /// <param name="elements">The number of elements in the sequence.</param>
        public SequenceSize(int bytes, int elements)
        {
            Bytes = bytes;
            Elements = elements;
        }

        /// <summary>
        /// Creates and returns a copy of <see cref="SequenceSize"/> with the specified number of bytes added to it.
        /// </summary>
        /// <param name="bytes">The number of bytes that should be added to the <see cref="SequenceSize"/>.</param>
        /// <returns>The copy of <see cref="SequenceSize"/> with the specified number of bytes added to it.</returns>
        public SequenceSize AddBytes(int bytes)
        {
            return new SequenceSize(Bytes + bytes, Elements);
        }

        /// <summary>
        /// Creates and returns a copy of <see cref="SequenceSize"/> with the specified number of elements added to it.
        /// </summary>
        /// <param name="elements">The number of elements that should be added to the <see cref="SequenceSize"/>.</param>
        /// <returns>The copy of <see cref="SequenceSize"/> with the specified number of bytes added to it.</returns>
        public SequenceSize AddElements(int elements)
        {
            return new SequenceSize(Bytes, Elements + elements);
        }

        /// <summary>
        /// Creates and returns a copy of <see cref="SequenceSize"/> with the specified size added to it.
        /// The number of elements and the number of bytes are summed independently.
        /// </summary>
        /// <param name="size">The <see cref="SequenceSize"/> that should be added to this <see cref="SequenceSize"/>.</param>
        /// <returns>The copy of <see cref="SequenceSize"/> with the specified size added to it.</returns>
        public SequenceSize Add(SequenceSize size)
        {
            return new SequenceSize(Bytes + size.Bytes, Elements + size.Elements);
        }

        /// <summary>
        /// Creates and returns a new instance <see cref="SequenceSize"/> representing the sum of two arguments.
        /// The number of elements and the number of bytes are summed independently.
        /// </summary>
        /// <param name="x">The first <see cref="SequenceSize"/>.</param>
        /// <param name="y">The second <see cref="SequenceSize"/>.</param>
        /// <returns>The new instance <see cref="SequenceSize"/> representing the sum of two arguments.</returns>
        public static SequenceSize operator +(SequenceSize x, SequenceSize y)
        {
            return new SequenceSize(x.Bytes + y.Bytes, x.Elements + y.Elements);
        }
    }
}
