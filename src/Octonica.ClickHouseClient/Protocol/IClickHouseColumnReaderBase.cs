#region License Apache 2.0
/* Copyright 2021 Octonica
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

using System.Buffers;

namespace Octonica.ClickHouseClient.Protocol
{
    /// <summary>
    /// The basic interface for objects capable of reading columnar data from a binary buffer.
    /// </summary>
    public interface IClickHouseColumnReaderBase
    {
        /// <summary>
        /// When implemented reads as much bytes as possible from the binary buffer.
        /// </summary>
        /// <param name="sequence">The binary buffer.</param>
        /// <returns>The length of data that were read or <see cref="SequenceSize.Empty"/> if the provided buffer is too small.</returns>
        SequenceSize ReadNext(ReadOnlySequence<byte> sequence);
    }
}
