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

using System;

namespace Octonica.ClickHouseClient.Protocol
{
    /// <summary>
    /// The base interface for a parameter's value writer. When implemented writes a parameter value in a binary format.
    /// </summary>
    /// <remarks>
    /// Being a part of the ClickHouseClient's infrastructure, the interface <see cref="IClickHouseParameterValueWriter"/> is considered unstable. It can be changed between minor versions.
    /// </remarks>
    public interface IClickHouseParameterValueWriter
    {
        /// <summary>
        /// The length of the parameter's value in bytes.
        /// If the length is less than zero the parameter will not be passed to the query.
        /// </summary>
        int Length { get; }

        /// <summary>
        /// When implemented writes the value to the <paramref name="buffer"/>.
        /// </summary>
        /// <param name="buffer">The buffer for writing the value. A caller of this method must ensure that the size of the buffer is not less than <see cref="Length"/>.</param>
        /// <returns>The number of written bytes.</returns>
        int Write(Memory<byte> buffer);
    }
}
