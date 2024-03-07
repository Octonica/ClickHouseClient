#region License Apache 2.0
/* Copyright 2024 Octonica
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

namespace Octonica.ClickHouseClient.Types
{
    /// <summary>
    /// Provides supported serialization modes for <see cref="IClickHouseColumnTypeInfo.CreateColumnReader(int, ClickHouseColumnSerializationMode)"/>
    /// and <see cref="IClickHouseColumnTypeInfo.CreateSkippingColumnReader(int, ClickHouseColumnSerializationMode)"/>.
    /// </summary>
    public enum ClickHouseColumnSerializationMode
    {
        /// <summary>
        /// Default serialization. In this mode the reader should expect only column values.
        /// </summary>
        Default = 0,

        /// <summary>
        /// Sparse serialization. In this mode the reader should expect the list of 'granules' followed by column values.
        /// </summary>
        Sparse = 1,

        /// <summary>
        /// Custom serialization. In this mode the reader should expect serialization settings followed by actual column values.
        /// </summary>
        Custom = 0xAAAA
    }
}
