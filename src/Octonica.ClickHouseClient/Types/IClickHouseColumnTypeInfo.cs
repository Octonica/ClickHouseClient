#region License Apache 2.0
/* Copyright 2020-2021, 2023-2024 Octonica
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
using Octonica.ClickHouseClient.Protocol;

namespace Octonica.ClickHouseClient.Types
{
    /// <summary>
    /// Represents basic information about the ClickHouse type. Provides access to factory methods for creating column readers and writers.
    /// </summary>
    /// <remarks>
    /// Being a part of the ClickHouseClient's infrastructure, the interface <see cref="IClickHouseColumnTypeInfo"/> is considered unstable. It can be changed between minor versions.
    /// </remarks>
    public interface IClickHouseColumnTypeInfo : IClickHouseTypeInfo
    {
        /// <summary>
        /// Creates and returns a new instance of <see cref="IClickHouseColumnReader"/> configured to read the specified number of rows.
        /// </summary>
        /// <param name="rowCount">The number of rows that the reader should read.</param>
        /// <returns>The <see cref="IClickHouseColumnReader"/> that should read the specified number of rows.</returns>
        IClickHouseColumnReader CreateColumnReader(int rowCount);

        /// <summary>
        /// Creates and returns a new instance of <see cref="IClickHouseColumnReader"/> configured to read the specified number of rows.
        /// </summary>
        /// <param name="rowCount">The number of rows that the reader should read.</param>
        /// <param name="serializationMode">
        /// One of supported serialization modes (see <see cref="ClickHouseColumnSerializationMode"/> for details).
        /// When the mode is <see cref="ClickHouseColumnSerializationMode.Default"/> an implementation
        /// of this method must call <see cref="CreateColumnReader(int)"/>.
        /// </param>
        /// <returns>The <see cref="IClickHouseColumnReader"/> that should read the specified number of rows.</returns>
        IClickHouseColumnReader CreateColumnReader(int rowCount, ClickHouseColumnSerializationMode serializationMode)
        {
            switch (serializationMode)
            {
                case ClickHouseColumnSerializationMode.Default:
                    return CreateColumnReader(rowCount);

                case ClickHouseColumnSerializationMode.Sparse:
                case ClickHouseColumnSerializationMode.Custom:
                    return new CustomSerializationColumnReader(this, rowCount, serializationMode);

                default:
                    throw new ArgumentException($"Unknown serialization mode: {serializationMode}.", nameof(serializationMode));
            }
        }

        /// <summary>
        /// Creates and returns a new instance of <see cref="IClickHouseColumnReaderBase"/> configured to skip the specified number of rows.
        /// </summary>
        /// <param name="rowCount">The number of rows that the reader should skip.</param>
        /// <returns>The <see cref="IClickHouseColumnReaderBase"/> that should skip the specified number of rows.</returns>
        IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount);

        /// <summary>
        /// Creates and returns a new instance of <see cref="IClickHouseColumnReaderBase"/> configured to skip the specified number of rows.
        /// </summary>
        /// <param name="rowCount">The number of rows that the reader should skip.</param>
        /// <param name="serializationMode">
        /// One of supported serialization modes (see <see cref="ClickHouseColumnSerializationMode"/> for details).
        /// When the mode is <see cref="ClickHouseColumnSerializationMode.Default"/> an implementation
        /// of this method must call <see cref="CreateSkippingColumnReader(int)"/>.
        /// </param>
        /// <returns>The <see cref="IClickHouseColumnReaderBase"/> that should skip the specified number of rows.</returns>
        IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount, ClickHouseColumnSerializationMode serializationMode)
        {
            switch (serializationMode)
            {
                case ClickHouseColumnSerializationMode.Default:
                    return CreateSkippingColumnReader(rowCount);

                case ClickHouseColumnSerializationMode.Sparse:
                case ClickHouseColumnSerializationMode.Custom:
                    return new CustomSerializationSkippingColumnReader(this, rowCount, serializationMode);

                default:
                    throw new ArgumentException($"Unknown serialization mode: {serializationMode}.", nameof(serializationMode));
            }
        }

        /// <summary>
        /// Creates and returns a new instance of <see cref="IClickHouseColumnWriter"/> that can write specified rows to a binary stream.
        /// </summary>
        /// <typeparam name="T">The type of the list of rows.</typeparam>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="rows">The list of rows.</param>
        /// <param name="columnSettings">Optional argument. Additional settings for the column writer.</param>
        /// <returns>The <see cref="IClickHouseColumnWriter"/> that can write specified rows to a binary stream</returns>
        IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings);

        /// <summary>
        /// Returns an instance of <see cref="IClickHouseColumnTypeInfo"/> based on this type but with the specified list of type arguments.
        /// </summary>
        /// <param name="options">The list of strings. Each string in the list describes an argument of the type.</param>
        /// <param name="typeInfoProvider">The type provider that can be used to get other types.</param>
        /// <returns>The <see cref="IClickHouseColumnTypeInfo"/> with the same <see cref="IClickHouseTypeInfo.TypeName"/> as this type and with the specified list of type arguments</returns>
        IClickHouseColumnTypeInfo GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider);

        /// <summary>
        /// Creates an instance of <see cref="IClickHouseParameterWriter{T}"/> capable of writing a value of the type <typeparamref name="T"/>
        /// as a ClickHouse parameter.
        /// </summary>
        /// <returns>The <see cref="IClickHouseParameterWriter{T}"/> that can writer the value of the type <typeparamref name="T"/> as a parameter.</returns>
        IClickHouseParameterWriter<T> CreateParameterWriter<T>();
    }
}
