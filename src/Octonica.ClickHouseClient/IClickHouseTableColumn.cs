#region License Apache 2.0
/* Copyright 2019-2021, 2024 Octonica
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

using System.Diagnostics.CodeAnalysis;

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// The basic interface for <see cref="ClickHouseDataReader"/>'s internal columns.
    /// </summary>
    public interface IClickHouseTableColumn
    {
        /// <summary>
        /// Gets the number of rows in the column.
        /// </summary>
        int RowCount { get; }

        /// <summary>
        /// Gets the value indicating whether an actual value at the specified index is NULL.
        /// </summary>
        /// <param name="index">The zero-based index of row.</param>
        /// <returns><see langword="true"/> if an actual value at the specified index is NULL; otherwise <see langword="false"/>.</returns>
        bool IsNull(int index);

        /// <summary>
        /// Gets the value at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index of row.</param>
        /// <returns>The value at the specified index or <see cref="System.DBNull.Value"/> if the value is NULL.</returns>
        object GetValue(int index);

        /// <summary>
        /// Makes an attempt to convert this column to a column with the values of the type <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The desired type of column's values.</typeparam>
        /// <returns>The column converted to the type <see cref="IClickHouseTableColumn{T}"/> or <see langword="null"/> if such conversion is not supported.</returns>
        /// <remarks>This method may or may not return <see langword="null"/> when the column itself implements the interface <see cref="IClickHouseTableColumn{T}"/>.</remarks>
        IClickHouseTableColumn<T>? TryReinterpret<T>();

        /// <summary>
        /// Makes an attempt to convert this column to an array column with the type of array's element <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The desired type of array elements.</typeparam>
        /// <returns>The column converted to the type <see cref="IClickHouseArrayTableColumn{T}"/> or <see langword="null"/> if such conversion is not supported.</returns>
        /// <remarks>This method may or may not return <see langword="null"/> when the column itself implements the interface <see cref="IClickHouseArrayTableColumn{T}"/>.</remarks>
        IClickHouseArrayTableColumn<T>? TryReinterpretAsArray<T>()
        {
            return null;
        }

        /// <summary>
        /// If possible, performs double dispatch and provides this object as an instance of <see cref="IClickHouseTableColumn{TValue}"/>.
        /// </summary>
        /// <typeparam name="T">The type of the dispatched value.</typeparam>
        /// <param name="dispatcher">The dispatcher that receives an instance of <see cref="IClickHouseTableColumn{TValue}"/>.</param>
        /// <param name="dispatchedValue">When this method returns, the result of the dispatch operation, if the dispatcher was called; otherwise the default value of <typeparamref name="T"/>.</param>
        /// <returns><see langword="true"/> if this object is an instance of <see cref="IClickHouseTableColumn{TValue}"/> and the dispatch operation was performed; otherwise <see langword="false"/>.</returns>
        bool TryDipatch<T>(IClickHouseTableColumnDispatcher<T> dispatcher, [MaybeNullWhen(false)] out T dispatchedValue);
    }

    /// <summary>
    /// The generic interface for <see cref="ClickHouseDataReader"/>'s internal columns.
    /// </summary>
    public interface IClickHouseTableColumn<out T> : IClickHouseTableColumn
    {
        /// <summary>
        /// Gets the default value of the column for the sparse serialization.
        /// </summary>
        T DefaultValue { get; }

        /// <summary>
        /// Gets the value at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index of row.</param>
        /// <returns>The value at the specified index.</returns>
        /// <remarks>This method should never return <see langword="null"/>.</remarks>
        new T GetValue(int index);
    }
}
