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

using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// Represents a collection of columns associated with a <see cref="ClickHouseTableProvider"/>. This class cannot be inherited.
    /// </summary>
    public sealed class ClickHouseTableColumnCollection : IndexedCollectionBase<string, ClickHouseTableColumn>
    {
        /// <summary>
        /// Initializes a new instance of <see cref="ClickHouseTableColumnCollection"/> with the default capacity.
        /// </summary>
        public ClickHouseTableColumnCollection()
            : base(StringComparer.OrdinalIgnoreCase)
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="ClickHouseTableColumnCollection"/> with the specified capacity capacity.
        /// </summary>
        /// <param name="capacity">The initial number of elements that the collection can contain.</param>
        public ClickHouseTableColumnCollection(int capacity)
            : base(capacity, StringComparer.OrdinalIgnoreCase)
        {
        }

        /// <inheritdoc/>
        protected sealed override string GetKey(ClickHouseTableColumn item)
        {
            return item.ColumnName;
        }

        private string GetUniqueColumnName(string baseName)
        {
            int i = 0;
            string name;
            do
            {
                name = string.Format(CultureInfo.InvariantCulture, "{0}{1}", baseName, i++);
            } while (ContainsKey(name));

            return name;
        }

        /// <summary>
        /// Creates a new column with the default name and adds it to the collection.
        /// </summary>
        /// <typeparam name="T">The type of the column's values.</typeparam>
        /// <param name="column">The list of column's values.</param>
        /// <returns>A new column.</returns>
        public ClickHouseTableColumn AddColumn<T>(IReadOnlyList<T> column)
        {
            return AddColumn(GetUniqueColumnName("column"), column);
        }

        /// <summary>
        /// Creates a new column with the specified name and adds it to the collection.
        /// </summary>
        /// <typeparam name="T">The type of the column's values.</typeparam>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="column">The list of column's values.</param>
        /// <returns>A new column.</returns>
        public ClickHouseTableColumn AddColumn<T>(string columnName, IReadOnlyList<T> column)
        {
            ClickHouseTableColumn result = AddColumn(columnName, column, typeof(T));
            if (result.Settings?.ColumnType == null)
            {
                result.Settings = new ClickHouseColumnSettings(result.Settings?.StringEncoding, result.Settings?.EnumConverter, typeof(T));
            }

            return result;
        }

        /// <summary>
        /// Creates a new column with the default name and adds it to the collection.
        /// </summary>
        /// <param name="column">
        /// The object representing a column. It must implement one of interfaces:
        /// <see cref="IReadOnlyList{T}"/>,
        /// <see cref="IList{T}"/>,
        /// <see cref="IAsyncEnumerable{T}"/>,
        /// <see cref="IEnumerable{T}"/> or
        /// <see cref="IEnumerable"/>.
        /// </param>
        /// <returns>A new column.</returns>
        public ClickHouseTableColumn AddColumn(object column)
        {
            return AddColumn(GetUniqueColumnName("column"), column);
        }

        /// <summary>
        /// Creates a new column with the specified name and adds it to the collection.
        /// </summary>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="column">
        /// The object representing a column. It must implement one of interfaces:
        /// <see cref="IReadOnlyList{T}"/>,
        /// <see cref="IList{T}"/>,
        /// <see cref="IAsyncEnumerable{T}"/>,
        /// <see cref="IEnumerable{T}"/> or
        /// <see cref="IEnumerable"/>.
        /// </param>
        /// <returns>A new column.</returns>
        public ClickHouseTableColumn AddColumn(string columnName, object column)
        {
            if (column == null)
            {
                throw new ArgumentNullException(nameof(column));
            }

            Type columnType = column.GetType();
            Type? enumerable = null;
            Type? altEnumerable = null;
            Type? asyncEnumerable = null;
            Type? altAsyncEnumerable = null;
            Type? readOnlyList = null;
            Type? altReadOnlyList = null;
            Type? list = null;
            Type? altList = null;
            foreach (Type ifs in columnType.GetInterfaces())
            {
                if (!ifs.IsGenericType)
                {
                    continue;
                }

                Type ifsDefinition = ifs.GetGenericTypeDefinition();
                if (ifsDefinition == typeof(IEnumerable<>) && ifs.GetGenericArguments()[0] != typeof(object))
                {
                    altEnumerable ??= enumerable;
                    enumerable = ifs;
                }
                else if (ifsDefinition == typeof(IAsyncEnumerable<>) && ifs.GetGenericArguments()[0] != typeof(object))
                {
                    altAsyncEnumerable = asyncEnumerable;
                    asyncEnumerable = ifs;
                }
                else if (ifsDefinition == typeof(IReadOnlyList<>) && ifs.GetGenericArguments()[0] != typeof(object))
                {
                    altReadOnlyList = readOnlyList;
                    readOnlyList = ifs;
                }
                else if (ifsDefinition == typeof(IList<>) && ifs.GetGenericArguments()[0] != typeof(object))
                {
                    altList = list;
                    list = ifs;
                }
            }

            Type genericCollectionType;
            if (readOnlyList != null)
            {
                if (altReadOnlyList != null)
                {
                    throw CreateInterfaceAmbiguousException(readOnlyList, altReadOnlyList);
                }

                genericCollectionType = readOnlyList;
            }
            else if (list != null)
            {
                if (altList != null)
                {
                    throw CreateInterfaceAmbiguousException(list, altList);
                }

                genericCollectionType = list;
            }
            else if (asyncEnumerable != null)
            {
                if (altAsyncEnumerable != null)
                {
                    throw CreateInterfaceAmbiguousException(asyncEnumerable, altAsyncEnumerable);
                }

                genericCollectionType = asyncEnumerable;
            }
            else if (enumerable != null)
            {
                if (altEnumerable != null)
                {
                    throw CreateInterfaceAmbiguousException(enumerable, altEnumerable);
                }

                genericCollectionType = enumerable;
            }
            else
            {
                throw new ClickHouseException(ClickHouseErrorCodes.ColumnTypeMismatch, "The column is not a generic collection. A type of the column can't be detected.");
            }

            Type elementType = genericCollectionType.GetGenericArguments()[0];

            ClickHouseTableColumn result = AddColumn(columnName, column, elementType);
            if (result.Settings?.ColumnType == null)
            {
                result.Settings = new ClickHouseColumnSettings(result.Settings?.StringEncoding, result.Settings?.EnumConverter, elementType);
            }

            return result;
        }

        /// <summary>
        /// Creates a new column with the default name and adds it to the collection.
        /// </summary>
        /// <param name="column">
        /// The object representing a column. It must implement one of interfaces:
        /// <see cref="IReadOnlyList{T}"/>,
        /// <see cref="IList{T}"/>,
        /// <see cref="IAsyncEnumerable{T}"/>,
        /// <see cref="IEnumerable{T}"/> or
        /// <see cref="IEnumerable"/>.
        /// </param>
        /// <param name="columnType">The type of the column's values.</param>
        /// <returns>A new column.</returns>
        public ClickHouseTableColumn AddColumn(object column, Type columnType)
        {
            return AddColumn(GetUniqueColumnName("column"), column, columnType);
        }

        /// <summary>
        /// Creates a new column with the specified name and adds it to the collection.
        /// </summary>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="column">
        /// The object representing a column. It must implement one of interfaces:
        /// <see cref="IReadOnlyList{T}"/>,
        /// <see cref="IList{T}"/>,
        /// <see cref="IAsyncEnumerable{T}"/>,
        /// <see cref="IEnumerable{T}"/> or
        /// <see cref="IEnumerable"/>.
        /// </param>
        /// <param name="columnType">The type of the column's values.</param>
        /// <returns>A new column.</returns>
        public ClickHouseTableColumn AddColumn(string columnName, object column, Type columnType)
        {
            ClickHouseTableColumn result = new(columnName, column, columnType);
            Add(result);
            return result;
        }

        private static ClickHouseException CreateInterfaceAmbiguousException(Type itf, Type altItf)
        {
            return new ClickHouseException(ClickHouseErrorCodes.ColumnTypeMismatch, $"A type of the column is ambiguous. The column implements interfaces \"{itf}\" and \"{altItf}\".");
        }
    }
}
