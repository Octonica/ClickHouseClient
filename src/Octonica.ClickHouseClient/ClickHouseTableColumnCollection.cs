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
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;
using System;
using System.Collections.Generic;
using System.Globalization;

namespace Octonica.ClickHouseClient
{
    public class ClickHouseTableColumnCollection : IndexedCollectionBase<string, ClickHouseTableColumn>
    {
        public ClickHouseTableColumnCollection()
            : base(StringComparer.OrdinalIgnoreCase)
        {
        }

        public ClickHouseTableColumnCollection(int capacity)
            : base(capacity, StringComparer.OrdinalIgnoreCase)
        {
        }

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

        public ClickHouseTableColumn AddColumn<T>(IReadOnlyList<T> column)
        {
            return AddColumn(GetUniqueColumnName("column"), column);
        }

        public ClickHouseTableColumn AddColumn<T>(string columnName, IReadOnlyList<T> column)
        {
            var result = AddColumn(columnName, column, typeof(T));
            if (result.Settings?.ColumnType == null)
                result.Settings = new ClickHouseColumnSettings(result.Settings?.StringEncoding, result.Settings?.EnumConverter, typeof(T));

            return result;
        }

        public ClickHouseTableColumn AddColumn(object column)
        {
            return AddColumn(GetUniqueColumnName("column"), column);
        }

        public ClickHouseTableColumn AddColumn(string columnName, object column)
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            var columnType = column.GetType();
            Type? enumerable = null;
            Type? altEnumerable = null;
            Type? asyncEnumerable = null;
            Type? altAsyncEnumerable = null;
            Type? readOnlyList = null;
            Type? altReadOnlyList = null;
            Type? list = null;
            Type? altList = null;
            foreach (var ifs in columnType.GetInterfaces())
            {
                if (!ifs.IsGenericType)
                    continue;

                var ifsDefinition = ifs.GetGenericTypeDefinition();
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
                    throw CreateInterfaceAmbiguousException(readOnlyList, altReadOnlyList);

                genericCollectionType = readOnlyList;
            }
            else if (list != null)
            {
                if (altList != null)
                    throw CreateInterfaceAmbiguousException(list, altList);

                genericCollectionType = list;
            }
            else if (asyncEnumerable != null)
            {
                if (altAsyncEnumerable != null)
                    throw CreateInterfaceAmbiguousException(asyncEnumerable, altAsyncEnumerable);

                genericCollectionType = asyncEnumerable;
            }
            else if (enumerable != null)
            {
                if (altEnumerable != null)
                    throw CreateInterfaceAmbiguousException(enumerable, altEnumerable);

                genericCollectionType = enumerable;
            }
            else
            {
                throw new ClickHouseException(ClickHouseErrorCodes.ColumnTypeMismatch, "The column is not a generic collection. A type of the column can't be detected.");
            }

            var elementType = genericCollectionType.GetGenericArguments()[0];

            var result = AddColumn(columnName, column, elementType);
            if (result.Settings?.ColumnType == null)
                result.Settings = new ClickHouseColumnSettings(result.Settings?.StringEncoding, result.Settings?.EnumConverter, elementType);

            return result;
        }

        public ClickHouseTableColumn AddColumn(object column, Type columnType)
        {
            return AddColumn(GetUniqueColumnName("column"), column, columnType);
        }

        public ClickHouseTableColumn AddColumn(string columnName, object column, Type columnType)
        {
            var result = new ClickHouseTableColumn(columnName, column, columnType);
            Add(result);
            return result;
        }

        private static ClickHouseException CreateInterfaceAmbiguousException(Type itf, Type altItf)
        {
            return new ClickHouseException(ClickHouseErrorCodes.ColumnTypeMismatch, $"A type of the column is ambiguous. The column implements interfaces \"{itf}\" and \"{altItf}\".");
        }
    }
}
