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

using System;
using System.Collections.Generic;
using System.Text;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;

namespace Octonica.ClickHouseClient.Types
{
    /// <summary>
    /// Represents the base class for types that has no arguments.
    /// </summary>
    public abstract class SimpleTypeInfo : IClickHouseColumnTypeInfo
    {
        /// <inheritdoc/>
        public string ComplexTypeName => TypeName;

        /// <inheritdoc/>
        public string TypeName { get; }

        /// <summary>
        /// Gets the number of generic arguments in the list of arguments.
        /// </summary>
        /// <returns>Always returns 0.</returns>
        public int GenericArgumentsCount => 0;

        /// <summary>
        /// Initializes a new instance of <see cref="SimpleTypeInfo"/> with the specified name.
        /// </summary>
        /// <param name="typeName">The name of the type</param>
        protected SimpleTypeInfo(string typeName)
        {
            TypeName = typeName ?? throw new ArgumentNullException(nameof(typeName));
        }

        /// <inheritdoc/>
        public abstract IClickHouseColumnReader CreateColumnReader(int rowCount);

        /// <inheritdoc/>
        public abstract IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount);

        /// <inheritdoc/>
        public abstract IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings);

        /// <inheritdoc/>
        public abstract Type GetFieldType();

        /// <inheritdoc/>
        public abstract ClickHouseDbType GetDbType();

        /// <inheritdoc/>
        public abstract void FormatValue(StringBuilder queryStringBuilder, object? value);

        /// <summary>
        /// Gets the generic arguments at the specified position.
        /// </summary>
        /// <param name="index">The zero-based index of the generic argument.</param>
        /// <exception cref="NotSupportedException">Always throws <see cref="NotSupportedException"/>.</exception>
        public IClickHouseTypeInfo GetGenericArgument(int index)
        {
            throw new NotSupportedException($"The type \"{TypeName}\" doesn't have generic arguments.");
        }

        IClickHouseColumnTypeInfo IClickHouseColumnTypeInfo.GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{TypeName}\" does not support arguments.");
        }
    }
}
