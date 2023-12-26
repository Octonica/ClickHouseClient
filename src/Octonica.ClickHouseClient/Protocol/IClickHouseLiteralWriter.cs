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

using Octonica.ClickHouseClient.Types;
using System;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace Octonica.ClickHouseClient.Protocol
{
    /// <summary>
    /// The base interface for a value writer. When implemented writes a value as a ClickHouse literal.
    /// </summary>
    /// <typeparam name="T">The type of a value.</typeparam>
    public interface IClickHouseLiteralWriter<in T>
    {
        /// <summary>
        /// Creates an instance of <see cref="IClickHouseParameterValueWriter"/> which encapsulates the value.
        /// </summary>
        /// <param name="value">The value of the parameter.</param>
        /// <param name="isNested">The flag which indicates whether the value is a part of parameter's full value, e.g. an element of an array.</param>
        /// <param name="valueWriter">>When this method returns, contains the instance of <see cref="IClickHouseParameterValueWriter"/> encapsulating the value.</param>
        /// <returns>
        /// <see langword="false"/> if the value can't be represented in a binary format. Otherwise returns <see langword="true"/> and creates a <paramref name="valueWriter"/>.
        /// </returns>
        /// <remarks><see langword="false"/> returned by this method instructs a parameter's writer to interpolate the value directly to the query text.</remarks>
        bool TryCreateParameterValueWriter(T value, bool isNested, [NotNullWhen(true)] out IClickHouseParameterValueWriter? valueWriter);

        /// <summary>
        /// Appends the value to the query as a ClickHouse literal.
        /// </summary>
        /// <param name="queryBuilder">The builder of the SQL query</param>
        /// <param name="value">The value for writing as a ClickHouse literal.</param>
        /// <returns>The instance of the builder passed to the method (<paramref name="queryBuilder"/>).</returns>
        StringBuilder Interpolate(StringBuilder queryBuilder, T value);

        /// <summary>
        /// Appends the value to the query. If required, the method appends type cast.
        /// For writing a value it invokes the callback <paramref name="writeValue"/>.
        /// </summary>
        /// <param name="queryBuilder">The builder of the SQL query</param>
        /// <param name="typeInfoProvider">The provider of type information.</param>
        /// <param name="writeValue">
        /// The function that appends an actual value to the query. It gets three arguments: the query string builder; the type of the
        /// literal; the callback function for writing an external parts of the parameter. The type of the literal may differ from the type
        /// of the value. The function must return the same instance of the builder which it gets as an argument.
        /// </param>
        /// <returns>The instance of the builder passed to the method (<paramref name="queryBuilder"/>).</returns>
        StringBuilder Interpolate(StringBuilder queryBuilder, IClickHouseTypeInfoProvider typeInfoProvider, Func<StringBuilder, IClickHouseColumnTypeInfo, Func<StringBuilder, Func<StringBuilder, StringBuilder>, StringBuilder>, StringBuilder> writeValue);
    }
}
