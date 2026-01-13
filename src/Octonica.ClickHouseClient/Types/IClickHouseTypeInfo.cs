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

namespace Octonica.ClickHouseClient.Types
{
    /// <summary>
    /// Represents basic information about the ClickHouse type.
    /// </summary>
    public interface IClickHouseTypeInfo
    {
        /// <summary>
        /// Gets the full name of the type. The full name contains of the name of the type and the list of arguments.
        /// </summary>
        string ComplexTypeName { get; }

        /// <summary>
        /// Gets the name of the type without a list of arguments.
        /// </summary>
        string TypeName { get; }

        /// <summary>
        /// Gets the number of generic arguments in the list of arguments. Only arguments that are types themselves are counted as generics.
        /// </summary>        
        int GenericArgumentsCount { get; }

        /// <summary>
        /// Gets the number of arguments in the list of arguments.
        /// </summary>
        int TypeArgumentsCount => GenericArgumentsCount;

        /// <summary>
        /// Gets the CLR type to which this ClickHouse type is mapped.
        /// </summary>
        /// <returns>The <see cref="Type"/> to which this ClickHouse type is mapped.</returns>
        /// <remarks>If the ClickHouse type is mapped to a nullable structure this method returns <see langword="typeof"/>(<see cref="Nullable{T}"/>).</remarks>
        Type GetFieldType();

        /// <summary>
        /// Gets the type code specific to ClickHouse.
        /// </summary>
        ClickHouseDbType GetDbType();

        /// <summary>
        /// Gets the generic arguments at the specified position. Generic arguments are counted separately,
        /// which means that the index can't be greater than <see cref="GenericArgumentsCount"/>.
        /// </summary>
        /// <param name="index">The zero-based index of the generic argument. It can't be greater than <see cref="GenericArgumentsCount"/>.</param>
        /// <returns>The <see cref="IClickHouseTypeInfo"/> which represents the generic argument.</returns>
        IClickHouseTypeInfo GetGenericArgument(int index);

        /// <summary>
        /// Gets the argument of the type. For generic arguments this method returns an <see cref="IClickHouseTypeInfo"/>.
        /// </summary>
        /// <param name="index">The zero-based index of the argument. It can't be greater than <see cref="TypeArgumentsCount"/>.</param>
        /// <returns>The argument of the type.</returns>
        object GetTypeArgument(int index)
        {
            return TypeArgumentsCount == 0
                ? throw new NotSupportedException($"The type \"{TypeName}\" doesn't have arguments.")
                : (object)GetGenericArgument(index);
        }
    }
}
