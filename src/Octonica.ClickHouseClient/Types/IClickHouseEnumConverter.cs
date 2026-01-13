#region License Apache 2.0
/* Copyright 2020-2021 Octonica
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
using System.Diagnostics.CodeAnalysis;

namespace Octonica.ClickHouseClient.Types
{
    /// <summary>
    /// The basic interface for enum converters. Objects that implement this interface are responsible for the enum's type dispatching.
    /// </summary>
    public interface IClickHouseEnumConverter
    {
        /// <summary>
        /// When implemented in a derrived class must call <see cref="IClickHouseEnumConverterDispatcher{T}.Dispatch{TEnum}(IClickHouseEnumConverter{TEnum})"/>
        /// with an appropriate generic argument.
        /// </summary>
        /// <typeparam name="T">The type of the returned value.</typeparam>
        /// <param name="dispatcher">The dispatcher that requires the type of enum to be passed as the generic parameter.</param>
        /// <returns>The value returned by the dispatcher.</returns>
        T Dispatch<T>(IClickHouseEnumConverterDispatcher<T> dispatcher);
    }

    /// <summary>
    /// The basic interface for enum converters that can convert from and to .NET enums.
    /// Objects that implement this interface are responsible for the enum's type dispatching.
    /// </summary>
    /// <typeparam name="TEnum">The type of the enum.</typeparam>
    public interface IClickHouseEnumConverter<TEnum> : IClickHouseEnumConverter
        where TEnum : Enum
    {
        /// <summary>
        /// When implemented in a derived class returns a enum's value corresponding to a numeric value of the ClickHouse enum, a string value of the ClickHouse enum, or both.
        /// </summary>
        /// <param name="value">The numeric value of the ClickHouse enum.</param>
        /// <param name="stringValue">The string value of the ClickHouse enum.</param>
        /// <param name="enumValue">When this method returns, contains the value of enum or the default value of <typeparamref name="TEnum"/> when returns <see langword="false"/>.</param>
        /// <returns><see langword="true"/> if there are <typeparamref name="TEnum"/>'s value corresponding to the specified ClickHouse enum; otherwise <see langword="false"/>.</returns>
        bool TryMap(int value, string stringValue, [NotNullWhen(true)] out TEnum enumValue);
    }

    /// <summary>
    /// The interface for a enum dispatchers. 
    /// </summary>
    /// <typeparam name="T">The type of an object returned by this despatcher.</typeparam>
    public interface IClickHouseEnumConverterDispatcher<out T>
    {
        /// <summary>
        /// When implemented in a derrived class executes an arbitrary operation with respect to the enum converter
        /// and returns the result of this operation.
        /// </summary>
        /// <typeparam name="TEnum">The type of the enum.</typeparam>
        /// <param name="enumConverter">The enum converter for the enum of the specified type.</param>
        /// <returns>The result of an executed operations.</returns>
        /// <remarks>This method can be viewed as a closure of the type argument.</remarks>
        T Dispatch<TEnum>(IClickHouseEnumConverter<TEnum> enumConverter)
            where TEnum : Enum;
    }
}
