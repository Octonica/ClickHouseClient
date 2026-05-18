#region License Apache 2.0
/* Copyright 2026 Octonica
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

namespace Octonica.ClickHouseClient.Utils
{
    /// <summary>
    /// The interface for an object providing a type converter callback function.
    /// </summary>
    public interface IConverterDispatcher
    {
        /// <summary>
        /// When implemented, the method recieves the source type and an instance of an object for configuring with a callback function.
        /// </summary>
        /// <typeparam name="TFrom">The source type for a type converter callback function.</typeparam>
        /// <typeparam name="T">The type of a configurable object.</typeparam>
        /// <param name="converterDispatcher">The dispatcher which recieves a type converter callback function and returns a configured object.</param>
        /// <returns>The object configured with a type converter callback function.</returns>
        T Dispatch<TFrom, T>(IConverterDispatcher<T> converterDispatcher);
    }

    /// <summary>
    /// The interface for an object receiving a type converter callback function.
    /// </summary>
    public interface IConverterDispatcher<out T>
    {
        /// <summary>
        /// The method configures an object with a type converter callback function.
        /// </summary>
        /// <typeparam name="TFrom">The source type of the type converter callback function.</typeparam>
        /// <typeparam name="TTo">The result type of the type converter callback function.</typeparam>
        /// <param name="convert">The type converter callback function</param>
        /// <returns>The object configured with the type converter callback function.</returns>
        /// <remarks>
        /// If <typeparamref name="TFrom"/> and <typeparamref name="TTo"/> are the same type and the converter function is an identity function, call the method <see cref="DispatchNoConvert"/> instead of this method.
        /// </remarks>
        T Dispatch<TFrom, TTo>(Func<TFrom, TTo> convert);

        /// <summary>
        /// The method configures an object to not perform a type conversion.
        /// </summary>
        /// <returns>The object configured with no type converter.</returns>
        T DispatchNoConvert();
    }
}
