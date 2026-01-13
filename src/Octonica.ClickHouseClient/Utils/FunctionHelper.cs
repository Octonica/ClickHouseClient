#region License Apache 2.0
/* Copyright 2023-2024 Octonica
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
    internal static class FunctionHelper
    {
        public static TOut Apply<TIn, TOut>(TIn input, Func<TIn, TOut> func)
        {
            return func(input);
        }

        public static Func<T1, T3> Combine<T1, T2, T3>(Func<T1, T2> func1, Func<T2, T3> func2)
        {
            return v => func2(func1(v));
        }
    }
}
