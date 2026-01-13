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

namespace Octonica.ClickHouseClient.Utils
{
    internal static class TypeDispatcher
    {
        public static TOut Dispatch<TOut>(Type type, ITypeDispatcher<TOut> dispatcher)
        {
            Type dispatcherType = typeof(Dispatcher<>).MakeGenericType(type);
            ITypeDispatcher dsp = (ITypeDispatcher)Activator.CreateInstance(dispatcherType)!;
            return dsp.Dispatch(dispatcher);
        }

        public static ITypeDispatcher Create(Type type)
        {
            Type dispatcherType = typeof(Dispatcher<>).MakeGenericType(type);
            ITypeDispatcher dsp = (ITypeDispatcher)Activator.CreateInstance(dispatcherType)!;
            return dsp;
        }

        private class Dispatcher<TValue> : ITypeDispatcher
        {
            public T Dispatch<T>(ITypeDispatcher<T> dispatcher)
            {
                return dispatcher.Dispatch<TValue>();
            }
        }
    }
}
