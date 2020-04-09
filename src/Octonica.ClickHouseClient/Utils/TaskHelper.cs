#region License Apache 2.0
/* Copyright 2019-2020 Octonica LLC
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
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;

namespace Octonica.ClickHouseClient.Utils
{
    internal static class TaskHelper
    {
        /// <summary>
        /// Call this method only for the task which is not async in any execution path
        /// </summary>
        internal static void WaitNonAsyncTask(ValueTask task)
        {
            try
            {
                task.AsTask().Wait();
            }
            catch (AggregateException aggrEx)
            {
                if (aggrEx.InnerExceptions.Count == 1)
                    ExceptionDispatchInfo.Capture(aggrEx.InnerExceptions[0]).Throw();

                throw;
            }
        }

        /// <summary>
        /// Call this method only for the task which is not async in any execution path
        /// </summary>
        internal static T WaitNonAsyncTask<T>(ValueTask<T> task)
        {
            try
            {
                return task.Result;
            }
            catch (AggregateException aggrEx)
            {
                if (aggrEx.InnerExceptions.Count == 1)
                    ExceptionDispatchInfo.Capture(aggrEx.InnerExceptions[0]).Throw();

                throw;
            }
        }

        internal static T WaitSynchronously<T>(Func<Task<T>> taskFactory)
        {
            if (taskFactory == null)
                throw new ArgumentNullException(nameof(taskFactory));

            var innerTask = WaitInternal(taskFactory);
            try
            {
                var result = innerTask.Result;
                return result;
            }
            catch (AggregateException aggrEx)
            {
                if (aggrEx.InnerExceptions.Count == 1)
                    ExceptionDispatchInfo.Capture(aggrEx.InnerExceptions[0]).Throw();

                throw;
            }
        }

        internal static void WaitSynchronously([NotNull] Func<Task> taskFactory)
        {
            if (taskFactory == null)
                throw new ArgumentNullException(nameof(taskFactory));

            var innerTask = WaitInternal(taskFactory);
            try
            {
                innerTask.Wait();
            }
            catch (AggregateException aggrEx)
            {
                if (aggrEx.InnerExceptions.Count == 1)
                    ExceptionDispatchInfo.Capture(aggrEx.InnerExceptions[0]).Throw();

                throw;
            }
        }

        private static async Task<T> WaitInternal<T>([NotNull] Func<Task<T>> taskFactory)
        {
            return await Task.Run(async () => await taskFactory().ConfigureAwait(false)).ConfigureAwait(false);
        }

        private static async Task WaitInternal([NotNull] Func<Task> taskFactory)
        {
            await Task.Run(async () => await taskFactory().ConfigureAwait(false)).ConfigureAwait(false);
        }
    }
}
