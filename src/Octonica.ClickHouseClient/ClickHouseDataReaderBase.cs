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

using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// This is an infrastracture class. It allows it's descendants to overload the method <see cref="ReadAsync(CancellationToken)"/>.    
    /// </summary>
    public abstract class ClickHouseDataReaderBase : DbDataReader
    {
        private protected ClickHouseDataReaderBase()
        {
        }

        /// <inheritdoc /> 
        public sealed override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            return ReadAsyncInternal(cancellationToken);
        }

        /// <summary>
        /// When overriden in a derived class should asyncronously advance the reader to the next record in a result set.
        /// </summary>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>
        /// A <see cref="Task{T}"/> representing asyncronous operation. The result (<see cref="Task{TResult}.Result"/>) is <see langword="true"/>
        /// if there are more rows or <see langword="false"/> if there aren't.
        /// </returns>
        protected abstract Task<bool> ReadAsyncInternal(CancellationToken cancellationToken);
    }
}
