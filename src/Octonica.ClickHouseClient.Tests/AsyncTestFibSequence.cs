#region License Apache 2.0
/* Copyright 2019-2020 Octonica
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

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Octonica.ClickHouseClient.Tests
{
    internal sealed class AsyncTestFibSequence : IAsyncEnumerable<decimal>
    {
        public IAsyncEnumerator<decimal> GetAsyncEnumerator(CancellationToken cancellationToken = new CancellationToken())
        {
            return new Enumerator();
        }

        private sealed class Enumerator : IAsyncEnumerator<decimal>
        {
            private decimal _prev;
            private decimal _next = 1;

            public decimal Current { get; private set; }

            public ValueTask DisposeAsync()
            {
                return default;
            }

            public ValueTask<bool> MoveNextAsync()
            {
                _prev = Current;
                Current = _next;
                _next = _prev + Current;

                return new ValueTask<bool>(true);
            }
        }
    }
}
