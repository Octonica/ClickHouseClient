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
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Octonica.ClickHouseClient.Utils
{
    internal sealed class SimpleReadOnlySequenceSegment<T> : ReadOnlySequenceSegment<T>
    {
        private readonly SimpleReadOnlySequenceSegment<T>? _lastSegment;

        public SimpleReadOnlySequenceSegment<T> LastSegment => _lastSegment ?? this;

        public SimpleReadOnlySequenceSegment(IReadOnlyList<ReadOnlyMemory<T>> segments)
        {
            var runningIndex = segments.Aggregate(0, (v, s) => v + s.Length);

            SimpleReadOnlySequenceSegment<T>? nextSegment = null;
            for (int i = segments.Count - 1; i > 0; i--)
            {
                var segment = segments[i];
                runningIndex -= segment.Length;
                nextSegment = new SimpleReadOnlySequenceSegment<T>(segment, runningIndex, nextSegment);
            }

            if (segments.Count > 0)
            {
                var firstSegment = segments[0];
                runningIndex -= firstSegment.Length;
                
                Memory = firstSegment;
            }

            Debug.Assert(runningIndex == 0);
            RunningIndex = runningIndex;
            Next = nextSegment;
            _lastSegment = nextSegment?.LastSegment;
        }

        private SimpleReadOnlySequenceSegment(ReadOnlyMemory<T> memory, int runningIndex, SimpleReadOnlySequenceSegment<T>? nextSegment)
        {
            Memory = memory;
            RunningIndex = runningIndex;
            Next = nextSegment;
            _lastSegment = nextSegment?.LastSegment;
        }
    }
}
