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
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Octonica.ClickHouseClient.Utils
{
    internal sealed class SimpleReadOnlySequenceSegment<T> : ReadOnlySequenceSegment<T>
    {
        public SimpleReadOnlySequenceSegment<T> LastSegment => field ?? this;

        public SimpleReadOnlySequenceSegment(ReadOnlyMemory<T> firstSegment, ReadOnlySequence<T> sequence)
        {
            Stack<ReadOnlyMemory<T>> segments = new(2);
            long runningIndex = firstSegment.Length;
            if (firstSegment.Length > 0)
            {
                segments.Push(firstSegment);
            }

            foreach (ReadOnlyMemory<T> segment in sequence)
            {
                if (segment.Length == 0)
                {
                    continue;
                }

                runningIndex += segment.Length;
                segments.Push(segment);
            }

            SimpleReadOnlySequenceSegment<T>? nextSegment = null;
            if (segments.Count == 0)
            {
                Memory = firstSegment;
                Debug.Assert(runningIndex == 0);
            }
            else
            {
                while (segments.Count > 1)
                {
                    ReadOnlyMemory<T> segment = segments.Pop();
                    runningIndex -= segment.Length;
                    nextSegment = new SimpleReadOnlySequenceSegment<T>(segment, runningIndex, nextSegment);
                }

                Memory = segments.Pop();
                Debug.Assert(runningIndex == Memory.Length);
            }

            RunningIndex = 0;
            Next = nextSegment;
            LastSegment = nextSegment?.LastSegment;
        }

        public SimpleReadOnlySequenceSegment(IReadOnlyList<ReadOnlyMemory<T>> segments)
        {
            long runningIndex = segments.Aggregate((long)0, (v, s) => v + s.Length);

            SimpleReadOnlySequenceSegment<T>? nextSegment = null;
            for (int i = segments.Count - 1; i > 0; i--)
            {
                ReadOnlyMemory<T> segment = segments[i];
                runningIndex -= segment.Length;
                nextSegment = new SimpleReadOnlySequenceSegment<T>(segment, runningIndex, nextSegment);
            }

            if (segments.Count > 0)
            {
                ReadOnlyMemory<T> firstSegment = segments[0];
                runningIndex -= firstSegment.Length;

                Memory = firstSegment;
            }

            Debug.Assert(runningIndex == 0);
            RunningIndex = runningIndex;
            Next = nextSegment;
            LastSegment = nextSegment?.LastSegment;
        }

        private SimpleReadOnlySequenceSegment(ReadOnlyMemory<T> memory, long runningIndex, SimpleReadOnlySequenceSegment<T>? nextSegment)
        {
            Memory = memory;
            RunningIndex = runningIndex;
            Next = nextSegment;
            LastSegment = nextSegment?.LastSegment;
        }
    }
}
