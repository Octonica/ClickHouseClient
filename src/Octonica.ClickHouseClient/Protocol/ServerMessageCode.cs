#region License Apache 2.0
/* Copyright 2019-2021, 2023 Octonica
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

namespace Octonica.ClickHouseClient.Protocol
{
    internal enum ServerMessageCode
    {
        Hello = 0,
        Data = 1,
        Error = 2,
        Progress = 3,
        Pong = 4,
        EndOfStream = 5,
        ProfileInfo = 6,
        Totals = 7,
        Extremes = 8,
        TableStatusResponse = 9,
        Log = 10,
        TableColumns = 11,
        PartUuids = 12,
        ReadTaskRequest = 13,
        ProfileEvents = 14,
        MergeTreeAllRangesAnnouncement = 15,
        MergeTreeReadTaskRequest = 16,
        TimezoneUpdate = 17
    }
}
