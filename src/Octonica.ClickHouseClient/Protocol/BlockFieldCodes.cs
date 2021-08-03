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

namespace Octonica.ClickHouseClient.Protocol
{
    /// <summary>
    /// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/BlockInfo.h
    /// </summary>
    internal static class BlockFieldCodes
    {
        public const int End = 0;

        /// <summary>
        /// * After running GROUP BY ... WITH TOTALS with the max_rows_to_group_by and group_by_overflow_mode = 'any' settings,
        /// * a row is inserted in the separate block with aggregated values that have not passed max_rows_to_group_by.
        /// * If it is such a block, then is_overflows is set to true for it.
        /// </summary>
        public const int IsOverflows = 1;

        /// <summary>
        ///* When using the two-level aggregation method, data with different key groups are scattered across different buckets.
        ///* In this case, the bucket number is indicated here. It is used to optimize the merge for distributed aggregation.
        ///* Otherwise -1.
        /// </summary>
        public const int BucketNum = 2;
    }
}
