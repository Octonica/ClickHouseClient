#region License Apache 2.0
/* Copyright 2021-2022 Octonica
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

#if NET6_0_OR_GREATER

using System;
using System.Runtime.CompilerServices;

namespace Octonica.ClickHouseClient.Utils
{
    partial class TimeZoneHelper
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static partial void GetTimeZoneInfoImpl(string timeZone, ref TimeZoneInfo? timeZoneInfo)
        {
            timeZoneInfo = TimeZoneInfo.FindSystemTimeZoneById(timeZone);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static partial void GetTimeZoneIdImpl(TimeZoneInfo timeZoneInfo, ref string? timeZoneCode)
        {
            if (timeZoneInfo.HasIanaId)
            {
                timeZoneCode = timeZoneInfo.Id;
            }
            else if (!TimeZoneInfo.TryConvertWindowsIdToIanaId(timeZoneInfo.Id, out timeZoneCode))
            {
                throw new TimeZoneNotFoundException($"The IANA time zone identifier for the time zone '{timeZoneInfo.Id}' was not found.");
            }
        }
    }
}

#endif