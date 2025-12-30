#region License Apache 2.0
/* Copyright 2021 Octonica
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
using NodaTime;

namespace Octonica.ClickHouseClient.Utils
{
    internal static partial class TimeZoneHelper
    {
        public static DateTimeZone GetDateTimeZone(string timeZone)
        {
            DateTimeZone? DateTimeZone = null;
            GetDateTimeZoneImpl(timeZone, ref DateTimeZone);
            if (DateTimeZone == null)
                throw new NotImplementedException($"Internal error. The method {nameof(GetDateTimeZoneImpl)} is not implemented properly.");

            return DateTimeZone;
        }

        public static string GetTimeZoneId(DateTimeZone timeZone)
        {
            string? timeZoneCode = null;
            GetTimeZoneIdImpl(timeZone, ref timeZoneCode);
            if (timeZoneCode == null)
                throw new NotImplementedException($"Internal error. The method {nameof(GetTimeZoneIdImpl)} is not implemented properly.");

            return timeZoneCode;
        }

        static partial void GetDateTimeZoneImpl(string timeZone, ref DateTimeZone? DateTimeZone);

        static partial void GetTimeZoneIdImpl(DateTimeZone DateTimeZone, ref string? timeZoneCode);
    }
}
