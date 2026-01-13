#region License Apache 2.0
/* Copyright 2020 Octonica
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

using Octonica.ClickHouseClient.Protocol;
using System;
using System.Collections.ObjectModel;

namespace Octonica.ClickHouseClient.Utils
{
    internal static class CommonUtils
    {
        internal static int GetColumnIndex(ReadOnlyCollection<ColumnInfo> columns, string name)
        {
            int? caseInsensitiveIdx = null;
            for (int i = 0; i < columns.Count; i++)
            {
                if (string.Equals(columns[i].Name, name, StringComparison.Ordinal))
                {
                    return i;
                }

                if (string.Equals(columns[i].Name, name, StringComparison.OrdinalIgnoreCase))
                {
                    caseInsensitiveIdx = caseInsensitiveIdx == null ? i : -1;
                }
            }

            if (caseInsensitiveIdx >= 0)
            {
                return caseInsensitiveIdx.Value;
            }

            if (caseInsensitiveIdx == null)
            {
                throw new IndexOutOfRangeException($"There is no column with the name \"{name}\" in the table.");
            }

            throw new IndexOutOfRangeException($"There are two or more columns with the name \"{name}\" in the table. Please, provide an exact name (case-sensitive) of the column.");
        }
    }
}
