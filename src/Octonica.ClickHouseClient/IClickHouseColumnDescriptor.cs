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

using Octonica.ClickHouseClient.Protocol;
using System;

namespace Octonica.ClickHouseClient
{
    public interface IClickHouseColumnDescriptor
    {
        string ColumnName { get; }

        ClickHouseColumnSettings? Settings { get; }

        ClickHouseDbType? ClickHouseDbType { get; }

        Type ValueType { get; }

        bool? IsNullable { get; }

        int Size { get; }

        byte? Precision { get; }

        byte? Scale { get; }        

        TimeZoneInfo? TimeZone { get; }

        int? ArrayRank { get; }
    }
}
