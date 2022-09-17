﻿#region License Apache 2.0
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

using Octonica.ClickHouseClient.Types;

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// Represents a set of properties describing a column and its type.
    /// </summary>
    public interface IClickHouseColumnDescriptor : IClickHouseColumnTypeDescriptor
    {
        /// <summary>
        /// Gets the name of the column.
        /// </summary>
        string ColumnName { get; }

        /// <summary>
        /// Gets the settings that should be applied when writing the column.
        /// </summary>
        ClickHouseColumnSettings? Settings { get; }
    }
}
