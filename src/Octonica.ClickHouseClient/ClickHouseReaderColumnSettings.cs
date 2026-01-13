#region License Apache 2.0
/* Copyright 2024 Octonica
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

using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Types;
using System.Diagnostics;

namespace Octonica.ClickHouseClient
{
    internal readonly struct ClickHouseReaderColumnSettings
    {
        public ClickHouseColumnSettings? Column { get; }

        public IClickHouseColumnReinterpreter? Reinterpreter { get; }

        public ClickHouseReaderColumnSettings(ClickHouseColumnSettings? column, IClickHouseColumnReinterpreter? reinterpreter)
        {
            Column = column;
            Reinterpreter = reinterpreter;
        }

        public ClickHouseReaderColumnSettings WithColumnSettings(string columnName, ClickHouseColumnSettings column, IClickHouseColumnReinterpreter? reinterpreter)
        {
            if (Reinterpreter != null && Column?.ColumnType == null)
            {
                if (reinterpreter != null)
                {
                    Debug.Assert(column.ColumnType != null);
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidColumnSettings, $"An external callback function for converting values was defined for the column \"{columnName}\". The type of the column was implicityly defined and can't be redefined in the column settings.");
                }

                return new ClickHouseReaderColumnSettings(column, Reinterpreter);
            }

            Debug.Assert(column.ColumnType == null == (reinterpreter == null));
            return new ClickHouseReaderColumnSettings(column, reinterpreter);
        }

        public ClickHouseReaderColumnSettings WithUserDefinedReader(string columnName, IClickHouseColumnReinterpreter? reinterpreter)
        {
            return reinterpreter == null
                ? Column?.ColumnType != null || Reinterpreter == null ? this : new ClickHouseReaderColumnSettings(Column, null)
                : Column?.ColumnType != null
                ? throw new ClickHouseException(ClickHouseErrorCodes.InvalidColumnSettings, $"An external callback function for converting values can't be set for the column \"{columnName}\" because the type of the column was already defined in the column settings.")
                : new ClickHouseReaderColumnSettings(Column, reinterpreter);
        }
    }
}
