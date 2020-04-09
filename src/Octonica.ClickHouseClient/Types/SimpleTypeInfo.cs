#region License Apache 2.0
/* Copyright 2019-2020 Octonica
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
using System.Collections.Generic;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;

namespace Octonica.ClickHouseClient.Types
{
    public abstract class SimpleTypeInfo : IClickHouseTypeInfo
    {
        public string ComplexTypeName => TypeName;

        public string TypeName { get; }

        public SimpleTypeInfo(string typeName)
        {
            TypeName = typeName ?? throw new ArgumentNullException(nameof(typeName));
        }

        public abstract IClickHouseColumnReader CreateColumnReader(int rowCount);

        public abstract IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings);

        public abstract Type GetFieldType();

        IClickHouseTypeInfo IClickHouseTypeInfo.GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{TypeName}\" does not support arguments.");
        }
    }
}
