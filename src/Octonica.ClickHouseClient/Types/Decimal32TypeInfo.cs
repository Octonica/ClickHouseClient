#region License Apache 2.0
/* Copyright 2019-2020 Octonica LLC
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

using System.Diagnostics;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class Decimal32TypeInfo : DecimalTypeInfoBase
    {
        private const int Precision = 9;

        public Decimal32TypeInfo()
            : base("Decimal32", Precision)
        {
        }

        private Decimal32TypeInfo(string typeName, string complexTypeName, int precision, int scale)
            : base(typeName, complexTypeName, precision, scale)
        {
        }

        protected override DecimalTypeInfoBase CloneWithOptions(string complexTypeName, int? precision, int scale)
        {
            Debug.Assert(precision == null);
            return new Decimal32TypeInfo(TypeName, complexTypeName, Precision, scale);
        }
    }
}
