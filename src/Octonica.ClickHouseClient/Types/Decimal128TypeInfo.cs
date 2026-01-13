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

using Octonica.ClickHouseClient.Exceptions;
using System;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class Decimal128TypeInfo : DecimalTypeInfoBase
    {
        private const int Precision = 38;

        public override int TypeArgumentsCount => Math.Min(1, base.TypeArgumentsCount);

        public Decimal128TypeInfo()
            : base("Decimal128")
        {
        }

        private Decimal128TypeInfo(string typeName, string complexTypeName, int scale)
            : base(typeName, complexTypeName, Precision, scale)
        {
        }

        protected override DecimalTypeInfoBase CloneWithOptions(string complexTypeName, int? precision, int scale)
        {
            return precision != null
                ? throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The value of the precision can not be redefined for the type \"{TypeName}\".")
                : (DecimalTypeInfoBase)new Decimal128TypeInfo(TypeName, complexTypeName, scale);
        }

        public override object GetTypeArgument(int index)
        {
            return base.TypeArgumentsCount == 0
                ? base.GetTypeArgument(index)
                : index != 0 ? throw new IndexOutOfRangeException() : base.GetTypeArgument(1);
        }
    }
}
