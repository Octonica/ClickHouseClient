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

namespace Octonica.ClickHouseClient.Types
{
    internal sealed partial class Date32TypeInfo : SimpleTypeInfo
    {
        public const int MinValue = -16436;
        public const int MaxValue = 114635;

        public Date32TypeInfo()
            :base("Date32")
        {
        }

        public override IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new Date32Reader(rowCount);
        }

        public override IClickHouseColumnReaderBase CreateSkippingColumnReader(int rowCount)
        {
            return new SimpleSkippingColumnReader(sizeof(uint), rowCount);
        }

        public override ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Date32;
        }

        private sealed partial class Date32Reader
        {
            protected override bool BitwiseCopyAllowed => true;

            public Date32Reader(int rowCount) 
                : base(sizeof(int), rowCount)
            {
            }

            protected override int ReadElement(ReadOnlySpan<byte> source)
            {
                return BitConverter.ToInt32(source);
            }
        }

        private sealed partial class Date32Writer
        {
        }
    }
}
