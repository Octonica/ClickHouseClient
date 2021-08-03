#region License Apache 2.0
/* Copyright 2020-2021 Octonica
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

using System.Data.Common;

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// Represents a set of methods for creating instances of ClickHouseClient's implementation of the data source classes.
    /// </summary>
    public class ClickHouseDbProviderFactory : DbProviderFactory
    {
        /// <summary>
        /// Returns a new instance of <see cref="ClickHouseConnectionStringBuilder"/>.
        /// </summary>
        /// <returns>A new instance of <see cref="ClickHouseConnectionStringBuilder"/>.</returns>
        public override DbConnectionStringBuilder CreateConnectionStringBuilder()
        {
            return new ClickHouseConnectionStringBuilder();
        }

        /// <summary>
        /// Returns a new instance of <see cref="ClickHouseConnection"/>.
        /// </summary>
        /// <returns>A new instance of <see cref="ClickHouseConnection"/>.</returns>
        public override DbConnection CreateConnection()
        {
            return new ClickHouseConnection();
        }

        /// <summary>
        /// Returns a new instance of <see cref="ClickHouseCommand"/>.
        /// </summary>
        /// <returns>A new instance of <see cref="ClickHouseCommand"/>.</returns>
        public override DbCommand CreateCommand()
        {
            return new ClickHouseCommand();
        }

        /// <summary>
        /// Returns a new instance of <see cref="ClickHouseParameter"/>.
        /// </summary>
        /// <returns>A new instance of <see cref="ClickHouseParameter"/>.</returns>
        public override DbParameter CreateParameter()
        {
            return new ClickHouseParameter();
        }
    }
}
