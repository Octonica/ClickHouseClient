#region License Apache 2.0
/* Copyright 2022 Octonica
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

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// Specifies the list of available modes of passing parameters to the query.
    /// </summary>
    public enum ClickHouseParameterMode
    {
        /// <summary>
        /// The default mode. Currently the default mode is <see cref="Binary"/>.
        /// </summary>
        Default = 0,

        /// <summary>
        /// This value indicates that the mode should be inherited.
        /// <see cref="ClickHouseParameter">Parameters</see> inherit the mode from a <see cref="ClickHouseCommand">command</see>.
        /// A <see cref="ClickHouseCommand">command</see> inherits the mode from a <see cref="ClickHouseCommand">connection</see>.
        /// For a <see cref="ClickHouseCommand">connection</see> this value is equivalent to <see cref="Default"/>.
        /// </summary>
        Inherit = 1,

        /// <summary>
        /// This value indicates that parameters should be passed to the query in the binary format. This is the default mode.
        /// </summary>
        /// <remarks>
        /// In this mode parameters will be passed to the query as a table with the single row. Each parameter will be replaced by SELECT subquery.
        /// This mode doesn't allow to pass parameters in parts of the query where scalar subqueries are not allowed.
        /// </remarks>
        Binary = 2,

        /// <summary>
        /// This value indicates that parameters should be passed to the query as constant literals.
        /// </summary>
        /// <remarks>
        /// In this mode parameters' values will be interpolated to the query string as constant literals.
        /// This mode allows to use parameters in any part of the query where a constant is allowed.
        /// </remarks>
        Interpolate = 3
    }
}
