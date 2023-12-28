#region License Apache 2.0
/* Copyright 2023 Octonica
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
    /// Describes a password complexity rule provided by the ClickHouse server.
    /// </summary>
    public sealed class ClickHousePasswordComplexityRule
    {
        /// <summary>
        /// Gets the rule pattern, provided by the server.
        /// </summary>
        public string OriginalPattern { get; }

        /// <summary>
        /// Gets the rule message.
        /// </summary>
        public string ExceptionMessage { get; }

        /// <summary>
        /// Initializes a new instance of <see cref="ClickHousePasswordComplexityRule"/> with specified arguments.
        /// </summary>
        /// <param name="originalPattern">The rule pattern.</param>
        /// <param name="exceptionMessage">Ther rule message.</param>
        public ClickHousePasswordComplexityRule(string originalPattern, string exceptionMessage)
        {
            OriginalPattern = originalPattern;
            ExceptionMessage = exceptionMessage;
        }
    }
}
