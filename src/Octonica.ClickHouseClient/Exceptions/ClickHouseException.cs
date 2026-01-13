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

using System;

namespace Octonica.ClickHouseClient.Exceptions
{
    /// <summary>
    /// Represents an exception specific to ClickHouse client or server.
    /// </summary>
    public class ClickHouseException : Exception
    {
        /// <summary>
        /// The numeric code of the error. For the full list of errors see <see cref="ClickHouseErrorCodes"/>.
        /// </summary>
        public int ErrorCode { get; }

        /// <summary>
        /// Initializes a new instance of the exception with the specified error code.
        /// </summary>
        /// <param name="errorCode">The code of the error. It should be one of the values from <see cref="ClickHouseErrorCodes"/>.</param>
        public ClickHouseException(int errorCode)
        {
            ErrorCode = errorCode;
        }

        /// <summary>
        /// Initializes a new instance of the exception with specified error code and message.
        /// </summary>
        /// <param name="errorCode">The code of the error. It should be one of the values from <see cref="ClickHouseErrorCodes"/>.</param>
        /// <param name="message">The error message that explains the reason for the exception.</param>
        public ClickHouseException(int errorCode, string? message)
            : base(message)
        {
            ErrorCode = errorCode;
        }

        /// <summary>
        /// Initializes a new instance of the exception with specified error code, message, and inner exception.
        /// </summary>
        /// <param name="errorCode">The code of the error. It should be one of the values from <see cref="ClickHouseErrorCodes"/>.</param>
        /// <param name="message">The error message that explains the reason for the exception.</param>
        /// <param name="innerException">The exception that is the cause of the current exception.</param>
        public ClickHouseException(int errorCode, string? message, Exception? innerException)
            : base(message, innerException)
        {
            ErrorCode = errorCode;
        }
    }
}
