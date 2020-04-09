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

namespace Octonica.ClickHouseClient.Exceptions
{
    public class ClickHouseServerException : ClickHouseException
    {
        public int ServerErrorCode { get; }

        public string ServerErrorType { get; }

        public string ServerStackTrace { get; }
        
        public string? Query { get; }

        public ClickHouseServerException(int serverErrorCode, string serverErrorType, string message, string serverStackTrace)
            : base(ClickHouseErrorCodes.ServerError, message)
        {
            ServerErrorCode = serverErrorCode;
            ServerErrorType = serverErrorType;
            ServerStackTrace = serverStackTrace;
        }

        public ClickHouseServerException(int serverErrorCode, string serverErrorType, string message, string serverStackTrace, Exception innerException)
            : base(ClickHouseErrorCodes.ServerError, message, innerException)
        {
            ServerErrorCode = serverErrorCode;
            ServerErrorType = serverErrorType;
            ServerStackTrace = serverStackTrace;
        }

        private ClickHouseServerException(int serverErrorCode, string serverErrorType, string message, string serverStackTrace, string query)
            : base(ClickHouseErrorCodes.ServerError, message)
        {
            ServerErrorCode = serverErrorCode;
            ServerErrorType = serverErrorType;
            ServerStackTrace = serverStackTrace;
            Query = query;
        }

        private ClickHouseServerException(int serverErrorCode, string serverErrorType, string message, string serverStackTrace, string query, Exception innerException)
            : base(ClickHouseErrorCodes.ServerError, message, innerException)
        {
            ServerErrorCode = serverErrorCode;
            ServerErrorType = serverErrorType;
            ServerStackTrace = serverStackTrace;
            Query = query;
        }

        public ClickHouseServerException CopyWithQuery(string query)
        {
            if (InnerException == null)
                return new ClickHouseServerException(ServerErrorCode, ServerErrorType, Message, ServerStackTrace, query);

            return new ClickHouseServerException(ServerErrorCode, ServerErrorType, Message, ServerStackTrace, query, InnerException);
        }
    }
}
