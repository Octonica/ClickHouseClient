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
    /// The static class that provides access to error codes.
    /// </summary>
    public static class ClickHouseErrorCodes
    {
        /// <summary>
        /// The code for an unspecified error. This code means that an error of an unknown origin was wrapped in <see cref="ClickHouseException"/>.
        /// </summary>
        public const int Unspecified = 0;
        
        /// <summary>
        /// The code for an error provided by the ClickHouse server.
        /// </summary>
        public const int ServerError = 1;

        /// <summary>
        /// The code for an error caused by an invalid state of the connection.
        /// </summary>
        public const int InvalidConnectionState = 2;

        /// <summary>
        /// The code for an error caused by an attempt to execute an operation when the connection is closed.
        /// </summary>
        public const int ConnectionClosed = 3;

        /// <summary>
        /// The code for an error caused by an unexpected response from the server.
        /// </summary>
        public const int ProtocolUnexpectedResponse = 4;

        /// <summary>
        /// The code for an error caused by an attempt to use a feature that is not supported by the revision negotiated between the client and the server.
        /// </summary>
        public const int ProtocolRevisionNotSupported = 5;

        /// <summary>
        /// The code for an error caused by an attempt to read or write a column of an unknown type.
        /// </summary>
        public const int TypeNotSupported = 6;

        /// <summary>
        /// The code for an error caused by an invalid full name of the type. This code usually means that the type was specified with invalid parameters.
        /// </summary>
        public const int InvalidTypeName = 7;

        /// <summary>
        /// The code for an error caused by an attempt to use a parametrizable type without parameters.
        /// </summary>
        public const int TypeNotFullySpecified = 8;

        /// <summary>
        /// The code for an error caused by a lack of an expected response from the server.
        /// </summary>
        public const int QueryTypeMismatch = 9;

        /// <summary>
        /// The code for a data reading error.
        /// </summary>
        public const int DataReaderError = 10;

        /// <summary>
        /// The code for an error caused by a lack of required query parameter.
        /// </summary>
        public const int QueryParameterNotFound = 11;

        /// <summary>
        /// The code for an error caused by a configuration of the query parameter (<see cref="ClickHouseParameter"/>).
        /// </summary>
        public const int InvalidQueryParameterConfiguration = 12;

        /// <summary>
        /// An obsolete error code. It was replaced with <see cref="ColumnTypeMismatch"/>, <see cref="NotSupportedInSyncronousMode"/>, and <see cref="InvalidRowCount"/>.
        /// </summary>
        [Obsolete("ColumnMismatch was replaced with " + nameof(ColumnTypeMismatch) + ", " + nameof(NotSupportedInSyncronousMode) + " and " + nameof(InvalidRowCount))]
        public const int ColumnMismatch = 13;

        /// <summary>
        /// The common code for unexpected situations. This code indicates errors (bugs) in the client's code.
        /// </summary>
        public const int InternalError = 14;

        /// <summary>
        /// The code for an error caused by a compression decoder.
        /// </summary>
        public const int CompressionDecoderError = 15;

        /// <summary>
        /// The code for an error caused by a network error.
        /// </summary>
        public const int NetworkError = 16;
        
        /// <summary>
        /// The code for an error caused by invalid settings (<see cref="Protocol.ClickHouseColumnSettings"/>).
        /// </summary>
        public const int InvalidColumnSettings = 17;

        /// <summary>
        /// The code for a column type error. This code means that there is no mapping between the ClickHouse column's type and the provided type.
        /// </summary>
        public const int ColumnTypeMismatch = 18;

        /// <summary>
        /// The code for an error caused by a wrong number of rows in a column. Usually this error code means that there are less rows in the colum than expected.
        /// </summary>
        public const int InvalidRowCount = 19;

        /// <summary>
        /// The code for an error caused by an attempt to execute an asyncronous operation in a syncronous method.
        /// </summary>
        public const int NotSupportedInSyncronousMode = 20;
        
        /// <summary>
        /// The code for an error caused by the callback to an external code.
        /// </summary>
        public const int CallbackError = 21;
    }
}
