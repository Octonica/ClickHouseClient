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

namespace Octonica.ClickHouseClient
{
    internal enum ClickHouseTcpClientState
    {
        /// <summary>
        /// TCP client is ready to open a session
        /// </summary>
        Ready = 0,

        /// <summary>
        /// There is an active session associated with the client
        /// </summary>
        Active = 1,

        /// <summary>
        /// A session was failed. TCP client was forced to close
        /// </summary>
        Failed = 2
    }
}
