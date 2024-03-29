﻿#region License Apache 2.0
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
    /// <summary>
    /// Specifies the list of available options for establishing a secure connection with the TLS protocol.
    /// </summary>
    public enum ClickHouseTlsMode
    {
        /// <summary>
        /// TLS is disabled. Data exchange between the client and the server will be performed without encryption.
        /// The connection will fail to open if the server requires TLS.
        /// </summary>
        Disable = 0,

        /// <summary>
        /// TLS is required. The connection will fail to open if the server doesn't support TLS.
        /// </summary>
        Require = 1
    }
}
