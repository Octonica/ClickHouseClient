#region License Apache 2.0
/* Copyright 2026 Octonica
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

namespace Octonica.ClickHouseClient;

/// <summary>
/// Specifies how a command behaves when the connection already has an active session.
/// </summary>
public enum ClickHouseBusyConnectionMode
{
    /// <summary>
    /// Wait until the active session is released. This is the default.
    /// A second synchronous command on the same thread that acquired the session throws
    /// <see cref="ClickHouseException"/> with the error code <see cref="ClickHouseErrorCodes.OperationInProgress"/>,
    /// because that call can only deadlock.
    /// </summary>
    Wait = 0,

    /// <summary>
    /// Throw <see cref="ClickHouseException"/> with the error code <see cref="ClickHouseErrorCodes.OperationInProgress"/>
    /// instead of waiting when the connection already has an active session. An active session means an open
    /// <see cref="ClickHouseDataReader"/>, an open <see cref="ClickHouseColumnWriter"/>, or another command still executing.
    /// Same-thread synchronous reentry throws in this mode as well.
    /// </summary>
    Throw = 1
}
