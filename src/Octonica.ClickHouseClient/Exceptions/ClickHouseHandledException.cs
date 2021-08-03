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
    /// An exception which doesn't break the connection. It always has <see cref="Exception.InnerException"/>. This class can't be inherited.
    /// </summary>
    public sealed class ClickHouseHandledException : ClickHouseException
    {
        private ClickHouseHandledException(int errorCode, string message, Exception innerException)
            : base(errorCode, message, innerException)
        {
        }

        internal static ClickHouseHandledException Wrap(Exception exception)
        {
            if (exception is ClickHouseHandledException nfException)
                return nfException;
            
            if (exception is ClickHouseException chException)
                return new ClickHouseHandledException(chException.ErrorCode, chException.Message, chException);

            return new ClickHouseHandledException(ClickHouseErrorCodes.Unspecified, exception.Message, exception);
        }
    }
}
