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
using Octonica.ClickHouseClient.Types;
using System;
using System.Linq;

namespace Octonica.ClickHouseClient.Tests;

internal sealed class TestRejectingTypeInfoProvider : IClickHouseTypeInfoProvider
{
    private readonly IClickHouseTypeInfoProvider _inner;
    private readonly string _rejectedType;

    public TestRejectingTypeInfoProvider(string rejectedType)
        : this(ClickHouseTypeInfoProvider.Instance, rejectedType)
    {
    }

    public TestRejectingTypeInfoProvider(IClickHouseTypeInfoProvider inner, string rejectedType)
    {
        _inner = inner;
        _rejectedType = rejectedType;
    }

    public IClickHouseColumnTypeInfo GetTypeInfo(string typeName)
    {
        if (typeName == _rejectedType)
            throw CreateException();

        return _inner.GetTypeInfo(typeName);
    }

    public IClickHouseColumnTypeInfo GetTypeInfo(ReadOnlyMemory<char> typeName)
    {
        if (typeName.Span.SequenceEqual(_rejectedType))
            throw CreateException();

        return _inner.GetTypeInfo(typeName);
    }

    public IClickHouseColumnTypeInfo GetTypeInfo(IClickHouseColumnTypeDescriptor typeDescriptor)
    {
        return _inner.GetTypeInfo(typeDescriptor);
    }

    public IClickHouseTypeInfoProvider Configure(ClickHouseServerInfo serverInfo)
    {
        return new TestRejectingTypeInfoProvider(_inner.Configure(serverInfo), _rejectedType);
    }

    private ClickHouseException CreateException()
    {
        return new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{_rejectedType}\" is not supported.");
    }
}
