#region License Apache 2.0
/* Copyright 2019-2021, 2023 Octonica
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
using System;
using System.Collections.Generic;

namespace Octonica.ClickHouseClient.Protocol
{
    internal sealed class ClientQueryMessage : IClientMessage
    {
        public ClientMessageCode MessageCode => ClientMessageCode.Query;
        
        public QueryKind QueryKind { get; }

        public string? InitialUser { get; }

        public string? InitialQueryId { get; }

        public string RemoteAddress { get; }

        public string Host { get; }

        public string ClientName { get; }

        public ClickHouseVersion ClientVersion { get; }

        public int ProtocolRevision { get; }

        public string Query { get; }

        public bool CompressionEnabled { get; }

        // https://github.com/ClickHouse/ClickHouse/blob/master/dbms/src/Core/Settings.h
        public IReadOnlyCollection<KeyValuePair<string, string>>? Settings { get; }

        public IReadOnlyCollection<KeyValuePair<string, ClickHouseParameterWriter>>? Parameters { get; }

        private ClientQueryMessage(Builder builder)
        {
            QueryKind = builder.QueryKind ?? throw new ArgumentException("The kind of the query is required.", nameof(QueryKind));
            InitialUser = builder.InitialUser;
            InitialQueryId = builder.InitialQueryId;
            RemoteAddress = builder.RemoteAddress ?? throw new ArgumentException("The remote address is required.", nameof(RemoteAddress));
            Host = builder.Host ?? throw new ArgumentException("The name of the host is required.", nameof(Host));
            ClientName = builder.ClientName ?? throw new ArgumentException("The name of the client is required.", nameof(ClientName));
            ClientVersion = builder.ClientVersion ?? throw new ArgumentException("The version of the client is required.", nameof(ClientVersion));
            ProtocolRevision = builder.ProtocolRevision ?? throw new ArgumentException("The revision of the protocol is required.", nameof(ProtocolRevision));
            Query = builder.Query ?? throw new ArgumentException("The query is required.", nameof(Query));
            CompressionEnabled = builder.CompressionEnabled ?? throw new ArgumentException("Unknown compression mode.", nameof(CompressionEnabled));
            Settings = builder.Settings == null || builder.Settings.Count == 0 ? null : builder.Settings;
            Parameters = builder.Parameters == null || builder.Parameters.Count == 0 ? null : builder.Parameters;
        }

        public void Write(ClickHouseBinaryProtocolWriter writer)
        {
            writer.Write7BitInt32((int) MessageCode);
            writer.WriteString(string.Empty);
            switch (QueryKind)
            {
                case QueryKind.NoQuery:
                    break;

                case QueryKind.InitialQuery:
                    writer.Write7BitInt32((int) QueryKind);

                    writer.WriteString(InitialUser?? string.Empty); //initial user
                    writer.WriteString(InitialQueryId?? string.Empty); //initial query id
                    writer.WriteString(RemoteAddress); //initial IP address

                    if (ProtocolRevision >= ClickHouseProtocolRevisions.MinRevisionWithInitialQueryStartTime)
                    {
                        // Initial query start time in microseconds. An actual value of this property should be set by the server.
                        Span<byte> zero = stackalloc byte[sizeof(ulong)];
                        writer.WriteBytes(zero);
                    }

                    writer.Write7BitInt32(1); //TCP

                    writer.WriteString(string.Empty); //OS user

                    writer.WriteString(Host);
                    writer.WriteString(ClientName);

                    writer.Write7BitInt32(ClientVersion.Major);
                    writer.Write7BitInt32(ClientVersion.Minor);
                    writer.Write7BitInt32(ProtocolRevision);

                    writer.WriteString(string.Empty); //quota key

                    if (ProtocolRevision >= ClickHouseProtocolRevisions.MinRevisionWithDistributedDepth)
                        writer.Write7BitInt32(0); //distributed depth

                    writer.Write7BitInt32(ClientVersion.Build);

                    if (ProtocolRevision >= ClickHouseProtocolRevisions.MinRevisionWithOpenTelemetry)
                        writer.WriteByte(0); // TODO: add support for Open Telemetry headers

                    if (ProtocolRevision >= ClickHouseProtocolRevisions.MinRevisionWithParallelReplicas)
                    {
                        writer.WriteByte(0); // collaborate_with_initiator
                        writer.WriteByte(0); // count_participating_replicas
                        writer.WriteByte(0); // number_of_current_replica
                    }

                    break;

                case QueryKind.SecondaryQuery:
                    throw new NotImplementedException();

                default:
                    throw new NotSupportedException();
            }

            if (Settings != null)
            {
                // All settings are serialized as strings. Before each value the flag `is_important` is serialized.
                // https://github.com/ClickHouse/ClickHouse/blob/97d97f6b2e50ab3cf21a25a18cbf1aa327f242e5/src/Core/BaseSettings.h#L19

                const int isImportantFlag = 0x1;
                foreach (var pair in Settings)
                {
                    writer.WriteString(pair.Key);
                    writer.Write7BitInt32(isImportantFlag);
                    writer.WriteString(pair.Value);
                }
            }

            writer.WriteString(string.Empty); // empty string is a marker of the end of the settings

            if (ProtocolRevision >= ClickHouseProtocolRevisions.MinRevisionWithInterserverSecret)
                writer.WriteString(string.Empty);

            writer.Write7BitInt32(StateCodes.Complete);

            writer.WriteBool(CompressionEnabled);

            writer.WriteString(Query);

            if (ProtocolRevision >= ClickHouseProtocolRevisions.MinRevisionWithParameters)
            {
                if (Parameters != null)
                {
                    const int isCustomFlag = 0x2;
                    foreach (var pair in Parameters)
                    {
                        if (pair.Value.Length < 0)
                            continue;

                        writer.WriteString(pair.Key);
                        writer.Write7BitInt32(isCustomFlag);

                        var lenght = pair.Value.Length;
                        writer.Write7BitInt32(lenght + 2);
                        writer.WriteByte((byte)'\'');

                        if (lenght > 0)
                        {
                            var size = writer.WriteRaw(lenght, buffer => new SequenceSize(pair.Value.Write(buffer), 1));

                            // The lenght must be calculated by the parameter writer correctly
                            if (size.Bytes != lenght)
                                throw new ClickHouseException(ClickHouseErrorCodes.InternalError, $"Internal error. The length of the parameter \"{pair.Key}\" in bytes is {lenght}, but the number of written bytes is {size.Bytes}.");
                        }

                        writer.WriteByte((byte)'\'');
                    }
                }

                writer.WriteString(string.Empty); // empty string is a marker of the end of parameters
            }
            else if (Parameters != null)
            {
                var errMsg =
                    "The server doesn't support parameters in the query. " +
                    $"This error is caused by one or more parameters passed in the mode \"{nameof(ClickHouseParameterMode.Literal)}\". " +
                    $"Only \"{nameof(ClickHouseParameterMode.Binary)}\" or \"{nameof(ClickHouseParameterMode.Interpolate)}\" modes are supported.";

                throw new ClickHouseException(ClickHouseErrorCodes.ProtocolRevisionNotSupported, errMsg);
            }
        }

        public class Builder
        {
            /// <summary>
            /// Required
            /// </summary>
            public QueryKind? QueryKind { get; set; }

            /// <summary>
            /// Optional
            /// </summary>
            public string? InitialUser { get; set; }

            /// <summary>
            /// Optional
            /// </summary>
            public string? InitialQueryId { get; set; }

            /// <summary>
            /// Required
            /// </summary>
            public string? RemoteAddress { get; set; }

            /// <summary>
            /// Required
            /// </summary>
            public string? Host { get; set; }

            /// <summary>
            /// Required
            /// </summary>
            public string? ClientName { get; set; }

            /// <summary>
            /// Required
            /// </summary>
            public ClickHouseVersion? ClientVersion { get; set; }

            /// <summary>
            /// Required
            /// </summary>
            public int? ProtocolRevision { get; set; }

            /// <summary>
            /// Required
            /// </summary>
            public string? Query { get; set; }

            /// <summary>
            /// Required
            /// </summary>
            public bool? CompressionEnabled { get; set; }

            /// <summary>
            /// Optional
            /// </summary>
            public IReadOnlyCollection<KeyValuePair<string, string>>? Settings { get; set; }

            /// <summary>
            /// Optional
            /// </summary>
            public IReadOnlyCollection<KeyValuePair<string, ClickHouseParameterWriter>>? Parameters { get; set; }

            public ClientQueryMessage Build()
            {
                return new ClientQueryMessage(this);
            }
        }
    }
}
