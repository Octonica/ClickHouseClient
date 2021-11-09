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

#if !NET5_0_OR_GREATER

using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace Octonica.ClickHouseClient.Utils
{
    partial class CertificateHelper
    {
        static partial void ImportPemCertificates(string filePath, X509Certificate2Collection collection)
        {
            /* PEM file without private keys looks like a bunch of concatenated CRT files
             * 
             * -----BEGIN CERTIFICATE-----
             * Base64-encoded Certificate 1
             * -----END CERTIFICATE-----
             * ...
             * -----BEGIN CERTIFICATE-----
             * Base64-encoded Certificate N
             * -----END CERTIFICATE-----
             */

            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            using var streamReader = new StreamReader(fs);

            var sb = new StringBuilder();
            string? entity = null;

            int lineNumber = 0;
            const string delimiter = "-----";
            const string beginPrefix = delimiter + "BEGIN ";
            const string endPrefix = delimiter + "END ";
            while (!streamReader.EndOfStream)
            {
                ++lineNumber;
                var line = streamReader.ReadLine();
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                if (entity == null)
                {
                    if (!line.StartsWith(beginPrefix))
                    {
                        var actualValue = line.Length <= beginPrefix.Length ? line : line.Substring(0, beginPrefix.Length);
                        throw new InvalidDataException($"Unexpected value '{actualValue}' at the line {lineNumber}, position {0} of the certificate file. Expected value is '{beginPrefix}'.");
                    }
                    else if (!line.EndsWith(delimiter))
                    {
                        var actualValue = line.Substring(line.Length - delimiter.Length);
                        throw new InvalidDataException($"Unexpected value '{actualValue}' at the line {lineNumber}, position {line.Length - delimiter.Length} of the certificate file. Expected value is '{delimiter}'.");
                    }

                    entity = line.Substring(beginPrefix.Length, line.Length - beginPrefix.Length - delimiter.Length);
                    if (string.IsNullOrWhiteSpace(entity))
                    {
                        throw new InvalidDataException($"Missing non-empty value between '{beginPrefix}' and '{delimiter}' at the line {lineNumber}, position {beginPrefix.Length} of the certificate file.");
                    }

                    sb.AppendLine(line);
                }
                else if (!line.StartsWith(endPrefix))
                {
                    sb.AppendLine(line);
                }
                else if (!line.EndsWith(delimiter))
                {
                    var actualValue = line.Substring(line.Length - delimiter.Length);
                    throw new InvalidDataException($"Unexpected value '{actualValue}' at the line {lineNumber}, position {line.Length - delimiter.Length} of the certificate file. Expected value is '{delimiter}'.");
                }
                else
                {
                    var endEntity = line.Substring(endPrefix.Length, line.Length - endPrefix.Length - delimiter.Length);
                    if (entity != endEntity)
                    {
                        throw new InvalidDataException($"The END value '{endEntity}' at the line {lineNumber}, position {beginPrefix.Length} of the certificate file is not equal to the BEGIN value '{entity}'.");
                    }

                    if (entity == "CERTIFICATE")
                    {
                        sb.AppendLine(line);
                        collection.Import(Encoding.UTF8.GetBytes(sb.ToString()));
                    }

                    sb.Clear();
                    entity = null;
                }
            }

            if (entity != null)
                throw new InvalidDataException($"Unexpected end of the certificate file. Missing '{endPrefix}{entity}{delimiter}'.");
        }
    }
}

#endif