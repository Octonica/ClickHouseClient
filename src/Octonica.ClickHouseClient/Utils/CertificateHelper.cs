#region License Apache 2.0
/* Copyright 2021, 2026 Octonica
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

using System.IO;
using System.Security.Cryptography.X509Certificates;

namespace Octonica.ClickHouseClient.Utils
{
    internal static class CertificateHelper
    {
        public static X509Certificate2Collection LoadFromFile(string filePath)
        {
            var collection = new X509Certificate2Collection();
            switch (Path.GetExtension(filePath).ToLowerInvariant())
            {
                case ".pem":
                case ".crt":
                    collection.ImportFromPemFile(filePath);
                    break;
                default:
#if NET10_0_OR_GREATER
                    var cert = X509CertificateLoader.LoadCertificateFromFile(filePath);
                    collection.Add(cert);
#else
                    collection.Import(filePath);
#endif
                    break;
            }

            return collection;
        }
    }
}
