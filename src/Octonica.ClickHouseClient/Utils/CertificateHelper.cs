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

using System.IO;
using System.Security.Cryptography.X509Certificates;

namespace Octonica.ClickHouseClient.Utils
{
    internal static partial class CertificateHelper
    {
        public static X509Certificate2Collection LoadFromFile(string filePath)
        {
            X509Certificate2Collection collection = [];
            switch (Path.GetExtension(filePath).ToLowerInvariant())
            {
                case ".pem":
                case ".crt":
                    ImportPemCertificates(filePath, collection);
                    break;
                default:
                    X509Certificate2 cert = X509CertificateLoader.LoadCertificateFromFile(filePath);
                    _ = collection.Add(cert);
                    break;
            }

            return collection;
        }

        static partial void ImportPemCertificates(string filePath, X509Certificate2Collection collection);
    }
}
