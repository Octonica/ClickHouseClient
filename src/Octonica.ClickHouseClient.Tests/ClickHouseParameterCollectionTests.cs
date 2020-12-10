#region License Apache 2.0
/* Copyright 2020 Octonica
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

using Xunit;

namespace Octonica.ClickHouseClient.Tests
{
    public class ClickHouseParameterCollectionTests
    {
        [Fact]
        public void AddParameter()
        {
            var parameterNames = new[] {"abc", "{abc}", "@abc"};

            foreach (var name in parameterNames)
            {
                var collection = new ClickHouseParameterCollection();
                var parameter = new ClickHouseParameter(name);
                collection.Add(parameter);

                Assert.Single(collection);
                Assert.Same(parameter, collection[0]);

                foreach (var altName in parameterNames)
                {
                    Assert.True(collection.Contains(altName));
                    Assert.True(collection.TryGetValue(altName, out var collectionParameter));
                    Assert.Same(parameter, collectionParameter);
                }
            }
        }
    }
}
