#region License Apache 2.0
/* Copyright 2019-2020 Octonica
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
using System.Globalization;

namespace Octonica.ClickHouseClient
{
    public struct ClickHouseVersion : IEquatable<ClickHouseVersion>
    {
        public int Major { get; }

        public int Minor { get; }

        public int Build { get; }

        public ClickHouseVersion(int major, int minor, int build)
        {
            Major = major;
            Minor = minor;
            Build = build;
        }

        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}.{1}.{2}", Major, Minor, Build);
        }

        public bool Equals(ClickHouseVersion other)
        {
            return Major == other.Major && Minor == other.Minor && Build == other.Build;
        }

        public override bool Equals(object? obj)
        {
            return obj is ClickHouseVersion other && Equals(other);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = Major;
                hashCode = (hashCode * 397) ^ Minor;
                hashCode = (hashCode * 397) ^ Build;
                return hashCode;
            }
        }

        public static ClickHouseVersion Parse(string value)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            var firstIndex = value.IndexOf('.');
            var lastIndex = firstIndex < 0 ? -1 : value.IndexOf('.', firstIndex + 1);
            var nextIndex = lastIndex < 0 ? -1 : value.IndexOf('.', lastIndex + 1);

            const NumberStyles numberStyle = NumberStyles.Integer & ~NumberStyles.AllowLeadingSign;
            if (firstIndex < 0)
            {
                if (int.TryParse(value, numberStyle, CultureInfo.InvariantCulture, out var major))
                    return new ClickHouseVersion(major, 0, 0);
            }
            else if (nextIndex < 0)
            {
                var span = value.AsSpan();
                if (int.TryParse(span.Slice(0, firstIndex), numberStyle, CultureInfo.InvariantCulture, out var major))
                {
                    if (lastIndex < 0)
                    {
                        if (int.TryParse(span.Slice(firstIndex + 1), numberStyle, CultureInfo.InvariantCulture, out var minor))
                            return new ClickHouseVersion(major, minor, 0);
                    }
                    else
                    {
                        if (int.TryParse(span.Slice(firstIndex + 1, lastIndex - firstIndex - 1), numberStyle, CultureInfo.InvariantCulture, out var minor) &&
                            int.TryParse(span.Slice(lastIndex + 1), numberStyle, CultureInfo.InvariantCulture, out var build))
                        {
                            return new ClickHouseVersion(major, minor, build);
                        }
                    }
                }
            }

            throw new ArgumentException("The value is not a valid version number.", nameof(value));
        }

        public static bool operator ==(ClickHouseVersion left, ClickHouseVersion right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(ClickHouseVersion left, ClickHouseVersion right)
        {
            return !left.Equals(right);
        }
    }
}
