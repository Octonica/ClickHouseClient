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
using System.Globalization;

namespace Octonica.ClickHouseClient
{
    /// <summary>
    /// Represents a version of the ClickHouse client or of the ClickHouse server.
    /// </summary>
    public readonly struct ClickHouseVersion : IEquatable<ClickHouseVersion>
    {
        /// <summary>
        /// Gets the value of the major component of the version.
        /// </summary>
        public int Major { get; }

        /// <summary>
        /// Gets the value of the minor component of the version.
        /// </summary>
        public int Minor { get; }

        /// <summary>
        /// Gets the value of the build component of the version.
        /// </summary>
        public int Build { get; }

        /// <summary>
        /// Initializes a new instance of <see cref="ClickHouseVersion"/> with the specified components.
        /// </summary>
        /// <param name="major">The value of the major component of the version.</param>
        /// <param name="minor">the value of the minor component of the version.</param>
        /// <param name="build">the value of the build component of the version.</param>
        public ClickHouseVersion(int major, int minor, int build)
        {
            Major = major;
            Minor = minor;
            Build = build;
        }

        /// <summary>
        /// Returns the string representation of the version in the format {Major}.{Minor}.{Build}.
        /// </summary>
        /// <returns>The string representation of the version.</returns>
        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}.{1}.{2}", Major, Minor, Build);
        }

        /// <inheritdoc/>
        public bool Equals(ClickHouseVersion other)
        {
            return Major == other.Major && Minor == other.Minor && Build == other.Build;
        }

        /// <inheritdoc/>
        public override bool Equals(object? obj)
        {
            return obj is ClickHouseVersion other && Equals(other);
        }

        /// <inheritdoc/>
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

        /// <summary>
        /// Parses the string in the format "{Major}.{Minor}.{Build}".
        /// </summary>
        /// <param name="value">The string to parse.</param>
        /// <returns>The parsed <see cref="ClickHouseVersion"/>.</returns>
        /// <exception cref="ArgumentNullException">Throws an <see cref="ArgumentNullException"/> when the value is null.</exception>
        /// <exception cref="ArgumentException">Throws an <see cref="ArgumentException"/> when the value is not a valid string representation of the <see cref="ClickHouseVersion"/>.</exception>
        public static ClickHouseVersion Parse(string value)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            int firstIndex = value.IndexOf('.');
            int lastIndex = firstIndex < 0 ? -1 : value.IndexOf('.', firstIndex + 1);
            int nextIndex = lastIndex < 0 ? -1 : value.IndexOf('.', lastIndex + 1);

            const NumberStyles numberStyle = NumberStyles.Integer & ~NumberStyles.AllowLeadingSign;
            if (firstIndex < 0)
            {
                if (int.TryParse(value, numberStyle, CultureInfo.InvariantCulture, out int major))
                {
                    return new ClickHouseVersion(major, 0, 0);
                }
            }
            else if (nextIndex < 0)
            {
                ReadOnlySpan<char> span = value.AsSpan();
                if (int.TryParse(span[..firstIndex], numberStyle, CultureInfo.InvariantCulture, out int major))
                {
                    if (lastIndex < 0)
                    {
                        if (int.TryParse(span[(firstIndex + 1)..], numberStyle, CultureInfo.InvariantCulture, out int minor))
                        {
                            return new ClickHouseVersion(major, minor, 0);
                        }
                    }
                    else
                    {
                        if (int.TryParse(span.Slice(firstIndex + 1, lastIndex - firstIndex - 1), numberStyle, CultureInfo.InvariantCulture, out int minor) &&
                            int.TryParse(span[(lastIndex + 1)..], numberStyle, CultureInfo.InvariantCulture, out int build))
                        {
                            return new ClickHouseVersion(major, minor, build);
                        }
                    }
                }
            }

            throw new ArgumentException("The value is not a valid version number.", nameof(value));
        }

        /// <summary>
        /// Compares two objects of type <see cref="ClickHouseVersion"/>.
        /// </summary>
        /// <param name="left">The left operand.</param>
        /// <param name="right">The right operand.</param>
        /// <returns><see langword="true"/> if the two objects are equal; oterwise <see langword="false"/>.</returns>
        public static bool operator ==(ClickHouseVersion left, ClickHouseVersion right)
        {
            return left.Equals(right);
        }

        /// <summary>
        /// Compares two objects of type <see cref="ClickHouseVersion"/>.
        /// </summary>
        /// <param name="left">The left operand.</param>
        /// <param name="right">The right operand.</param>
        /// <returns><see langword="false"/> if the two objects are equal; oterwise <see langword="true"/>.</returns>
        public static bool operator !=(ClickHouseVersion left, ClickHouseVersion right)
        {
            return !left.Equals(right);
        }
    }
}
