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

using System;
using System.Text;
using System.Text.RegularExpressions;

namespace Octonica.ClickHouseClient.Protocol
{
    internal static class ClickHouseSyntaxHelper
    {
        private static readonly Regex IdentifierRegex = new Regex("^[a-zA-Z_][0-9a-zA-Z_]*$");

        public static StringBuilder AppendIdentifierLiteral(StringBuilder builder, string identifierLiteral)
        {
            if (IdentifierRegex.IsMatch(identifierLiteral))
                return builder.Append(identifierLiteral);

            builder.Append('`');
            foreach(var ch in identifierLiteral)
            {
                switch (ch)
                {
                    case '\r':
                        builder.Append("\\r");
                        break;
                    case '\n':
                        builder.Append("\\n");
                        break;
                    case '\t':
                        builder.Append("\\t");
                        break;

                    case '\\':
                    case '`':
                        builder.Append("\\");
                        goto default;
                        
                    default:
                        builder.Append(ch);
                        break;
                }
            }

            return builder.Append('`');            
        }

        public static int GetIdentifierLiteralLength(string str, int startIndex)
        {
            return GetIdentifierLiteralLength(((ReadOnlySpan<char>)str).Slice(startIndex));
        }

        public static int GetIdentifierLiteralLength(ReadOnlySpan<char> str)
        {
            if (str.IsEmpty)
                return -1;

            if (str[0] == '`')
            {
                int idx = 1;
                while (idx < str.Length)
                {
                    var slice = str.Slice(idx);
                    var nextIdx = slice.IndexOf('`');

                    if (nextIdx < 0)
                        return -1;

                    if (nextIdx == 0 || slice[nextIdx - 1] != '\\')
                    {
                        int length = idx + nextIdx + 1;
                        if (length > 2)
                            return length;

                        return -1;
                    }

                    idx += nextIdx + 1;
                }
            }
            else if ((str[0] >= 'a' && str[0] <= 'z') || (str[0] >= 'A' && str[0] <= 'Z') || str[0] == '_')
            {
                int length = 1;
                for (; length < str.Length; length++)
                {
                    var ch = str[length];
                    if (char.IsWhiteSpace(ch))
                        break;

                    if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_')
                        continue;

                    return -1;
                }

                return length;
            }

            return -1;
        }

        public static string GetIdentifier(ReadOnlySpan<char> identifierLiteral)
        {
            if (identifierLiteral.Length == 0)
                throw new ArgumentException($"The string \"{identifierLiteral.ToString()}\" is not a valid identifier.", nameof(identifierLiteral));

            var sb = new StringBuilder(identifierLiteral.Length);

            if (identifierLiteral[0] == '`')
            {
                if (identifierLiteral.Length <= 2 || identifierLiteral[^1] != '`')
                    throw new ArgumentException($"The string \"{identifierLiteral.ToString()}\" is not a valid identifier.", nameof(identifierLiteral));

                for (int i = 1; i < identifierLiteral.Length - 1; i++)
                {
                    if (identifierLiteral[i] == '\\')
                    {
                        if (i + 1 == identifierLiteral.Length - 1)
                            throw new ArgumentException($"The string \"{identifierLiteral.ToString()}\" is not a valid identifier.", nameof(identifierLiteral));

                        switch (identifierLiteral[i + 1])
                        {
                            case 'r':
                                sb.Append('\r');
                                break;
                            case 'n':
                                sb.Append('\n');
                                break;
                            case 't':
                                sb.Append('\t');
                                break;
                            default:
                                sb.Append(identifierLiteral[i + 1]);
                                break;
                        }

                        i++;
                    }
                    else
                    {
                        sb.Append(identifierLiteral[i]);
                    }
                }

                return sb.ToString();
            }

            if ((identifierLiteral[0] >= 'a' && identifierLiteral[0] <= 'z') || (identifierLiteral[0] >= 'A' && identifierLiteral[0] <= 'Z') || identifierLiteral[0] == '_')
            {
                sb.Append(identifierLiteral[0]);
                for (int i = 1; i < identifierLiteral.Length; i++)
                {
                    var ch = identifierLiteral[i];
                    if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_')
                    {
                        sb.Append(ch);
                        continue;
                    }

                    throw new ArgumentException($"The string \"{identifierLiteral.ToString()}\" is not a valid identifier.", nameof(identifierLiteral));
                }

                return sb.ToString();
            }

            throw new ArgumentException($"The string \"{identifierLiteral.ToString()}\" is not a valid identifier.", nameof(identifierLiteral));
        }
    }
}
