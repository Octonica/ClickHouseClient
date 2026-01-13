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
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.RegularExpressions;

namespace Octonica.ClickHouseClient.Protocol
{
    internal static class ClickHouseSyntaxHelper
    {
        private static readonly Regex IdentifierRegex = new("^[a-zA-Z_][0-9a-zA-Z_]*$");

        private const string EscapeChars = "''\"\"\\\\0\0a\ab\be\u001bf\fn\nr\rt\tv\v";

        private static bool TryGetUnescapedCharacter(char ch, out char unescapedCh)
        {
            for (int i = 0; i < EscapeChars.Length; i += 2)
            {
                if (ch == EscapeChars[i])
                {
                    unescapedCh = EscapeChars[i + 1];
                    return true;
                }
            }

            unescapedCh = default;
            return false;
        }

        public static int GetIdentifierLiteralLength(string str, int startIndex)
        {
            return GetIdentifierLiteralLength(((ReadOnlySpan<char>)str)[startIndex..]);
        }

        public static int GetIdentifierLiteralLength(ReadOnlySpan<char> str)
        {
            if (str.IsEmpty)
            {
                return -1;
            }

            if (str[0] == '`')
            {
                int length = GetQuotedTokenLength(str, '`');
                if (length > 2)
                {
                    return length;
                }
            }
            else if (str[0] is (>= 'a' and <= 'z') or (>= 'A' and <= 'Z') or '_')
            {
                int length = 1;
                for (; length < str.Length; length++)
                {
                    char ch = str[length];
                    if (char.IsWhiteSpace(ch))
                    {
                        break;
                    }

                    if (ch is (>= 'a' and <= 'z') or (>= 'A' and <= 'Z') or (>= '0' and <= '9') or '_')
                    {
                        continue;
                    }

                    return -1;
                }

                return length;
            }

            return -1;
        }

        public static int GetSingleQuoteStringLength(string str, int startIndex)
        {
            return GetSingleQuoteStringLength(((ReadOnlySpan<char>)str)[startIndex..]);
        }

        public static int GetSingleQuoteStringLength(ReadOnlySpan<char> str)
        {
            return GetQuotedTokenLength(str, '\'');
        }

        public static int GetQuotedTokenLength(ReadOnlySpan<char> str, char quoteSign)
        {
            if (str.IsEmpty || str[0] != quoteSign)
            {
                return -1;
            }

            int idx = 1;
            while (idx < str.Length)
            {
                ReadOnlySpan<char> slice = str[idx..];
                int nextIdx = slice.IndexOfAny(quoteSign, '\\');

                if (nextIdx < 0)
                {
                    return -1;
                }

                if (slice[nextIdx] == '\\')
                {
                    idx += nextIdx + 2;
                    continue;
                }

                return idx + nextIdx + 1;
            }

            return -1;
        }

        public static string GetIdentifier(ReadOnlySpan<char> identifierLiteral)
        {
            if (identifierLiteral.Length == 0)
            {
                throw new ArgumentException($"The string \"{identifierLiteral}\" is not a valid identifier.", nameof(identifierLiteral));
            }

            StringBuilder sb = new(identifierLiteral.Length);

            if (identifierLiteral[0] == '`')
            {
                return !TryParseQuotedToken(identifierLiteral, '`', out string? parsedLiteral) || parsedLiteral == string.Empty
                    ? throw new ArgumentException($"The string \"{identifierLiteral}\" is not a valid identifier.", nameof(identifierLiteral))
                    : parsedLiteral;
            }

            if (identifierLiteral[0] is (>= 'a' and <= 'z') or (>= 'A' and <= 'Z') or '_')
            {
                _ = sb.Append(identifierLiteral[0]);
                for (int i = 1; i < identifierLiteral.Length; i++)
                {
                    char ch = identifierLiteral[i];
                    if (ch is (>= 'a' and <= 'z') or (>= 'A' and <= 'Z') or (>= '0' and <= '9') or '_')
                    {
                        _ = sb.Append(ch);
                        continue;
                    }

                    throw new ArgumentException($"The string \"{identifierLiteral}\" is not a valid identifier.", nameof(identifierLiteral));
                }

                return sb.ToString();
            }

            throw new ArgumentException($"The string \"{identifierLiteral}\" is not a valid identifier.", nameof(identifierLiteral));
        }

        public static string GetSingleQuoteString(ReadOnlySpan<char> stringToken)
        {
            return !TryParseQuotedToken(stringToken, '\'', out string? result)
                ? throw new ArgumentException($"The value \"{stringToken}\" is not a valid string token.")
                : result;
        }

        private static bool TryParseQuotedToken(ReadOnlySpan<char> token, char quoteSign, [MaybeNullWhen(false)] out string value)
        {
            if (token.Length < 2 || token[0] != quoteSign || token[^1] != quoteSign)
            {
                value = null;
                return false;
            }

            StringBuilder sb = new(token.Length);
            for (int i = 1; i < token.Length - 1; i++)
            {
                if (token[i] == '\\')
                {
                    if (i + 1 == token.Length - 1)
                    {
                        value = null;
                        return false;
                    }

                    _ = token[i + 1] == quoteSign
                        ? sb.Append(quoteSign)
                        : TryGetUnescapedCharacter(token[i + 1], out char unescapedCh)
                            ? sb.Append(unescapedCh)
                            : sb.Append(token[i]).Append(token[i + 1]);

                    i++;
                }
                else
                {
                    _ = sb.Append(token[i]);
                }
            }

            value = sb.ToString();
            return true;
        }
    }
}
