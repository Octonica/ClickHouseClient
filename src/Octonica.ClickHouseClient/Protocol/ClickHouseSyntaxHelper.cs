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
        private static readonly Regex IdentifierRegex = new Regex("^[a-zA-Z_][0-9a-zA-Z_]*$");

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
            return GetIdentifierLiteralLength(((ReadOnlySpan<char>)str).Slice(startIndex));
        }

        public static int GetIdentifierLiteralLength(ReadOnlySpan<char> str)
        {
            if (str.IsEmpty)
                return -1;

            if (str[0] == '`')
            {
                var length = GetQuotedTokenLength(str, '`');
                if (length > 2)
                    return length;
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

        public static int GetSingleQuoteStringLength(string str, int startIndex)
        {
            return GetSingleQuoteStringLength(((ReadOnlySpan<char>)str).Slice(startIndex));
        }

        public static int GetSingleQuoteStringLength(ReadOnlySpan<char> str)
        {
            return GetQuotedTokenLength(str, '\'');
        }

        private static int GetQuotedTokenLength(ReadOnlySpan<char> str, char quoteSign)
        {
            if (str.IsEmpty || str[0] != quoteSign)
                return -1;

            int idx = 1;
            while (idx < str.Length)
            {
                var slice = str.Slice(idx);
                var nextIdx = slice.IndexOfAny(quoteSign, '\\');

                if (nextIdx < 0)
                    return -1;

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
                throw new ArgumentException($"The string \"{identifierLiteral.ToString()}\" is not a valid identifier.", nameof(identifierLiteral));

            var sb = new StringBuilder(identifierLiteral.Length);

            if (identifierLiteral[0] == '`')
            {
                if (!TryParseQuotedToken(identifierLiteral, '`', out var parsedLiteral) || parsedLiteral == string.Empty)
                    throw new ArgumentException($"The string \"{identifierLiteral.ToString()}\" is not a valid identifier.", nameof(identifierLiteral));

                return parsedLiteral;
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

        public static string GetSingleQuoteString(ReadOnlySpan<char> stringToken)
        {
            if (!TryParseQuotedToken(stringToken, '\'', out var result))
                throw new ArgumentException($"The value \"{stringToken.ToString()}\" is not a valid string token.");

            return result;
        }

        private static bool TryParseQuotedToken(ReadOnlySpan<char> token, char quoteSign, [MaybeNullWhen(false)] out string value)
        {
            if (token.Length < 2 || token[0] != quoteSign || token[^1] != quoteSign)
            {
                value = null;
                return false;
            }
            
            var sb = new StringBuilder(token.Length);
            for (int i = 1; i < token.Length - 1; i++)
            {
                if (token[i] == '\\')
                {
                    if (i + 1 == token.Length - 1)
                    {
                        value = null;
                        return false;
                    }

                    if (token[i + 1] == quoteSign)
                    {
                        sb.Append(quoteSign);
                    }
                    else if (TryGetUnescapedCharacter(token[i + 1], out var unescapedCh))
                    {
                        sb.Append(unescapedCh);
                    }
                    else
                    {
                        sb.Append(token[i]).Append(token[i + 1]);
                    }

                    i++;
                }
                else
                {
                    sb.Append(token[i]);
                }
            }

            value = sb.ToString();
            return true;
        }
    }
}
