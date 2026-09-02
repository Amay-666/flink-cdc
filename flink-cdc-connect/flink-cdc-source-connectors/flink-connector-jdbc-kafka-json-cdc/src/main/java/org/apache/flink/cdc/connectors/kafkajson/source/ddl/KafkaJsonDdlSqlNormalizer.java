/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.kafkajson.source.ddl;

/**
 * Normalizes DDL statements that standard MySQL parsers reject, so they can be handed to Druid's
 * MySQL grammar.
 *
 * <p>TiDB is not 100% MySQL-compatible and accepts idempotence modifiers standard MySQL does not —
 * {@code ALTER TABLE ... ADD COLUMN IF NOT EXISTS ...}, {@code DROP COLUMN IF EXISTS ...}, {@code
 * ADD INDEX IF NOT EXISTS ...}. A TiDB-sourced canal stream can therefore carry such a statement,
 * which Druid's MySQL parser rejects with a {@code ParserException} (verified on Druid 1.2.18). The
 * only operation here is {@link #stripIfExistsClauses}: it removes the {@code IF [NOT] EXISTS}
 * modifier token by token so the surrounding statement stays valid standard MySQL.
 *
 * <p>Stripping is semantically safe for a statement that was already executed successfully
 * upstream: the DDL reaches the connector because it committed, and a committed {@code ADD COLUMN
 * IF NOT EXISTS}/{@code DROP COLUMN IF EXISTS} only succeeds when the guard was satisfied — the
 * column was absent / present — so dropping the modifier does not change the effective schema
 * change. (The complementary guards, e.g. a redundant {@code ADD COLUMN IF NOT EXISTS} that was a
 * no-op, are never emitted by a committed binlog record.)
 *
 * <p>Token stripping is done by a small state machine rather than a regex so occurrences inside
 * string literals, quoted identifiers and comments are left untouched.
 */
final class KafkaJsonDdlSqlNormalizer {

    private KafkaJsonDdlSqlNormalizer() {}

    /**
     * Returns a copy of {@code ddl} with every {@code IF [NOT] EXISTS} clause outside of literals,
     * quoted identifiers and comments removed; returns the input unchanged when none is found.
     */
    static String stripIfExistsClauses(String ddl) {
        StringBuilder result = new StringBuilder(ddl.length());
        int i = 0;
        int n = ddl.length();
        while (i < n) {
            char c = ddl.charAt(i);
            if (c == '\'' || c == '"' || c == '`') {
                int end = quotedEnd(ddl, i, c);
                result.append(ddl, i, end);
                i = end;
            } else if (c == '-' && i + 1 < n && ddl.charAt(i + 1) == '-') {
                int end = lineEnd(ddl, i);
                result.append(ddl, i, end);
                i = end;
            } else if (c == '#') {
                int end = lineEnd(ddl, i);
                result.append(ddl, i, end);
                i = end;
            } else if (c == '/' && i + 1 < n && ddl.charAt(i + 1) == '*') {
                int end = blockCommentEnd(ddl, i);
                result.append(ddl, i, end);
                i = end;
            } else {
                int clauseEnd = ifExistsClauseEnd(ddl, i);
                if (clauseEnd < 0) {
                    result.append(c);
                    i++;
                } else {
                    // Drop "IF [NOT] EXISTS" together with the whitespace around it and bridge the
                    // surrounding text with a single space, so the statement stays well-formed (and
                    // adjacent tokens never fuse across the removed clause).
                    trimTrailingWhitespace(result);
                    int next = skipWhitespace(ddl, clauseEnd);
                    if (result.length() > 0 && next < n) {
                        result.append(' ');
                    }
                    i = next;
                }
            }
        }
        return result.toString();
    }

    /**
     * Returns the end index (exclusive) of an {@code IF [NOT] EXISTS} clause starting exactly at
     * {@code start} with word boundaries on both sides, or {@code -1} when no such clause starts
     * there.
     */
    private static int ifExistsClauseEnd(String ddl, int start) {
        int n = ddl.length();
        if (start > 0 && isIdentifierPart(ddl.charAt(start - 1))) {
            return -1; // e.g. "XIF EXISTS": not a standalone IF
        }
        if (!ddl.regionMatches(true, start, "IF", 0, 2)) {
            return -1;
        }
        if (start + 2 < n && isIdentifierPart(ddl.charAt(start + 2))) {
            return -1; // e.g. "IFX": not the keyword IF
        }
        int p = skipWhitespace(ddl, start + 2);
        if (matchesWord(ddl, p, "NOT")) {
            int afterNot = skipWhitespace(ddl, p + 3);
            if (matchesWord(ddl, afterNot, "EXISTS")) {
                return endOfWord(ddl, afterNot + 6);
            }
            return -1; // "IF NOT <anything but EXISTS>": not an idempotence clause
        }
        if (matchesWord(ddl, p, "EXISTS")) {
            return endOfWord(ddl, p + 6);
        }
        return -1;
    }

    /** Returns whether the word {@code word} appears case-insensitively at {@code pos}. */
    private static boolean matchesWord(String ddl, int pos, String word) {
        int n = ddl.length();
        if (pos < 0 || pos + word.length() > n) {
            return false;
        }
        if (!ddl.regionMatches(true, pos, word, 0, word.length())) {
            return false;
        }
        return endOfWord(ddl, pos + word.length()) >= 0;
    }

    /** Returns {@code pos} when it is a word boundary (end of input or a non-identifier char). */
    private static int endOfWord(String ddl, int pos) {
        return pos >= ddl.length() || !isIdentifierPart(ddl.charAt(pos)) ? pos : -1;
    }

    private static int skipWhitespace(String ddl, int pos) {
        int n = ddl.length();
        while (pos < n && isWhitespace(ddl.charAt(pos))) {
            pos++;
        }
        return pos;
    }

    private static void trimTrailingWhitespace(StringBuilder result) {
        while (result.length() > 0 && isWhitespace(result.charAt(result.length() - 1))) {
            result.setLength(result.length() - 1);
        }
    }

    private static boolean isWhitespace(char c) {
        return c == ' ' || c == '\t' || c == '\r' || c == '\n';
    }

    private static boolean isIdentifierPart(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$';
    }

    /**
     * Returns the end index of the literal/identifier opened by {@code quote} at {@code start},
     * honoring doubled-quote and backslash escapes; swallows to the end when unterminated.
     */
    private static int quotedEnd(String ddl, int start, char quote) {
        int i = start + 1;
        int n = ddl.length();
        while (i < n) {
            char c = ddl.charAt(i);
            if (c == '\\') {
                i += 2; // backslash-escaped character, never the closing quote
            } else if (c == quote) {
                if (i + 1 < n && ddl.charAt(i + 1) == quote) {
                    i += 2; // doubled quote is an escaped quote, e.g. 'it''s'
                } else {
                    return i + 1;
                }
            } else {
                i++;
            }
        }
        return n;
    }

    /**
     * Returns the end index (exclusive, newline kept for the outer pass) of a {@code --}/# comment.
     */
    private static int lineEnd(String ddl, int start) {
        int i = start;
        int n = ddl.length();
        while (i < n && ddl.charAt(i) != '\n') {
            i++;
        }
        return i;
    }

    /** Returns the end index (exclusive) of a {@code /* ... *}{@code /} block comment. */
    private static int blockCommentEnd(String ddl, int start) {
        int end = ddl.indexOf("*/", start + 2);
        return end < 0 ? ddl.length() : end + 2;
    }
}
