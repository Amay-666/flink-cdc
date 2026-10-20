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

package org.apache.flink.cdc.connectors.kafkajson.source.type;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Parses the label list of a MySQL {@code ENUM}/{@code SET} type expression. */
public final class MySqlEnumSetLabels {

    /**
     * A quoted label; a doubled quote inside is the SQL escape ({@code 'it''s'}), and the label may
     * itself contain commas and quotes — a plain {@code split(",")} would break those.
     */
    private static final Pattern QUOTED_LABEL = Pattern.compile("'((?:[^']|'')*)'");

    private MySqlEnumSetLabels() {}

    /**
     * Parses the label list of a full type expression like {@code enum('a','b','c')} or {@code
     * set('x','y')}. Returns {@code null} for any other (or malformed) expression.
     */
    public static List<String> parse(String typeExpression) {
        if (typeExpression == null) {
            return null;
        }
        String lower = typeExpression.toLowerCase(Locale.ROOT).trim();
        if (!lower.startsWith("enum(") && !lower.startsWith("set(")) {
            return null;
        }
        int open = typeExpression.indexOf('(');
        int close = typeExpression.lastIndexOf(')');
        if (open < 0 || close <= open + 1) {
            return null;
        }
        String params = typeExpression.substring(open + 1, close);
        List<String> values = new ArrayList<>();
        Matcher matcher = QUOTED_LABEL.matcher(params);
        while (matcher.find()) {
            values.add(matcher.group(1).replace("''", "'"));
        }
        return values;
    }
}
