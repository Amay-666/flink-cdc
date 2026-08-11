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

package org.apache.flink.cdc.connectors.canal.source.convert;

import org.apache.flink.cdc.connectors.canal.source.utils.CanalTableUtils;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.relational.Column;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Converts the {@code String} value delivered by canal into the Java object that the Debezium value
 * converter of the corresponding column expects.
 *
 * <p>canal always renders column values as {@code String}s, while the {@code TableSchemaBuilder}
 * converters (driven by {@link MySqlValueConverters}) accept different Java types per column type:
 *
 * <ul>
 *   <li>signed numerics, decimals, floats/doubles, strings, JSON, ENUM, SET and YEAR accept the
 *       plain {@code String} and are returned unchanged;
 *   <li>{@code TINYINT/SMALLINT/MEDIUMINT/INT UNSIGNED} require a {@link Number};
 *   <li>{@code BOOLEAN}/{@code BIT(1)} (whose converter has no {@code String} branch) require a
 *       {@link Number} or {@link Boolean};
 *   <li>{@code DATE} requires a {@link java.sql.Date}, {@code TIME} a {@link java.time.Duration}
 *       ({@link MySqlValueConverters#stringToDuration}), {@code DATETIME} a {@link Timestamp} and
 *       MySQL {@code TIMESTAMP} an {@link OffsetDateTime};
 *   <li>{@code BIT(n>1)} and the binary types require a {@code byte[]} — canal base64-encodes
 *       binary columns.
 * </ul>
 */
public class CanalValueConverter {

    private static final long serialVersionUID = 1L;

    /** Matches MySQL zero dates ({@code 0000-00-00[ 00:00:00[.000000]]}). */
    private static final Pattern ZERO_DATE =
            Pattern.compile("^0000-00-00(\\s.*)?$");

    private final ZoneId serverZoneId;
    private final DateTimeFormatter fractionFormatter;

    public CanalValueConverter(ZoneId serverZoneId) {
        this.serverZoneId = serverZoneId;
        // MySQL renders fractions with 1..6 digits; ISO_LOCAL_DATE_TIME accepts an optional
        // variable-length fraction, so reuse it after replacing the space separator.
        this.fractionFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    }

    /**
     * Converts a single canal {@code String} value for the given column.
     *
     * @return {@code null} when the value is {@code null} or a zero date
     */
    public Object convert(Column column, String value) {
        if (value == null) {
            return null;
        }
        String typeName = column.typeName().toUpperCase(Locale.ROOT);
        // YEAR is mapped to jdbcType DATE but must NOT be parsed as a date:
        // MySqlValueConverters.convertYearToInt accepts the plain String
        if ("YEAR".equals(typeName)) {
            return value;
        }
        switch (column.jdbcType()) {
            case java.sql.Types.BOOLEAN: // BOOL / BOOLEAN
                return toNumber(value, column);
            case java.sql.Types.BIT:
                return toBit(column, value);
            case java.sql.Types.DATE:
                return toDate(column, value);
            case java.sql.Types.TIME:
                return MySqlValueConverters.stringToDuration(value);
            case java.sql.Types.TIMESTAMP: // MySQL DATETIME
                return toTimestamp(column, value);
            case java.sql.Types.TIMESTAMP_WITH_TIMEZONE: // MySQL TIMESTAMP
                return toTimestampWithZone(column, value);
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.BLOB:
                return decodeBase64(value);
            default:
                break;
        }
        // UNSIGNED integer converters require a Number input
        if (CanalTableUtils.isUnsigned(typeName)) {
            if (typeName.startsWith("BIGINT")) {
                // convertBigInt / convertUnsignedBigint both accept a String
                return value;
            }
            return toNumber(value, column);
        }
        // All remaining types (signed numerics, decimal, float/double, char/text, JSON, ENUM, SET,
        // YEAR) accept the plain String
        return value;
    }

    private Object toBit(Column column, String value) {
        if (column.length() > 1) {
            // canal renders BIT(n>1) either as an integer string (e.g. "9") or as a binary digit
            // string (e.g. "1001"); convert both to a big-endian byte array
            return toBitBytes(column.length(), value);
        }
        // BIT(1): converter expects a Boolean/Short/Integer/Long
        return Short.valueOf(value);
    }

    private Object toNumber(String value, Column column) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new FlinkRuntimeException(
                    "Failed to convert canal value '" + value + "' for column " + column.name(), e);
        }
    }

    private Date toDate(Column column, String value) {
        if (ZERO_DATE.matcher(value).find()) {
            return null;
        }
        try {
            return Date.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new FlinkRuntimeException(
                    "Failed to convert canal DATE value '" + value + "' for column " + column.name(),
                    e);
        }
    }

    private Timestamp toTimestamp(Column column, String value) {
        if (ZERO_DATE.matcher(value).find()) {
            return null;
        }
        try {
            // Timestamp.valueOf handles "yyyy-[m]m-[d]d hh:mm:ss[.fraction]"
            return Timestamp.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new FlinkRuntimeException(
                    "Failed to convert canal DATETIME value '"
                            + value
                            + "' for column "
                            + column.name(),
                    e);
        }
    }

    private OffsetDateTime toTimestampWithZone(Column column, String value) {
        if (ZERO_DATE.matcher(value).find()) {
            return null;
        }
        try {
            LocalDateTime local = LocalDateTime.parse(value.replace(' ', 'T'), fractionFormatter);
            return OffsetDateTime.of(local, serverZoneId.getRules().getOffset(local));
        } catch (DateTimeParseException e) {
            throw new FlinkRuntimeException(
                    "Failed to convert canal TIMESTAMP value '"
                            + value
                            + "' for column "
                            + column.name(),
                    e);
        }
    }

    private byte[] decodeBase64(String value) {
        try {
            return Base64.getDecoder().decode(value.getBytes(StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) {
            throw new FlinkRuntimeException("Failed to base64-decode canal binary value: " + value, e);
        }
    }

    private static byte[] toBitBytes(int numBits, String value) {
        long longValue;
        if (value.matches("[01]+")) {
            longValue = Long.parseLong(value, 2);
        } else {
            longValue = Long.parseLong(value);
        }
        int numBytes = numBits / 8 + (numBits % 8 == 0 ? 0 : 1);
        byte[] bytes = new byte[numBytes];
        for (int i = 0; i < numBytes; i++) {
            bytes[numBytes - 1 - i] = (byte) (longValue >> (8 * i));
        }
        return bytes;
    }
}
