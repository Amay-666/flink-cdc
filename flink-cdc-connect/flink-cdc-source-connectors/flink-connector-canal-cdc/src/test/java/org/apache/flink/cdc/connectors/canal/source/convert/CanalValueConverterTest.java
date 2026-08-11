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

import io.debezium.relational.Column;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Unit test for {@link CanalValueConverter}. */
class CanalValueConverterTest {

    private final CanalValueConverter converter = new CanalValueConverter(ZoneId.of("UTC"));

    private Column column(int jdbcType, String typeName, int length) {
        return Column.editor()
                .name("c")
                .jdbcType(jdbcType)
                .type(typeName)
                .length(length)
                .optional(true)
                .create();
    }

    @Test
    void testNullReturnsNull() {
        assertNull(converter.convert(column(Types.INTEGER, "INT", 11), null));
    }

    @Test
    void testSignedIntegersPassThrough() {
        // signed numerics accept the plain String
        assertEquals("123", converter.convert(column(Types.INTEGER, "INT", 11), "123"));
        assertEquals("-1", converter.convert(column(Types.BIGINT, "BIGINT", 20), "-1"));
        assertEquals("3.14", converter.convert(column(Types.DECIMAL, "DECIMAL", 10), "3.14"));
        assertEquals("abc", converter.convert(column(Types.VARCHAR, "VARCHAR", 255), "abc"));
        assertEquals("{\"a\":1}", converter.convert(column(Types.LONGVARCHAR, "JSON", 65535), "{\"a\":1}"));
    }

    @Test
    void testUnsignedIntegersConvertedToNumber() {
        assertEquals(255L, converter.convert(column(Types.TINYINT, "TINYINT UNSIGNED", 3), "255"));
        assertEquals(65535L, converter.convert(column(Types.SMALLINT, "SMALLINT UNSIGNED", 5), "65535"));
        assertEquals(4294967295L, converter.convert(column(Types.INTEGER, "INT UNSIGNED", 10), "4294967295"));
        // BIGINT UNSIGNED passes through as String: both LONG and PRECISE converters accept it
        assertEquals(
                "18446744073709551615",
                converter.convert(column(Types.BIGINT, "BIGINT UNSIGNED", 20), "18446744073709551615"));
    }

    @Test
    void testBooleanAndBit() {
        assertEquals(1L, converter.convert(column(Types.BOOLEAN, "BOOLEAN", 1), "1"));
        assertEquals(0L, converter.convert(column(Types.BOOLEAN, "BOOLEAN", 1), "0"));
        // BIT(1) -> Short so that convertBit can render it as Boolean
        assertEquals((short) 1, converter.convert(column(Types.BIT, "BIT", 1), "1"));
        // BIT(16) -> big-endian byte array
        assertArrayEquals(
                new byte[] {2, 1},
                (byte[]) converter.convert(column(Types.BIT, "BIT", 16), "513"));
        // binary-digit representation of the same value
        assertArrayEquals(
                new byte[] {2, 1},
                (byte[]) converter.convert(column(Types.BIT, "BIT", 16), "1000000001"));
    }

    @Test
    void testDate() {
        assertEquals(java.sql.Date.valueOf("2020-08-13"), converter.convert(column(Types.DATE, "DATE", 10), "2020-08-13"));
    }

    @Test
    void testZeroDatesBecomeNull() {
        assertNull(converter.convert(column(Types.DATE, "DATE", 10), "0000-00-00"));
        assertNull(converter.convert(column(Types.TIMESTAMP, "DATETIME", 6), "0000-00-00 00:00:00"));
        assertNull(converter.convert(column(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP", 6), "0000-00-00 00:00:00"));
    }

    @Test
    void testTime() {
        assertEquals(
                Duration.ofHours(15).plusMinutes(3).plusSeconds(2),
                converter.convert(column(Types.TIME, "TIME", 0), "15:03:02"));
        assertEquals(
                Duration.ofHours(-1).minusMinutes(30),
                converter.convert(column(Types.TIME, "TIME", 0), "-01:30:00"));
    }

    @Test
    void testDatetime() {
        assertEquals(
                java.sql.Timestamp.valueOf("2020-08-13 15:03:02"),
                converter.convert(column(Types.TIMESTAMP, "DATETIME", 6), "2020-08-13 15:03:02"));
        assertEquals(
                java.sql.Timestamp.valueOf("2020-08-13 15:03:02.123456"),
                converter.convert(column(Types.TIMESTAMP, "DATETIME", 6), "2020-08-13 15:03:02.123456"));
    }

    @Test
    void testTimestampWithTimeZone() {
        // MySQL TIMESTAMP is interpreted as local time in the server time zone (UTC here)
        OffsetDateTime expected =
                OffsetDateTime.of(LocalDateTime.parse("2020-08-13T15:03:02"), ZoneOffset.UTC);
        assertEquals(
                expected,
                converter.convert(
                        column(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP", 6), "2020-08-13 15:03:02"));
    }

    @Test
    void testTimestampWithFractionAndNonUtcZone() {
        CanalValueConverter shanghai = new CanalValueConverter(ZoneId.of("Asia/Shanghai"));
        OffsetDateTime expected =
                OffsetDateTime.of(
                        LocalDateTime.parse("2020-08-13T15:03:02.5"), ZoneOffset.ofHours(8));
        assertEquals(
                expected,
                shanghai.convert(
                        column(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP", 3),
                        "2020-08-13 15:03:02.5"));
    }

    @Test
    void testBinaryColumnsBase64Decoded() {
        // canal base64-encodes binary columns
        byte[] expected = new byte[] {1, 2, 3};
        String base64 = java.util.Base64.getEncoder().encodeToString(expected);
        assertArrayEquals(
                expected, (byte[]) converter.convert(column(Types.BINARY, "BINARY", 4), base64));
        assertArrayEquals(
                expected, (byte[]) converter.convert(column(Types.BLOB, "BLOB", 65535), base64));
    }

    @Test
    void testYearEnumSetPassThrough() {
        assertEquals("2020", converter.convert(column(Types.DATE, "YEAR", 4), "2020"));
        assertEquals("red", converter.convert(column(Types.VARCHAR, "ENUM", 255), "red"));
        assertEquals("a,b", converter.convert(column(Types.VARCHAR, "SET", 255), "a,b"));
    }
}
