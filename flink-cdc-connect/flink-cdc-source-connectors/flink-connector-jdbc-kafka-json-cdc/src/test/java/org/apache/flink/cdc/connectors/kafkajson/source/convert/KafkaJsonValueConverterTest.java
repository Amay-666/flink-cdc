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

package org.apache.flink.cdc.connectors.kafkajson.source.convert;

import org.apache.flink.util.FlinkRuntimeException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.debezium.relational.Column;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit test for {@link KafkaJsonValueConverter}. */
class KafkaJsonValueConverterTest {

    private final KafkaJsonValueConverter converter = new KafkaJsonValueConverter(ZoneId.of("UTC"));

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
        assertEquals(
                "{\"a\":1}",
                converter.convert(column(Types.LONGVARCHAR, "JSON", 65535), "{\"a\":1}"));
    }

    @Test
    void testUnsignedIntegersConvertedToNumber() {
        assertEquals(255L, converter.convert(column(Types.TINYINT, "TINYINT UNSIGNED", 3), "255"));
        assertEquals(
                65535L, converter.convert(column(Types.SMALLINT, "SMALLINT UNSIGNED", 5), "65535"));
        assertEquals(
                4294967295L,
                converter.convert(column(Types.INTEGER, "INT UNSIGNED", 10), "4294967295"));
        // BIGINT UNSIGNED passes through as String: both LONG and PRECISE converters accept it
        assertEquals(
                "18446744073709551615",
                converter.convert(
                        column(Types.BIGINT, "BIGINT UNSIGNED", 20), "18446744073709551615"));
    }

    @Test
    void testBooleanAndBit() {
        assertEquals(1L, converter.convert(column(Types.BOOLEAN, "BOOLEAN", 1), "1"));
        assertEquals(0L, converter.convert(column(Types.BOOLEAN, "BOOLEAN", 1), "0"));
        // BIT(1) -> Short so that convertBit can render it as Boolean
        assertEquals((short) 1, converter.convert(column(Types.BIT, "BIT", 1), "1"));
        // BIT(16) -> big-endian byte array
        assertArrayEquals(
                new byte[] {2, 1}, (byte[]) converter.convert(column(Types.BIT, "BIT", 16), "513"));
        // binary-digit representation of the same value
        assertArrayEquals(
                new byte[] {2, 1},
                (byte[]) converter.convert(column(Types.BIT, "BIT", 16), "1000000001"));
    }

    @Test
    void testDate() {
        assertEquals(
                java.sql.Date.valueOf("2020-08-13"),
                converter.convert(column(Types.DATE, "DATE", 10), "2020-08-13"));
    }

    @Test
    void testZeroDatesBecomeNull() {
        assertNull(converter.convert(column(Types.DATE, "DATE", 10), "0000-00-00"));
        assertNull(
                converter.convert(column(Types.TIMESTAMP, "DATETIME", 6), "0000-00-00 00:00:00"));
        assertNull(
                converter.convert(
                        column(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP", 6),
                        "0000-00-00 00:00:00"));
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
                converter.convert(
                        column(Types.TIMESTAMP, "DATETIME", 6), "2020-08-13 15:03:02.123456"));
    }

    @Test
    void testTimestampWithTimeZone() {
        // MySQL TIMESTAMP is interpreted as local time in the server time zone (UTC here)
        OffsetDateTime expected =
                OffsetDateTime.of(LocalDateTime.parse("2020-08-13T15:03:02"), ZoneOffset.UTC);
        assertEquals(
                expected,
                converter.convert(
                        column(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP", 6),
                        "2020-08-13 15:03:02"));
    }

    @Test
    void testTimestampWithFractionAndNonUtcZone() {
        KafkaJsonValueConverter shanghai = new KafkaJsonValueConverter(ZoneId.of("Asia/Shanghai"));
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

    @Test
    void testIntegerBoundariesPassThrough() {
        // signed integer extremes stay String: the Debezium converters parse them from String
        assertEquals(
                "2147483647", converter.convert(column(Types.INTEGER, "INT", 11), "2147483647"));
        assertEquals(
                "-2147483648", converter.convert(column(Types.INTEGER, "INT", 11), "-2147483648"));
        assertEquals(
                "9223372036854775807",
                converter.convert(column(Types.BIGINT, "BIGINT", 20), "9223372036854775807"));
        assertEquals(
                "-9223372036854775808",
                converter.convert(column(Types.BIGINT, "BIGINT", 20), "-9223372036854775808"));
    }

    @Test
    void testFloatAndDoublePassThrough() {
        assertEquals("1.5", converter.convert(column(Types.FLOAT, "FLOAT", 12), "1.5"));
        assertEquals("-1.5", converter.convert(column(Types.DOUBLE, "DOUBLE", 22), "-1.5"));
        // scientific notation is preserved verbatim (the Debezium converter parses it from String)
        assertEquals(
                "1.7976931348623157E308",
                converter.convert(column(Types.DOUBLE, "DOUBLE", 22), "1.7976931348623157E308"));
        // UNSIGNED float/double are not in the unsigned-integer list and pass through too
        assertEquals("1.5", converter.convert(column(Types.DOUBLE, "DOUBLE UNSIGNED", 22), "1.5"));
    }

    @Test
    void testDecimalPrecisionAndScalePassThrough() {
        assertEquals(
                "12345678901234567890.1234567890",
                converter.convert(
                        column(Types.DECIMAL, "DECIMAL", 38), "12345678901234567890.1234567890"));
        assertEquals(
                "0.0000000001",
                converter.convert(column(Types.DECIMAL, "DECIMAL", 30), "0.0000000001"));
        // DECIMAL UNSIGNED (fractional) must NOT go through the integer Number path
        assertEquals(
                "99.99", converter.convert(column(Types.DECIMAL, "DECIMAL UNSIGNED", 10), "99.99"));
    }

    @Test
    void testVarcharEdgeCases() {
        assertEquals("", converter.convert(column(Types.VARCHAR, "VARCHAR", 255), ""));
        assertEquals(
                "你好, wörld", converter.convert(column(Types.VARCHAR, "VARCHAR", 255), "你好, wörld"));
        assertEquals(
                "  padded  ",
                converter.convert(column(Types.VARCHAR, "VARCHAR", 255), "  padded  "));
    }

    @Test
    void testTimeWithFraction() {
        assertEquals(
                Duration.ofHours(15).plusMinutes(3).plusSeconds(2).plusNanos(123456000),
                converter.convert(column(Types.TIME, "TIME", 6), "15:03:02.123456"));
        // a negative time with a fraction negates every component
        assertEquals(
                Duration.ofHours(-1).minusMinutes(30).minusNanos(500000000),
                converter.convert(column(Types.TIME, "TIME", 1), "-01:30:00.5"));
    }

    @Test
    void testDateBoundaries() {
        assertEquals(
                java.sql.Date.valueOf("1000-01-01"),
                converter.convert(column(Types.DATE, "DATE", 10), "1000-01-01"));
        assertEquals(
                java.sql.Date.valueOf("9999-12-31"),
                converter.convert(column(Types.DATE, "DATE", 10), "9999-12-31"));
    }

    @Test
    void testDatetimePrecisionAndBoundaries() {
        assertEquals(
                java.sql.Timestamp.valueOf("1000-01-01 00:00:00"),
                converter.convert(column(Types.TIMESTAMP, "DATETIME", 6), "1000-01-01 00:00:00"));
        assertEquals(
                java.sql.Timestamp.valueOf("9999-12-31 23:59:59.999999"),
                converter.convert(
                        column(Types.TIMESTAMP, "DATETIME", 6), "9999-12-31 23:59:59.999999"));
    }

    @Test
    void testTimestampWithZoneLeapDayAndMaxPrecision() {
        OffsetDateTime expected =
                OffsetDateTime.of(
                        LocalDateTime.parse("2020-02-29T23:59:59.999999"), ZoneOffset.UTC);
        assertEquals(
                expected,
                converter.convert(
                        column(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP", 6),
                        "2020-02-29 23:59:59.999999"));
    }

    @Test
    void testConvertFromJsonNull() {
        // both an absent field (Java null) and an explicit JSON null bind as no value
        assertNull(converter.convertFromJson(column(Types.INTEGER, "INT", 11), null));
        assertNull(
                converter.convertFromJson(
                        column(Types.INTEGER, "INT", 11), JsonNodeFactory.instance.nullNode()));
    }

    @Test
    void testConvertFromJsonBoolean() {
        assertEquals(
                Boolean.TRUE,
                converter.convertFromJson(
                        column(Types.BOOLEAN, "BOOLEAN", 1),
                        JsonNodeFactory.instance.booleanNode(true)));
        assertEquals(
                Boolean.FALSE,
                converter.convertFromJson(
                        column(Types.BIT, "BIT", 1), JsonNodeFactory.instance.booleanNode(false)));
    }

    @Test
    void testConvertFromJsonNumericPassThrough() {
        // numeric values render back to their text form, which the canal converter parses
        assertEquals(
                "123",
                converter.convertFromJson(
                        column(Types.INTEGER, "INT", 11),
                        JsonNodeFactory.instance.numberNode(123)));
        assertEquals(
                "9223372036854775807",
                converter.convertFromJson(
                        column(Types.BIGINT, "BIGINT", 20),
                        JsonNodeFactory.instance.numberNode(Long.MAX_VALUE)));
        // a DECIMAL rendered as a float64 (decimal.handling.mode=double / TiCDC) parses back
        assertEquals(
                "3.14",
                converter.convertFromJson(
                        column(Types.DECIMAL, "DECIMAL", 10),
                        JsonNodeFactory.instance.numberNode(3.14d)));
        // a numeric BOOLEAN (0/1) goes through the Number branch
        assertEquals(
                1L,
                converter.convertFromJson(
                        column(Types.BOOLEAN, "BOOLEAN", 1),
                        JsonNodeFactory.instance.numberNode(1)));
    }

    @Test
    void testConvertFromJsonEpochTemporals() {
        // Debezium adaptive precision mode encodes temporals as epoch numbers
        long epochDay = LocalDate.of(2020, 8, 13).toEpochDay();
        assertEquals(
                java.sql.Date.valueOf("2020-08-13"),
                converter.convertFromJson(
                        column(Types.DATE, "DATE", 10),
                        JsonNodeFactory.instance.numberNode(epochDay)));

        Duration expectedTime = Duration.ofHours(15).plusMinutes(3).plusSeconds(2);
        assertEquals(
                expectedTime,
                converter.convertFromJson(
                        column(Types.TIME, "TIME", 0),
                        JsonNodeFactory.instance.numberNode(expectedTime.toMillis())));

        java.sql.Timestamp expectedDatetime = java.sql.Timestamp.valueOf("2020-08-13 15:03:02");
        assertEquals(
                expectedDatetime,
                converter.convertFromJson(
                        column(Types.TIMESTAMP, "DATETIME", 6),
                        JsonNodeFactory.instance.numberNode(expectedDatetime.getTime() * 1000L)));

        long expectedTimestampMillis =
                LocalDateTime.parse("2020-08-13T15:03:02").toInstant(ZoneOffset.UTC).toEpochMilli();
        assertEquals(
                OffsetDateTime.of(LocalDateTime.parse("2020-08-13T15:03:02"), ZoneOffset.UTC),
                converter.convertFromJson(
                        column(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP", 6),
                        JsonNodeFactory.instance.numberNode(expectedTimestampMillis)));
    }

    @Test
    void testConvertFromJsonTextualPassThrough() {
        // TiCDC (and Debezium with connect precision / string decimals) emits MySQL-formatted text
        assertEquals(
                java.sql.Date.valueOf("2020-08-13"),
                converter.convertFromJson(
                        column(Types.DATE, "DATE", 10),
                        JsonNodeFactory.instance.textNode("2020-08-13")));
        assertEquals(
                java.sql.Timestamp.valueOf("2020-08-13 15:03:02.5"),
                converter.convertFromJson(
                        column(Types.TIMESTAMP, "DATETIME", 6),
                        JsonNodeFactory.instance.textNode("2020-08-13 15:03:02.5")));
        assertEquals(
                Duration.ofHours(15).plusMinutes(3).plusSeconds(2),
                converter.convertFromJson(
                        column(Types.TIME, "TIME", 0),
                        JsonNodeFactory.instance.textNode("15:03:02")));
        assertEquals(
                "1234567890.123",
                converter.convertFromJson(
                        column(Types.DECIMAL, "DECIMAL", 30),
                        JsonNodeFactory.instance.textNode("1234567890.123")));
        assertEquals(
                "hello",
                converter.convertFromJson(
                        column(Types.VARCHAR, "VARCHAR", 255),
                        JsonNodeFactory.instance.textNode("hello")));
    }

    @Test
    void testConvertFromJsonBinaryAndJsonColumn() {
        byte[] expected = new byte[] {1, 2, 3};
        assertArrayEquals(
                expected,
                (byte[])
                        converter.convertFromJson(
                                column(Types.BINARY, "BINARY", 4),
                                JsonNodeFactory.instance.textNode(
                                        java.util.Base64.getEncoder().encodeToString(expected))));
        // a JSON column may arrive as nested JSON instead of a JSON text
        JsonNode objectNode = JsonNodeFactory.instance.objectNode().put("a", 1).put("b", "x");
        assertEquals(
                "{\"a\":1,\"b\":\"x\"}",
                converter.convertFromJson(column(Types.LONGVARCHAR, "JSON", 65535), objectNode));
    }

    @Test
    void testConvertFromJsonYearIsNotEpochDate() {
        // YEAR is jdbcType DATE but a numeric YEAR is a plain year, not epoch days
        assertEquals(
                "2020",
                converter.convertFromJson(
                        column(Types.DATE, "YEAR", 4), JsonNodeFactory.instance.numberNode(2020)));
    }

    @Test
    void testInvalidValuesThrow() {
        // a non-numeric unsigned integer
        assertThrows(
                FlinkRuntimeException.class,
                () -> converter.convert(column(Types.INTEGER, "INT UNSIGNED", 10), "abc"));
        // a malformed date
        assertThrows(
                FlinkRuntimeException.class,
                () -> converter.convert(column(Types.DATE, "DATE", 10), "2020-13-01"));
        // a malformed time (stringToDuration throws a plain RuntimeException)
        assertThrows(
                RuntimeException.class,
                () -> converter.convert(column(Types.TIME, "TIME", 0), "abc"));
        // a malformed timestamp
        assertThrows(
                FlinkRuntimeException.class,
                () ->
                        converter.convert(
                                column(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP", 6),
                                "2020-13-40 00:00:00"));
    }
}
