/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.kafkajson.source.convert;

import io.debezium.relational.Column;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit test for {@link MySqlValueConverters#parseIntegralText}, the lossless replacement of the
 * former {@code Math.round(Double.parseDouble(...))} String branch.
 */
class MySqlValueConvertersTest {

    private Column column() {
        return Column.editor()
                .name("c")
                .jdbcType(Types.BIGINT)
                .type("BIGINT")
                .length(20)
                .optional(true)
                .create();
    }

    @Test
    void testIntegralLiteralsParseExactly() {
        assertEquals(123L, MySqlValueConverters.parseIntegralText(column(), "123"));
        // surrounding whitespace is trimmed
        assertEquals(42L, MySqlValueConverters.parseIntegralText(column(), " 42 "));
        // explicit sign
        assertEquals(7L, MySqlValueConverters.parseIntegralText(column(), "+7"));
        // the boundaries parse exactly
        assertEquals(
                Long.MAX_VALUE,
                MySqlValueConverters.parseIntegralText(column(), "9223372036854775807"));
        assertEquals(
                Long.MIN_VALUE,
                MySqlValueConverters.parseIntegralText(column(), "-9223372036854775808"));
        // the regression anchor: a snowflake id beyond 2^53 must not pass through double,
        // whose grid at 2^58 would round it to the nearest 64-multiple (...664)
        assertEquals(
                332805230706017666L,
                MySqlValueConverters.parseIntegralText(column(), "332805230706017666"));
    }

    @Test
    void testDecimalLiteralsRoundThroughBigDecimal() {
        // MySQL permits decimal literals as default values of integer columns; they take the
        // BigDecimal path — decided by the character scan, without an exception round-trip
        assertEquals(4L, MySqlValueConverters.parseIntegralText(column(), "3.5"));
        assertEquals(3L, MySqlValueConverters.parseIntegralText(column(), "3.4"));
        assertEquals(0L, MySqlValueConverters.parseIntegralText(column(), "0.4"));
        // exponent notation is a decimal literal too
        assertEquals(1000L, MySqlValueConverters.parseIntegralText(column(), "1e3"));
        assertEquals(190L, MySqlValueConverters.parseIntegralText(column(), "1.9e2"));
        // HALF_UP rounds a .5 fraction away from zero: -2.5 -> -3 (Math.round gave -2; the
        // edge only concerns DDL default literals, never row data)
        assertEquals(-3L, MySqlValueConverters.parseIntegralText(column(), "-2.5"));
    }

    @Test
    void testOverflowFailsInsteadOfSaturating() {
        // Math.round(Double.parseDouble(...)) silently saturated these to Long.MAX_VALUE
        ConnectException overflow =
                assertThrows(
                        ConnectException.class,
                        () ->
                                MySqlValueConverters.parseIntegralText(
                                        column(), "9223372036854775808"));
        assertEquals(
                "Value '9223372036854775808' for column c overflows a signed long",
                overflow.getMessage());
        assertThrows(
                ConnectException.class,
                () -> MySqlValueConverters.parseIntegralText(column(), "-9223372036854775809"));
        // the full unsigned bigint range cannot ride the signed-long path either
        assertThrows(
                ConnectException.class,
                () -> MySqlValueConverters.parseIntegralText(column(), "18446744073709551615"));
    }

    @Test
    void testNonNumericTextFailsFast() {
        assertThrows(
                ConnectException.class,
                () -> MySqlValueConverters.parseIntegralText(column(), "abc"));
        assertThrows(
                ConnectException.class, () -> MySqlValueConverters.parseIntegralText(column(), ""));
        assertThrows(
                ConnectException.class,
                () -> MySqlValueConverters.parseIntegralText(column(), "+"));
        assertThrows(
                ConnectException.class,
                () -> MySqlValueConverters.parseIntegralText(column(), "12 3"));
    }

    @Test
    void testNullIsRejected() {
        // the callers reach the method only behind an instanceof String check or a null guard;
        // the requireNonNull keeps that contract explicit for future callers
        assertThrows(
                NullPointerException.class,
                () -> MySqlValueConverters.parseIntegralText(column(), null));
    }
}
