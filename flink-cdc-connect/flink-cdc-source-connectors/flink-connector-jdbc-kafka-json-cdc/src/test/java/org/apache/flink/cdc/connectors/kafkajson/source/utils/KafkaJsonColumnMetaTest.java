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

package org.apache.flink.cdc.connectors.kafkajson.source.utils;

import org.apache.flink.cdc.common.types.DataType;

import io.debezium.relational.Column;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for the canonical MySQL/TiDB &rarr; CDC common {@link DataType} mapping of {@link
 * KafkaJsonColumnMeta}. The pipeline layer consumes {@link KafkaJsonColumnMeta#toCdcDataType}
 * directly (see the {@code KafkaJsonEventDeserializer} and {@code KafkaJsonSchemaUtils}), so the
 * clamping rules shared with the SQL layer are exercised here too.
 */
class KafkaJsonColumnMetaTest {

    private Column column(String typeName, int length) {
        return Column.editor().name("c").type(typeName).length(length).optional(true).create();
    }

    private String toCdc(String typeName, int length) {
        return KafkaJsonColumnMeta.fromColumn(column(typeName, length))
                .toCdcDataType(true)
                .toString();
    }

    @Test
    void testIntegerTypes() {
        assertEquals("INT", toCdc("INT", 11));
        assertEquals("INT", toCdc("INTEGER", 11));
        assertEquals("INT", toCdc("MEDIUMINT", 9));
        assertEquals("BIGINT", toCdc("BIGINT", 20));
        assertEquals("SMALLINT", toCdc("SMALLINT", 6));
        assertEquals("TINYINT", toCdc("TINYINT", 4));
        assertEquals("INT", toCdc("YEAR", 4));
        // tinyint(1) is a number like any other tinyint: mapping it to BOOLEAN collapsed 2/3
        // to 1, because the record layer keeps the number while the sink read the SMALLINT
        // slot through the BOOLEAN getter's non-zero test. Only bit(1) stays BOOLEAN.
        assertEquals("TINYINT", toCdc("TINYINT", 1));
        assertEquals("SMALLINT", toCdc("TINYINT UNSIGNED", 1));
    }

    @Test
    void testUnsignedWidening() {
        assertEquals("BIGINT", toCdc("INT UNSIGNED", 11));
        assertEquals("DECIMAL(20, 0)", toCdc("BIGINT UNSIGNED", 20));
        assertEquals("INT", toCdc("SMALLINT UNSIGNED", 6));
        assertEquals("BIGINT", toCdc("INT UNSIGNED ZEROFILL", 11));
    }

    @Test
    void testBit() {
        assertEquals("BOOLEAN", toCdc("BIT", 1));
        assertEquals("BINARY(2)", toCdc("BIT", 16));
    }

    @Test
    void testStringTypes() {
        assertEquals("VARCHAR(255)", toCdc("VARCHAR", 255));
        assertEquals("CHAR(10)", toCdc("CHAR", 10));
        assertEquals("STRING", toCdc("TEXT", 65535));
        assertEquals("STRING", toCdc("LONGTEXT", 2147483647));
        assertEquals("STRING", toCdc("JSON", 2147483647));
        // A bare `char` declares no (M) and MySQL's implicit default is 1 — the JDBC path
        // reports exactly that length, so CHAR keeps the 1 fill. VARCHAR has no implicit
        // default (the (M) is mandatory), so a zero/absent length can only be a quirk: the
        // fill is the type's own maximum instead of a one-character clamp.
        assertEquals("CHAR(1)", toCdc("CHAR", 0));
        assertEquals("VARCHAR(65535)", toCdc("VARCHAR", 0));
    }

    @Test
    void testTemporalTypes() {
        // the message path (KafkaJsonTableUtils#buildColumn) parses the DDL text, so the length is
        // the fractional-seconds precision already
        assertEquals("TIMESTAMP(3)", toCdc("DATETIME", 3));
        assertEquals("TIMESTAMP(6)", toCdc("DATETIME", 6));
        assertEquals("TIMESTAMP_LTZ(3)", toCdc("TIMESTAMP", 3));
        assertEquals("TIME(3)", toCdc("TIME", 3));
        assertEquals("TIME(0)", toCdc("TIME", 0));
        assertEquals("DATE", toCdc("DATE", 10));
        // the JDBC snapshot path reports the display width on MySQL/TiDB (DATETIME(6) -> 26,
        // DATETIME(3) -> 23, DATETIME -> 19, TIME(6) -> 15, TIME(3) -> 12, TIME -> 8); the FSP is
        // recovered from it, so a DATETIME(0) stays TIMESTAMP(0) instead of being inflated
        assertEquals("TIMESTAMP(6)", toCdc("DATETIME", 26));
        assertEquals("TIMESTAMP(3)", toCdc("DATETIME", 23));
        assertEquals("TIMESTAMP(0)", toCdc("DATETIME", 19));
        assertEquals("TIMESTAMP_LTZ(6)", toCdc("TIMESTAMP", 26));
        assertEquals("TIMESTAMP_LTZ(3)", toCdc("TIMESTAMP", 23));
        assertEquals("TIMESTAMP_LTZ(0)", toCdc("TIMESTAMP", 19));
        assertEquals("TIME(6)", toCdc("TIME", 15));
        assertEquals("TIME(3)", toCdc("TIME", 12));
        assertEquals("TIME(0)", toCdc("TIME", 8));
    }

    @Test
    void testDecimal() {
        Column decimal =
                Column.editor()
                        .name("c")
                        .type("DECIMAL")
                        .length(10)
                        .scale(2)
                        .optional(true)
                        .create();
        assertEquals(
                "DECIMAL(10, 2)",
                KafkaJsonColumnMeta.fromColumn(decimal).toCdcDataType(true).toString());

        // scale above the precision is clamped to the precision
        Column invalidScale =
                Column.editor()
                        .name("c")
                        .type("DECIMAL")
                        .length(10)
                        .scale(20)
                        .optional(true)
                        .create();
        assertEquals(
                "DECIMAL(10, 10)",
                KafkaJsonColumnMeta.fromColumn(invalidScale).toCdcDataType(true).toString());

        // a DECIMAL without a usable precision falls back to DECIMAL(38, 18)
        Column noPrecision =
                Column.editor()
                        .name("c")
                        .type("DECIMAL")
                        .length(0)
                        .scale(0)
                        .optional(true)
                        .create();
        assertEquals(
                "DECIMAL(38, 18)",
                KafkaJsonColumnMeta.fromColumn(noPrecision).toCdcDataType(true).toString());

        // a precision within the CDC max keeps its real precision/scale
        Column wideWithinMax =
                Column.editor()
                        .name("c")
                        .type("DECIMAL")
                        .length(30)
                        .scale(10)
                        .optional(true)
                        .create();
        assertEquals(
                "DECIMAL(30, 10)",
                KafkaJsonColumnMeta.fromColumn(wideWithinMax).toCdcDataType(true).toString());

        // a precision above the CDC max (38) is carried as STRING, matching the released row-type
        // inference (DebeziumSchemaDataTypeInference) that emits STRING for such Connect Decimals
        Column overMax =
                Column.editor()
                        .name("c")
                        .type("DECIMAL")
                        .length(65)
                        .scale(30)
                        .optional(true)
                        .create();
        assertEquals(
                "STRING", KafkaJsonColumnMeta.fromColumn(overMax).toCdcDataType(true).toString());
        assertEquals("STRING", toCdc("DECIMAL UNSIGNED", 65));
    }

    @Test
    void testBinaryTypes() {
        assertEquals("VARBINARY(255)", toCdc("VARBINARY", 255));
        assertEquals("BYTES", toCdc("BLOB", 65535));
        assertEquals("BYTES", toCdc("LONGBLOB", 2147483647));
    }

    @Test
    void testSetAsString() {
        // the official Doris MySQL mapping keeps SET as the STRING of delimited labels; an
        // ARRAY<STRING> could not even cross the pipeline's binary record format
        assertEquals("STRING", toCdc("SET", 255));
    }

    @Test
    void testEnumAndSetTypeNamesCarryingLabels() {
        // MySQL Connector/J 8.0.x reports the bare TYPE_NAME ("ENUM" — verified live against
        // a real MySQL), but readTableColumn stores the TYPE_NAME verbatim and other
        // driver forms embed the label list ("enum('a','b','c')") or the arguments
        // ("int(10) unsigned") in it. Every shape must resolve to the same base name and map
        // ENUM/SET to STRING; before the base name was split at the first '(' too, the
        // label-carrying form reached no mapping case at all.
        assertEquals("STRING", toCdc("enum('a','b','c')", 1));
        assertEquals("STRING", toCdc("set('x','y')", 1));
        assertEquals("STRING", toCdc("ENUM", 0));
        assertEquals("STRING", toCdc("SET", 0));
        // parenthesized TYPE_NAMEs stay intact after the '(' split too
        assertEquals("BIGINT", toCdc("int(10) unsigned", 10));
        assertEquals("DECIMAL(10, 0)", toCdc("decimal(10,2)", 10));
    }

    @Test
    void testNullableAndNotNull() {
        DataType optional = KafkaJsonColumnMeta.fromColumn(column("INT", 11)).toCdcDataType(true);
        assertTrue(optional.isNullable());

        DataType notNull = KafkaJsonColumnMeta.fromColumn(column("INT", 11)).toCdcDataType(false);
        assertFalse(notNull.isNullable());
    }

    @Test
    void testUnsupportedType() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> KafkaJsonColumnMeta.fromColumn(column("GEOGRAPHY", 0)).toCdcDataType(true));
    }
}
