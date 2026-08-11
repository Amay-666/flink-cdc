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

import org.apache.flink.table.types.DataType;

import io.debezium.relational.Column;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test for {@link KafkaJsonTypeUtils}. */
class KafkaJsonTypeUtilsTest {

    private Column column(String typeName, int length) {
        return Column.editor().name("c").type(typeName).length(length).optional(true).create();
    }

    @Test
    void testIntegerTypes() {
        assertEquals("INT", KafkaJsonTypeUtils.fromDbzColumn(column("INT", 11)).toString());
        assertEquals("INT", KafkaJsonTypeUtils.fromDbzColumn(column("INTEGER", 11)).toString());
        assertEquals("INT", KafkaJsonTypeUtils.fromDbzColumn(column("MEDIUMINT", 9)).toString());
        assertEquals("BIGINT", KafkaJsonTypeUtils.fromDbzColumn(column("BIGINT", 20)).toString());
        assertEquals("SMALLINT", KafkaJsonTypeUtils.fromDbzColumn(column("SMALLINT", 6)).toString());
        assertEquals("TINYINT", KafkaJsonTypeUtils.fromDbzColumn(column("TINYINT", 4)).toString());
    }

    @Test
    void testUnsignedWidening() {
        // unsigned int needs one more byte, widen to BIGINT
        assertEquals("BIGINT", KafkaJsonTypeUtils.fromDbzColumn(column("INT UNSIGNED", 11)).toString());
        // unsigned bigint may not fit in a signed BIGINT, widen to DECIMAL(20,0)
        assertEquals(
                "DECIMAL(20, 0)",
                KafkaJsonTypeUtils.fromDbzColumn(column("BIGINT UNSIGNED", 20)).toString());
        assertEquals(
                "INT", KafkaJsonTypeUtils.fromDbzColumn(column("SMALLINT UNSIGNED", 6)).toString());
        // zerofill implies unsigned
        assertEquals(
                "BIGINT",
                KafkaJsonTypeUtils.fromDbzColumn(column("INT UNSIGNED ZEROFILL", 11)).toString());
    }

    @Test
    void testTinyIntBoolean() {
        // tinyint(1) represents boolean
        assertEquals("BOOLEAN", KafkaJsonTypeUtils.fromDbzColumn(column("TINYINT", 1)).toString());
    }

    @Test
    void testBit() {
        assertEquals("BOOLEAN", KafkaJsonTypeUtils.fromDbzColumn(column("BIT", 1)).toString());
        assertEquals("BINARY(2)", KafkaJsonTypeUtils.fromDbzColumn(column("BIT", 16)).toString());
    }

    @Test
    void testStringTypes() {
        assertEquals(
                "VARCHAR(255)", KafkaJsonTypeUtils.fromDbzColumn(column("VARCHAR", 255)).toString());
        assertEquals("CHAR(10)", KafkaJsonTypeUtils.fromDbzColumn(column("CHAR", 10)).toString());
        assertEquals("STRING", KafkaJsonTypeUtils.fromDbzColumn(column("TEXT", 65535)).toString());
        assertEquals("STRING", KafkaJsonTypeUtils.fromDbzColumn(column("LONGTEXT", 2147483647)).toString());
        assertEquals("STRING", KafkaJsonTypeUtils.fromDbzColumn(column("JSON", 2147483647)).toString());
    }

    @Test
    void testTemporalTypes() {
        assertEquals(
                "TIMESTAMP(6)", KafkaJsonTypeUtils.fromDbzColumn(column("DATETIME", 6)).toString());
        assertEquals(
                "TIMESTAMP_LTZ(3)",
                KafkaJsonTypeUtils.fromDbzColumn(column("TIMESTAMP", 3)).toString());
        assertEquals("DATE", KafkaJsonTypeUtils.fromDbzColumn(column("DATE", 10)).toString());
        assertEquals("TIME(0)", KafkaJsonTypeUtils.fromDbzColumn(column("TIME", 0)).toString());
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
        assertEquals("DECIMAL(10, 2)", KafkaJsonTypeUtils.fromDbzColumn(decimal).toString());
    }

    @Test
    void testBinaryTypes() {
        assertEquals(
                "VARBINARY(255)",
                KafkaJsonTypeUtils.fromDbzColumn(column("VARBINARY", 255)).toString());
        assertEquals("BYTES", KafkaJsonTypeUtils.fromDbzColumn(column("BLOB", 65535)).toString());
        assertEquals("BYTES", KafkaJsonTypeUtils.fromDbzColumn(column("LONGBLOB", 2147483647)).toString());
    }

    @Test
    void testOptionalAndNotNull() {
        DataType optional = KafkaJsonTypeUtils.fromDbzColumn(column("INT", 11));
        assertTrue(optional.getLogicalType().isNullable());

        Column notNullColumn =
                Column.editor().name("c").type("INT").length(11).optional(false).create();
        DataType notNull = KafkaJsonTypeUtils.fromDbzColumn(notNullColumn);
        assertFalse(notNull.getLogicalType().isNullable());
    }

    @Test
    void testUnsupportedType() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> KafkaJsonTypeUtils.fromDbzColumn(column("GEOGRAPHY", 0)));
    }
}
