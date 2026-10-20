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

import io.debezium.relational.Column;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit test for {@link KafkaJsonTableUtils}: the MySQL type expression to Debezium {@link Column}
 * mapping shared by the DDL parsers.
 */
class KafkaJsonTableUtilsTest {

    private static Column column(String name, String mysqlType) {
        return KafkaJsonTableUtils.buildColumn(name, mysqlType, 1).create();
    }

    @Test
    void testBasicTypes() {
        Column id = column("id", "bigint(20)");
        assertEquals(Types.BIGINT, id.jdbcType());
        assertEquals("BIGINT", id.typeName());
        assertEquals(20, id.length());

        Column amount = column("amount", "decimal(10,2)");
        assertEquals(Types.DECIMAL, amount.jdbcType());
        assertEquals(10, amount.length());
        assertEquals(Integer.valueOf(2), amount.scale().get());

        Column createdAt = column("created_at", "datetime");
        assertEquals(Types.TIMESTAMP, createdAt.jdbcType());
        assertEquals("DATETIME", createdAt.typeName());

        Column updatedAt = column("updated_at", "timestamp(3)");
        assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, updatedAt.jdbcType());
        assertEquals("TIMESTAMP", updatedAt.typeName());
        assertEquals(3, updatedAt.length());

        Column unsigned = column("unsigned_col", "int(10) unsigned");
        assertEquals("INT UNSIGNED", unsigned.typeName());
        assertEquals(Types.INTEGER, unsigned.jdbcType());

        Column tags = column("tags", "set('a','b')");
        assertEquals("SET", tags.typeName());
        assertEquals(Arrays.asList("a", "b"), tags.enumValues());
    }

    @Test
    void testEnumWithEscapedQuotes() {
        Column color = column("color", "enum('red','it''s')");
        assertEquals(Arrays.asList("red", "it's"), color.enumValues());
    }

    /**
     * The full MySQL type matrix: every type the connector claims to support, mapped through the
     * type-expression path the DDL parsers use. This is the authoritative mapping the DDL-parser
     * matrix tests must stay consistent with: geometry types are the only exclusion (they fail
     * fast, see {@link #testGeometryThrows()}).
     */
    @Test
    void testFullTypeMatrix() {
        Map<String, String> mysqlType = new LinkedHashMap<>();
        // integers, signed and unsigned
        mysqlType.put("c_tinyint", "tinyint(4)");
        mysqlType.put("c_tinyint_u", "tinyint(3) unsigned");
        mysqlType.put("c_smallint", "smallint(6)");
        mysqlType.put("c_smallint_u", "smallint unsigned");
        mysqlType.put("c_mediumint", "mediumint(9)");
        mysqlType.put("c_mediumint_u", "mediumint unsigned");
        mysqlType.put("c_int", "int(11)");
        mysqlType.put("c_int_u", "int(10) unsigned");
        mysqlType.put("c_bigint", "bigint(20)");
        mysqlType.put("c_bigint_u", "bigint(20) unsigned");
        // approximate numbers — TiCDC renders the display width ("float(12,0)")
        mysqlType.put("c_float", "float(12,0)");
        mysqlType.put("c_double", "double(22,0)");
        mysqlType.put("c_real", "real");
        // exact numbers
        mysqlType.put("c_decimal", "decimal(20,4)");
        mysqlType.put("c_decimal_nop", "decimal");
        // bit and boolean
        mysqlType.put("c_bit", "bit(8)");
        mysqlType.put("c_bool", "boolean");
        // date and time
        mysqlType.put("c_date", "date");
        mysqlType.put("c_time", "time");
        mysqlType.put("c_time_p", "time(3)");
        mysqlType.put("c_datetime", "datetime");
        mysqlType.put("c_datetime_p", "datetime(3)");
        mysqlType.put("c_datetime_p6", "datetime(6)");
        mysqlType.put("c_timestamp", "timestamp");
        mysqlType.put("c_year", "year");
        // composite types
        mysqlType.put("c_json", "json");
        mysqlType.put("c_enum", "enum('a','b','c')");
        mysqlType.put("c_set", "set('x','y')");
        // character strings
        mysqlType.put("c_char", "char(16)");
        mysqlType.put("c_varchar", "varchar(100)");
        mysqlType.put("c_tinytext", "tinytext");
        mysqlType.put("c_text", "text");
        mysqlType.put("c_mediumtext", "mediumtext");
        mysqlType.put("c_longtext", "longtext");
        // binary strings
        mysqlType.put("c_binary", "binary(8)");
        mysqlType.put("c_varbinary", "varbinary(64)");
        mysqlType.put("c_tinyblob", "tinyblob");
        mysqlType.put("c_blob", "blob");
        mysqlType.put("c_mediumblob", "mediumblob");
        mysqlType.put("c_longblob", "longblob");

        Map<String, Column> columns = new LinkedHashMap<>();
        for (Map.Entry<String, String> type : mysqlType.entrySet()) {
            columns.put(type.getKey(), column(type.getKey(), type.getValue()));
        }
        assertEquals(mysqlType.size(), columns.size());

        assertColumn(columns, "c_tinyint", "TINYINT", Types.SMALLINT, 4);
        assertColumn(columns, "c_tinyint_u", "TINYINT UNSIGNED", Types.SMALLINT, 3);
        assertColumn(columns, "c_smallint", "SMALLINT", Types.SMALLINT, 6);
        assertColumn(columns, "c_smallint_u", "SMALLINT UNSIGNED", Types.SMALLINT, 0);
        assertColumn(columns, "c_mediumint", "MEDIUMINT", Types.INTEGER, 9);
        assertColumn(columns, "c_mediumint_u", "MEDIUMINT UNSIGNED", Types.INTEGER, 0);
        assertColumn(columns, "c_int", "INT", Types.INTEGER, 11);
        assertColumn(columns, "c_int_u", "INT UNSIGNED", Types.INTEGER, 10);
        assertColumn(columns, "c_bigint", "BIGINT", Types.BIGINT, 20);
        assertColumn(columns, "c_bigint_u", "BIGINT UNSIGNED", Types.BIGINT, 20);
        assertColumn(columns, "c_float", "FLOAT", Types.FLOAT, 12);
        // the display-width scale is stripped: a present scale suppresses the FLOAT32 schema
        // branch in MySqlValueConverters and corrupts the value (write 8 bytes, read 4)
        assertFalse(columns.get("c_float").scale().isPresent());
        assertColumn(columns, "c_double", "DOUBLE", Types.DOUBLE, 22);
        assertFalse(columns.get("c_double").scale().isPresent());
        // REAL is MySQL's synonym of DOUBLE
        assertColumn(columns, "c_real", "DOUBLE", Types.DOUBLE, 0);
        assertColumn(columns, "c_decimal", "DECIMAL", Types.DECIMAL, 20);
        assertEquals(Integer.valueOf(4), columns.get("c_decimal").scale().get());
        // a precision-less decimal defaults to 36 (Debezium requires a non-zero precision)
        assertColumn(columns, "c_decimal_nop", "DECIMAL", Types.DECIMAL, 36);
        assertColumn(columns, "c_bit", "BIT", Types.BIT, 8);
        assertColumn(columns, "c_bool", "BOOLEAN", Types.BOOLEAN, 0);
        assertColumn(columns, "c_date", "DATE", Types.DATE, 0);
        assertColumn(columns, "c_time", "TIME", Types.TIME, 0);
        assertColumn(columns, "c_time_p", "TIME", Types.TIME, 3);
        assertColumn(columns, "c_datetime", "DATETIME", Types.TIMESTAMP, 0);
        assertColumn(columns, "c_datetime_p", "DATETIME", Types.TIMESTAMP, 3);
        assertColumn(columns, "c_datetime_p6", "DATETIME", Types.TIMESTAMP, 6);
        assertColumn(columns, "c_timestamp", "TIMESTAMP", Types.TIMESTAMP_WITH_TIMEZONE, 0);
        // YEAR keeps its own type name but maps to the DATE jdbc type
        assertColumn(columns, "c_year", "YEAR", Types.DATE, 0);
        assertColumn(columns, "c_json", "JSON", Types.LONGVARCHAR, 0);
        assertColumn(columns, "c_enum", "ENUM", Types.VARCHAR, 0);
        assertColumn(columns, "c_set", "SET", Types.VARCHAR, 0);
        assertEquals(Arrays.asList("a", "b", "c"), columns.get("c_enum").enumValues());
        assertEquals(Arrays.asList("x", "y"), columns.get("c_set").enumValues());
        // text family members collapse to VARCHAR
        assertColumn(columns, "c_char", "VARCHAR", Types.VARCHAR, 16);
        assertColumn(columns, "c_varchar", "VARCHAR", Types.VARCHAR, 100);
        assertColumn(columns, "c_tinytext", "VARCHAR", Types.VARCHAR, 0);
        assertColumn(columns, "c_text", "VARCHAR", Types.VARCHAR, 0);
        assertColumn(columns, "c_mediumtext", "VARCHAR", Types.VARCHAR, 0);
        assertColumn(columns, "c_longtext", "VARCHAR", Types.VARCHAR, 0);
        // blob family members collapse to BLOB
        assertColumn(columns, "c_binary", "BINARY", Types.BINARY, 8);
        assertColumn(columns, "c_varbinary", "VARBINARY", Types.VARBINARY, 64);
        assertColumn(columns, "c_tinyblob", "BLOB", Types.BLOB, 0);
        assertColumn(columns, "c_blob", "BLOB", Types.BLOB, 0);
        assertColumn(columns, "c_mediumblob", "BLOB", Types.BLOB, 0);
        assertColumn(columns, "c_longblob", "BLOB", Types.BLOB, 0);
    }

    private static void assertColumn(
            Map<String, Column> columns, String name, String typeName, int jdbcType, int length) {
        Column column = columns.get(name);
        assertNotNull(column, "column " + name + " is missing");
        assertEquals(typeName, column.typeName(), "typeName of " + name);
        assertEquals(jdbcType, column.jdbcType(), "jdbcType of " + name);
        assertEquals(length, column.length(), "length of " + name);
    }

    @Test
    void testGeometryThrows() {
        assertThrows(
                org.apache.flink.util.FlinkRuntimeException.class,
                () -> KafkaJsonTableUtils.buildColumn("geo", "geometry", 1));
    }
}
