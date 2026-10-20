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

import org.apache.flink.cdc.connectors.kafkajson.source.message.canal.CanalMessage;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test for {@link KafkaJsonTableUtils}. */
class KafkaJsonTableUtilsTest {

    private static CanalMessage message(
            Map<String, String> mysqlType, Map<String, String> row, String... pkNames) {
        CanalMessage message = new CanalMessage();
        message.setDatabase("test");
        message.setTable("users");
        message.setPkNames(Arrays.asList(pkNames));
        message.setMysqlType(mysqlType);
        if (row != null) {
            message.setData(Collections.singletonList(row));
        }
        return message;
    }

    @Test
    void testBuildTableBasicTypes() {
        Map<String, String> mysqlType = new HashMap<>();
        mysqlType.put("id", "bigint(20)");
        mysqlType.put("name", "varchar(255)");
        mysqlType.put("amount", "decimal(10,2)");
        mysqlType.put("created_at", "datetime");
        mysqlType.put("updated_at", "timestamp(3)");
        mysqlType.put("score", "double");
        mysqlType.put("flag", "tinyint(1)");
        mysqlType.put("unsigned_col", "int(10) unsigned");
        mysqlType.put("tags", "set('a','b')");

        // Jackson deserializes JSON objects into LinkedHashMap preserving field order,
        // so the reconstructed table follows the (deterministic) data-row order
        Map<String, String> row = new java.util.LinkedHashMap<>();
        row.put("id", "1");
        row.put("name", "Alice");
        row.put("amount", "3.14");
        row.put("created_at", "2020-08-13 15:00:00");
        row.put("updated_at", "2020-08-13 15:00:00");
        row.put("score", "1.5");
        row.put("flag", "1");
        row.put("unsigned_col", "1");
        row.put("tags", "a");

        Table table = KafkaJsonTableUtils.buildTable(message(mysqlType, row, "id"));

        assertEquals("test.users", table.id().toString());
        assertEquals(Arrays.asList("id"), table.primaryKeyColumnNames());

        Column id = table.columnWithName("id");
        assertEquals(Types.BIGINT, id.jdbcType());
        assertEquals("BIGINT", id.typeName());
        assertEquals(20, id.length());

        Column amount = table.columnWithName("amount");
        assertEquals(Types.DECIMAL, amount.jdbcType());
        assertEquals(10, amount.length());
        assertEquals(Integer.valueOf(2), amount.scale().get());

        Column createdAt = table.columnWithName("created_at");
        assertEquals(Types.TIMESTAMP, createdAt.jdbcType());
        assertEquals("DATETIME", createdAt.typeName());

        Column updatedAt = table.columnWithName("updated_at");
        assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, updatedAt.jdbcType());
        assertEquals("TIMESTAMP", updatedAt.typeName());
        assertEquals(3, updatedAt.length());

        Column unsigned = table.columnWithName("unsigned_col");
        assertEquals("INT UNSIGNED", unsigned.typeName());
        assertEquals(Types.INTEGER, unsigned.jdbcType());

        Column tags = table.columnWithName("tags");
        assertEquals("SET", tags.typeName());
        assertEquals(Arrays.asList("a", "b"), tags.enumValues());

        // columns follow the order of the data row
        assertEquals("id", table.columns().get(0).name());
        assertEquals("tags", table.columns().get(table.columns().size() - 1).name());
    }

    @Test
    void testEnumWithEscapedQuotes() {
        Map<String, String> mysqlType = new HashMap<>();
        mysqlType.put("color", "enum('red','it''s')");
        Table table =
                KafkaJsonTableUtils.buildTable(
                        message(mysqlType, Collections.singletonMap("color", "red")));
        assertEquals(Arrays.asList("red", "it's"), table.columnWithName("color").enumValues());
    }

    /**
     * The full MySQL type matrix: every type the connector claims to support, mapped through the
     * canal {@code mysqlType} path (a LinkedHashMap keeps the column order deterministic). This is
     * the authoritative mapping the DDL-parser matrix tests must stay consistent with: geometry
     * types are the only exclusion (they fail fast, see {@link #testGeometryThrows()}).
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

        Table table = KafkaJsonTableUtils.buildTable(message(mysqlType, null, "c_int"));
        assertEquals(mysqlType.size(), table.columns().size());

        assertColumn(table, "c_tinyint", "TINYINT", Types.SMALLINT, 4);
        assertColumn(table, "c_tinyint_u", "TINYINT UNSIGNED", Types.SMALLINT, 3);
        assertColumn(table, "c_smallint", "SMALLINT", Types.SMALLINT, 6);
        assertColumn(table, "c_smallint_u", "SMALLINT UNSIGNED", Types.SMALLINT, 0);
        assertColumn(table, "c_mediumint", "MEDIUMINT", Types.INTEGER, 9);
        assertColumn(table, "c_mediumint_u", "MEDIUMINT UNSIGNED", Types.INTEGER, 0);
        assertColumn(table, "c_int", "INT", Types.INTEGER, 11);
        assertColumn(table, "c_int_u", "INT UNSIGNED", Types.INTEGER, 10);
        assertColumn(table, "c_bigint", "BIGINT", Types.BIGINT, 20);
        assertColumn(table, "c_bigint_u", "BIGINT UNSIGNED", Types.BIGINT, 20);
        assertColumn(table, "c_float", "FLOAT", Types.FLOAT, 12);
        // the display-width scale is stripped: a present scale suppresses the FLOAT32 schema
        // branch in MySqlValueConverters and corrupts the value (write 8 bytes, read 4)
        assertTrue(!table.columnWithName("c_float").scale().isPresent());
        assertColumn(table, "c_double", "DOUBLE", Types.DOUBLE, 22);
        assertTrue(!table.columnWithName("c_double").scale().isPresent());
        // REAL is MySQL's synonym of DOUBLE
        assertColumn(table, "c_real", "DOUBLE", Types.DOUBLE, 0);
        assertColumn(table, "c_decimal", "DECIMAL", Types.DECIMAL, 20);
        assertEquals(Integer.valueOf(4), table.columnWithName("c_decimal").scale().get());
        // a precision-less decimal defaults to 36 (Debezium requires a non-zero precision)
        assertColumn(table, "c_decimal_nop", "DECIMAL", Types.DECIMAL, 36);
        assertColumn(table, "c_bit", "BIT", Types.BIT, 8);
        assertColumn(table, "c_bool", "BOOLEAN", Types.BOOLEAN, 0);
        assertColumn(table, "c_date", "DATE", Types.DATE, 0);
        assertColumn(table, "c_time", "TIME", Types.TIME, 0);
        assertColumn(table, "c_time_p", "TIME", Types.TIME, 3);
        assertColumn(table, "c_datetime", "DATETIME", Types.TIMESTAMP, 0);
        assertColumn(table, "c_datetime_p", "DATETIME", Types.TIMESTAMP, 3);
        assertColumn(table, "c_datetime_p6", "DATETIME", Types.TIMESTAMP, 6);
        assertColumn(table, "c_timestamp", "TIMESTAMP", Types.TIMESTAMP_WITH_TIMEZONE, 0);
        // YEAR keeps its own type name but maps to the DATE jdbc type
        assertColumn(table, "c_year", "YEAR", Types.DATE, 0);
        assertColumn(table, "c_json", "JSON", Types.LONGVARCHAR, 0);
        assertColumn(table, "c_enum", "ENUM", Types.VARCHAR, 0);
        assertColumn(table, "c_set", "SET", Types.VARCHAR, 0);
        assertEquals(Arrays.asList("a", "b", "c"), table.columnWithName("c_enum").enumValues());
        assertEquals(Arrays.asList("x", "y"), table.columnWithName("c_set").enumValues());
        // text family members collapse to VARCHAR
        assertColumn(table, "c_char", "VARCHAR", Types.VARCHAR, 16);
        assertColumn(table, "c_varchar", "VARCHAR", Types.VARCHAR, 100);
        assertColumn(table, "c_tinytext", "VARCHAR", Types.VARCHAR, 0);
        assertColumn(table, "c_text", "VARCHAR", Types.VARCHAR, 0);
        assertColumn(table, "c_mediumtext", "VARCHAR", Types.VARCHAR, 0);
        assertColumn(table, "c_longtext", "VARCHAR", Types.VARCHAR, 0);
        // blob family members collapse to BLOB
        assertColumn(table, "c_binary", "BINARY", Types.BINARY, 8);
        assertColumn(table, "c_varbinary", "VARBINARY", Types.VARBINARY, 64);
        assertColumn(table, "c_tinyblob", "BLOB", Types.BLOB, 0);
        assertColumn(table, "c_blob", "BLOB", Types.BLOB, 0);
        assertColumn(table, "c_mediumblob", "BLOB", Types.BLOB, 0);
        assertColumn(table, "c_longblob", "BLOB", Types.BLOB, 0);

        // the column order follows the mysqlType map (the data row is absent)
        assertEquals("c_int", table.columns().get(6).name());
        assertEquals("c_longblob", table.columns().get(table.columns().size() - 1).name());
    }

    private static void assertColumn(
            Table table, String name, String typeName, int jdbcType, int length) {
        Column column = table.columnWithName(name);
        assertNotNull(column, "column " + name + " is missing");
        assertEquals(typeName, column.typeName(), "typeName of " + name);
        assertEquals(jdbcType, column.jdbcType(), "jdbcType of " + name);
        assertEquals(length, column.length(), "length of " + name);
    }

    @Test
    void testTidbUniqueKeyInPkNamesBecomesPrimaryKey() {
        // TiCDC's canal-json protocol carries the keyed columns (a UNIQUE KEY when the table has no
        // PRIMARY KEY) in pkNames; buildTable must key the reconstructed table on them.
        Map<String, String> mysqlType = new HashMap<>();
        mysqlType.put("id", "bigint(20)");
        mysqlType.put("email", "varchar(64)");
        Map<String, String> row = new HashMap<>();
        row.put("id", "1");
        row.put("email", "a@b.com");
        Table table = KafkaJsonTableUtils.buildTable(message(mysqlType, row, "email"));
        assertEquals(Collections.singletonList("email"), table.primaryKeyColumnNames());
        assertEquals(2, table.columns().size());
    }

    @Test
    void testNoPrimaryKey() {
        Map<String, String> mysqlType = new HashMap<>();
        mysqlType.put("id", "int(11)");
        Table table =
                KafkaJsonTableUtils.buildTable(
                        message(mysqlType, Collections.singletonMap("id", "1")));
        assertTrue(table.primaryKeyColumnNames().isEmpty());
    }

    @Test
    void testNullMysqlTypeFallsBackToVarchar() {
        Table table =
                KafkaJsonTableUtils.buildTable(
                        message(Collections.emptyMap(), Collections.singletonMap("id", "1")));
        // no mysqlType metadata at all -> single column with a best-effort type
        assertEquals("VARCHAR", table.columnWithName("id").typeName());
    }

    @Test
    void testEmptyMessageReturnsNull() {
        CanalMessage message = new CanalMessage();
        message.setDatabase("test");
        message.setTable("users");
        message.setData(Collections.emptyList());
        assertNull(KafkaJsonTableUtils.buildTable(message));
    }

    @Test
    void testGeometryThrows() {
        Map<String, String> mysqlType = new HashMap<>();
        mysqlType.put("geo", "geometry");
        assertThrows(
                org.apache.flink.util.FlinkRuntimeException.class,
                () ->
                        KafkaJsonTableUtils.buildTable(
                                message(mysqlType, Collections.singletonMap("geo", ""))));
    }
}
