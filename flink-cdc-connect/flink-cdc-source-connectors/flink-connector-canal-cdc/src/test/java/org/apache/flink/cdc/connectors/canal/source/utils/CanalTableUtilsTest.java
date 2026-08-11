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

package org.apache.flink.cdc.connectors.canal.source.utils;

import org.apache.flink.cdc.connectors.canal.source.message.CanalFlatMessage;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test for {@link CanalTableUtils}. */
class CanalTableUtilsTest {

    private static CanalFlatMessage message(
            Map<String, String> mysqlType, Map<String, String> row, String... pkNames) {
        CanalFlatMessage message = new CanalFlatMessage();
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

        Table table = CanalTableUtils.buildTable(message(mysqlType, row, "id"));

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
                CanalTableUtils.buildTable(
                        message(mysqlType, Collections.singletonMap("color", "red")));
        assertEquals(Arrays.asList("red", "it's"), table.columnWithName("color").enumValues());
    }

    @Test
    void testNoPrimaryKey() {
        Map<String, String> mysqlType = new HashMap<>();
        mysqlType.put("id", "int(11)");
        Table table =
                CanalTableUtils.buildTable(
                        message(mysqlType, Collections.singletonMap("id", "1")));
        assertTrue(table.primaryKeyColumnNames().isEmpty());
    }

    @Test
    void testNullMysqlTypeFallsBackToVarchar() {
        Table table =
                CanalTableUtils.buildTable(
                        message(Collections.emptyMap(), Collections.singletonMap("id", "1")));
        // no mysqlType metadata at all -> single column with a best-effort type
        assertEquals("VARCHAR", table.columnWithName("id").typeName());
    }

    @Test
    void testEmptyMessageReturnsNull() {
        CanalFlatMessage message = new CanalFlatMessage();
        message.setDatabase("test");
        message.setTable("users");
        message.setData(Collections.emptyList());
        assertNull(CanalTableUtils.buildTable(message));
    }

    @Test
    void testGeometryThrows() {
        Map<String, String> mysqlType = new HashMap<>();
        mysqlType.put("geo", "geometry");
        assertThrows(
                org.apache.flink.util.FlinkRuntimeException.class,
                () ->
                        CanalTableUtils.buildTable(
                                message(mysqlType, Collections.singletonMap("geo", ""))));
    }
}
