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

package org.apache.flink.cdc.connectors.canal.source.ddl;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChangeType;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Unit test for {@link CanalDebeziumDdlParser} (Debezium's MySQL ANTLR parser). */
class CanalDebeziumDdlParserTest {

    private static final TableId TABLE_ID = new TableId("test", null, "users");

    private final CanalDebeziumDdlParser parser = new CanalDebeziumDdlParser();

    @Test
    void testParseCreateTable() {
        CanalDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        null,
                        "CREATE TABLE `test`.`users` "
                                + "(`id` BIGINT NOT NULL, `name` VARCHAR(255), "
                                + "PRIMARY KEY (`id`)) ENGINE=InnoDB");

        assertEquals(TableChangeType.CREATE, result.getType());
        Table table = result.getTable();
        assertEquals(TABLE_ID, table.id());
        assertEquals(2, table.columns().size());
        Column id = table.columnWithName("id");
        assertEquals("BIGINT", id.typeName());
        assertEquals(Types.BIGINT, id.jdbcType());
        assertEquals(1, id.position());
        Column name = table.columnWithName("name");
        assertEquals("VARCHAR", name.typeName());
        assertEquals(Types.VARCHAR, name.jdbcType());
        assertEquals(Collections.singletonList("id"), table.primaryKeyColumnNames());
    }

    @Test
    void testParseAlterAddColumn() {
        CanalDdlParsedResult result =
                parser.parse(
                        "test", TABLE_ID, baseTable(), "ALTER TABLE `test`.`users` ADD COLUMN `age` INT");

        assertEquals(TableChangeType.ALTER, result.getType());
        Table table = result.getTable();
        assertEquals(3, table.columns().size());
        Column age = table.columnWithName("age");
        assertEquals("INT", age.typeName());
        assertEquals(Types.INTEGER, age.jdbcType());
    }

    @Test
    void testParseDropTable() {
        CanalDdlParsedResult result =
                parser.parse("test", TABLE_ID, baseTable(), "DROP TABLE `test`.`users`");

        assertEquals(TableChangeType.DROP, result.getType());
        assertNull(result.getTable());
    }

    private static Table baseTable() {
        return Table.editor()
                .tableId(TABLE_ID)
                .addColumn(
                        Column.editor()
                                .name("id")
                                .type("BIGINT")
                                .jdbcType(Types.BIGINT)
                                .length(20)
                                .optional(false)
                                .position(1)
                                .create())
                .addColumn(
                        Column.editor()
                                .name("name")
                                .type("VARCHAR")
                                .jdbcType(Types.VARCHAR)
                                .length(255)
                                .optional(true)
                                .position(2)
                                .create())
                .setPrimaryKeyNames("id")
                .create();
    }
}
