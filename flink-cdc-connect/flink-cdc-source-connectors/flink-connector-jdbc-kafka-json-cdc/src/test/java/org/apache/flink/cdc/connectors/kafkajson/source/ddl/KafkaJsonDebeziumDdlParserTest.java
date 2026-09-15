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

package org.apache.flink.cdc.connectors.kafkajson.source.ddl;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Unit test for {@link KafkaJsonDebeziumDdlParser} (Debezium's MySQL ANTLR parser). */
class KafkaJsonDebeziumDdlParserTest {

    private static final TableId TABLE_ID = new TableId("test", null, "users");

    private final KafkaJsonDebeziumDdlParser parser = new KafkaJsonDebeziumDdlParser();

    @Test
    void testParseCreateTable() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        null,
                        "CREATE TABLE `test`.`users` "
                                + "(`id` BIGINT NOT NULL, `name` VARCHAR(255), "
                                + "PRIMARY KEY (`id`)) ENGINE=InnoDB");

        assertEquals(KafkaJsonTableChangeType.CREATE, result.getType());
        Table table = result.getNewTable();
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
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        baseTable(),
                        "ALTER TABLE `test`.`users` ADD COLUMN `age` INT");

        assertEquals(KafkaJsonTableChangeType.ALTER, result.getType());
        Table table = result.getNewTable();
        assertEquals(3, table.columns().size());
        Column age = table.columnWithName("age");
        assertEquals("INT", age.typeName());
        assertEquals(Types.INTEGER, age.jdbcType());
    }

    @Test
    void testParseDropTable() {
        KafkaJsonDdlParsedResult result =
                parser.parse("test", TABLE_ID, baseTable(), "DROP TABLE `test`.`users`");

        assertEquals(KafkaJsonTableChangeType.DROP, result.getType());
        assertNull(result.getNewTable());
    }

    @Test
    void testParseRenameTable() {
        KafkaJsonDdlParsedResult result =
                parser.parse("test", TABLE_ID, baseTable(), "RENAME TABLE `users` TO `vip_users`");

        assertEquals(KafkaJsonTableChangeType.RENAME_TABLE, result.getType());
        assertEquals(TABLE_ID, result.getTableId());
        assertEquals(new TableId("test", null, "vip_users"), result.getNewTableId());
        assertEquals(2, result.getNewTable().columns().size());
    }

    @Test
    void testParseRenameTableOfSeveralPairs() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        baseTable(),
                        "RENAME TABLE `test`.`users` TO `test`.`orders`, "
                                + "`test`.`orders` TO `test`.`users`");

        assertEquals(KafkaJsonTableChangeType.RENAME_TABLE, result.getType());
        List<KafkaJsonRenamePair> pairs = result.getRenamePairs();
        assertEquals(2, pairs.size());
        assertEquals(TABLE_ID, pairs.get(0).getOldTableId());
        assertEquals(new TableId("test", null, "orders"), pairs.get(0).getNewTableId());
        assertEquals(new TableId("test", null, "orders"), pairs.get(1).getOldTableId());
        assertEquals(TABLE_ID, pairs.get(1).getNewTableId());
    }

    @Test
    void testParseRenameTableWithoutAnAnnouncedTable() {
        // A Debezium schema-change record carries no table name at all, so the handler hands the
        // parser a null id: both ids still come from the statement. The schemas stay null here and
        // the handler resolves them from the shared registry.
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test", null, null, "RENAME TABLE `test`.`users` TO `test`.`vip_users`");

        assertEquals(KafkaJsonTableChangeType.RENAME_TABLE, result.getType());
        assertEquals(TABLE_ID, result.getTableId());
        assertEquals(new TableId("test", null, "vip_users"), result.getNewTableId());
        assertNull(result.getNewTable());
    }

    @Test
    void testParseTruncateTable() {
        KafkaJsonDdlParsedResult result =
                parser.parse("test", TABLE_ID, baseTable(), "TRUNCATE TABLE `test`.`users`");

        assertEquals(KafkaJsonTableChangeType.TRUNCATE, result.getType());
        assertEquals(TABLE_ID, result.getTableId());
        // a truncate does not change the schema: the table is preserved as-is
        assertEquals(baseTable(), result.getOldTable());
        assertEquals(baseTable(), result.getNewTable());
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
