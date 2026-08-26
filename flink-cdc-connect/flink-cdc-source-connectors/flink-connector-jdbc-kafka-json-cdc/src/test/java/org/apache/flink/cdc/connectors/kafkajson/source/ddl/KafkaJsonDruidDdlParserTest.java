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

import org.apache.flink.cdc.connectors.kafkajson.source.utils.KafkaJsonTableUtils;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test for {@link KafkaJsonDruidDdlParser} (the default DDL parser). */
class KafkaJsonDruidDdlParserTest {

    private static final TableId TABLE_ID = new TableId("test", null, "users");

    private final KafkaJsonDruidDdlParser parser = new KafkaJsonDruidDdlParser();

    @Test
    void testParseCreateTable() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        null,
                        "CREATE TABLE `test`.`users` "
                                + "(`id` bigint(20) NOT NULL, `name` varchar(255) DEFAULT NULL, "
                                + "PRIMARY KEY (`id`)) ENGINE=InnoDB");

        assertEquals(KafkaJsonTableChangeType.CREATE, result.getType());
        Table table = result.getNewTable();
        assertEquals(TABLE_ID, table.id());
        assertEquals(2, table.columns().size());
        Column id = table.columnWithName("id");
        assertEquals("BIGINT", id.typeName());
        assertEquals(Types.BIGINT, id.jdbcType());
        assertEquals(20, id.length());
        assertEquals(1, id.position());
        Column name = table.columnWithName("name");
        assertEquals("VARCHAR", name.typeName());
        assertEquals(Types.VARCHAR, name.jdbcType());
        assertEquals(255, name.length());
        assertEquals(Collections.singletonList("id"), table.primaryKeyColumnNames());
    }

    @Test
    void testParseAlterAddColumn() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test", TABLE_ID, baseTable(), "ALTER TABLE `test`.`users` ADD COLUMN `age` int");

        assertEquals(KafkaJsonTableChangeType.ALTER, result.getType());
        Table table = result.getNewTable();
        assertEquals(3, table.columns().size());
        Column age = table.columnWithName("age");
        assertEquals("INT", age.typeName());
        assertEquals(Types.INTEGER, age.jdbcType());
        assertEquals(3, age.position());
    }

    @Test
    void testParseAlterDropColumn() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test", TABLE_ID, baseTable(), "ALTER TABLE `test`.`users` DROP COLUMN `name`");

        assertEquals(KafkaJsonTableChangeType.ALTER, result.getType());
        Table table = result.getNewTable();
        assertEquals(1, table.columns().size());
        assertNull(table.columnWithName("name"));
        assertEquals("id", table.columns().get(0).name());
    }

    @Test
    void testParseAlterModifyColumn() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        baseTable(),
                        "ALTER TABLE `test`.`users` MODIFY COLUMN `name` varchar(100)");

        Table table = result.getNewTable();
        Column name = table.columnWithName("name");
        assertEquals("VARCHAR", name.typeName());
        assertEquals(100, name.length());
        assertEquals(2, name.position());
    }

    @Test
    void testParseAlterChangeColumnRename() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        baseTable(),
                        "ALTER TABLE `test`.`users` CHANGE COLUMN `name` `nickname` varchar(64)");

        Table table = result.getNewTable();
        assertNull(table.columnWithName("name"));
        Column nickname = table.columnWithName("nickname");
        assertEquals("VARCHAR", nickname.typeName());
        assertEquals(64, nickname.length());
        assertEquals(2, nickname.position());
    }

    @Test
    void testParseDropTable() {
        KafkaJsonDdlParsedResult result =
                parser.parse("test", TABLE_ID, baseTable(), "DROP TABLE `test`.`users`");

        assertEquals(KafkaJsonTableChangeType.DROP, result.getType());
        assertNull(result.getNewTable());
    }

    @Test
    void testAlterWithoutCurrentTableIsIgnored() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test", TABLE_ID, null, "ALTER TABLE `test`.`users` ADD COLUMN `age` int");

        assertNull(result);
    }

    @Test
    void testUnrelatedDdlIsIgnored() {
        assertNull(
                parser.parse(
                        "test",
                        TABLE_ID,
                        baseTable(),
                        "CREATE INDEX `idx_name` ON `test`.`users` (`name`)"));
    }

    @Test
    void testParseTruncateTable() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test", TABLE_ID, baseTable(), "TRUNCATE TABLE `test`.`users`");

        assertEquals(KafkaJsonTableChangeType.TRUNCATE, result.getType());
        assertEquals(TABLE_ID, result.getTableId());
        // a truncate does not change the schema: the table is preserved as-is
        assertEquals(baseTable(), result.getOldTable());
        assertEquals(baseTable(), result.getNewTable());
    }

    @Test
    void testTruncateWithoutCurrentTableIsIgnored() {
        KafkaJsonDdlParsedResult result =
                parser.parse("test", TABLE_ID, null, "TRUNCATE TABLE `test`.`users`");

        assertNull(result);
    }

    @Test
    void testUnsignedColumnType() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        new TableId("test", null, "orders"),
                        null,
                        "CREATE TABLE `test`.`orders` (`amount` bigint(20) unsigned NOT NULL)");

        Table table = result.getNewTable();
        Column amount = table.columnWithName("amount");
        assertEquals("BIGINT UNSIGNED", amount.typeName());
        assertTrue(KafkaJsonTableUtils.isUnsigned(amount.typeName()));
    }

    @Test
    void testParseRenameTableStatement() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        baseTable(),
                        "RENAME TABLE `test`.`users` TO `test`.`vip_users`");

        assertEquals(KafkaJsonTableChangeType.RENAME_TABLE, result.getType());
        assertEquals(TABLE_ID, result.getTableId());
        assertEquals(new TableId("test", null, "vip_users"), result.getNewTableId());
        assertEquals(baseTable(), result.getOldTable());
        // the resulting table keeps the schema and only changes its id
        assertEquals("vip_users", result.getNewTable().id().table());
        assertEquals(2, result.getNewTable().columns().size());
    }

    @Test
    void testParseAlterRenameTable() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        baseTable(),
                        "ALTER TABLE `test`.`users` RENAME TO `vip_users`");

        assertEquals(KafkaJsonTableChangeType.RENAME_TABLE, result.getType());
        assertEquals(TABLE_ID, result.getTableId());
        assertEquals(new TableId("test", null, "vip_users"), result.getNewTableId());
    }

    @Test
    void testParseAlterRenameColumn() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        baseTable(),
                        "ALTER TABLE `test`.`users` RENAME COLUMN `name` TO `nickname`");

        assertEquals(KafkaJsonTableChangeType.RENAME_COLUMN, result.getType());
        Table table = result.getNewTable();
        assertNull(table.columnWithName("name"));
        Column nickname = table.columnWithName("nickname");
        assertEquals("VARCHAR", nickname.typeName());
        assertEquals(255, nickname.length());
        assertEquals(2, nickname.position());
    }

    @Test
    void testParseChangeColumnSameTypeIsColumnRename() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        baseTable(),
                        "ALTER TABLE `test`.`users` CHANGE COLUMN `name` `nickname` varchar(255)");

        assertEquals(KafkaJsonTableChangeType.RENAME_COLUMN, result.getType());
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
