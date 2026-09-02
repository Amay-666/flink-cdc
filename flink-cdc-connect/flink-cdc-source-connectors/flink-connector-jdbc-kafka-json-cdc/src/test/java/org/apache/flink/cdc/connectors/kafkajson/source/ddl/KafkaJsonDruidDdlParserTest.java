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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
    void testCreateTableWithoutPrimaryKeyFallsBackToUniqueKey() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        null,
                        "CREATE TABLE `test`.`users` ("
                                + "`id` bigint(20) NOT NULL, "
                                + "`email` varchar(255) NOT NULL, "
                                + "UNIQUE KEY `uk_email` (`email`))");

        assertEquals(KafkaJsonTableChangeType.CREATE, result.getType());
        Table table = result.getNewTable();
        // no PRIMARY KEY: the declared unique key becomes the primary key
        assertEquals(Collections.singletonList("email"), table.primaryKeyColumnNames());
        assertEquals(2, table.columns().size());
    }

    @Test
    void testCreateTableFallsBackToCompositeUniqueKey() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        null,
                        "CREATE TABLE `test`.`users` ("
                                + "`tenant` bigint(20) NOT NULL, "
                                + "`email` varchar(255) NOT NULL, "
                                + "UNIQUE KEY `uk_tenant_email` (`tenant`,`email`))");

        assertEquals(
                Arrays.asList("tenant", "email"), result.getNewTable().primaryKeyColumnNames());
    }

    @Test
    void testCreateTablePrefersPrimaryKeyOverUniqueKey() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        null,
                        "CREATE TABLE `test`.`users` ("
                                + "`id` bigint(20) NOT NULL, "
                                + "`email` varchar(255) NOT NULL, "
                                + "PRIMARY KEY (`id`), "
                                + "UNIQUE KEY `uk_email` (`email`))");

        assertEquals(Collections.singletonList("id"), result.getNewTable().primaryKeyColumnNames());
    }

    @Test
    void testCreateTableWithoutAnyKeyKeepsNoPrimaryKey() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        null,
                        "CREATE TABLE `test`.`users` (`id` bigint(20) NOT NULL, `name` varchar(255))");

        assertEquals(Collections.emptyList(), result.getNewTable().primaryKeyColumnNames());
    }

    @Test
    void testParseAlterAddColumn() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        baseTable(),
                        "ALTER TABLE `test`.`users` ADD COLUMN `age` int");

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
                        "test",
                        TABLE_ID,
                        baseTable(),
                        "ALTER TABLE `test`.`users` DROP COLUMN `name`");

        assertEquals(KafkaJsonTableChangeType.ALTER, result.getType());
        Table table = result.getNewTable();
        assertEquals(1, table.columns().size());
        assertNull(table.columnWithName("name"));
        assertEquals("id", table.columns().get(0).name());
    }

    @Test
    void testParseAlterAddColumnIfNotExists() {
        // TiDB emits "ADD COLUMN IF NOT EXISTS", which Druid's MySQL grammar rejects without the
        // retry that strips the idempotence modifier; the schema change must still be captured.
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        baseTable(),
                        "ALTER TABLE `test`.`users` ADD COLUMN IF NOT EXISTS `age` int");

        assertEquals(KafkaJsonTableChangeType.ALTER, result.getType());
        Table table = result.getNewTable();
        assertEquals(3, table.columns().size());
        Column age = table.columnWithName("age");
        assertEquals("INT", age.typeName());
        assertEquals(Types.INTEGER, age.jdbcType());
        assertEquals(3, age.position());
    }

    @Test
    void testParseAlterDropColumnIfExists() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        baseTable(),
                        "ALTER TABLE `test`.`users` DROP COLUMN IF EXISTS `name`");

        assertEquals(KafkaJsonTableChangeType.ALTER, result.getType());
        Table table = result.getNewTable();
        assertEquals(1, table.columns().size());
        assertNull(table.columnWithName("name"));
        assertEquals("id", table.columns().get(0).name());
    }

    @Test
    void testParseAlterAddIndexIfNotExistsIsIgnored() {
        // Index changes do not affect the column model: even with the TiDB modifier the statement
        // is recognized and dropped, not treated as an unparsable DDL.
        assertNull(
                parser.parse(
                        "test",
                        TABLE_ID,
                        baseTable(),
                        "ALTER TABLE `test`.`users` ADD INDEX IF NOT EXISTS `idx_name` (`name`)"));
    }

    @Test
    void testParseTiDbIfExistsModifierIsCaseInsensitive() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        baseTable(),
                        "alter table `test`.`users` add column if not exists `age` int");

        assertEquals(KafkaJsonTableChangeType.ALTER, result.getType());
        assertNotNull(result.getNewTable().columnWithName("age"));
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
    void testParseAlterModifyColumnType() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        baseTable(),
                        "ALTER TABLE `test`.`users` MODIFY COLUMN `name` varchar(100)");

        assertEquals(KafkaJsonTableChangeType.ALTER_COLUMN_TYPE, result.getType());
        // the column-level change carries the old and the new type
        List<ColumnChangeInfo> changes = result.getColumnChanges();
        assertEquals(1, changes.size());
        ColumnChangeInfo change = changes.get(0);
        assertEquals("name", change.getColumnName());
        assertEquals(KafkaJsonTableChangeType.ALTER_COLUMN_TYPE, change.getChangeType());
    }

    @Test
    void testParseAlterModifyColumnComment() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        baseTable(),
                        "ALTER TABLE `test`.`users` MODIFY COLUMN `name` varchar(255) COMMENT 'nickname'");

        assertEquals(KafkaJsonTableChangeType.ALTER_COLUMN_COMMENT, result.getType());
        assertEquals("nickname", result.getNewTable().columnWithName("name").comment());
        List<ColumnChangeInfo> changes = result.getColumnChanges();
        assertEquals(1, changes.size());
        assertEquals(KafkaJsonTableChangeType.ALTER_COLUMN_COMMENT, changes.get(0).getChangeType());
    }

    @Test
    void testParseAlterModifyOnlyOptionalIsIgnored() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        baseTable(),
                        "ALTER TABLE `test`.`users` MODIFY COLUMN `name` varchar(255) NULL");

        assertNull(result);
    }

    @Test
    void testParseAlterModifyNoChangeIsIgnored() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        baseTable(),
                        "ALTER TABLE `test`.`users` MODIFY COLUMN `name` varchar(255)");

        assertNull(result);
    }

    @Test
    void testCreateTableCapturesColumnComment() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        null,
                        "CREATE TABLE `test`.`users` "
                                + "(`id` bigint(20) NOT NULL COMMENT 'primary key', "
                                + "`name` varchar(255) DEFAULT NULL)");

        assertEquals("primary key", result.getNewTable().columnWithName("id").comment());
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
                parser.parse("test", TABLE_ID, baseTable(), "TRUNCATE TABLE `test`.`users`");

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
