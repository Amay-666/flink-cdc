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
    void testParseAlterModifyColumnReplacesEnumList() {
        // MySQL MODIFY COLUMN replaces the whole ENUM list — the DDL text carries the complete
        // new expression. The registry column (labels filled from information_schema by
        // KafkaJsonJdbcConnection) is superseded by the new list.
        //
        // The Doris column type is unchanged (VARCHAR stays VARCHAR), so NO pipeline schema
        // change event is emitted — the new table (with the new labels) is re-registered
        // source-side only, which is all that's needed to map TiCDC's numeric index through
        // the NEW list.
        Table tableWithEnum =
                Table.editor()
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
                                        .name("status")
                                        .type("ENUM")
                                        .jdbcType(Types.VARCHAR)
                                        .length(1)
                                        .optional(true)
                                        .position(2)
                                        .enumValues(
                                                java.util.Arrays.asList(
                                                        "active", "inactive", "pending"))
                                        .create())
                        .setPrimaryKeyNames("id")
                        .create();

        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        tableWithEnum,
                        "ALTER TABLE `test`.`users` MODIFY COLUMN `status` "
                                + "ENUM('active','archived','retired','pending')");

        // the new table carries the replaced label list ...
        Column status = result.getNewTable().columnWithName("status");
        assertEquals(
                java.util.Arrays.asList("active", "archived", "retired", "pending"),
                status.enumValues());
        // ... but no column change is emitted (the sink column type is unchanged)
        assertTrue(result.getColumnChanges().isEmpty());
    }

    @Test
    void testEnumListChangeWithCommentChangeEmitsCommentOnly() {
        // ENUM('a','b') → ENUM('a','c') COMMENT 'em': the label list is source-side only,
        // but the comment change must still reach the sink
        Table table =
                Table.editor()
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
                                        .name("status")
                                        .type("ENUM")
                                        .jdbcType(Types.VARCHAR)
                                        .length(1)
                                        .optional(true)
                                        .position(2)
                                        .enumValues(java.util.Arrays.asList("a", "b"))
                                        .comment("old")
                                        .create())
                        .setPrimaryKeyNames("id")
                        .create();

        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        table,
                        "ALTER TABLE `test`.`users` MODIFY COLUMN `status` "
                                + "ENUM('a','c') COMMENT 'em'");

        // the comment change IS emitted ...
        List<ColumnChangeInfo> changes = result.getColumnChanges();
        assertEquals(1, changes.size());
        assertEquals(KafkaJsonTableChangeType.ALTER_COLUMN_COMMENT, changes.get(0).getChangeType());
        assertEquals("em", changes.get(0).getNewValue());
        // ... and the new table carries the replaced labels
        assertEquals(
                java.util.Arrays.asList("a", "c"),
                result.getNewTable().columnWithName("status").enumValues());
    }

    @Test
    void testEnumToVarcharIsATypeChange() {
        // ENUM('a','b') → VARCHAR(50): a real type change that must NOT be swallowed
        // by the same-type ENUM/SET branch
        Table table =
                Table.editor()
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
                                        .name("status")
                                        .type("ENUM")
                                        .jdbcType(Types.VARCHAR)
                                        .length(1)
                                        .optional(true)
                                        .position(2)
                                        .enumValues(java.util.Arrays.asList("a", "b"))
                                        .create())
                        .setPrimaryKeyNames("id")
                        .create();

        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        table,
                        "ALTER TABLE `test`.`users` MODIFY COLUMN `status` VARCHAR(50)");

        List<ColumnChangeInfo> changes = result.getColumnChanges();
        assertEquals(1, changes.size());
        assertEquals(KafkaJsonTableChangeType.ALTER_COLUMN_TYPE, changes.get(0).getChangeType());
        // the new column is VARCHAR with no enum values
        assertTrue(result.getNewTable().columnWithName("status").enumValues().isEmpty());
    }

    @Test
    void testEnumListChangeWithNullabilityIsIgnored() {
        // ENUM('a','b') NOT NULL → ENUM('a','c') NULL: nullability is already ignored by
        // design, so this behaves the same as a pure label replacement — no pipeline event
        Table table =
                Table.editor()
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
                                        .name("status")
                                        .type("ENUM")
                                        .jdbcType(Types.VARCHAR)
                                        .length(1)
                                        .optional(false)
                                        .position(2)
                                        .enumValues(java.util.Arrays.asList("a", "b"))
                                        .create())
                        .setPrimaryKeyNames("id")
                        .create();

        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        table,
                        "ALTER TABLE `test`.`users` MODIFY COLUMN `status` "
                                + "ENUM('a','c') NULL");

        // no changes (nullability is ignored, label change is source-side only)
        assertTrue(result.getColumnChanges().isEmpty());
        assertEquals(
                java.util.Arrays.asList("a", "c"),
                result.getNewTable().columnWithName("status").enumValues());
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

    /**
     * The full MySQL type matrix through the DDL path: the same types as the canal-mysqlType matrix
     * ({@code KafkaJsonTableUtilsTest#testFullTypeMatrix}) parsed from one CREATE TABLE, asserting
     * that a DDL-derived column and a canal-message-derived column of the same type are built
     * identically — the property the shared {@code KafkaJsonTableUtils} mapping guarantees.
     */
    @Test
    void testParseCreateTableFullTypeMatrix() {
        TableId matrixId = new TableId("test", null, "type_matrix");
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        matrixId,
                        null,
                        "CREATE TABLE `test`.`type_matrix` ("
                                + "`c_tinyint` tinyint(4), "
                                + "`c_tinyint_u` tinyint(3) unsigned, "
                                + "`c_smallint` smallint(6), "
                                + "`c_smallint_u` smallint unsigned, "
                                + "`c_mediumint` mediumint(9), "
                                + "`c_mediumint_u` mediumint unsigned, "
                                + "`c_int` int(11), "
                                + "`c_int_u` int(10) unsigned, "
                                + "`c_bigint` bigint(20), "
                                + "`c_bigint_u` bigint(20) unsigned, "
                                + "`c_float` float, "
                                + "`c_double` double, "
                                + "`c_real` real, "
                                + "`c_decimal` decimal(20,4), "
                                + "`c_decimal_nop` decimal, "
                                + "`c_bit` bit(8), "
                                + "`c_bool` boolean, "
                                + "`c_date` date, "
                                + "`c_time` time, "
                                + "`c_time_p` time(3), "
                                + "`c_datetime` datetime, "
                                + "`c_datetime_p` datetime(3), "
                                + "`c_datetime_p6` datetime(6), "
                                + "`c_timestamp` timestamp, "
                                + "`c_year` year, "
                                + "`c_json` json, "
                                + "`c_enum` enum('a','b','c'), "
                                + "`c_set` set('x','y'), "
                                + "`c_char` char(16), "
                                + "`c_varchar` varchar(100), "
                                + "`c_tinytext` tinytext, "
                                + "`c_text` text, "
                                + "`c_mediumtext` mediumtext, "
                                + "`c_longtext` longtext, "
                                + "`c_binary` binary(8), "
                                + "`c_varbinary` varbinary(64), "
                                + "`c_tinyblob` tinyblob, "
                                + "`c_blob` blob, "
                                + "`c_mediumblob` mediumblob, "
                                + "`c_longblob` longblob, "
                                + "PRIMARY KEY (`c_int`))");

        assertEquals(KafkaJsonTableChangeType.CREATE, result.getType());
        Table table = result.getNewTable();
        assertEquals(40, table.columns().size());
        assertEquals(Collections.singletonList("c_int"), table.primaryKeyColumnNames());

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
        assertColumn(table, "c_float", "FLOAT", Types.FLOAT, 0);
        assertColumn(table, "c_double", "DOUBLE", Types.DOUBLE, 0);
        assertColumn(table, "c_real", "DOUBLE", Types.DOUBLE, 0);
        assertColumn(table, "c_decimal", "DECIMAL", Types.DECIMAL, 20);
        assertEquals(Integer.valueOf(4), table.columnWithName("c_decimal").scale().get());
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
        assertColumn(table, "c_year", "YEAR", Types.DATE, 0);
        assertColumn(table, "c_json", "JSON", Types.LONGVARCHAR, 0);
        assertColumn(table, "c_enum", "ENUM", Types.VARCHAR, 0);
        assertColumn(table, "c_set", "SET", Types.VARCHAR, 0);
        assertEquals(Arrays.asList("a", "b", "c"), table.columnWithName("c_enum").enumValues());
        assertEquals(Arrays.asList("x", "y"), table.columnWithName("c_set").enumValues());
        assertColumn(table, "c_char", "VARCHAR", Types.VARCHAR, 16);
        assertColumn(table, "c_varchar", "VARCHAR", Types.VARCHAR, 100);
        assertColumn(table, "c_tinytext", "VARCHAR", Types.VARCHAR, 0);
        assertColumn(table, "c_text", "VARCHAR", Types.VARCHAR, 0);
        assertColumn(table, "c_mediumtext", "VARCHAR", Types.VARCHAR, 0);
        assertColumn(table, "c_longtext", "VARCHAR", Types.VARCHAR, 0);
        assertColumn(table, "c_binary", "BINARY", Types.BINARY, 8);
        assertColumn(table, "c_varbinary", "VARBINARY", Types.VARBINARY, 64);
        assertColumn(table, "c_tinyblob", "BLOB", Types.BLOB, 0);
        assertColumn(table, "c_blob", "BLOB", Types.BLOB, 0);
        assertColumn(table, "c_mediumblob", "BLOB", Types.BLOB, 0);
        assertColumn(table, "c_longblob", "BLOB", Types.BLOB, 0);

        // positions follow the declaration order
        assertEquals(1, table.columnWithName("c_tinyint").position());
        assertEquals(40, table.columnWithName("c_longblob").position());
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
    void testParseRenameTableStatementOfSeveralPairs() {
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
        // both ids of every pair come from the statement: the canal message announces the
        // post-rename name, so nothing may be read off the announced table id
        assertEquals(TABLE_ID, pairs.get(0).getOldTableId());
        assertEquals(new TableId("test", null, "orders"), pairs.get(0).getNewTableId());
        assertEquals(new TableId("test", null, "orders"), pairs.get(1).getOldTableId());
        assertEquals(TABLE_ID, pairs.get(1).getNewTableId());
        // the parser resolves the schema of the table it was handed (the announced one); the other
        // pair is resolved by the handler from the shared schema registry
        assertNotNull(pairs.get(0).getNewTable());
        assertEquals("orders", pairs.get(0).getNewTable().id().table());
        assertNull(pairs.get(1).getNewTable());
        // the single-pair getters report the first pair, which represents the statement
        assertEquals(TABLE_ID, result.getTableId());
        assertEquals(new TableId("test", null, "orders"), result.getNewTableId());
    }

    @Test
    void testParseRenameTableStatementAcrossDatabasesKeepsBothNames() {
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        baseTable(),
                        "RENAME TABLE `test`.`users` TO `archive`.`users_old`");

        assertEquals(KafkaJsonTableChangeType.RENAME_TABLE, result.getType());
        assertEquals(TABLE_ID, result.getTableId());
        assertEquals(new TableId("archive", null, "users_old"), result.getNewTableId());
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
