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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
    void testParseCreateTableInlinePrimaryKey() {
        // ANTLR reads inline constraints natively
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        null,
                        "CREATE TABLE `test`.`users` "
                                + "(`id` BIGINT NOT NULL PRIMARY KEY, "
                                + "`name` VARCHAR(255)) ENGINE=InnoDB");

        assertEquals(KafkaJsonTableChangeType.CREATE, result.getType());
        Table table = result.getNewTable();
        assertEquals(TABLE_ID, table.id());
        assertEquals(Collections.singletonList("id"), table.primaryKeyColumnNames());
    }

    @Test
    void testParseCreateTableInlineUniqueKeyPromoted() {
        // no PRIMARY KEY, an inline UNIQUE on `id` — promoted to the keyed identity
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        TABLE_ID,
                        null,
                        "CREATE TABLE `test`.`users` "
                                + "(`id` BIGINT NOT NULL UNIQUE, "
                                + "`name` VARCHAR(255)) ENGINE=InnoDB");

        assertEquals(KafkaJsonTableChangeType.CREATE, result.getType());
        Table table = result.getNewTable();
        assertEquals(Collections.singletonList("id"), table.primaryKeyColumnNames());
    }

    @Test
    void testParseCreateTableLike() {
        // ① both unqualified — same database, resolve the source against it
        KafkaJsonDdlParsedResult result =
                parser.parse("test", TABLE_ID, null, "CREATE TABLE `users` LIKE `users2`");
        assertEquals(KafkaJsonTableChangeType.CREATE_LIKE, result.getType());
        assertEquals(new TableId("test", null, "users2"), result.getTableId());
        assertEquals(TABLE_ID, result.getNewTableId());

        // ② source unqualified, target qualified — the source's database is ambiguous
        // (it belongs to the database the DDL executed in, not the target's)
        result =
                parser.parse(
                        "test",
                        new TableId("other", null, "users"),
                        null,
                        "CREATE TABLE `other`.`users` LIKE `users2`");
        assertEquals(KafkaJsonTableChangeType.CREATE_LIKE, result.getType());
        assertNull(result.getTableId());
        assertEquals(new TableId("other", null, "users"), result.getNewTableId());

        // ③ source qualified — the database is explicit in the SQL
        result = parser.parse("test", TABLE_ID, null, "CREATE TABLE `users` LIKE test.`users2`");
        assertEquals(KafkaJsonTableChangeType.CREATE_LIKE, result.getType());
        assertEquals(new TableId("test", null, "users2"), result.getTableId());
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

    @Test
    void testParseCreateTableFullTypeMatrix() {
        // The same type matrix as KafkaJsonTableUtilsTest#testFullTypeMatrix and the Druid parser
        // matrix, parsed with Debezium's ANTLR parser. The assertions document THIS parser's
        // mapping, which is Debezium's own (MySqlAntlrDdlParser), not the shared
        // KafkaJsonTableUtils mapping — differences between the two are noted inline.
        TableId matrixId = new TableId("test", null, "type_matrix");
        KafkaJsonDdlParsedResult result =
                parser.parse(
                        "test",
                        matrixId,
                        null,
                        "CREATE TABLE `test`.`type_matrix` ("
                                + "`c_tinyint` TINYINT(4), "
                                + "`c_tinyint_u` TINYINT(3) UNSIGNED, "
                                + "`c_smallint` SMALLINT(6), "
                                + "`c_mediumint` MEDIUMINT(9), "
                                + "`c_int` INT(11), "
                                + "`c_int_u` INT(10) UNSIGNED, "
                                + "`c_bigint` BIGINT(20), "
                                + "`c_bigint_u` BIGINT(20) UNSIGNED, "
                                + "`c_float` FLOAT, "
                                + "`c_double` DOUBLE, "
                                + "`c_real` REAL, "
                                + "`c_decimal` DECIMAL(20,4), "
                                + "`c_bit` BIT(8), "
                                + "`c_bool` BOOLEAN, "
                                + "`c_date` DATE, "
                                + "`c_time` TIME, "
                                + "`c_datetime` DATETIME, "
                                + "`c_datetime_p` DATETIME(3), "
                                + "`c_timestamp` TIMESTAMP, "
                                + "`c_year` YEAR, "
                                + "`c_json` JSON, "
                                + "`c_enum` ENUM('a','b','c'), "
                                + "`c_set` SET('x','y'), "
                                + "`c_char` CHAR(16), "
                                + "`c_varchar` VARCHAR(100), "
                                + "`c_tinytext` TINYTEXT, "
                                + "`c_text` TEXT, "
                                + "`c_binary` BINARY(8), "
                                + "`c_varbinary` VARBINARY(64), "
                                + "`c_tinyblob` TINYBLOB, "
                                + "`c_blob` BLOB, "
                                + "PRIMARY KEY (`c_int`))");

        assertEquals(KafkaJsonTableChangeType.CREATE, result.getType());
        Table table = result.getNewTable();
        assertEquals(31, table.columns().size());
        assertEquals(Collections.singletonList("c_int"), table.primaryKeyColumnNames());

        assertColumn(table, "c_tinyint", "TINYINT", Types.SMALLINT, 4);
        assertColumn(table, "c_tinyint_u", "TINYINT UNSIGNED", Types.SMALLINT, 3);
        assertColumn(table, "c_smallint", "SMALLINT", Types.SMALLINT, 6);
        assertColumn(table, "c_mediumint", "MEDIUMINT", Types.INTEGER, 9);
        assertColumn(table, "c_int", "INT", Types.INTEGER, 11);
        assertColumn(table, "c_int_u", "INT UNSIGNED", Types.INTEGER, 10);
        assertColumn(table, "c_bigint", "BIGINT", Types.BIGINT, 20);
        assertColumn(table, "c_bigint_u", "BIGINT UNSIGNED", Types.BIGINT, 20);
        // a length-less type reports -1 (Debezium) rather than 0 (KafkaJsonTableUtils)
        assertColumn(table, "c_float", "FLOAT", Types.FLOAT, -1);
        assertColumn(table, "c_double", "DOUBLE", Types.DOUBLE, -1);
        // Debezium maps REAL to Types.REAL and keeps the name (KafkaJsonTableUtils normalizes
        // both to DOUBLE); one deployment uses a single parser, so the difference never mixes
        assertColumn(table, "c_real", "REAL", Types.REAL, -1);
        assertColumn(table, "c_decimal", "DECIMAL", Types.DECIMAL, 20);
        assertEquals(Integer.valueOf(4), table.columnWithName("c_decimal").scale().get());
        assertColumn(table, "c_bit", "BIT", Types.BIT, 8);
        assertColumn(table, "c_bool", "BOOLEAN", Types.BOOLEAN, -1);
        assertColumn(table, "c_date", "DATE", Types.DATE, -1);
        assertColumn(table, "c_time", "TIME", Types.TIME, -1);
        assertColumn(table, "c_datetime", "DATETIME", Types.TIMESTAMP, -1);
        assertColumn(table, "c_datetime_p", "DATETIME", Types.TIMESTAMP, 3);
        assertColumn(table, "c_timestamp", "TIMESTAMP", Types.TIMESTAMP_WITH_TIMEZONE, -1);
        assertColumn(table, "c_year", "YEAR", Types.INTEGER, -1);
        // Debezium maps JSON to Types.OTHER (KafkaJsonTableUtils uses LONGVARCHAR)
        assertColumn(table, "c_json", "JSON", Types.OTHER, -1);
        // Debezium maps ENUM/SET to Types.CHAR; ENUM length is the longest value, SET length the
        // comma-joined width (KafkaJsonTableUtils uses VARCHAR/0 instead)
        assertColumn(table, "c_enum", "ENUM", Types.CHAR, 1);
        assertColumn(table, "c_set", "SET", Types.CHAR, 3);
        // Debezium keeps the quotes on enum values (KafkaJsonTableUtils strips them)
        assertEquals(
                Arrays.asList("'a'", "'b'", "'c'"), table.columnWithName("c_enum").enumValues());
        assertEquals(Arrays.asList("'x'", "'y'"), table.columnWithName("c_set").enumValues());
        assertColumn(table, "c_char", "CHAR", Types.CHAR, 16);
        assertColumn(table, "c_varchar", "VARCHAR", Types.VARCHAR, 100);
        // Debezium keeps the declared text-family name (KafkaJsonTableUtils collapses to VARCHAR)
        assertColumn(table, "c_tinytext", "TINYTEXT", Types.VARCHAR, -1);
        assertColumn(table, "c_text", "TEXT", Types.VARCHAR, -1);
        assertColumn(table, "c_binary", "BINARY", Types.BINARY, 8);
        assertColumn(table, "c_varbinary", "VARBINARY", Types.VARBINARY, 64);
        // Debezium keeps the declared blob-family name (KafkaJsonTableUtils collapses to BLOB)
        assertColumn(table, "c_tinyblob", "TINYBLOB", Types.BLOB, -1);
        assertColumn(table, "c_blob", "BLOB", Types.BLOB, -1);
    }

    private static void assertColumn(
            Table table, String name, String typeName, int jdbcType, int length) {
        Column column = table.columnWithName(name);
        assertNotNull(column, "column " + name + " is missing");
        assertEquals(typeName, column.typeName(), "typeName of " + name);
        assertEquals(jdbcType, column.jdbcType(), "jdbcType of " + name);
        assertEquals(length, column.length(), "length of " + name);
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
