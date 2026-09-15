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

package org.apache.flink.cdc.connectors.kafkajson.source.handler;

import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.dialect.KafkaJsonDialect;
import org.apache.flink.cdc.connectors.kafkajson.source.fetch.KafkaJsonSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.kafkajson.source.message.canal.CanalMessage;
import org.apache.flink.cdc.connectors.kafkajson.source.message.canal.CanalMessageParser;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.document.Array;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for {@link KafkaJsonSchemaChangeHandler}: the canal DDL message updates the shared
 * schema and, when {@code include.schema.changes} is enabled, produces the Debezium-shaped
 * schema-change {@link SourceRecord} that the base {@code IncrementalSourceRecordEmitter} consumes.
 */
class KafkaJsonSchemaChangeHandlerTest {

    private static final TableId TABLE_ID = new TableId("test", null, "users");

    @Test
    void testDdlAppliesSchemaChangeWithoutRecord() throws Exception {
        KafkaJsonSourceFetchTaskContext context = context(false);

        handle(context, "ALTER TABLE `test`.`users` ADD COLUMN `age` int", 2000);

        // the shared schema is updated so that subsequent data records use the new column
        Table updated = context.getDatabaseSchema().tableFor(TABLE_ID);
        assertEquals(3, updated.columns().size());
        assertEquals("age", updated.columnWithName("age").name());
        // include.schema.changes is off: no schema-change record is enqueued
        assertTrue(drain(context.getQueue(), 1).isEmpty());
    }

    @Test
    void testDdlEnqueuesSchemaChangeRecord() throws Exception {
        KafkaJsonSourceFetchTaskContext context = context(true);

        handle(context, "ALTER TABLE `test`.`users` ADD COLUMN `age` int", 2000);

        List<SourceRecord> records = drain(context.getQueue(), 1);
        assertEquals(1, records.size());
        SourceRecord record = records.get(0);

        // recognized as a schema change by the base framework (key schema name + history record)
        assertTrue(SourceRecordUtils.isSchemaChangeEvent(record));
        Struct key = (Struct) record.key();
        assertEquals("test", key.getString("databaseName"));

        Struct value = (Struct) record.value();
        Struct source = value.getStruct("source");
        assertEquals("test", source.getString("db"));
        assertEquals("users", source.getString("table"));

        // the history record carries the table changes, which the emitter replays into the
        // stream-split state; the pre-change schema leads as an ALTER change so the pipeline
        // deserializer can diff the change even for tables it never observed a CREATE for
        HistoryRecord historyRecord = SourceRecordUtils.getHistoryRecord(record);
        Array tableChangesArray =
                historyRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
        TableChanges changes =
                new FlinkJsonTableChangeSerializer().deserialize(tableChangesArray, true);
        List<TableChange> changeList = new ArrayList<>();
        changes.forEach(changeList::add);
        assertEquals(2, changeList.size());
        TableChange oldChange = changeList.get(0);
        assertEquals(
                io.debezium.relational.history.TableChanges.TableChangeType.ALTER,
                oldChange.getType());
        assertEquals(TABLE_ID, oldChange.getId());
        assertEquals(2, oldChange.getTable().columns().size());
        TableChange newChange = changeList.get(1);
        assertEquals(
                io.debezium.relational.history.TableChanges.TableChangeType.ALTER,
                newChange.getType());
        assertEquals(TABLE_ID, newChange.getId());
        assertEquals(3, newChange.getTable().columns().size());
    }

    @Test
    void testRenameTableUpdatesSchemaAndCarriesNewTableId() throws Exception {
        KafkaJsonSourceFetchTaskContext context = context(true);

        // TiDB announces the post-rename name in the message's `table` field, so the old name has
        // to
        // come from the DDL; this message reproduces that shape.
        handle(context, "RENAME TABLE `test`.`users` TO `test`.`vip_users`", "vip_users", 2000);

        // the shared schema moves the table to the new id
        assertNull(context.getDatabaseSchema().tableFor(TABLE_ID));
        TableId vipTableId = new TableId("test", null, "vip_users");
        Table renamed = context.getDatabaseSchema().tableFor(vipTableId);
        assertEquals("vip_users", renamed.id().table());
        assertEquals(2, renamed.columns().size());
        assertEquals("id", renamed.primaryKeyColumnNames().get(0));

        // the schema-change record carries the rename in the custom history-record fields
        List<SourceRecord> records = drain(context.getQueue(), 1);
        assertEquals(1, records.size());
        HistoryRecord historyRecord = SourceRecordUtils.getHistoryRecord(records.get(0));
        assertEquals(
                KafkaJsonSchemaChangeHandler.TABLE_CHANGE_TYPE_RENAME_TABLE,
                historyRecord.document().getString(KafkaJsonSchemaChangeHandler.TABLE_CHANGE_TYPE));
        Array pairs = historyRecord.document().getArray(KafkaJsonSchemaChangeHandler.RENAME_PAIRS);
        assertEquals(1, pairs.size());
        assertEquals(
                TABLE_ID.toString(),
                pairs.get(0).asDocument().getString(KafkaJsonSchemaChangeHandler.OLD_TABLE_ID));
        assertEquals(
                vipTableId.toString(),
                pairs.get(0).asDocument().getString(KafkaJsonSchemaChangeHandler.NEW_TABLE_ID));
    }

    @Test
    void testRenameTableOfSeveralPairsCarriesEveryPair() throws Exception {
        KafkaJsonSourceFetchTaskContext context = context(true);
        TableId users = TABLE_ID;
        TableId vipUsers = new TableId("test", null, "vip_users");
        TableId orders = new TableId("test", null, "orders");
        TableId ordersArchive = new TableId("test", null, "orders_archive");
        context.getDatabaseSchema()
                .registerTable(
                        Table.editor()
                                .tableId(orders)
                                .addColumn(
                                        Column.editor()
                                                .name("id")
                                                .type("BIGINT")
                                                .jdbcType(Types.BIGINT)
                                                .length(20)
                                                .optional(false)
                                                .position(1)
                                                .create())
                                .create());

        // one statement renaming two tables, and the message announces the new name of the first of
        // them -- the pairs have to be read from the statement, and the second pair's table has to
        // be resolved after the first one has been applied
        handle(
                context,
                "RENAME TABLE `test`.`users` TO `test`.`vip_users`, "
                        + "`test`.`orders` TO `test`.`orders_archive`",
                "vip_users",
                2000);

        // each table keeps its own columns and only its name moves
        assertNull(context.getDatabaseSchema().tableFor(users));
        assertEquals(2, context.getDatabaseSchema().tableFor(vipUsers).columns().size());
        assertNull(context.getDatabaseSchema().tableFor(orders));
        assertEquals(1, context.getDatabaseSchema().tableFor(ordersArchive).columns().size());

        List<SourceRecord> records = drain(context.getQueue(), 1);
        HistoryRecord historyRecord = SourceRecordUtils.getHistoryRecord(records.get(0));
        Array pairs = historyRecord.document().getArray(KafkaJsonSchemaChangeHandler.RENAME_PAIRS);
        assertEquals(2, pairs.size());
        assertEquals(
                "test.users",
                pairs.get(0).asDocument().getString(KafkaJsonSchemaChangeHandler.OLD_TABLE_ID));
        assertEquals(
                "test.vip_users",
                pairs.get(0).asDocument().getString(KafkaJsonSchemaChangeHandler.NEW_TABLE_ID));
        assertEquals(
                "test.orders",
                pairs.get(1).asDocument().getString(KafkaJsonSchemaChangeHandler.OLD_TABLE_ID));
        assertEquals(
                "test.orders_archive",
                pairs.get(1).asDocument().getString(KafkaJsonSchemaChangeHandler.NEW_TABLE_ID));
    }

    /**
     * The temporary-name idiom a swap has to be written with: {@code RENAME TABLE a TO a_tmp, b TO
     * a, a_tmp TO b} renames {@code users} onto a name that exists only inside the statement and
     * moves it on again in the last pair.
     *
     * <p>The direct exchange {@code RENAME TABLE users TO orders, orders TO users} is not a
     * statement MySQL or TiDB would ever produce — both refuse it with {@code ERROR 1050 Table
     * 'orders' already exists}, verified on MySQL 8.0 — so the pairs are applied in statement
     * order, each resolving its source against the state the previous one left, which is what makes
     * the temporary name resolvable at all.
     */
    @Test
    void testRenameTableOfASwapAppliesInStatementOrder() throws Exception {
        KafkaJsonSourceFetchTaskContext context = context(true);
        TableId vipUsers = new TableId("test", null, "vip_users");
        context.getDatabaseSchema()
                .registerTable(
                        Table.editor()
                                .tableId(vipUsers)
                                .addColumn(
                                        Column.editor()
                                                .name("vip")
                                                .type("INT")
                                                .jdbcType(Types.INTEGER)
                                                .optional(true)
                                                .position(1)
                                                .create())
                                .create());

        handle(
                context,
                "RENAME TABLE `test`.`users` TO `test`.`users_tmp`, "
                        + "`test`.`vip_users` TO `test`.`users`, "
                        + "`test`.`users_tmp` TO `test`.`vip_users`",
                "users_tmp",
                2000);

        // the two tables exchanged their names, and nothing is left registered under the name that
        // only existed inside the statement
        Table users = context.getDatabaseSchema().tableFor(TABLE_ID);
        Table vips = context.getDatabaseSchema().tableFor(vipUsers);
        assertEquals(1, users.columns().size());
        assertEquals("vip", users.columns().get(0).name());
        assertEquals(2, vips.columns().size());
        assertEquals("name", vips.columnWithName("name").name());
        assertNull(context.getDatabaseSchema().tableFor(new TableId("test", null, "users_tmp")));

        // The three pairs of the statement are reported as the two renames a consumer can act on:
        // the pair parking `users` under the temporary name and the pair moving it on again are one
        // rename of `users` onto `vip_users`, and the transient name appears nowhere. Each reported
        // pair carries its post-rename schema, so the CREATE changes have to line up with them.
        HistoryRecord historyRecord =
                SourceRecordUtils.getHistoryRecord(drain(context.getQueue(), 1).get(0));
        Array pairs = historyRecord.document().getArray(KafkaJsonSchemaChangeHandler.RENAME_PAIRS);
        assertEquals(2, pairs.size());
        assertEquals(
                "test.users",
                pairs.get(0).asDocument().getString(KafkaJsonSchemaChangeHandler.OLD_TABLE_ID));
        assertEquals(
                "test.vip_users",
                pairs.get(0).asDocument().getString(KafkaJsonSchemaChangeHandler.NEW_TABLE_ID));
        assertEquals(
                "test.vip_users",
                pairs.get(1).asDocument().getString(KafkaJsonSchemaChangeHandler.OLD_TABLE_ID));
        assertEquals(
                "test.users",
                pairs.get(1).asDocument().getString(KafkaJsonSchemaChangeHandler.NEW_TABLE_ID));
        TableChanges changes =
                new FlinkJsonTableChangeSerializer()
                        .deserialize(
                                historyRecord
                                        .document()
                                        .getArray(HistoryRecord.Fields.TABLE_CHANGES),
                                true);
        List<TableChange> created = new ArrayList<>();
        for (TableChange change : changes) {
            if (change.getType() == TableChanges.TableChangeType.CREATE) {
                created.add(change);
            }
        }
        assertEquals(2, created.size(), "one CREATE per reported pair");
        // each CREATE carries the schema of the table under its post-statement name: the columns of
        // `users` are now the columns of `vip_users`, and the other way round
        assertEquals(vipUsers, created.get(0).getId());
        assertEquals(2, created.get(0).getTable().columns().size());
        assertEquals(TABLE_ID, created.get(1).getId());
        assertEquals("vip", created.get(1).getTable().columns().get(0).name());
    }

    @Test
    void testRenameOfAnUnobservedTableFailsFast() throws Exception {
        KafkaJsonSourceFetchTaskContext context = context(true);

        // The pre-rename schema was never seen (the job started after the table was created), so
        // the
        // renamed table would be registered without columns and every later row of it would be
        // lost.
        IllegalStateException failure =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                handle(
                                        context,
                                        "RENAME TABLE `test`.`ghost` TO `test`.`ghost2`",
                                        "ghost2",
                                        2000));
        assertTrue(failure.getMessage().contains("test.ghost"), failure.getMessage());
    }

    @Test
    void testDropRemovesTableFromSchema() throws Exception {
        KafkaJsonSourceFetchTaskContext context = context(false);

        handle(context, "DROP TABLE `test`.`users`", 2000);

        assertNull(context.getDatabaseSchema().tableFor(TABLE_ID));
    }

    @Test
    void testTruncateKeepsSchemaAndCarriesTypeMarker() throws Exception {
        KafkaJsonSourceFetchTaskContext context = context(true);

        handle(context, "TRUNCATE TABLE `test`.`users`", 2000);

        // a truncate does not change the schema: the shared schema keeps the table as-is
        Table table = context.getDatabaseSchema().tableFor(TABLE_ID);
        assertEquals(2, table.columns().size());
        assertEquals("id", table.primaryKeyColumnNames().get(0));

        // the schema-change record carries the truncate in the custom history-record fields
        List<SourceRecord> records = drain(context.getQueue(), 1);
        assertEquals(1, records.size());
        HistoryRecord historyRecord = SourceRecordUtils.getHistoryRecord(records.get(0));
        assertEquals(
                KafkaJsonSchemaChangeHandler.TABLE_CHANGE_TYPE_TRUNCATE_TABLE,
                historyRecord.document().getString(KafkaJsonSchemaChangeHandler.TABLE_CHANGE_TYPE));
    }

    private static void handle(KafkaJsonSourceFetchTaskContext context, String sql, long es)
            throws Exception {
        handle(context, sql, "users", es);
    }

    private static void handle(
            KafkaJsonSourceFetchTaskContext context, String sql, String table, long es)
            throws Exception {
        KafkaJsonSourceConfig config = context.getSourceConfig();
        CanalMessage message = new CanalMessageParser().parse(ddlMessage(sql, table, es));
        new KafkaJsonSchemaChangeHandler(config)
                .handle(context, message, new KafkaJsonOffset(es, 0, 2));
    }

    private static KafkaJsonSourceFetchTaskContext context(boolean includeSchemaChanges) {
        KafkaJsonSourceConfig config =
                new KafkaJsonSourceConfigFactory()
                        .hostname("localhost")
                        .username("root")
                        .password("x")
                        .databaseList("test")
                        .tableList("test.users")
                        .kafkaBootstrapServers("bootstrap")
                        .kafkaTopics("t")
                        .serverTimeZone("UTC")
                        .includeSchemaChanges(includeSchemaChanges)
                        .create(0);
        KafkaJsonSourceFetchTaskContext context =
                new KafkaJsonSourceFetchTaskContext(config, new KafkaJsonDialect(config));
        StreamSplit split =
                new StreamSplit(
                        StreamSplit.STREAM_SPLIT_ID,
                        KafkaJsonOffset.INITIAL_OFFSET,
                        KafkaJsonOffset.NO_STOPPING_OFFSET,
                        new ArrayList<>(),
                        new HashMap<>(),
                        0);
        context.configure(split);
        context.getDatabaseSchema().registerTable(baseTable());
        return context;
    }

    private static String ddlMessage(String sql, String table, long es) {
        return "{\"data\":null,\"database\":\"test\",\"es\":"
                + es
                + ",\"id\":2,"
                + "\"isDdl\":true,\"mysqlType\":null,\"old\":null,\"pkNames\":null,"
                + "\"sql\":\""
                + sql
                + "\",\"sqlType\":null,\"table\":\""
                + table
                + "\",\"ts\":"
                + (es + 500)
                + ",\"type\":\"ALTER\"}";
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

    /** Polls the queue until {@code expected} records have been drained (or a timeout elapses). */
    private static List<SourceRecord> drain(ChangeEventQueue<DataChangeEvent> queue, int expected)
            throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 5000L;
        while (System.currentTimeMillis() < deadline && records.size() < expected) {
            for (DataChangeEvent event : queue.poll()) {
                records.add(event.getRecord());
            }
        }
        return records;
    }
}
