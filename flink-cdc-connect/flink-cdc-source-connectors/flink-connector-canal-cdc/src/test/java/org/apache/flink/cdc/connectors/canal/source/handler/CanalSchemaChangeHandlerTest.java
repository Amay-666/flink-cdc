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

package org.apache.flink.cdc.connectors.canal.source.handler;

import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils;
import org.apache.flink.cdc.connectors.canal.source.CanalDialect;
import org.apache.flink.cdc.connectors.canal.source.config.CanalSourceConfig;
import org.apache.flink.cdc.connectors.canal.source.config.CanalSourceConfigFactory;
import org.apache.flink.cdc.connectors.canal.source.fetch.CanalSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.canal.source.message.CanalFlatMessage;
import org.apache.flink.cdc.connectors.canal.source.message.CanalFlatMessageParser;
import org.apache.flink.cdc.connectors.canal.source.offset.CanalOffset;
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
import io.debezium.relational.history.TableChanges.TableChangeType;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for {@link CanalSchemaChangeHandler}: the canal DDL message updates the shared schema
 * and, when {@code include.schema.changes} is enabled, produces the Debezium-shaped schema-change
 * {@link SourceRecord} that the base {@code IncrementalSourceRecordEmitter} consumes.
 */
class CanalSchemaChangeHandlerTest {

    private static final TableId TABLE_ID = new TableId("test", null, "users");

    @Test
    void testDdlAppliesSchemaChangeWithoutRecord() throws Exception {
        CanalSourceFetchTaskContext context = context(false);

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
        CanalSourceFetchTaskContext context = context(true);

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

        // the history record carries the table change, which the emitter replays into the
        // stream-split state
        HistoryRecord historyRecord = SourceRecordUtils.getHistoryRecord(record);
        Array tableChangesArray =
                historyRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
        TableChanges changes =
                new FlinkJsonTableChangeSerializer().deserialize(tableChangesArray, true);
        TableChange change = changes.iterator().next();
        assertEquals(TableChangeType.ALTER, change.getType());
        assertEquals(TABLE_ID, change.getId());
        assertEquals(3, change.getTable().columns().size());
    }

    @Test
    void testDropRemovesTableFromSchema() throws Exception {
        CanalSourceFetchTaskContext context = context(false);

        handle(context, "DROP TABLE `test`.`users`", 2000);

        assertNull(context.getDatabaseSchema().tableFor(TABLE_ID));
    }

    private static void handle(CanalSourceFetchTaskContext context, String sql, long es)
            throws Exception {
        CanalSourceConfig config = context.getSourceConfig();
        CanalFlatMessage message =
                CanalFlatMessageParser.parse(ddlMessage(sql, es));
        new CanalSchemaChangeHandler(config)
                .handle(context, message, new CanalOffset(es, 0, 2));
    }

    private static CanalSourceFetchTaskContext context(boolean includeSchemaChanges) {
        CanalSourceConfig config =
                new CanalSourceConfigFactory()
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
        CanalSourceFetchTaskContext context =
                new CanalSourceFetchTaskContext(config, new CanalDialect(config));
        StreamSplit split =
                new StreamSplit(
                        StreamSplit.STREAM_SPLIT_ID,
                        CanalOffset.INITIAL_OFFSET,
                        CanalOffset.NO_STOPPING_OFFSET,
                        new ArrayList<>(),
                        new HashMap<>(),
                        0);
        context.configure(split);
        context.getDatabaseSchema().registerTable(baseTable());
        return context;
    }

    private static String ddlMessage(String sql, long es) {
        return "{\"data\":null,\"database\":\"test\",\"es\":" + es + ",\"id\":2,"
                + "\"isDdl\":true,\"mysqlType\":null,\"old\":null,\"pkNames\":null,"
                + "\"sql\":\"" + sql + "\",\"sqlType\":null,\"table\":\"users\",\"ts\":"
                + (es + 500) + ",\"type\":\"ALTER\"}";
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
    private static List<SourceRecord> drain(
            ChangeEventQueue<DataChangeEvent> queue, int expected)
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
