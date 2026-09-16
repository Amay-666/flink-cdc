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

package org.apache.flink.cdc.connectors.kafkajson.source.reader.external;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.dialect.KafkaJsonDialect;
import org.apache.flink.cdc.connectors.kafkajson.source.fetch.KafkaJsonSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.kafkajson.source.handler.KafkaJsonSchemaChangeHandler;
import org.apache.flink.cdc.connectors.kafkajson.source.message.canal.CanalMessageParser;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffsetFactory;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.util.Collector;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.relational.history.TableChanges.TableChangeType;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for the state half of the rename fix: the schema-change record that {@link
 * KafkaJsonSchemaChangeHandler} enqueues, consumed by the released {@code
 * IncrementalSourceRecordEmitter}, is what writes the renamed table into the stream split's {@code
 * tableSchemas} — the map that goes into the checkpoint.
 *
 * <p>This is the leg a restarted job depends on. The emission rule ({@code
 * KafkaJsonIncrementalSourceStreamFetcher#shouldEmit}) decides with the schema store, and on
 * restore the store is rebuilt from the checkpointed split state, so a job that failed after a
 * rename knows the new name only if the record reached the emitter before the checkpoint. The
 * record must therefore not be withheld at the source, which is why the handler enqueues it
 * unconditionally — the {@code include.schema.changes} switch only decides whether the emitter
 * forwards it downstream, not whether the state is written.
 *
 * <p>Both tests run the real released emitter over the real record the handler produces, so what is
 * asserted is the state a running job checkpoints, not a hand-built map.
 */
class KafkaJsonSchemaChangeStateTest {

    private static final TableId USERS = new TableId("test", null, "users");
    private static final TableId VIP_USERS = new TableId("test", null, "vip_users");

    /**
     * The state must learn the new name even with {@code include.schema.changes} off (the
     * production default): the record is enqueued, the emitter writes the split state and withholds
     * the record from downstream in the same call.
     */
    @Test
    void testARenameLandsInTheStreamSplitStateWithoutSchemaChanges() throws Exception {
        KafkaJsonSourceFetchTaskContext context = context();
        // the split state a checkpoint carries before the rename: the table under its old name
        Map<TableId, TableChange> checkpointed = schemas(USERS, table(USERS));
        StreamSplit split = streamSplit(checkpointed);
        context.configure(split);
        StreamSplitState state = new StreamSplitState(split);
        List<Event> emitted = new ArrayList<>();

        // the handler applies the rename and enqueues the schema-change record
        renameToVipUsers(context);
        SourceRecord record = nextSchemaChangeRecord(context);

        newEmitter(false).emit(record, output(emitted), state);

        // the switch is off: the record does not reach downstream ...
        assertTrue(emitted.isEmpty(), "the DDL must not be forwarded downstream");
        // ... and yet the checkpointed state knows the table under its new name
        Map<TableId, TableChange> restored = state.toSourceSplit().getTableSchemas();
        assertTrue(restored.containsKey(VIP_USERS), "the state must carry the post-rename name");
        assertEquals(VIP_USERS, restored.get(VIP_USERS).getId());
        // the pre-rename entry stays, which costs nothing: a record that was converted before the
        // DDL is still resolvable, and the store keeps both names on restore
        assertTrue(restored.containsKey(USERS));
    }

    /**
     * The restart leg: the state written above goes through the checkpoint's own serializer and
     * comes back as the split a restored job starts from — its store is rebuilt from the restored
     * table schemas, and the emission rule then emits the renamed table's records instead of
     * dropping them.
     *
     * <p>The round trip is what makes this a statement about a failure rather than about an
     * in-memory map: Flink checkpoints a split by serializing it with the source's {@link
     * SourceSplitSerializer}, and a restarted job only ever sees the deserialized form.
     */
    @Test
    void testTheRenamedNameSurvivesTheCheckpointAndTheRestoredJobEmits() throws Exception {
        KafkaJsonSourceFetchTaskContext context = context();
        Map<TableId, TableChange> checkpointed = schemas(USERS, table(USERS));
        StreamSplit split = streamSplit(checkpointed);
        context.configure(split);
        StreamSplitState state = new StreamSplitState(split);

        // the rename, and the record that carries it into the split state, before the checkpoint
        renameToVipUsers(context);
        newEmitter(false).emit(nextSchemaChangeRecord(context), output(new ArrayList<>()), state);

        // the checkpoint: the split is serialized exactly as the source serializes it
        SourceSplitSerializer serializer = splitSerializer();
        byte[] checkpointedBytes = serializer.serialize(state.toSourceSplit());
        // the restore: a restarted job reads those bytes back
        StreamSplit restored =
                serializer.deserialize(serializer.getVersion(), checkpointedBytes).asStreamSplit();
        Map<TableId, TableChange> restoredSchemas = restored.getTableSchemas();
        assertTrue(
                restoredSchemas.containsKey(VIP_USERS),
                "the checkpoint must carry the post-rename name");
        assertEquals(VIP_USERS, restoredSchemas.get(VIP_USERS).getId());

        // the restarted job: a fresh context whose store is rebuilt from the restored split
        KafkaJsonSourceFetchTaskContext restoredContext = context();
        KafkaJsonIncrementalSourceStreamFetcher fetcher = fetcher(restored, restoredContext);
        try {
            assertNotNull(restoredContext.getDatabaseSchema().tableFor(VIP_USERS));
            // the record converts — the store knows the new name — and the emission rule emits it:
            // no split info of this split names the table, and the store is what settles it
            SourceRecord inserted = insert(restoredContext, "vip_users", 1L, 9000L, 9L);
            assertTrue(fetcher.shouldEmit(inserted));
        } finally {
            fetcher.close();
        }
    }

    /**
     * The serializer the source builds for its splits: the released {@code IncrementalSource}
     * constructs an anonymous one that only supplies the offset factory, which is what the test
     * mirrors so the checkpoint's real format is exercised.
     */
    private static SourceSplitSerializer splitSerializer() {
        return new SourceSplitSerializer() {
            @Override
            public KafkaJsonOffsetFactory getOffsetFactory() {
                return new KafkaJsonOffsetFactory();
            }
        };
    }

    // ---------------------------------------------------------------------------------------------
    // the released emitter, driven over the record the handler produces
    // ---------------------------------------------------------------------------------------------

    /** Exposes the released emitter's protected {@code processElement} to the test. */
    private static final class TestEmitter<T> extends IncrementalSourceRecordEmitter<T> {

        private TestEmitter(boolean includeSchemaChanges) {
            super(
                    new NoopDeserializationSchema<>(),
                    unregisteredMetrics(),
                    includeSchemaChanges,
                    new KafkaJsonOffsetFactory());
        }

        private void emit(SourceRecord record, SourceOutput<T> output, SourceSplitState state)
                throws Exception {
            processElement(record, output, state);
        }
    }

    private static TestEmitter<Event> newEmitter(boolean includeSchemaChanges) {
        return new TestEmitter<>(includeSchemaChanges);
    }

    /**
     * Consumes nothing: the emitter hands a schema-change record to the deserialization schema only
     * when it forwards it downstream, and what that produces is a different test's business (see
     * {@code MySqlCanalRenameITCase}). The state is written either way, which is the point here.
     */
    private static final class NoopDeserializationSchema<T>
            implements DebeziumDeserializationSchema<T> {

        @Override
        public void deserialize(SourceRecord record, Collector<T> out) {}

        @Override
        public TypeInformation<T> getProducedType() {
            return null;
        }
    }

    private static SourceOutput<Event> output(List<Event> sink) {
        return new SourceOutput<Event>() {
            @Override
            public void collect(Event record) {
                sink.add(record);
            }

            @Override
            public void collect(Event record, long timestamp) {
                sink.add(record);
            }

            @Override
            public void emitWatermark(Watermark watermark) {}

            @Override
            public void markIdle() {}

            @Override
            public void markActive() {}
        };
    }

    /**
     * A {@link SourceReaderMetrics} that goes nowhere: the emitter reports metrics for data records
     * only, and the test feeds it a schema-change record. The metric group is a proxy because the
     * unregistered one Flink ships is package-private.
     */
    private static SourceReaderMetrics unregisteredMetrics() {
        SourceReaderMetricGroup group =
                (SourceReaderMetricGroup)
                        Proxy.newProxyInstance(
                                SourceReaderMetricGroup.class.getClassLoader(),
                                new Class<?>[] {SourceReaderMetricGroup.class},
                                (proxy, method, args) ->
                                        "getNumRecordsInErrorsCounter".equals(method.getName())
                                                ? new SimpleCounter()
                                                : defaultValue(method.getReturnType()));
        return new SourceReaderMetrics(group);
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive() || type == void.class) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        return null;
    }

    // ---------------------------------------------------------------------------------------------
    // the source-side fixtures
    // ---------------------------------------------------------------------------------------------

    /** Applies a canal {@code RENAME TABLE} message, as the stream fetch task would. */
    private static void renameToVipUsers(KafkaJsonSourceFetchTaskContext context) throws Exception {
        // the announced table is the post-rename name and the pre-rename one appears only in the
        // SQL, which is the shape TiCDC writes (see src/test/resources/kafkajson/captured)
        String message =
                "{\"data\":null,\"database\":\"test\",\"es\":4000,\"id\":2,\"isDdl\":true,"
                        + "\"mysqlType\":null,\"old\":null,\"pkNames\":null,"
                        + "\"sql\":\"RENAME TABLE `test`.`users` TO `test`.`vip_users`\","
                        + "\"sqlType\":null,\"table\":\"vip_users\",\"ts\":4100,\"type\":\"RENAME\"}";
        new KafkaJsonSchemaChangeHandler(context.getSourceConfig())
                .handle(
                        context,
                        new CanalMessageParser().parse(message),
                        new KafkaJsonOffset(4000, 0, 3));
    }

    /** Takes the schema-change record the handler enqueued. */
    private static SourceRecord nextSchemaChangeRecord(KafkaJsonSourceFetchTaskContext context)
            throws InterruptedException {
        ChangeEventQueue<DataChangeEvent> queue = context.getQueue();
        long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline) {
            for (DataChangeEvent event : queue.poll()) {
                return event.getRecord();
            }
        }
        throw new IllegalStateException("the handler enqueued no schema-change record");
    }

    /** Converts a canal INSERT into the record the fetcher is handed by the reader. */
    private static SourceRecord insert(
            KafkaJsonSourceFetchTaskContext context,
            String table,
            long id,
            long eventTime,
            long kafkaOffset) {
        String message =
                "{\"data\":[{\"id\":\""
                        + id
                        + "\",\"name\":\"Alice\"}],\"database\":\"test\",\"es\":"
                        + eventTime
                        + ",\"id\":1,\"isDdl\":false,"
                        + "\"mysqlType\":{\"id\":\"bigint(20)\",\"name\":\"varchar(255)\"},"
                        + "\"old\":null,\"pkNames\":[\"id\"],\"sql\":\"\",\"sqlType\":{},"
                        + "\"table\":\""
                        + table
                        + "\",\"ts\":"
                        + (eventTime + 100)
                        + ",\"type\":\"INSERT\"}";
        List<SourceRecord> records =
                context.getRecordConverter()
                        .convert(new CanalMessageParser().parse(message), "t", 0, kafkaOffset);
        assertEquals(1, records.size(), "the table has to be registered for its INSERT to convert");
        return records.get(0);
    }

    private static KafkaJsonSourceFetchTaskContext context() {
        KafkaJsonSourceConfig config = config();
        return new KafkaJsonSourceFetchTaskContext(config, new KafkaJsonDialect(config));
    }

    /**
     * The fetcher on the given split: starting it takes the split's finished snapshot split infos
     * for the watermark maps and registers the split's table schemas in the store — the two things
     * the emission rule decides with. The fetch task itself reads nothing.
     */
    private static KafkaJsonIncrementalSourceStreamFetcher fetcher(
            StreamSplit split, KafkaJsonSourceFetchTaskContext context) {
        KafkaJsonIncrementalSourceStreamFetcher fetcher =
                new KafkaJsonIncrementalSourceStreamFetcher(context, 0);
        fetcher.submitTask(new IdleFetchTask(split));
        return fetcher;
    }

    private static StreamSplit streamSplit(Map<TableId, TableChange> tableSchemas) {
        return new StreamSplit(
                StreamSplit.STREAM_SPLIT_ID,
                new KafkaJsonOffset(2500, Integer.MAX_VALUE, Long.MAX_VALUE),
                KafkaJsonOffset.NO_STOPPING_OFFSET,
                Collections.singletonList(
                        new FinishedSnapshotSplitInfo(
                                USERS,
                                "users-split-0",
                                new Object[] {1L},
                                new Object[] {10L},
                                new KafkaJsonOffset(3000, Integer.MAX_VALUE, Long.MAX_VALUE),
                                new KafkaJsonOffsetFactory())),
                tableSchemas,
                1);
    }

    private static Map<TableId, TableChange> schemas(TableId tableId, Table table) {
        Map<TableId, TableChange> schemas = new HashMap<>();
        schemas.put(tableId, new TableChange(TableChangeType.CREATE, table));
        return schemas;
    }

    /** The schema of a table as the snapshot phase discovers it (read from MySQL via JDBC). */
    private static Table table(TableId tableId) {
        return Table.editor()
                .tableId(tableId)
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

    /**
     * {@code include.schema.changes} stays at its production default (off): the record must be
     * enqueued and reach the emitter anyway, which is the whole point of the handler change.
     */
    private static KafkaJsonSourceConfig config() {
        return new KafkaJsonSourceConfigFactory()
                .hostname("localhost")
                .username("root")
                .password("x")
                .databaseList("test")
                .tableList("test.users")
                .kafkaBootstrapServers("bootstrap")
                .kafkaTopics("t")
                .serverTimeZone("UTC")
                .create(0);
    }

    /** A fetch task that reads nothing, so the fetcher's executor has somewhere to sit. */
    private static final class IdleFetchTask implements FetchTask<SourceSplitBase> {

        private final StreamSplit split;
        private final CountDownLatch closed = new CountDownLatch(1);

        private IdleFetchTask(StreamSplit split) {
            this.split = split;
        }

        @Override
        public void execute(FetchTask.Context context) throws Exception {
            closed.await();
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public SourceSplitBase getSplit() {
            return split;
        }

        @Override
        public void close() {
            closed.countDown();
        }
    }
}
