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

import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.dialect.KafkaJsonDialect;
import org.apache.flink.cdc.connectors.kafkajson.source.fetch.KafkaJsonSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.kafkajson.source.handler.KafkaJsonSchemaChangeHandler;
import org.apache.flink.cdc.connectors.kafkajson.source.message.canal.CanalMessageParser;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffsetFactory;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.relational.history.TableChanges.TableChangeType;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for the emission rule of the streaming phase, {@link
 * KafkaJsonIncrementalSourceStreamFetcher#shouldEmit}, and in particular for the case it was
 * extended for: a table whose records no longer match the snapshot splits this split keeps, because
 * the table was renamed after its snapshot.
 *
 * <p>The rule is exercised with the real record path — the converter of the fetch task context
 * turns a canal message into the {@link SourceRecord} the fetcher is handed — and against real
 * split state, so what is asserted is the decision the reader makes in a running job.
 */
class KafkaJsonIncrementalSourceStreamFetcherTest {

    private static final TableId USERS = new TableId("test", null, "users");
    private static final TableId VIP_USERS = new TableId("test", null, "vip_users");

    /** The high watermark of the finished snapshot split of {@code users}. */
    private static final KafkaJsonOffset SPLIT_HIGH_WATERMARK =
            new KafkaJsonOffset(3000, Integer.MAX_VALUE, Long.MAX_VALUE);

    /**
     * The rename happens after the snapshot of {@code users}: the split's finished infos and its
     * watermarks keep the pre-rename name and can never match a record of the table again, while
     * the shared schema — the source of the fallback — knows the table under its new name.
     */
    @Test
    void testRenamedTableIsEmittedInTheSameRun() throws Exception {
        KafkaJsonSourceFetchTaskContext context = context();
        KafkaJsonIncrementalSourceStreamFetcher fetcher =
                fetcher(streamSplit(schemas(USERS, table(USERS))), context);
        try {
            renameToVipUsers(context);

            // the shared schema moved the table to its new name and dropped the old one
            assertNull(context.getDatabaseSchema().tableFor(USERS));
            assertEquals(VIP_USERS, context.getDatabaseSchema().tableFor(VIP_USERS).id());

            // its records are emitted: no split info of this split names the table any more, and
            // the
            // schema store is what settles the decision
            assertTrue(fetcher.shouldEmit(insert(context, "vip_users", 1L, 5000L, 7L)));
        } finally {
            fetcher.close();
        }
    }

    /**
     * The same decision after a failure: the restored split state names the table as the snapshot
     * saw it (the rename happened after it) while its table schemas carry the post-rename name —
     * which is what the schema-change record of the rename put into the split state before it was
     * checkpointed. A restarted job must therefore know the table and keep emitting its records.
     */
    @Test
    void testRenamedTableIsEmittedAfterARestart() throws Exception {
        KafkaJsonSourceFetchTaskContext context = context();
        KafkaJsonIncrementalSourceStreamFetcher fetcher =
                fetcher(streamSplit(schemas(VIP_USERS, table(VIP_USERS))), context);
        try {
            assertTrue(fetcher.shouldEmit(insert(context, "vip_users", 1L, 5000L, 7L)));
        } finally {
            fetcher.close();
        }
    }

    /**
     * The watermark rule still drops what it was written for: a record inside a finished snapshot
     * split whose event time is at or before the split's high watermark was already emitted by that
     * split's backfill, and must not be emitted a second time.
     */
    @Test
    void testRecordInsideASnapshotSplitIsStillDroppedBelowItsHighWatermark() throws Exception {
        KafkaJsonSourceFetchTaskContext context = context();
        KafkaJsonIncrementalSourceStreamFetcher fetcher =
                fetcher(streamSplit(schemas(USERS, table(USERS))), context);
        try {
            // pk 5 lies in the split's range [1, 10) and the event time is below its high watermark
            assertFalse(fetcher.shouldEmit(insert(context, "users", 5L, 2500L, 4L)));
            // the same row after the high watermark is new: the watermark rule lets it through
            assertTrue(fetcher.shouldEmit(insert(context, "users", 5L, 3500L, 5L)));
        } finally {
            fetcher.close();
        }
    }

    /**
     * A record of a table that a DDL removed from the shared schema — {@code DROP TABLE}, or the
     * old name of a renamed table — must not fail the read.
     *
     * <p>The fetch task applies a DDL as it consumes it, while the reader may still be draining
     * records that were converted before it, so this state is reachable in a running job. The
     * watermark rule cannot judge such a record at all: it needs the table's split column to tell
     * whether the record lies inside a snapshot split, and the table's schema is exactly what is
     * gone. Consulting it would throw a {@code NullPointerException} out of the read, and the
     * restarted job would rebuild the same state and fail again.
     */
    @Test
    void testRecordOfATableRemovedFromTheSchemaDoesNotFailTheRead() throws Exception {
        KafkaJsonSourceFetchTaskContext context = context();
        KafkaJsonIncrementalSourceStreamFetcher fetcher =
                fetcher(streamSplit(schemas(USERS, table(USERS))), context);
        try {
            // a record converted while the table still existed, and below its split's high
            // watermark
            // — the record the watermark rule would take the split column from
            SourceRecord inFlight = insert(context, "users", 5L, 2500L, 4L);
            context.getDatabaseSchema().removeTable(USERS);

            assertTrue(fetcher.shouldEmit(inFlight));
        } finally {
            fetcher.close();
        }
    }

    /**
     * Builds a stream split whose finished snapshot split info and table schemas belong to the
     * given table: the state a job that snapshotted {@code users} reads its stream split with.
     */
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
                                SPLIT_HIGH_WATERMARK,
                                new KafkaJsonOffsetFactory())),
                tableSchemas,
                1);
    }

    /**
     * Creates the fetcher and starts it on the given split: the fetcher takes the split's finished
     * snapshot split infos for its watermark maps and registers the split's table schemas in the
     * shared schema — the two things {@code shouldEmit} decides with. The fetch task itself reads
     * nothing; no Kafka consumer is involved.
     */
    private static KafkaJsonIncrementalSourceStreamFetcher fetcher(
            StreamSplit split, KafkaJsonSourceFetchTaskContext context) {
        KafkaJsonIncrementalSourceStreamFetcher fetcher =
                new KafkaJsonIncrementalSourceStreamFetcher(context, 0);
        fetcher.submitTask(new IdleFetchTask(split));
        return fetcher;
    }

    /** Applies a canal {@code RENAME TABLE} message, as the stream fetch task would. */
    private static void renameToVipUsers(KafkaJsonSourceFetchTaskContext context) throws Exception {
        // the announced table is the post-rename name and the pre-rename one appears only in the
        // SQL,
        // which is the shape TiCDC writes (see src/test/resources/kafkajson/captured)
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

    private static Map<TableId, TableChange> schemas(TableId tableId, Table table) {
        Map<TableId, TableChange> schemas = new HashMap<>();
        schemas.put(tableId, new TableChange(TableChangeType.CREATE, table));
        return schemas;
    }

    /**
     * The schema of a table as the snapshot phase discovers it (read from MySQL via JDBC): the
     * converter converts a message of a table registered like this, and the split carries it.
     */
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

    private static KafkaJsonSourceConfig config() {
        return new KafkaJsonSourceConfigFactory()
                .hostname("localhost")
                .username("root")
                .password("x")
                .databaseList("test")
                // The filter names the pre-rename table only, as a job configured before the rename
                // would: the renamed table is outside it, so the decision cannot be taken by the
                // filter and falls to the schema store — which is the case under test.
                .tableList("test.users")
                .kafkaBootstrapServers("bootstrap")
                .kafkaTopics("t")
                .serverTimeZone("UTC")
                .create(0);
    }

    /**
     * A fetch task that reads nothing: the fetcher needs the split (its finished split infos
     * configure the watermark maps) and a task for its executor to sit in until the fetcher closes.
     */
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
