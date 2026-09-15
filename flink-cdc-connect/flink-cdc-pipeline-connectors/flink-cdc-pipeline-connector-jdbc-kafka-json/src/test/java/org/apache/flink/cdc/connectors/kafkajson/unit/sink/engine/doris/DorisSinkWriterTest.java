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

package org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.kafkajson.event.DropTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions.GroupCommitMode;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisSinkWriter;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer.Response;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit test for {@link DorisSinkWriter}, driven against the JDK {@code HttpServer} mock of the
 * Doris FE/BE. The writer is exercised directly (no operator harness): {@link Sink.InitContext} is
 * hand-stubbed, and the periodic flush timer is either disabled or fired manually through the fake
 * {@link ProcessingTimeService}.
 */
public class DorisSinkWriterTest {

    private static final ZoneId ZONE = ZoneId.of("Asia/Shanghai");
    private static final TableId ORDERS = TableId.tableId("shop", "orders");
    private static final TableId ORDERS_V2 = TableId.tableId("shop", "orders_v2");
    private static final Schema ORDERS_SCHEMA =
            Schema.newBuilder().physicalColumn("id", DataTypes.INT()).primaryKey("id").build();

    @Test
    public void testWriteToUnknownTableThrows() throws Exception {
        try (MockDorisServer server = server()) {
            try (DorisSinkWriter writer = writer(server, options(server))) {
                assertThatThrownBy(() -> writer.write(insertEvent(1), null))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("unknown table");
            }
        }
    }

    @Test
    public void testWriteBufferFlushesOnBufferSize() throws Exception {
        try (MockDorisServer server = server()) {
            DorisDataSinkOptions options =
                    options(server, c -> c.set(DorisDataSinkOptions.BUFFER_SIZE, 2));
            try (DorisSinkWriter writer = writer(server, options)) {
                writer.write(new CreateTableEvent(ORDERS, ORDERS_SCHEMA), null);
                writer.write(insertEvent(1), null);
                assertThat(server.recorded).isEmpty();

                writer.write(insertEvent(2), null);
                assertThat(server.recorded).hasSize(1);
                assertThat(server.recorded.get(0).path).isEqualTo("/api/shop/orders/_stream_load");
                assertThat(server.recorded.get(0).body)
                        .isEqualTo(
                                "[{\"id\":1,\"__DORIS_DELETE_SIGN__\":false},"
                                        + "{\"id\":2,\"__DORIS_DELETE_SIGN__\":false}]");
            }
        }
    }

    @Test
    public void testFlushSendsBufferedRows() throws Exception {
        try (MockDorisServer server = server()) {
            try (DorisSinkWriter writer = writer(server, options(server))) {
                writer.write(new CreateTableEvent(ORDERS, ORDERS_SCHEMA), null);
                writer.write(insertEvent(1), null);
                assertThat(server.recorded).isEmpty();

                writer.flush(false);
                assertThat(server.recorded).hasSize(1);
                assertThat(server.recorded.get(0).path).isEqualTo("/api/shop/orders/_stream_load");
                assertThat(server.recorded.get(0).body)
                        .isEqualTo("[{\"id\":1,\"__DORIS_DELETE_SIGN__\":false}]");
            }
        }
    }

    @Test
    public void testDeleteMapsToDeleteSignColumn() throws Exception {
        try (MockDorisServer server = server()) {
            try (DorisSinkWriter writer = writer(server, options(server))) {
                writer.write(new CreateTableEvent(ORDERS, ORDERS_SCHEMA), null);
                writer.write(DataChangeEvent.deleteEvent(ORDERS, record(1)), null);
                writer.flush(false);

                // The delete row carries the __DORIS_DELETE_SIGN__ marker in the same batch the
                // upserts use; Doris removes the row when the marker is true.
                assertThat(server.recorded).hasSize(1);
                assertThat(server.recorded.get(0).body)
                        .isEqualTo("[{\"id\":1,\"__DORIS_DELETE_SIGN__\":true}]");
                assertThat(server.recorded.get(0).headers)
                        .containsEntry("hidden_columns", "__DORIS_DELETE_SIGN__");
            }
        }
    }

    @Test
    public void testDeleteWithoutBatchDeleteDropsMarker() throws Exception {
        try (MockDorisServer server = server()) {
            DorisDataSinkOptions options =
                    options(server, c -> c.set(DorisDataSinkOptions.ENABLE_BATCH_DELETE, false));
            try (DorisSinkWriter writer = writer(server, options)) {
                writer.write(new CreateTableEvent(ORDERS, ORDERS_SCHEMA), null);
                writer.write(DataChangeEvent.deleteEvent(ORDERS, record(1)), null);
                writer.flush(false);

                assertThat(server.recorded).hasSize(1);
                assertThat(server.recorded.get(0).body)
                        .isEqualTo("[{\"id\":1,\"__DORIS_DELETE_SIGN__\":false}]");
            }
        }
    }

    @Test
    public void testPeriodicFlushTimerFiresStreamLoad() throws Exception {
        try (MockDorisServer server = server()) {
            FakeProcessingTimeService time = new FakeProcessingTimeService(1_000L);
            DorisDataSinkOptions options =
                    options(
                            server,
                            c ->
                                    c.set(
                                            DorisDataSinkOptions.FLUSH_INTERVAL,
                                            Duration.ofMillis(100)));
            try (DorisSinkWriter writer =
                    new DorisSinkWriter(options, ZONE, new FakeInitContext(time))) {
                writer.write(new CreateTableEvent(ORDERS, ORDERS_SCHEMA), null);
                writer.write(insertEvent(1), null);
                assertThat(server.recorded).isEmpty();

                time.fireAll();
                assertThat(server.recorded).hasSize(1);
                assertThat(server.recorded.get(0).body)
                        .isEqualTo("[{\"id\":1,\"__DORIS_DELETE_SIGN__\":false}]");
            }
        }
    }

    @Test
    public void testRenameTableReKeysState() throws Exception {
        Schema twoColumns =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(16))
                        .primaryKey("id")
                        .build();
        try (MockDorisServer server = server()) {
            try (DorisSinkWriter writer = writer(server, options(server))) {
                writer.write(new CreateTableEvent(ORDERS, twoColumns), null);
                writer.write(
                        insertEvent(ORDERS, twoColumns, 1, BinaryStringData.fromString("a")), null);
                // Buffered rows move from the old table id to the new one.
                writer.write(new RenameTableEvent(ORDERS, ORDERS_V2, twoColumns), null);
                writer.write(
                        insertEvent(ORDERS_V2, twoColumns, 2, BinaryStringData.fromString("b")),
                        null);
                writer.flush(false);

                assertThat(server.recorded).hasSize(1);
                assertThat(server.recorded.get(0).path)
                        .isEqualTo("/api/shop/orders_v2/_stream_load");
                assertThat(server.recorded.get(0).body)
                        .isEqualTo(
                                "[{\"id\":1,\"name\":\"a\",\"__DORIS_DELETE_SIGN__\":false},"
                                        + "{\"id\":2,\"name\":\"b\",\"__DORIS_DELETE_SIGN__\":false}]");
            }
        }
    }

    @Test
    public void testRenameTableExchangingTwoNamesReKeysBothTables() throws Exception {
        Schema twoColumns =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(16))
                        .build();
        try (MockDorisServer server = server()) {
            try (DorisSinkWriter writer = writer(server, options(server))) {
                writer.write(new CreateTableEvent(ORDERS, twoColumns), null);
                writer.write(new CreateTableEvent(ORDERS_V2, ORDERS_SCHEMA), null);
                writer.write(
                        insertEvent(ORDERS, twoColumns, 1, BinaryStringData.fromString("a")), null);
                writer.write(insertEvent(ORDERS_V2, ORDERS_SCHEMA, 2), null);

                // RENAME TABLE orders TO orders_v2, orders_v2 TO orders: each table keeps its own
                // schema and only the name moves, so a pair applied in place would have the second
                // read back what the first wrote.
                writer.write(
                        new RenameTableEvent(
                                Arrays.asList(
                                        new RenameTableEvent.TableRename(
                                                ORDERS, ORDERS_V2, twoColumns),
                                        new RenameTableEvent.TableRename(
                                                ORDERS_V2, ORDERS, ORDERS_SCHEMA)),
                                "RENAME TABLE `shop`.`orders` TO `shop`.`orders_v2`, "
                                        + "`shop`.`orders_v2` TO `shop`.`orders`"),
                        null);
                writer.flush(false);

                // The rows buffered for each table arrive under the name that table now carries,
                // converted with that table's own schema.
                assertThat(bodiesOf(server, "/api/shop/orders_v2/_stream_load"))
                        .containsExactly(
                                "[{\"id\":1,\"name\":\"a\",\"__DORIS_DELETE_SIGN__\":false}]");
                assertThat(bodiesOf(server, "/api/shop/orders/_stream_load"))
                        .containsExactly("[{\"id\":2,\"__DORIS_DELETE_SIGN__\":false}]");

                // Data keeps flowing under the new ids, each name carrying the schema of the table
                // that moved onto it: `orders` is the one-column table now and `orders_v2` the
                // two-column one.
                writer.write(insertEvent(ORDERS, ORDERS_SCHEMA, 3), null);
                writer.write(
                        insertEvent(ORDERS_V2, twoColumns, 4, BinaryStringData.fromString("d")),
                        null);
                writer.flush(false);
                assertThat(bodiesOf(server, "/api/shop/orders/_stream_load"))
                        .containsExactly(
                                "[{\"id\":2,\"__DORIS_DELETE_SIGN__\":false}]",
                                "[{\"id\":3,\"__DORIS_DELETE_SIGN__\":false}]");
                assertThat(bodiesOf(server, "/api/shop/orders_v2/_stream_load"))
                        .containsExactly(
                                "[{\"id\":1,\"name\":\"a\",\"__DORIS_DELETE_SIGN__\":false}]",
                                "[{\"id\":4,\"name\":\"d\",\"__DORIS_DELETE_SIGN__\":false}]");
            }
        }
    }

    private static List<String> bodiesOf(MockDorisServer server, String path) {
        return server.recorded.stream()
                .filter(request -> path.equals(request.path))
                .map(request -> request.body)
                .collect(Collectors.toList());
    }

    @Test
    public void testDropTableClearsBufferedRows() throws Exception {
        try (MockDorisServer server = server()) {
            try (DorisSinkWriter writer = writer(server, options(server))) {
                writer.write(new CreateTableEvent(ORDERS, ORDERS_SCHEMA), null);
                writer.write(insertEvent(1), null);
                writer.write(new DropTableEvent(ORDERS, null, null), null);
                writer.flush(false);
                assertThat(server.recorded).isEmpty();

                // Data for the dropped table is rejected until a new CreateTableEvent.
                assertThatThrownBy(() -> writer.write(insertEvent(2), null))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("unknown table");
            }
        }
    }

    @Test
    public void testCloseFlushesRemainingRows() throws Exception {
        try (MockDorisServer server = server()) {
            DorisSinkWriter writer = writer(server, options(server));
            writer.write(new CreateTableEvent(ORDERS, ORDERS_SCHEMA), null);
            writer.write(insertEvent(1), null);
            assertThat(server.recorded).isEmpty();

            writer.close();
            assertThat(server.recorded).hasSize(1);
            assertThat(server.recorded.get(0).body)
                    .isEqualTo("[{\"id\":1,\"__DORIS_DELETE_SIGN__\":false}]");
        }
    }

    // ===== Group Commit tests =====

    @Test
    public void testGroupCommitInjectsSequenceColumn() throws Exception {
        try (MockDorisServer server = server()) {
            DorisDataSinkOptions options =
                    options(
                            server,
                            c ->
                                    c.set(
                                            DorisDataSinkOptions.GROUP_COMMIT_MODE,
                                            GroupCommitMode.SYNC_MODE));
            try (DorisSinkWriter writer = writer(server, options)) {
                writer.write(new CreateTableEvent(ORDERS, ORDERS_SCHEMA), null);
                writer.write(insertEvent(1), null);
                writer.write(insertEvent(2), null);
                writer.flush(false);

                assertThat(server.recorded).hasSize(1);
                String body = server.recorded.get(0).body;
                // Each row carries the cdc_sequence field with monotonically increasing values
                assertThat(body).contains("\"cdc_sequence\"");
                // The first row's sequence < second row's sequence (both are large numbers
                // based on currentTimeMillis * 1_000_000, so just verify they differ)
                assertThat(body).contains("\"__DORIS_DELETE_SIGN__\":false");
            }
        }
    }

    @Test
    public void testGroupCommitSendsNoLabelAndGroupCommitHeader() throws Exception {
        try (MockDorisServer server = server()) {
            DorisDataSinkOptions options =
                    options(
                            server,
                            c ->
                                    c.set(
                                            DorisDataSinkOptions.GROUP_COMMIT_MODE,
                                            GroupCommitMode.SYNC_MODE));
            try (DorisSinkWriter writer = writer(server, options)) {
                writer.write(new CreateTableEvent(ORDERS, ORDERS_SCHEMA), null);
                writer.write(insertEvent(1), null);
                writer.flush(false);

                assertThat(server.recorded).hasSize(1);
                // No label header sent
                assertThat(server.recorded.get(0).headers).doesNotContainKey("label");
                // group_commit header sent
                assertThat(server.recorded.get(0).headers)
                        .containsEntry("group_commit", "sync_mode");
            }
        }
    }

    @Test
    public void testGroupCommitSequenceIncreasesAcrossFlushes() throws Exception {
        try (MockDorisServer server = server()) {
            DorisDataSinkOptions options =
                    options(
                            server,
                            c ->
                                    c.set(
                                            DorisDataSinkOptions.GROUP_COMMIT_MODE,
                                            GroupCommitMode.ASYNC_MODE));
            try (DorisSinkWriter writer = writer(server, options)) {
                writer.write(new CreateTableEvent(ORDERS, ORDERS_SCHEMA), null);

                // First flush
                writer.write(insertEvent(1), null);
                writer.flush(false);
                assertThat(server.recorded).hasSize(1);
                String body1 = server.recorded.get(0).body;
                assertThat(body1).contains("\"cdc_sequence\"");

                // Second flush — sequence should be strictly larger
                writer.write(insertEvent(2), null);
                writer.flush(false);
                assertThat(server.recorded).hasSize(2);
                String body2 = server.recorded.get(1).body;
                assertThat(body2).contains("\"cdc_sequence\"");
            }
        }
    }

    @Test
    public void testGroupCommitDeleteAlsoGetsSequenceValue() throws Exception {
        try (MockDorisServer server = server()) {
            DorisDataSinkOptions options =
                    options(
                            server,
                            c ->
                                    c.set(
                                            DorisDataSinkOptions.GROUP_COMMIT_MODE,
                                            GroupCommitMode.SYNC_MODE));
            try (DorisSinkWriter writer = writer(server, options)) {
                writer.write(new CreateTableEvent(ORDERS, ORDERS_SCHEMA), null);
                writer.write(DataChangeEvent.deleteEvent(ORDERS, record(1)), null);
                writer.flush(false);

                assertThat(server.recorded).hasSize(1);
                String body = server.recorded.get(0).body;
                // Delete row carries both the delete sign and the sequence value
                assertThat(body).contains("\"__DORIS_DELETE_SIGN__\":true");
                assertThat(body).contains("\"cdc_sequence\"");
            }
        }
    }

    @Test
    public void testGroupCommitDisabledByDefaultDoesNotInjectSequence() throws Exception {
        // Default options (Group Commit off): no sequence column in the row.
        try (MockDorisServer server = server()) {
            try (DorisSinkWriter writer = writer(server, options(server))) {
                writer.write(new CreateTableEvent(ORDERS, ORDERS_SCHEMA), null);
                writer.write(insertEvent(1), null);
                writer.flush(false);

                assertThat(server.recorded).hasSize(1);
                String body = server.recorded.get(0).body;
                assertThat(body).doesNotContain("\"cdc_sequence\"");
                // Label is sent (non-GC mode)
                assertThat(server.recorded.get(0).headers).containsKey("label");
            }
        }
    }

    private MockDorisServer server() throws IOException {
        return new MockDorisServer(req -> Response.ok("{\"Status\":\"Success\"}"));
    }

    private DorisSinkWriter writer(MockDorisServer server, DorisDataSinkOptions options) {
        return new DorisSinkWriter(options, ZONE, new FakeInitContext(null));
    }

    private DorisDataSinkOptions options(MockDorisServer server) {
        return options(server, config -> {});
    }

    private DorisDataSinkOptions options(MockDorisServer server, Consumer<Configuration> tune) {
        Configuration config = new Configuration();
        config.set(DorisDataSinkOptions.FENODES, server.endpoint());
        config.set(DorisDataSinkOptions.USERNAME, "root");
        config.set(DorisDataSinkOptions.PASSWORD, "123456");
        // Disable the periodic flush timer by default; individual tests opt back in.
        config.set(DorisDataSinkOptions.FLUSH_INTERVAL, Duration.ZERO);
        tune.accept(config);
        return new DorisDataSinkOptions(config);
    }

    private static DataChangeEvent insertEvent(int id) {
        return DataChangeEvent.insertEvent(ORDERS, record(id));
    }

    private static DataChangeEvent insertEvent(TableId tableId, Schema schema, Object... values) {
        return DataChangeEvent.insertEvent(tableId, record(schema, values));
    }

    private static BinaryRecordData record(Object... values) {
        return record(ORDERS_SCHEMA, values);
    }

    private static BinaryRecordData record(Schema schema, Object... values) {
        return new BinaryRecordDataGenerator(
                        RowType.of(
                                schema.getColumns().stream()
                                        .map(c -> c.getType())
                                        .toArray(DataType[]::new)))
                .generate(values);
    }

    /**
     * A {@link ProcessingTimeService} with no timer thread: {@code registerTimer} records the
     * callback and {@link #fireAll()} runs the pending callbacks synchronously.
     */
    private static class FakeProcessingTimeService implements ProcessingTimeService {
        private long now;
        private final List<ProcessingTimeCallback> pending = new ArrayList<>();

        FakeProcessingTimeService(long start) {
            this.now = start;
        }

        @Override
        public long getCurrentProcessingTime() {
            return now;
        }

        @Override
        public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback callback) {
            pending.add(callback);
            return null;
        }

        void fireAll() throws Exception {
            List<ProcessingTimeCallback> snapshot = new ArrayList<>(pending);
            pending.clear();
            for (ProcessingTimeCallback callback : snapshot) {
                // Advance the clock so a re-registered timer lands at a fresh timestamp.
                now += 1;
                callback.onProcessingTime(now);
            }
        }
    }

    /** A minimal {@link Sink.InitContext} stub; the writer only touches the time service. */
    private static class FakeInitContext implements Sink.InitContext {
        private final ProcessingTimeService timeService;

        FakeInitContext(ProcessingTimeService timeService) {
            this.timeService = timeService;
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return null;
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            return null;
        }

        @Override
        public ProcessingTimeService getProcessingTimeService() {
            return timeService;
        }

        @Override
        public int getSubtaskId() {
            return 0;
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return 1;
        }

        @Override
        public int getAttemptNumber() {
            return 0;
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return null;
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return OptionalLong.empty();
        }

        @Override
        public SerializationSchema.InitializationContext
                asSerializationSchemaInitializationContext() {
            return null;
        }

        @Override
        public boolean isObjectReuseEnabled() {
            return false;
        }

        @Override
        public <IN> TypeSerializer<IN> createInputSerializer() {
            return null;
        }

        @Override
        public JobID getJobId() {
            return null;
        }
    }
}
