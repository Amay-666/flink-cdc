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

package org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.writer;

import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.state.DorisWriterState;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.state.DorisWriterStateSerializer;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.writer.StatefulDorisSink;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.writer.StatefulDorisSinkWriter;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.DorisSinkFixtures;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.DorisSinkFixtures.FakeInitContext;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer;

import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit test for the {@code sink.writer=stateful} writer: the released write behaviour plus the
 * state that makes it restartable. The writer is exercised through its own sink ({@link
 * StatefulDorisSink#createWriter}), so the test covers the pair the way Flink builds it.
 */
public class StatefulDorisSinkWriterTest {

    @Test
    public void testStateRoundTripsThroughTheSerializer() throws IOException {
        DorisWriterStateSerializer serializer = new DorisWriterStateSerializer();
        DorisWriterState state = new DorisWriterState("cdc", 1_234_567_890L);

        assertThat(serializer.getVersion()).isEqualTo(1);
        DorisWriterState restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(state));

        assertThat(restored.getLabelPrefix()).isEqualTo("cdc");
        assertThat(restored.getSequenceCounter()).isEqualTo(1_234_567_890L);
    }

    @Test
    public void testUnknownStateVersionIsRejected() {
        DorisWriterStateSerializer serializer = new DorisWriterStateSerializer();

        assertThatThrownBy(() -> serializer.deserialize(99, new byte[] {0, 0}))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version");
    }

    @Test
    public void testSnapshotCarriesTheCounterAndTheConfiguredLabelPrefix() throws Exception {
        try (MockDorisServer server = DorisSinkFixtures.successServer()) {
            // Sampled before the writer is built: the counter is based on the clock as it was at
            // construction, so a value read afterwards can already be a millisecond ahead.
            long base = wallClockBase();
            StatefulDorisSinkWriter writer = writer(server, new FakeInitContext());

            List<DorisWriterState> state = writer.snapshotState(7);

            assertThat(state).hasSize(1);
            assertThat(state.get(0).getLabelPrefix())
                    .isEqualTo(DorisSinkFixtures.DEFAULT_LABEL_PREFIX);
            // The wall-clock base, since this subtask has written nothing yet.
            assertThat(state.get(0).getSequenceCounter()).isBetween(base, base + 2_000_000L);
            writer.close();
        }
    }

    @Test
    public void testRestoredCounterContinuesAboveTheCheckpointedValue() throws Exception {
        try (MockDorisServer server = DorisSinkFixtures.successServer()) {
            long aboveTheWallClock = wallClockBase() + 5_000_000_000L;
            StatefulDorisSinkWriter writer =
                    writer(
                            server,
                            new FakeInitContext(),
                            Collections.singletonList(
                                    new DorisWriterState(
                                            DorisSinkFixtures.DEFAULT_LABEL_PREFIX,
                                            aboveTheWallClock)));

            List<DorisWriterState> state = writer.snapshotState(7);

            // One above what was checkpointed, which is above the wall clock: a replayed row can
            // never carry a Group Commit sequence smaller than the row it has to overwrite.
            assertThat(state.get(0).getSequenceCounter()).isEqualTo(aboveTheWallClock + 1);
            writer.close();
        }
    }

    @Test
    public void testRestoreFallsBackToTheWallClockWhenTheStateIsBehind() throws Exception {
        try (MockDorisServer server = DorisSinkFixtures.successServer()) {
            long base = wallClockBase();
            StatefulDorisSinkWriter writer =
                    writer(
                            server,
                            new FakeInitContext(),
                            Collections.singletonList(
                                    new DorisWriterState(
                                            DorisSinkFixtures.DEFAULT_LABEL_PREFIX, 5L)));

            List<DorisWriterState> state = writer.snapshotState(7);

            // A state whose value is far below the clock comes from a life that wrote with a
            // counter
            // the state never captured (an older savepoint, say). The counter moves up to the clock
            // rather than staying at 6, where a replayed row would lose to what Doris already
            // holds.
            assertThat(state.get(0).getSequenceCounter()).isGreaterThanOrEqualTo(base);
            writer.close();
        }
    }

    @Test
    public void testRestoreTakesTheHighestOfSeveralRedistributedStates() throws Exception {
        try (MockDorisServer server = DorisSinkFixtures.successServer()) {
            long highest = wallClockBase() + 5_000_000_000L;
            // A rescale hands one subtask the states of several old subtasks.
            StatefulDorisSinkWriter writer =
                    writer(
                            server,
                            new FakeInitContext(),
                            java.util.Arrays.asList(
                                    new DorisWriterState("cdc", 10L),
                                    new DorisWriterState("cdc", highest),
                                    new DorisWriterState("cdc", 30L)));

            List<DorisWriterState> state = writer.snapshotState(7);

            assertThat(state.get(0).getSequenceCounter()).isEqualTo(highest + 1);
            writer.close();
        }
    }

    /**
     * The counter starts at {@code System.currentTimeMillis() * 1_000_000}, so a state value
     * derived from this base plus a margin is above it, while a fixed constant that looks large may
     * not be: the base is around 1.8e18 in 2026.
     */
    private static long wallClockBase() {
        return System.currentTimeMillis() * 1_000_000L;
    }

    @Test
    public void testStateIsReadableAfterClose() throws Exception {
        try (MockDorisServer server = DorisSinkFixtures.successServer()) {
            StatefulDorisSinkWriter writer = writer(server, new FakeInitContext());
            writer.write(DorisSinkFixtures.createOrdersEvent(), null);
            writer.write(DorisSinkFixtures.insertEvent(1), null);
            writer.flush(false);
            writer.close();

            // Flink takes the last snapshot around the same time as close(); it must still answer.
            assertThat(writer.snapshotState(9)).hasSize(1);
        }
    }

    @Test
    public void testTheStatefulWriterStillLoadsWithoutTwoPhaseCommit() throws Exception {
        try (MockDorisServer server = DorisSinkFixtures.successServer()) {
            StatefulDorisSinkWriter writer = writer(server, new FakeInitContext());
            writer.write(DorisSinkFixtures.createOrdersEvent(), null);
            writer.write(DorisSinkFixtures.insertEvent(1), null);

            writer.flush(false);

            // The single-phase writer is the released write path: one plain StreamLoad, no
            // transaction, and a random label rather than a derivable one.
            assertThat(server.recorded).hasSize(1);
            assertThat(server.recorded.get(0).path).isEqualTo("/api/shop/orders/_stream_load");
            assertThat(server.recorded.get(0).headers).doesNotContainKey("two_phase_commit");
            assertThat(server.recorded.get(0).headers.get("label")).startsWith("cdc_shop_orders_");
            writer.close();
        }
    }

    @Test
    public void testFlushSplitsBatchesAboveTheBatchByteCap() throws Exception {
        // Regression for the 2GB ByteArrayBuilder failure: 1024 wide rows serialized as ONE
        // request body exceeded the JVM array limit and killed the flush — and a batch that
        // kills its flush kills the task, repeatedly, because the restart replays into the same
        // oversized batch. A drained batch above the byte cap must land as several loads.
        try (MockDorisServer server = DorisSinkFixtures.successServer()) {
            TableId wide = TableId.tableId("shop", "wide");
            Schema wideSchema =
                    Schema.newBuilder()
                            .physicalColumn("id", DataTypes.INT())
                            .physicalColumn("payload", DataTypes.STRING())
                            .primaryKey("id")
                            .build();
            BinaryRecordDataGenerator generator =
                    new BinaryRecordDataGenerator(
                            RowType.of(DataTypes.INT(), DataTypes.STRING()));
            // Three rows of roughly 430 serialized bytes each under a 1000-byte cap: the first
            // two form one chunk, the third its own.
            String payload = new String(new char[400]).replace('\0', 'x');
            StatefulDorisSinkWriter writer =
                    writer(
                            server,
                            new FakeInitContext(),
                            (Consumer<Configuration>)
                                    config ->
                                            config.set(
                                                    DorisDataSinkOptions.MAX_BATCH_BYTES, 1000L));
            writer.write(new CreateTableEvent(wide, wideSchema), null);
            writer.write(insertEvent(wide, generator, 1, payload), null);
            writer.write(insertEvent(wide, generator, 2, payload), null);
            writer.write(insertEvent(wide, generator, 3, payload), null);

            writer.flush(false);

            assertThat(server.recorded).hasSize(2);
            assertThat(server.recorded.get(0).path).isEqualTo("/api/shop/wide/_stream_load");
            assertThat(server.recorded.get(0).body).contains("xxx");
            // chunk 1 carries two rows, chunk 2 the remaining one
            assertThat(countOccurrences(server.recorded.get(0).body, "\"payload\"")).isEqualTo(2);
            assertThat(countOccurrences(server.recorded.get(1).body, "\"payload\"")).isEqualTo(1);
            // each body stays under the cap (exact sizes, not an estimate)
            assertThat(server.recorded.get(0).body.length()).isLessThanOrEqualTo(1000);
            assertThat(server.recorded.get(1).body.length()).isLessThanOrEqualTo(1000);
            // every chunk carries a unique label: a repeated label would let Doris deduplicate
            // the second request as "Label Already Exists / FINISHED" and silently drop its rows
            String label0 = server.recorded.get(0).headers.get("label");
            String label1 = server.recorded.get(1).headers.get("label");
            assertThat(label0).startsWith("cdc_shop_wide_");
            assertThat(label1).startsWith("cdc_shop_wide_");
            assertThat(label0).isNotEqualTo(label1);
            writer.close();
        }
    }

    private static DataChangeEvent insertEvent(
            TableId tableId, BinaryRecordDataGenerator generator, int id, String payload) {
        BinaryRecordData record =
                generator.generate(new Object[] {id, BinaryStringData.fromString(payload)});
        return DataChangeEvent.insertEvent(tableId, record);
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + 1)) {
            count++;
        }
        return count;
    }

    @Test
    public void testCloseFlushesBufferedRowsInSinglePhaseMode() throws Exception {
        try (MockDorisServer server = DorisSinkFixtures.successServer()) {
            StatefulDorisSinkWriter writer = writer(server, new FakeInitContext());
            writer.write(DorisSinkFixtures.createOrdersEvent(), null);
            writer.write(DorisSinkFixtures.insertEvent(1), null);
            assertThat(server.recorded).isEmpty();

            writer.close();

            // An extra load only makes rows visible sooner, so this writer still flushes on close.
            assertThat(server.recorded).hasSize(1);
        }
    }

    private static StatefulDorisSinkWriter writer(
            MockDorisServer server, FakeInitContext initContext) throws IOException {
        return writer(server, initContext, Collections.emptyList());
    }

    private static StatefulDorisSinkWriter writer(
            MockDorisServer server,
            FakeInitContext initContext,
            java.util.Collection<DorisWriterState> states)
            throws IOException {
        DorisDataSinkOptions options = DorisSinkFixtures.options(server);
        StatefulDorisSink sink = new StatefulDorisSink(options, DorisSinkFixtures.ZONE);
        return states.isEmpty()
                ? sink.createWriter(initContext)
                : sink.restoreWriter(initContext, states);
    }

    private static StatefulDorisSinkWriter writer(
            MockDorisServer server,
            FakeInitContext initContext,
            Consumer<Configuration> tune)
            throws IOException {
        DorisDataSinkOptions options = DorisSinkFixtures.options(server, tune);
        StatefulDorisSink sink = new StatefulDorisSink(options, DorisSinkFixtures.ZONE);
        return sink.createWriter(initContext);
    }
}
