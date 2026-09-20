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

import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.commit.DorisCommittable;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.commit.DorisCommittableSerializer;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.commit.DorisCommitter;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.state.DorisWriterState;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.state.DorisWriterStateSerializer;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.writer.TwoPhaseDorisSink;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.writer.TwoPhaseDorisSinkWriter;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.DorisSinkFixtures.FakeInitContext;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer.RecordedRequest;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer.Response;

import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit test for the {@code sink.writer=stateful-2pc} writer: the labels it derives, the
 * transactions it hands to the committer, and what it does when a label is already taken.
 */
public class TwoPhaseDorisSinkWriterTest {

    private static final long RESTORED_CHECKPOINT = 7L;

    @Test
    public void testLabelNamesTableSubtaskEpochAndRung() throws Exception {
        try (MockDorisServer server = precommitServer()) {
            TwoPhaseDorisSinkWriter writer = writer(server);
            writer.write(DorisSinkFixtures.createOrdersEvent(), null);
            writer.write(DorisSinkFixtures.insertEvent(1), null);
            writer.flush(false);
            writer.write(DorisSinkFixtures.insertEvent(2), null);
            writer.flush(false);

            assertThat(server.recorded).hasSize(2);
            // Restored from checkpoint 7, so the rows written now belong to checkpoint 8; the rung
            // counts the batches within that epoch, so the second flush is not labelled the same.
            assertThat(labelOf(server, 0)).isEqualTo("cdc_shop_orders_0_8_0");
            assertThat(labelOf(server, 1)).isEqualTo("cdc_shop_orders_0_8_1");
            assertThat(server.recorded.get(0).path).isEqualTo("/api/shop/orders/_stream_load");
            assertThat(server.recorded.get(0).headers).containsEntry("two_phase_commit", "true");
            writer.close();
        }
    }

    @Test
    public void testFreshJobStartsAtTheInitialCheckpointEpoch() throws Exception {
        try (MockDorisServer server = precommitServer()) {
            TwoPhaseDorisSinkWriter writer = writer(server, new FakeInitContext(null, 3, null));
            writer.write(DorisSinkFixtures.createOrdersEvent(), null);
            writer.write(DorisSinkFixtures.insertEvent(1), null);
            writer.flush(false);

            assertThat(labelOf(server, 0)).isEqualTo("cdc_shop_orders_3_1_0");
            writer.close();
        }
    }

    @Test
    public void testSnapshotMovesTheEpochAndDropsTheRungs() throws Exception {
        try (MockDorisServer server = precommitServer()) {
            TwoPhaseDorisSinkWriter writer = writer(server);
            writer.write(DorisSinkFixtures.createOrdersEvent(), null);
            writer.write(DorisSinkFixtures.insertEvent(1), null);
            writer.flush(false);
            writer.write(DorisSinkFixtures.insertEvent(2), null);
            writer.flush(false);
            assertThat(labelOf(server, 1)).isEqualTo("cdc_shop_orders_0_8_1");

            writer.snapshotState(8);
            writer.write(DorisSinkFixtures.insertEvent(3), null);
            writer.flush(false);

            // The checkpoint that just ended was 8, so the rows after it belong to 9, and the rung
            // count starts over: reusing a rung within an epoch was the collision risk.
            assertThat(labelOf(server, 2)).isEqualTo("cdc_shop_orders_0_9_0");
            writer.close();
        }
    }

    @Test
    public void testPrepareCommitHandsOverTheEpochTransactions() throws Exception {
        try (MockDorisServer server = precommitServer()) {
            TwoPhaseDorisSinkWriter writer = writer(server);
            writer.write(DorisSinkFixtures.createOrdersEvent(), null);
            writer.write(DorisSinkFixtures.insertEvent(1), null);
            writer.flush(false);
            writer.write(DorisSinkFixtures.insertEvent(2), null);
            writer.flush(false);

            Collection<DorisCommittable> committables = writer.prepareCommit();

            assertThat(committables).hasSize(2);
            assertThat(committables)
                    .extracting(DorisCommittable::getTransactionId)
                    .containsExactly(3001L, 3002L);
            assertThat(committables).extracting(DorisCommittable::getDatabase).containsOnly("shop");
            assertThat(committables).extracting(DorisCommittable::getTable).containsOnly("orders");
            writer.close();
        }
    }

    @Test
    public void testPrepareCommitIsEmptyAfterTheSnapshot() throws Exception {
        try (MockDorisServer server = precommitServer()) {
            TwoPhaseDorisSinkWriter writer = writer(server);
            writer.write(DorisSinkFixtures.createOrdersEvent(), null);
            writer.write(DorisSinkFixtures.insertEvent(1), null);
            writer.flush(false);
            assertThat(writer.prepareCommit()).hasSize(1);

            writer.snapshotState(8);

            // The committer owns them from here: handing them over twice would commit a transaction
            // that is already committed, and the epoch they belong to is closed.
            assertThat(writer.prepareCommit()).isEmpty();
            writer.close();
        }
    }

    @Test
    public void testPrecommittedLabelCollisionIsAbortedAndTheLabelReused() throws Exception {
        AtomicInteger precommits = new AtomicInteger();
        try (MockDorisServer server =
                new MockDorisServer(
                        request -> {
                            if (isTwoPhaseOperation(request)) {
                                return Response.ok("{\"status\":\"Success\"}");
                            }
                            return precommits.getAndIncrement() == 0
                                    ? labelAlreadyExists("PRECOMMITTED", 4001L)
                                    : success(4002L);
                        })) {
            TwoPhaseDorisSinkWriter writer = writer(server);
            writer.write(DorisSinkFixtures.createOrdersEvent(), null);
            writer.write(DorisSinkFixtures.insertEvent(1), null);

            writer.flush(false);

            // Three requests: the collision, the abort of the transaction holding the label, and
            // the
            // load under the very same label — Doris frees a label when its transaction is aborted,
            // which is what makes a restart recover without asking Doris anything.
            assertThat(server.recorded).hasSize(3);
            assertThat(labelOf(server, 0)).isEqualTo("cdc_shop_orders_0_8_0");
            assertThat(server.recorded.get(1).path).isEqualTo("/api/shop/_stream_load_2pc");
            assertThat(server.recorded.get(1).headers)
                    .containsEntry("txn_operation", "abort")
                    .containsEntry("txn_id", "4001");
            assertThat(labelOf(server, 2)).isEqualTo("cdc_shop_orders_0_8_0");
            assertThat(writer.prepareCommit())
                    .extracting(DorisCommittable::getTransactionId)
                    .containsExactly(4002L);
            writer.close();
        }
    }

    @Test
    public void testFinishedLabelCollisionFailsAndNamesTheOption() throws Exception {
        try (MockDorisServer server =
                new MockDorisServer(request -> labelAlreadyExists("FINISHED", 4001L))) {
            TwoPhaseDorisSinkWriter writer = writer(server);
            writer.write(DorisSinkFixtures.createOrdersEvent(), null);
            writer.write(DorisSinkFixtures.insertEvent(1), null);

            assertThatThrownBy(() -> writer.flush(false))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("FINISHED")
                    .hasMessageContaining(DorisDataSinkOptions.LABEL_PREFIX.key());
            writer.close();
        }
    }

    @Test
    public void testLabelCollisionWithoutATransactionIdFails() throws Exception {
        try (MockDorisServer server =
                new MockDorisServer(
                        request ->
                                Response.ok(
                                        "{\"Status\":\"Label Already Exists\","
                                                + "\"ExistingJobStatus\":\"PRECOMMITTED\","
                                                + "\"Message\":\"Label [x] has already been used\"}"))) {
            TwoPhaseDorisSinkWriter writer = writer(server);
            writer.write(DorisSinkFixtures.createOrdersEvent(), null);
            writer.write(DorisSinkFixtures.insertEvent(1), null);

            // Nothing to abort without a transaction id, and retrying would only hide it.
            assertThatThrownBy(() -> writer.flush(false))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("cannot clear");
            writer.close();
        }
    }

    @Test
    public void testCloseDoesNotFlush() throws Exception {
        try (MockDorisServer server = precommitServer()) {
            TwoPhaseDorisSinkWriter writer = writer(server);
            writer.write(DorisSinkFixtures.createOrdersEvent(), null);
            writer.write(DorisSinkFixtures.insertEvent(1), null);

            writer.close();

            // A flush here would open a transaction nothing will ever commit: the committer only
            // commits what a checkpoint emitted, and a closing subtask is not taking one.
            assertThat(server.recorded).isEmpty();
        }
    }

    @Test
    public void testRestoredWriterContinuesTheCounterFromTheState() throws Exception {
        try (MockDorisServer server = precommitServer()) {
            DorisDataSinkOptions options = DorisSinkFixtures.options(server);
            long aboveTheWallClock = System.currentTimeMillis() * 1_000_000L + 5_000_000_000L;
            TwoPhaseDorisSink sink = new TwoPhaseDorisSink(options, DorisSinkFixtures.ZONE);
            TwoPhaseDorisSinkWriter writer =
                    sink.restoreWriter(
                            new FakeInitContext(null, 0, RESTORED_CHECKPOINT),
                            java.util.Collections.singletonList(
                                    new DorisWriterState("cdc", aboveTheWallClock)));

            List<DorisWriterState> state = writer.snapshotState(RESTORED_CHECKPOINT + 1);

            assertThat(state.get(0).getSequenceCounter()).isEqualTo(aboveTheWallClock + 1);
            writer.close();
        }
    }

    @Test
    public void testTheSinkIsBothStatefulAndTwoPhaseCommitting() throws Exception {
        try (MockDorisServer server = precommitServer()) {
            DorisDataSinkOptions options = DorisSinkFixtures.options(server);
            TwoPhaseDorisSink sink = new TwoPhaseDorisSink(options, DorisSinkFixtures.ZONE);

            assertThat(sink).isInstanceOf(StatefulSink.class);
            assertThat(sink).isInstanceOf(TwoPhaseCommittingSink.class);
            assertThat(sink.getWriterStateSerializer())
                    .isInstanceOf(DorisWriterStateSerializer.class);
            assertThat(sink.createCommitter()).isInstanceOf(DorisCommitter.class);

            // Declared on the sink itself, not inherited: the released composer finds the
            // committable
            // serializer with Class#getDeclaredMethod, which does not look at superclasses — a sink
            // that inherited it would fail to build the committer topology at all.
            assertThat(TwoPhaseDorisSink.class.getDeclaredMethod("getCommittableSerializer"))
                    .isNotNull();

            DorisCommittableSerializer serializer = new DorisCommittableSerializer();
            DorisCommittable committable = new DorisCommittable("shop", "orders", "cdc_l", 3001L);
            DorisCommittable restored =
                    serializer.deserialize(
                            serializer.getVersion(), serializer.serialize(committable));
            assertThat(restored.getDatabase()).isEqualTo("shop");
            assertThat(restored.getTable()).isEqualTo("orders");
            assertThat(restored.getLabel()).isEqualTo("cdc_l");
            assertThat(restored.getTransactionId()).isEqualTo(3001L);
        }
    }

    /** Every pre-commit succeeds with a fresh transaction id; every abort succeeds. */
    private static MockDorisServer precommitServer() throws IOException {
        AtomicLong transactionId = new AtomicLong(3000L);
        return new MockDorisServer(
                request -> {
                    if (isTwoPhaseOperation(request)) {
                        return Response.ok("{\"status\":\"Success\"}");
                    }
                    return success(transactionId.incrementAndGet());
                });
    }

    private static Response success(long transactionId) {
        return Response.ok(
                "{\"Status\":\"Success\",\"TxnId\":"
                        + transactionId
                        + ",\"TwoPhaseCommit\":\"true\"}");
    }

    private static Response labelAlreadyExists(String jobStatus, long transactionId) {
        return Response.ok(
                "{\"Status\":\"Label Already Exists\",\"ExistingJobStatus\":\""
                        + jobStatus
                        + "\",\"TxnId\":-1,\"Message\":\"Label [cdc_shop_orders_0_8_0] has already"
                        + " been used, relate to txn ["
                        + transactionId
                        + "], status ["
                        + jobStatus
                        + "].\"}");
    }

    private static boolean isTwoPhaseOperation(RecordedRequest request) {
        return request.path.endsWith("_stream_load_2pc");
    }

    private static String labelOf(MockDorisServer server, int index) {
        return server.recorded.get(index).headers.get("label");
    }

    private static TwoPhaseDorisSinkWriter writer(MockDorisServer server) throws IOException {
        return writer(server, new FakeInitContext(null, 0, RESTORED_CHECKPOINT));
    }

    private static TwoPhaseDorisSinkWriter writer(
            MockDorisServer server, FakeInitContext initContext) throws IOException {
        return new TwoPhaseDorisSink(DorisSinkFixtures.options(server), DorisSinkFixtures.ZONE)
                .createWriter(initContext);
    }
}
