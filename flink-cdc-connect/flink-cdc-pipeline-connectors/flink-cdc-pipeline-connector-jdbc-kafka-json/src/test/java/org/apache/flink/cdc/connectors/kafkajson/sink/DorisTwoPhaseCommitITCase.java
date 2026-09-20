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

package org.apache.flink.cdc.connectors.kafkajson.sink;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.kafkajson.example.DorisSinkExample;
import org.apache.flink.cdc.connectors.kafkajson.serializer.KafkaJsonEventSerializer;
import org.apache.flink.cdc.connectors.kafkajson.serializer.KafkaJsonEventTypeInfo;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer.RecordedRequest;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer.Response;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the {@code sink.writer} modes of the sink topology, without Docker: the real
 * {@link KafkaJsonDataSinkBuilder} chain runs on a MiniCluster against a mock Doris that models
 * pre-committed transactions, so the test can tell rows that are merely <em>written</em> from rows
 * that are <em>visible</em>.
 *
 * <p>The mock answers a pre-commit with a transaction id and keeps its rows to itself until a
 * {@code txn_operation=commit} arrives, which is the whole point of the two-phase mode: what the
 * write path sent is invisible until the checkpoint covering it completes. Three things are
 * asserted across the three modes:
 *
 * <ul>
 *   <li>{@code stateful} never touches the two-phase endpoints — its writer is not a {@link
 *       org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink}, so no committer is even
 *       built, and the rows are visible on the load's own return as before.
 *   <li>{@code stateful-2pc} pre-commits rows that stay invisible while no checkpoint completes.
 *   <li>{@code stateful-2pc} with checkpointing makes them visible, once, after the checkpoints.
 * </ul>
 *
 * <p>Everything runs at parallelism 1: the test asserts on exact row sets, and a single-subtask
 * source is the only way to know that each row was emitted once — a two-subtask {@code
 * SourceFunction} emits the whole sequence per subtask. The per-subtask parts of a label are
 * covered by the writer unit tests, which drive the subtask id directly.
 */
public class DorisTwoPhaseCommitITCase {

    private static final TableId ORDERS = TableId.tableId("shop", "orders");
    private static final int ROWS = 6;

    /** The values {@code sink.writer} accepts; see {@code DorisDataSinkOptions#getWriterMode}. */
    private static final String STATEFUL = "stateful";

    private static final String STATEFUL_2PC = "stateful-2pc";

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    /**
     * The single-phase stateful mode: the rows go in with a plain StreamLoad and are visible as
     * soon as it returns. Nothing in this mode may call the two-phase endpoints — the writer never
     * emits a committable, and a committer attached by mistake would have nothing to commit.
     */
    @Test(timeout = 120_000)
    public void testStatefulWriterLoadsWithoutTwoPhaseCommit() throws Exception {
        MockDoris doris = new MockDoris();
        try (MockDorisServer server = new MockDorisServer(doris)) {
            StreamExecutionEnvironment env = environment();
            env.enableCheckpointing(200);
            buildSink(env, server, source(env), STATEFUL);

            JobClient job = env.executeAsync("doris-stateful-itcase");
            await("the rows to be loaded", job, () -> doris.visibleIds().size() >= ROWS, 60_000);
            job.cancel().get(30, TimeUnit.SECONDS);

            assertThat(doris.twoPhaseRequests()).isEmpty();
            assertThat(doris.visibleIds()).containsExactlyInAnyOrderElementsOf(expectedIds());
            assertThat(doris.plainLoads())
                    .allSatisfy(
                            load -> {
                                assertThat(load.headers).doesNotContainKey("two_phase_commit");
                                assertThat(load.headers.get("label"))
                                        .startsWith("cdc_shop_orders_");
                            });
        }
    }

    /**
     * Two-phase commit without checkpointing: the rows are pre-committed — the write path really
     * sent them — and then stay invisible for as long as the job runs, because a transaction is
     * only committed by the committer of a completed checkpoint. This is the mode's dependency
     * stated plainly, and a reminder that a job configured this way would load into nowhere until
     * Doris timed the transactions out.
     */
    @Test(timeout = 120_000)
    public void testPrecommittedRowsStayInvisibleWithoutCheckpoints() throws Exception {
        MockDoris doris = new MockDoris();
        try (MockDorisServer server = new MockDorisServer(doris)) {
            StreamExecutionEnvironment env = environment();
            // No checkpointing at all.
            buildSink(env, server, source(env), STATEFUL_2PC);

            JobClient job = env.executeAsync("doris-2pc-invisible-itcase");
            await(
                    "every row to be pre-committed",
                    job,
                    () -> doris.precommits().size() >= ROWS,
                    60_000);
            // Long enough that a commit arriving late would still be caught.
            Thread.sleep(3_000);
            job.cancel().get(30, TimeUnit.SECONDS);

            assertThat(doris.precommits())
                    .allSatisfy(
                            precommit ->
                                    assertThat(precommit.headers)
                                            .containsEntry("two_phase_commit", "true"));
            assertThat(doris.commitRequests()).isEmpty();
            assertThat(doris.visibleIds()).isEmpty();
        }
    }

    /**
     * The working two-phase mode: a checkpoint every 200 ms, each batch pre-committed as it is
     * flushed, and the transactions committed once the checkpoint covering them has completed.
     * Every row ends up visible exactly once — the transaction that carries it was committed, and
     * the transactions of the failed or uncheckpointed attempts were not.
     */
    @Test(timeout = 120_000)
    public void testCheckpointsCommitThePrecommittedTransactions() throws Exception {
        MockDoris doris = new MockDoris();
        try (MockDorisServer server = new MockDorisServer(doris)) {
            StreamExecutionEnvironment env = environment();
            env.enableCheckpointing(200);
            buildSink(env, server, source(env), STATEFUL_2PC);

            JobClient job = env.executeAsync("doris-2pc-itcase");
            await(
                    "every row to become visible",
                    job,
                    () -> doris.visibleIds().size() >= ROWS,
                    60_000);
            job.cancel().get(30, TimeUnit.SECONDS);

            assertThat(doris.visibleIds()).containsExactlyInAnyOrderElementsOf(expectedIds());
            // Each transaction is committed once: a second commit of the same transaction would be
            // answered with "already visible" and would mean two committables were emitted for one
            // batch.
            assertThat(doris.duplicateCommits()).isEmpty();
            assertThat(doris.aborts()).isEmpty();
            assertThat(doris.precommits())
                    .allSatisfy(
                            precommit -> {
                                assertThat(precommit.headers)
                                        .containsEntry("two_phase_commit", "true");
                                assertThat(precommit.headers.get("label"))
                                        .matches("cdc_shop_orders_0_\\d+_\\d+");
                            });
            // The commit of a transaction always follows its pre-commit: the committer only ever
            // receives what a checkpoint emitted.
            assertThat(doris.commitsAfterTheirPrecommit()).isTrue();
        }
    }

    /**
     * The recovery path end to end: a job dies with the transactions of an unfinished checkpoint
     * still open, and the restart finds its own labels taken by them. It has to abort each one and
     * load the replayed rows under the very same label — the transactions of a checkpoint that
     * never completed are the only thing standing between a restarted job and its labels, and
     * clearing them is what makes the restart recover without asking Doris which transaction is
     * whose.
     */
    @Test(timeout = 180_000)
    public void testARestartClearsTheDeadAttemptsOpenTransactions() throws Exception {
        MockDoris doris = new MockDoris();
        try (MockDorisServer server = new MockDorisServer(doris)) {
            StreamExecutionEnvironment env = environment();
            // One restart, a second apart: the replay has to happen, and a second failure is a real
            // one rather than another attempt.
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 1_000L));
            // The failure has to land before the first checkpoint completes, or the attempt's
            // transactions would be committed rather than left open. The coordinator waits a random
            // delay in [min pause, interval) before its first trigger, so with a 9.5 s floor
            // against
            // a 10 s interval no checkpoint can complete during the couple of seconds the first
            // attempt runs.
            env.enableCheckpointing(10_000);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(9_500);
            buildSink(env, server, source(env, true), STATEFUL_2PC);

            JobClient job = env.executeAsync("doris-2pc-restart-itcase");
            await(
                    "the replayed rows to become visible",
                    job,
                    () -> doris.visibleIds().size() >= ROWS,
                    120_000);
            job.cancel().get(30, TimeUnit.SECONDS);

            assertThat(doris.visibleIds()).containsExactlyInAnyOrderElementsOf(expectedIds());
            // Every label of the dead attempt was reused by the replay and cleared before the row
            // was loaded again. Nothing else in this topology aborts a transaction, so this is what
            // proves the path was taken rather than the collision being avoided by chance.
            assertThat(doris.labelReuse()).hasSize(ROWS);
            assertThat(doris.aborts()).hasSize(ROWS);
            assertThat(doris.duplicateCommits()).isEmpty();
        }
    }

    private static StreamExecutionEnvironment environment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        return env;
    }

    private static void buildSink(
            StreamExecutionEnvironment env,
            MockDorisServer server,
            DataStream<Event> source,
            String writerMode) {
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(DorisDataSinkOptions.FENODES, server.endpoint());
        sinkConfig.set(DorisDataSinkOptions.USERNAME, "root");
        sinkConfig.set(DorisDataSinkOptions.PASSWORD, "123456");
        sinkConfig.set(DorisDataSinkOptions.WRITER_MODE, writerMode);
        // One row per batch: every row is flushed — and in the two-phase mode pre-committed — as it
        // arrives, so the test does not have to wait for a buffer threshold to see a batch.
        sinkConfig.set(DorisDataSinkOptions.BUFFER_SIZE, 1);
        sinkConfig.set(DorisDataSinkOptions.FLUSH_INTERVAL, Duration.ZERO);
        DorisSinkExample.buildSink(
                source,
                new DorisDataSinkOptions(sinkConfig),
                Duration.ofSeconds(30),
                SchemaChangeBehavior.EVOLVE,
                "Asia/Shanghai");
    }

    private static DataStream<Event> source(StreamExecutionEnvironment env) {
        return source(env, false);
    }

    private static DataStream<Event> source(StreamExecutionEnvironment env, boolean failOnce) {
        return env.addSource(
                        new IdleAfterTheRowsSource(events(), failOnce),
                        "test-source",
                        new KafkaJsonEventTypeInfo())
                .global();
    }

    private static Set<Integer> expectedIds() {
        Set<Integer> ids = new LinkedHashSet<>();
        for (int id = 1; id <= ROWS; id++) {
            ids.add(id);
        }
        return ids;
    }

    private static List<Event> events() {
        Schema schema =
                Schema.newBuilder()
                        .column(Column.physicalColumn("id", DataTypes.INT()))
                        .column(Column.physicalColumn("name", DataTypes.VARCHAR(64)))
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.INT(), DataTypes.VARCHAR(64)));
        List<Event> events = new ArrayList<>();
        events.add(new CreateTableEvent(ORDERS, schema));
        for (int id = 1; id <= ROWS; id++) {
            events.add(
                    DataChangeEvent.insertEvent(
                            ORDERS,
                            generator.generate(
                                    new Object[] {id, BinaryStringData.fromString("row_" + id)})));
        }
        return events;
    }

    /** Polls {@code condition}, failing as soon as the job itself has failed. */
    private static void await(String what, JobClient job, BooleanSupplier condition, long timeout)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            JobStatus status = job.getJobStatus().get(10, TimeUnit.SECONDS);
            assertThat(status)
                    .as("the job must still be running while waiting for " + what)
                    .isNotEqualTo(JobStatus.FAILED);
            Thread.sleep(100);
        }
        throw new AssertionError("Timed out waiting for " + what);
    }

    /**
     * A mock Doris that keeps pre-committed rows to itself until their transaction is committed, so
     * "written" and "visible" are different states here. It is also the smallest implementation of
     * the protocol the client depends on: a label collision names the transaction holding the
     * label, and the control calls answer with the idempotency messages the client recognises.
     */
    private static final class MockDoris implements Function<RecordedRequest, Response> {

        private static final Pattern ID = Pattern.compile("\"id\":(\\d+)");

        private final Map<String, Long> openByLabel = new LinkedHashMap<>();
        private final Set<String> finishedLabels = new HashSet<>();
        private final Map<Long, List<Integer>> rowsByTransaction = new HashMap<>();
        private final Set<Long> committed = new LinkedHashSet<>();
        private final Set<Long> aborted = new HashSet<>();
        private final Set<Integer> visibleIds = new LinkedHashSet<>();
        private final List<Precommit> precommits = new ArrayList<>();
        private final List<PlainLoad> plainLoads = new ArrayList<>();
        private final List<Long> commitRequests = new ArrayList<>();
        private final List<String> twoPhasePaths = new ArrayList<>();
        private final List<Long> aborts = new ArrayList<>();
        private long nextTransactionId = 7000L;

        @Override
        public synchronized Response apply(RecordedRequest request) {
            if (!"PUT".equals(request.method)) {
                // The metadata applier's DDL.
                return Response.ok("{\"code\":0,\"msg\":\"OK\"}");
            }
            if (request.path.endsWith("_stream_load_2pc")) {
                twoPhasePaths.add(request.path);
                return control(request);
            }
            return load(request);
        }

        private Response load(RecordedRequest request) {
            String label = request.headers.get("label");
            List<Integer> ids = idsOf(request.body);
            if (!"true".equals(request.headers.get("two_phase_commit"))) {
                plainLoads.add(new PlainLoad(request.headers, ids));
                visibleIds.addAll(ids);
                return Response.ok(
                        "{\"Status\":\"Success\",\"NumberLoadedRows\":" + ids.size() + "}");
            }
            if (finishedLabels.contains(label)) {
                return Response.ok(
                        "{\"Status\":\"Label Already Exists\",\"ExistingJobStatus\":\"FINISHED\","
                                + "\"Message\":\"Label ["
                                + label
                                + "] has already been used, relate to txn [1], status"
                                + " [FINISHED].\"}");
            }
            Long open = openByLabel.get(label);
            if (open != null) {
                return Response.ok(
                        "{\"Status\":\"Label Already Exists\",\"ExistingJobStatus\":\"PRECOMMITTED\","
                                + "\"TxnId\":-1,\"Message\":\"Label ["
                                + label
                                + "] has already been used, relate to txn ["
                                + open
                                + "], status [PRECOMMITTED].\"}");
            }
            long transactionId = nextTransactionId++;
            openByLabel.put(label, transactionId);
            rowsByTransaction.put(transactionId, ids);
            precommits.add(new Precommit(request.headers, transactionId, ids));
            return Response.ok(
                    "{\"Status\":\"Success\",\"TxnId\":"
                            + transactionId
                            + ",\"TwoPhaseCommit\":\"true\"}");
        }

        private Response control(RecordedRequest request) {
            long transactionId = Long.parseLong(request.headers.get("txn_id"));
            String operation = request.headers.get("txn_operation");
            if (committed.contains(transactionId)) {
                // Committing twice is what a restart after a recorded-but-unfinished commit looks
                // like; aborting a committed transaction has nothing left to discard.
                return Response.ok(
                        "{\"status\":\"Fail\",\"msg\":\"transaction ["
                                + transactionId
                                + "] is already visible\"}");
            }
            if ("abort".equals(operation)) {
                if (aborted.contains(transactionId)
                        || !rowsByTransaction.containsKey(transactionId)) {
                    return Response.ok(
                            "{\"status\":\"Fail\",\"msg\":\"transaction ["
                                    + transactionId
                                    + "] not found\"}");
                }
                aborted.add(transactionId);
                aborts.add(transactionId);
                openByLabel.values().remove(transactionId);
                return Response.ok("{\"status\":\"Success\"}");
            }
            commitRequests.add(transactionId);
            if (aborted.contains(transactionId) || !rowsByTransaction.containsKey(transactionId)) {
                return Response.ok(
                        "{\"status\":\"Fail\",\"msg\":\"transaction ["
                                + transactionId
                                + "] is already aborted\"}");
            }
            committed.add(transactionId);
            visibleIds.addAll(rowsByTransaction.get(transactionId));
            openByLabel.values().remove(transactionId);
            finishedLabels.add(labelOf(transactionId));
            return Response.ok("{\"status\":\"Success\"}");
        }

        private String labelOf(long transactionId) {
            for (Precommit precommit : precommits) {
                if (precommit.transactionId == transactionId) {
                    return precommit.headers.get("label");
                }
            }
            return "";
        }

        private static List<Integer> idsOf(String body) {
            List<Integer> ids = new ArrayList<>();
            Matcher matcher = ID.matcher(body);
            while (matcher.find()) {
                ids.add(Integer.parseInt(matcher.group(1)));
            }
            return ids;
        }

        synchronized Set<Integer> visibleIds() {
            return new LinkedHashSet<>(visibleIds);
        }

        synchronized List<Precommit> precommits() {
            return new ArrayList<>(precommits);
        }

        synchronized List<PlainLoad> plainLoads() {
            return new ArrayList<>(plainLoads);
        }

        synchronized List<Long> commitRequests() {
            return new ArrayList<>(commitRequests);
        }

        synchronized List<Long> aborts() {
            return new ArrayList<>(aborts);
        }

        /** Transactions that were committed more than once. */
        synchronized Set<Long> duplicateCommits() {
            Set<Long> seen = new HashSet<>();
            Set<Long> duplicates = new LinkedHashSet<>();
            for (Long transactionId : commitRequests) {
                if (!seen.add(transactionId)) {
                    duplicates.add(transactionId);
                }
            }
            return duplicates;
        }

        /** The paths of every control request the sink made. */
        synchronized List<String> twoPhaseRequests() {
            return new ArrayList<>(twoPhasePaths);
        }

        /**
         * The labels that opened a transaction more than once. Doris frees a label only when the
         * transaction holding it ends, so a label in this set was taken by an earlier transaction
         * that has since been aborted — which is what makes it evidence that a collision was
         * cleared rather than avoided.
         */
        synchronized Set<String> labelReuse() {
            Set<String> seen = new HashSet<>();
            Set<String> reused = new LinkedHashSet<>();
            for (Precommit precommit : precommits) {
                if (!seen.add(precommit.headers.get("label"))) {
                    reused.add(precommit.headers.get("label"));
                }
            }
            return reused;
        }

        /** Whether every commit request came after the pre-commit that opened its transaction. */
        synchronized boolean commitsAfterTheirPrecommit() {
            for (Long transactionId : commitRequests) {
                if (!rowsByTransaction.containsKey(transactionId)) {
                    return false;
                }
            }
            return true;
        }
    }

    /** One pre-commit as the mock saw it. */
    private static final class Precommit {
        private final Map<String, String> headers;
        private final long transactionId;
        private final List<Integer> ids;

        private Precommit(Map<String, String> headers, long transactionId, List<Integer> ids) {
            this.headers = headers;
            this.transactionId = transactionId;
            this.ids = ids;
        }
    }

    /** One single-phase load as the mock saw it. */
    private static final class PlainLoad {
        private final Map<String, String> headers;
        private final List<Integer> ids;

        private PlainLoad(Map<String, String> headers, List<Integer> ids) {
            this.headers = headers;
            this.ids = ids;
        }
    }

    /**
     * Emits a fixed {@link Event} sequence and then idles, so the job keeps running while the test
     * waits for checkpoints and cancels it. Bounded events and an unbounded run are both needed
     * here: the rows have to arrive promptly, and the checkpoints that commit them only happen
     * while the source is alive.
     *
     * <p>{@code failOnce} makes the first attempt die once it has emitted everything, which leaves
     * the rows it wrote in open transactions: it waits long enough for the pipelined channel to
     * deliver them (its buffer timeout is 100 ms) and for the sink to pre-commit each one, then
     * throws. Which attempt is the first is read from the runtime context rather than counted in a
     * static field, because the counter would live in the task manager's copy of this class while
     * the test would be resetting its own.
     *
     * <p>The events are pre-serialized to {@code byte[]} with the pipeline's own serializer, as in
     * {@code KafkaJsonDdlBlockingITCase}: the CDC events are not Java-serializable, and holding
     * only bytes keeps this source shippable to the task managers.
     */
    private static class IdleAfterTheRowsSource extends RichSourceFunction<Event> {
        private static final long serialVersionUID = 1L;

        private final List<byte[]> serializedEvents;
        private final boolean failOnce;
        private final KafkaJsonEventSerializer serializer = KafkaJsonEventSerializer.INSTANCE;
        private volatile boolean running = true;

        IdleAfterTheRowsSource(List<Event> events, boolean failOnce) {
            this.failOnce = failOnce;
            this.serializedEvents = new ArrayList<>(events.size());
            for (Event event : events) {
                DataOutputSerializer out = new DataOutputSerializer(64);
                try {
                    serializer.serialize(event, out);
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to pre-serialize source event", e);
                }
                serializedEvents.add(out.getCopyOfBuffer());
            }
        }

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            for (byte[] bytes : serializedEvents) {
                if (!running) {
                    return;
                }
                ctx.collect(serializer.deserialize(new DataInputDeserializer(bytes)));
            }
            if (failOnce && getRuntimeContext().getAttemptNumber() == 0) {
                Thread.sleep(2_000);
                throw new RuntimeException("deliberate failure of the first attempt");
            }
            while (running) {
                Thread.sleep(50);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
