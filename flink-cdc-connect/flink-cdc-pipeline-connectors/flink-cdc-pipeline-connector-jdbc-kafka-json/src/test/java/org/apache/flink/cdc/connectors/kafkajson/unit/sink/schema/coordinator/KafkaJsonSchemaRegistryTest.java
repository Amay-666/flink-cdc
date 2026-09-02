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

package org.apache.flink.cdc.connectors.kafkajson.unit.sink.schema.coordinator;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordinator.KafkaJsonSchemaRegistry;
import org.apache.flink.cdc.runtime.operators.schema.event.CoordinationResponseUtils;
import org.apache.flink.cdc.runtime.operators.schema.event.FlushSuccessEvent;
import org.apache.flink.cdc.runtime.operators.schema.event.GetEvolvedSchemaRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.GetEvolvedSchemaResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.GetOriginalSchemaRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.GetOriginalSchemaResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeResultRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeResultResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.SinkWriterRegisterEvent;
import org.apache.flink.metrics.groups.OperatorCoordinatorMetricGroup;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link KafkaJsonSchemaRegistry}, driving the coordinator over its own event loop.
 *
 * <p>Whereas the request-handler test talks to the state machine directly, this test goes through
 * the registry's {@link OperatorCoordinator} surface — request/event dispatch, the {@code
 * GetEvolvedSchema}/{@code GetOriginalSchema} responses the partitioning operator relies on, and
 * the checkpoint/restore round-trip of the schema manager.
 */
public class KafkaJsonSchemaRegistryTest {

    private static final TableId ORDERS = TableId.tableId("shop", "orders");
    private static final TableId ORDERS_V2 = TableId.tableId("shop", "orders_v2");
    private static final Schema SCHEMA =
            Schema.newBuilder().physicalColumn("id", DataTypes.INT()).primaryKey("id").build();

    private final TestCoordinatorContext context = new TestCoordinatorContext(1);
    private final TestCoordinatorContext twoSubtaskContext = new TestCoordinatorContext(2);
    private final RecordingMetadataApplier applier = new RecordingMetadataApplier();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @Test
    public void testCreateTableBlocksUntilAllSubtaskFlush() throws Exception {
        try (KafkaJsonSchemaRegistry registry = registry(twoSubtaskContext)) {
            registry.start();
            register(registry, 0);
            register(registry, 1);

            SchemaChangeResponse response =
                    send(
                            registry,
                            new SchemaChangeRequest(
                                    ORDERS, new CreateTableEvent(ORDERS, SCHEMA), 0));
            assertThat(response.isAccepted()).isTrue();

            // One flush is not enough: the DDL stays blocked.
            flush(registry, 0);
            assertThat(applier.applied).isEmpty();

            // The last flush unlocks the DDL.
            flush(registry, 1);
            SchemaChangeResultResponse result = pollResult(registry);
            assertThat(result.getFinishedSchemaChangeEvents())
                    .containsExactly(new CreateTableEvent(ORDERS, SCHEMA));
            assertThat(applier.applied).containsExactly(new CreateTableEvent(ORDERS, SCHEMA));

            // The partitioning operator can now pull the schemas it needs to rebuild hash
            // functions.
            GetEvolvedSchemaResponse evolved =
                    send(
                            registry,
                            new GetEvolvedSchemaRequest(
                                    ORDERS, GetEvolvedSchemaRequest.LATEST_SCHEMA_VERSION));
            assertThat(evolved.getSchema()).contains(SCHEMA);
            GetOriginalSchemaResponse original =
                    send(
                            registry,
                            new GetOriginalSchemaRequest(
                                    ORDERS, GetOriginalSchemaRequest.LATEST_SCHEMA_VERSION));
            assertThat(original.getSchema()).contains(SCHEMA);
            assertThat(context.failures).isEmpty();
        }
    }

    @Test
    public void testCustomRenameTableEventThroughRegistry() throws Exception {
        try (KafkaJsonSchemaRegistry registry = registry(context)) {
            registry.start();
            register(registry, 0);

            send(
                    registry,
                    new SchemaChangeRequest(ORDERS, new CreateTableEvent(ORDERS, SCHEMA), 0));
            flush(registry, 0);
            SchemaChangeResultResponse createResult = pollResult(registry);
            assertThat(createResult.getFinishedSchemaChangeEvents())
                    .containsExactly(new CreateTableEvent(ORDERS, SCHEMA));

            // The custom rename event survives the registry dispatch and is applied.
            SchemaChangeResponse renameResponse =
                    send(
                            registry,
                            new SchemaChangeRequest(
                                    ORDERS, new RenameTableEvent(ORDERS, ORDERS_V2, SCHEMA), 0));
            assertThat(renameResponse.isAccepted()).isTrue();
            flush(registry, 0);
            SchemaChangeResultResponse renameResult = pollResult(registry);
            assertThat(renameResult.getFinishedSchemaChangeEvents())
                    .containsExactly(new RenameTableEvent(ORDERS, ORDERS_V2, SCHEMA));
            assertThat(applier.applied)
                    .containsExactly(
                            new CreateTableEvent(ORDERS, SCHEMA),
                            new RenameTableEvent(ORDERS, ORDERS_V2, SCHEMA));
            assertThat(registry.getSchemaManager().get().getLatestOriginalSchema(ORDERS_V2))
                    .contains(SCHEMA);
            assertThat(context.failures).isEmpty();
        }
    }

    @Test
    public void testCheckpointRoundTripRestoresSchemaManager() throws Exception {
        byte[] checkpoint;
        try (KafkaJsonSchemaRegistry registry = registry(context)) {
            registry.start();
            register(registry, 0);

            send(
                    registry,
                    new SchemaChangeRequest(ORDERS, new CreateTableEvent(ORDERS, SCHEMA), 0));
            flush(registry, 0);
            pollResult(registry);

            CompletableFuture<byte[]> result = new CompletableFuture<>();
            registry.checkpointCoordinator(1L, result);
            checkpoint = result.get(10, TimeUnit.SECONDS);
        }

        // A fresh registry (own executor, since the first one was shut down on close) restores the
        // manager from the checkpoint bytes.
        try (KafkaJsonSchemaRegistry restored =
                new KafkaJsonSchemaRegistry(
                        "schema-operator",
                        context,
                        Executors.newSingleThreadExecutor(),
                        applier,
                        SchemaChangeBehavior.EVOLVE)) {
            restored.resetToCheckpoint(1L, checkpoint);
            assertThat(restored.getSchemaManager().get().getLatestOriginalSchema(ORDERS))
                    .contains(SCHEMA);

            // The restored manager considers the create event redundant (already applied).
            SchemaChangeResponse response =
                    send(
                            restored,
                            new SchemaChangeRequest(
                                    ORDERS, new CreateTableEvent(ORDERS, SCHEMA), 0));
            assertThat(response.isDuplicate()).isTrue();
        }
    }

    private KafkaJsonSchemaRegistry registry(TestCoordinatorContext ctx) {
        return new KafkaJsonSchemaRegistry(
                "schema-operator", ctx, executor, applier, SchemaChangeBehavior.EVOLVE);
    }

    private void register(KafkaJsonSchemaRegistry registry, int subtask) throws Exception {
        registry.handleEventFromOperator(subtask, 0, new SinkWriterRegisterEvent(subtask));
        barrier();
    }

    private void flush(KafkaJsonSchemaRegistry registry, int subtask) throws Exception {
        registry.handleEventFromOperator(subtask, 0, new FlushSuccessEvent(subtask, ORDERS));
        barrier();
    }

    /** Serializes the executor so previously submitted events are guaranteed processed. */
    private void barrier() throws Exception {
        executor.submit(() -> {}).get(10, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private static <T extends CoordinationResponse> T send(
            KafkaJsonSchemaRegistry registry, CoordinationRequest request) throws Exception {
        return CoordinationResponseUtils.unwrap(
                registry.handleCoordinationRequest(request).get(10, TimeUnit.SECONDS));
    }

    /** Polls the registry until the schema change result arrives. */
    private static SchemaChangeResultResponse pollResult(KafkaJsonSchemaRegistry registry)
            throws Exception {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
        while (System.currentTimeMillis() < deadline) {
            CompletableFuture<CoordinationResponse> future =
                    registry.handleCoordinationRequest(new SchemaChangeResultRequest());
            CoordinationResponse response =
                    CoordinationResponseUtils.unwrap(future.get(10, TimeUnit.SECONDS));
            if (response instanceof SchemaChangeResultResponse) {
                return (SchemaChangeResultResponse) response;
            }
            Thread.sleep(5);
        }
        throw new IllegalStateException("Timed out waiting for the schema change result");
    }

    /** A {@link MetadataApplier} that records the events handed to it. */
    private static class RecordingMetadataApplier implements MetadataApplier {
        final List<SchemaChangeEvent> applied = new ArrayList<>();

        @Override
        public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
            applied.add(schemaChangeEvent);
        }
    }

    /** A minimal {@link OperatorCoordinator.Context} stub; only {@code failJob} is meaningful. */
    private static class TestCoordinatorContext implements OperatorCoordinator.Context {
        private final int parallelism;
        final List<Throwable> failures = new ArrayList<>();

        TestCoordinatorContext(int parallelism) {
            this.parallelism = parallelism;
        }

        @Override
        public OperatorID getOperatorId() {
            return new OperatorID();
        }

        @Override
        public OperatorCoordinatorMetricGroup metricGroup() {
            return null;
        }

        @Override
        public void failJob(Throwable cause) {
            failures.add(cause);
        }

        @Override
        public int currentParallelism() {
            return parallelism;
        }

        @Override
        public ClassLoader getUserCodeClassloader() {
            return Thread.currentThread().getContextClassLoader();
        }

        @Override
        public CoordinatorStore getCoordinatorStore() {
            return null;
        }

        @Override
        public boolean isConcurrentExecutionAttemptsSupported() {
            return false;
        }
    }
}
