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

import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.kafkajson.event.BatchCreateTableEvents;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordination.KafkaJsonSchemaChangeRequest;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordination.KafkaJsonSchemaChangeResultRequest;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordination.SchemaChangeResponse;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordinator.KafkaJsonSchemaDerivation;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordinator.KafkaJsonSchemaManager;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordinator.KafkaJsonSchemaRegistryRequestHandler;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordinator.OldSchemaAwareMetadataApplier;
import org.apache.flink.cdc.runtime.operators.schema.event.CoordinationResponseUtils;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeResultResponse;
import org.apache.flink.metrics.groups.OperatorCoordinatorMetricGroup;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link KafkaJsonSchemaRegistryRequestHandler}, driving the blocking state machine
 * directly (no operator harness): a schema change request is accepted, upstream data is blocked
 * until every sink subtask flushes, the DDL is applied through the {@link MetadataApplier}, and the
 * finished events are handed back. Covers the standard events, the connector's custom {@link
 * RenameTableEvent}, and the busy-queuing of a concurrent request.
 *
 * <p>Each request and the flush acknowledgment it waits for are matched by a nonce (FLINK-36690),
 * so a flush from an earlier request can no longer release a later one.
 */
public class KafkaJsonSchemaRegistryRequestHandlerTest {

    private static final TableId ORDERS = TableId.tableId("shop", "orders");
    private static final TableId ORDERS_V2 = TableId.tableId("shop", "orders_v2");
    private static final Schema SCHEMA =
            Schema.newBuilder().physicalColumn("id", DataTypes.INT()).primaryKey("id").build();

    private final KafkaJsonSchemaManager manager = new KafkaJsonSchemaManager();
    private final TestCoordinatorContext context = new TestCoordinatorContext();
    private final AtomicLong nonceCounter = new AtomicLong(0);

    private long nextNonce() {
        return nonceCounter.incrementAndGet();
    }

    @Test
    public void testStandardCreateTableBlocksUntilAllFlush() throws Exception {
        RecordingMetadataApplier applier = new RecordingMetadataApplier();
        try (KafkaJsonSchemaRegistryRequestHandler handler = handler(applier)) {
            handler.registerSinkWriter(0);
            handler.registerSinkWriter(1);

            long nonce = nextNonce();
            KafkaJsonSchemaChangeRequest request =
                    new KafkaJsonSchemaChangeRequest(
                            ORDERS, new CreateTableEvent(ORDERS, SCHEMA), 0, nonce);

            // Only one of the two subtasks has flushed: the request must not be accepted yet.
            handler.flushSuccess(0, nonce);
            SchemaChangeResponse partialFlushResponse = send(handler, request);
            assertThat(partialFlushResponse.isWaitingForFlush()).isTrue();
            assertThat(applier.applied).isEmpty();

            // The last flush unlocks the request.
            handler.flushSuccess(1, nonce);
            SchemaChangeResponse response = send(handler, request);
            assertThat(response.isAccepted()).isTrue();

            SchemaChangeResultResponse result = awaitResult(handler, nonce);
            assertThat(result.getFinishedSchemaChangeEvents())
                    .containsExactly(new CreateTableEvent(ORDERS, SCHEMA));
            assertThat(applier.applied).containsExactly(new CreateTableEvent(ORDERS, SCHEMA));
            assertThat(manager.getLatestOriginalSchema(ORDERS)).contains(SCHEMA);
            assertThat(manager.getLatestEvolvedSchema(ORDERS)).contains(SCHEMA);
        }
    }

    @Test
    public void testConcurrentRequestsTrackedByNonce() throws Exception {
        RecordingMetadataApplier applier = new RecordingMetadataApplier();
        try (KafkaJsonSchemaRegistryRequestHandler handler = handler(applier)) {
            handler.registerSinkWriter(0);
            handler.registerSinkWriter(1);

            // Request 1: both writers flush for nonce₁ → accepted.
            long firstNonce = nextNonce();
            KafkaJsonSchemaChangeRequest first =
                    new KafkaJsonSchemaChangeRequest(
                            ORDERS, new CreateTableEvent(ORDERS, SCHEMA), 0, firstNonce);
            handler.flushSuccess(0, firstNonce);
            handler.flushSuccess(1, firstNonce);
            assertThat(send(handler, first).isAccepted()).isTrue();
            SchemaChangeResultResponse firstResult = awaitResult(handler, firstNonce);
            assertThat(firstResult.getFinishedSchemaChangeEvents())
                    .containsExactly(new CreateTableEvent(ORDERS, SCHEMA));

            // Request 2: only one writer has flushed for nonce₂ → waiting.
            long secondNonce = nextNonce();
            KafkaJsonSchemaChangeRequest second =
                    new KafkaJsonSchemaChangeRequest(
                            ORDERS_V2, new CreateTableEvent(ORDERS_V2, SCHEMA), 1, secondNonce);
            handler.flushSuccess(0, secondNonce);
            assertThat(send(handler, second).isWaitingForFlush()).isTrue();

            // The remaining writer flushes for nonce₂ → the resend is now accepted.
            handler.flushSuccess(1, secondNonce);
            assertThat(send(handler, second).isAccepted()).isTrue();
            SchemaChangeResultResponse secondResult = awaitResult(handler, secondNonce);
            assertThat(secondResult.getFinishedSchemaChangeEvents())
                    .containsExactly(new CreateTableEvent(ORDERS_V2, SCHEMA));

            assertThat(applier.applied)
                    .containsExactly(
                            new CreateTableEvent(ORDERS, SCHEMA),
                            new CreateTableEvent(ORDERS_V2, SCHEMA));
            assertThat(context.failures).isEmpty();
        }
    }

    @Test
    public void testFinishedRoundReleasesItsFlushBookkeeping() throws Exception {
        RecordingMetadataApplier applier = new RecordingMetadataApplier();
        try (KafkaJsonSchemaRegistryRequestHandler handler = handler(applier)) {
            handler.registerSinkWriter(0);

            long nonce = nextNonce();
            handler.flushSuccess(0, nonce);
            assertThat(
                            send(
                                            handler,
                                            new KafkaJsonSchemaChangeRequest(
                                                    ORDERS,
                                                    new CreateTableEvent(ORDERS, SCHEMA),
                                                    0,
                                                    nonce))
                                    .isAccepted())
                    .isTrue();
            awaitResult(handler, nonce);

            // The finished round released its flush record, so reusing its nonce waits for a
            // flush of its own instead of being released by the stale set.
            SchemaChangeResponse response =
                    send(
                            handler,
                            new KafkaJsonSchemaChangeRequest(
                                    ORDERS_V2, new CreateTableEvent(ORDERS_V2, SCHEMA), 0, nonce));
            assertThat(response.isWaitingForFlush()).isTrue();
            assertThat(applier.applied).containsExactly(new CreateTableEvent(ORDERS, SCHEMA));
        }
    }

    @Test
    public void testCustomRenameTableEventFullFlow() throws Exception {
        RecordingMetadataApplier applier = new RecordingMetadataApplier();
        try (KafkaJsonSchemaRegistryRequestHandler handler = handler(applier)) {
            handler.registerSinkWriter(0);

            long createNonce = nextNonce();
            KafkaJsonSchemaChangeRequest createRequest =
                    new KafkaJsonSchemaChangeRequest(
                            ORDERS, new CreateTableEvent(ORDERS, SCHEMA), 0, createNonce);
            handler.flushSuccess(0, createNonce);
            assertThat(send(handler, createRequest).isAccepted()).isTrue();
            SchemaChangeResultResponse createResult = awaitResult(handler, createNonce);
            assertThat(createResult.getFinishedSchemaChangeEvents())
                    .containsExactly(new CreateTableEvent(ORDERS, SCHEMA));

            // The custom rename event passes through untouched: accepted, applied, reported.
            long renameNonce = nextNonce();
            KafkaJsonSchemaChangeRequest renameRequest =
                    new KafkaJsonSchemaChangeRequest(
                            ORDERS,
                            new RenameTableEvent(ORDERS, ORDERS_V2, SCHEMA),
                            0,
                            renameNonce);
            handler.flushSuccess(0, renameNonce);
            assertThat(send(handler, renameRequest).isAccepted()).isTrue();
            SchemaChangeResultResponse renameResult = awaitResult(handler, renameNonce);
            assertThat(renameResult.getFinishedSchemaChangeEvents())
                    .containsExactly(new RenameTableEvent(ORDERS, ORDERS_V2, SCHEMA));

            assertThat(applier.applied)
                    .containsExactly(
                            new CreateTableEvent(ORDERS, SCHEMA),
                            new RenameTableEvent(ORDERS, ORDERS_V2, SCHEMA));
            assertThat(manager.getLatestOriginalSchema(ORDERS_V2)).contains(SCHEMA);
            assertThat(manager.getLatestOriginalSchema(ORDERS)).contains(SCHEMA);
            assertThat(context.failures).isEmpty();
        }
    }

    @Test
    public void testAlterColumnTypePassesPreChangeSchemaToCapableApplier() throws Exception {
        OldSchemaAwareRecordingMetadataApplier applier =
                new OldSchemaAwareRecordingMetadataApplier();
        try (KafkaJsonSchemaRegistryRequestHandler handler = handler(applier)) {
            handler.registerSinkWriter(0);

            Schema v1 =
                    Schema.newBuilder()
                            .physicalColumn("id", DataTypes.INT())
                            .physicalColumn("name", DataTypes.VARCHAR(100))
                            .primaryKey("id")
                            .build();
            long createNonce = nextNonce();
            KafkaJsonSchemaChangeRequest createRequest =
                    new KafkaJsonSchemaChangeRequest(
                            ORDERS, new CreateTableEvent(ORDERS, v1), 0, createNonce);
            handler.flushSuccess(0, createNonce);
            assertThat(send(handler, createRequest).isAccepted()).isTrue();
            awaitResult(handler, createNonce);

            AlterColumnTypeEvent alter =
                    new AlterColumnTypeEvent(
                            ORDERS, Collections.singletonMap("name", DataTypes.VARCHAR(300)));
            long alterNonce = nextNonce();
            KafkaJsonSchemaChangeRequest alterRequest =
                    new KafkaJsonSchemaChangeRequest(ORDERS, alter, 0, alterNonce);
            handler.flushSuccess(0, alterNonce);
            assertThat(send(handler, alterRequest).isAccepted()).isTrue();
            SchemaChangeResultResponse result = awaitResult(handler, alterNonce);
            assertThat(result.getFinishedSchemaChangeEvents()).containsExactly(alter);

            assertThat(applier.applied.get(applier.applied.size() - 1)).isEqualTo(alter);
            assertThat(applier.oldSchema).isPresent();
            assertThat(applier.oldSchema.get().getColumn("name"))
                    .hasValueSatisfying(
                            column ->
                                    assertThat(column.getType()).isEqualTo(DataTypes.VARCHAR(100)));
            assertThat(context.failures).isEmpty();
        }
    }

    @Test
    public void testCreateTableBatchAppliesEveryTableInOneRound() throws Exception {
        RecordingMetadataApplier applier = new RecordingMetadataApplier();
        try (KafkaJsonSchemaRegistryRequestHandler handler = handler(applier)) {
            handler.registerSinkWriter(0);

            CreateTableEvent orders = new CreateTableEvent(ORDERS, SCHEMA);
            CreateTableEvent ordersV2 = new CreateTableEvent(ORDERS_V2, SCHEMA);
            BatchCreateTableEvents batch =
                    new BatchCreateTableEvents(Arrays.asList(orders, ordersV2));

            // One flush acknowledges the whole batch, not one per table.
            long nonce = nextNonce();
            handler.flushSuccess(0, nonce);
            SchemaChangeResponse response =
                    send(
                            handler,
                            new KafkaJsonSchemaChangeRequest(
                                    BatchCreateTableEvents.BATCH_TABLE_ID, batch, 0, nonce));
            assertThat(response.isAccepted()).isTrue();
            assertThat(response.getSchemaChangeEvents()).containsExactly(orders, ordersV2);

            SchemaChangeResultResponse result = awaitResult(handler, nonce);
            assertThat(result.getFinishedSchemaChangeEvents()).containsExactly(orders, ordersV2);
            assertThat(applier.applied).containsExactly(orders, ordersV2);
            assertThat(manager.getLatestOriginalSchema(ORDERS)).contains(SCHEMA);
            assertThat(manager.getLatestEvolvedSchema(ORDERS_V2)).contains(SCHEMA);
            assertThat(context.failures).isEmpty();
        }
    }

    @Test
    public void testCreateTableBatchOnReplayAppliesOnlyMissingTables() throws Exception {
        RecordingMetadataApplier applier = new RecordingMetadataApplier();
        try (KafkaJsonSchemaRegistryRequestHandler handler = handler(applier)) {
            handler.registerSinkWriter(0);

            CreateTableEvent orders = new CreateTableEvent(ORDERS, SCHEMA);
            CreateTableEvent ordersV2 = new CreateTableEvent(ORDERS_V2, SCHEMA);

            // The first table is created on its own — the state a restart replaying a half-applied
            // batch would find.
            long createNonce = nextNonce();
            handler.flushSuccess(0, createNonce);
            send(handler, new KafkaJsonSchemaChangeRequest(ORDERS, orders, 0, createNonce));
            awaitResult(handler, createNonce);

            // The replayed batch is still one request; only the table the coordinator has not seen
            // yet is applied and reported back.
            long batchNonce = nextNonce();
            handler.flushSuccess(0, batchNonce);
            SchemaChangeResponse replay =
                    send(
                            handler,
                            new KafkaJsonSchemaChangeRequest(
                                    BatchCreateTableEvents.BATCH_TABLE_ID,
                                    new BatchCreateTableEvents(Arrays.asList(orders, ordersV2)),
                                    0,
                                    batchNonce));
            assertThat(replay.isAccepted()).isTrue();
            assertThat(replay.getSchemaChangeEvents()).containsExactly(ordersV2);
            SchemaChangeResultResponse result = awaitResult(handler, batchNonce);
            assertThat(result.getFinishedSchemaChangeEvents()).containsExactly(ordersV2);
            assertThat(applier.applied).containsExactly(orders, ordersV2);

            // A batch whose tables are all known is answered without applying anything.
            long knownNonce = nextNonce();
            handler.flushSuccess(0, knownNonce);
            SchemaChangeResponse onlyKnown =
                    send(
                            handler,
                            new KafkaJsonSchemaChangeRequest(
                                    BatchCreateTableEvents.BATCH_TABLE_ID,
                                    new BatchCreateTableEvents(Arrays.asList(orders, ordersV2)),
                                    0,
                                    knownNonce));
            assertThat(onlyKnown.isDuplicate()).isTrue();
            assertThat(applier.applied).containsExactly(orders, ordersV2);
            assertThat(context.failures).isEmpty();
        }
    }

    private KafkaJsonSchemaRegistryRequestHandler handler(MetadataApplier applier) {
        return new KafkaJsonSchemaRegistryRequestHandler(
                applier,
                manager,
                new KafkaJsonSchemaDerivation(),
                SchemaChangeBehavior.EVOLVE,
                context);
    }

    private static SchemaChangeResponse send(
            KafkaJsonSchemaRegistryRequestHandler handler, KafkaJsonSchemaChangeRequest request)
            throws Exception {
        CompletableFuture<CoordinationResponse> future = new CompletableFuture<>();
        handler.handleSchemaChangeRequest(request, future);
        return CoordinationResponseUtils.unwrap(future.get());
    }

    private static SchemaChangeResultResponse awaitResult(
            KafkaJsonSchemaRegistryRequestHandler handler, long nonce) throws Exception {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
        while (System.currentTimeMillis() < deadline) {
            CompletableFuture<CoordinationResponse> future = new CompletableFuture<>();
            handler.getSchemaChangeResult(new KafkaJsonSchemaChangeResultRequest(nonce), future);
            CoordinationResponse response = CoordinationResponseUtils.unwrap(future.get());
            if (response instanceof SchemaChangeResultResponse) {
                return (SchemaChangeResultResponse) response;
            }
            Thread.sleep(5);
        }
        throw new IllegalStateException("Timed out waiting for the schema change result");
    }

    private static class RecordingMetadataApplier implements MetadataApplier {
        final List<SchemaChangeEvent> applied = new ArrayList<>();

        @Override
        public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
            applied.add(schemaChangeEvent);
        }
    }

    private static class OldSchemaAwareRecordingMetadataApplier
            implements MetadataApplier, OldSchemaAwareMetadataApplier {
        final List<SchemaChangeEvent> applied = new ArrayList<>();
        Optional<Schema> oldSchema = Optional.empty();

        @Override
        public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
            applied.add(schemaChangeEvent);
        }

        @Override
        public void applyAlterColumnType(AlterColumnTypeEvent event, Optional<Schema> oldSchema) {
            applied.add(event);
            this.oldSchema = oldSchema;
        }
    }

    private static class TestCoordinatorContext implements OperatorCoordinator.Context {
        final List<Throwable> failures = new ArrayList<>();

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
            return 1;
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
