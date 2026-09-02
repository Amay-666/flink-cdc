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
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordinator.KafkaJsonSchemaDerivation;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordinator.KafkaJsonSchemaManager;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordinator.KafkaJsonSchemaRegistryRequestHandler;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordinator.OldSchemaAwareMetadataApplier;
import org.apache.flink.cdc.runtime.operators.schema.event.CoordinationResponseUtils;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeResultResponse;
import org.apache.flink.metrics.groups.OperatorCoordinatorMetricGroup;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link KafkaJsonSchemaRegistryRequestHandler}, driving the blocking state machine
 * directly (no operator harness): a schema change request is accepted, upstream data is blocked
 * until every sink subtask flushes, the DDL is applied through the {@link MetadataApplier}, and the
 * finished events are handed back. Covers the standard events, the connector's custom {@link
 * RenameTableEvent}, and the busy-queuing of a concurrent request.
 */
public class KafkaJsonSchemaRegistryRequestHandlerTest {

    private static final TableId ORDERS = TableId.tableId("shop", "orders");
    private static final TableId ORDERS_V2 = TableId.tableId("shop", "orders_v2");
    private static final Schema SCHEMA =
            Schema.newBuilder().physicalColumn("id", DataTypes.INT()).primaryKey("id").build();

    private final KafkaJsonSchemaManager manager = new KafkaJsonSchemaManager();
    private final TestCoordinatorContext context = new TestCoordinatorContext();

    @Test
    public void testStandardCreateTableBlocksUntilAllFlush() throws Exception {
        RecordingMetadataApplier applier = new RecordingMetadataApplier();
        try (KafkaJsonSchemaRegistryRequestHandler handler = handler(applier)) {
            handler.registerSinkWriter(0);
            handler.registerSinkWriter(1);

            SchemaChangeResponse response =
                    send(
                            handler,
                            new SchemaChangeRequest(
                                    ORDERS, new CreateTableEvent(ORDERS, SCHEMA), 0));
            assertThat(response.isAccepted()).isTrue();
            assertThat(response.getSchemaChangeEvents())
                    .containsExactly(new CreateTableEvent(ORDERS, SCHEMA));

            // Only one of the two subtasks has flushed: the DDL must not be applied yet.
            handler.flushSuccess(ORDERS, 0, 2);
            assertThat(applier.applied).isEmpty();

            // The last flush unlocks the DDL application.
            handler.flushSuccess(ORDERS, 1, 2);
            SchemaChangeResultResponse result = awaitResult(handler);
            assertThat(result.getFinishedSchemaChangeEvents())
                    .containsExactly(new CreateTableEvent(ORDERS, SCHEMA));
            assertThat(applier.applied).containsExactly(new CreateTableEvent(ORDERS, SCHEMA));
            assertThat(manager.getLatestOriginalSchema(ORDERS)).contains(SCHEMA);
            assertThat(manager.getLatestEvolvedSchema(ORDERS)).contains(SCHEMA);
        }
    }

    @Test
    public void testBusyRequestQueuedWhileFlushing() throws Exception {
        RecordingMetadataApplier applier = new RecordingMetadataApplier();
        try (KafkaJsonSchemaRegistryRequestHandler handler = handler(applier)) {
            handler.registerSinkWriter(0);
            handler.registerSinkWriter(1);

            // subtask 0's request is accepted; the registry now waits for the flush.
            SchemaChangeRequest first =
                    new SchemaChangeRequest(ORDERS, new CreateTableEvent(ORDERS, SCHEMA), 0);
            assertThat(send(handler, first).isAccepted()).isTrue();

            // A concurrent request from another subtask is answered busy and queued.
            SchemaChangeRequest second =
                    new SchemaChangeRequest(ORDERS_V2, new CreateTableEvent(ORDERS_V2, SCHEMA), 1);
            assertThat(send(handler, second).isRegistryBusy()).isTrue();

            // Complete the first request.
            handler.flushSuccess(ORDERS, 0, 2);
            handler.flushSuccess(ORDERS, 1, 2);
            SchemaChangeResultResponse firstResult = awaitResult(handler);
            assertThat(firstResult.getFinishedSchemaChangeEvents())
                    .containsExactly(new CreateTableEvent(ORDERS, SCHEMA));

            // The queued subtask resends and is now accepted.
            assertThat(send(handler, second).isAccepted()).isTrue();
            handler.flushSuccess(ORDERS_V2, 0, 2);
            handler.flushSuccess(ORDERS_V2, 1, 2);
            SchemaChangeResultResponse secondResult = awaitResult(handler);
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
    public void testCustomRenameTableEventFullFlow() throws Exception {
        RecordingMetadataApplier applier = new RecordingMetadataApplier();
        try (KafkaJsonSchemaRegistryRequestHandler handler = handler(applier)) {
            handler.registerSinkWriter(0);

            SchemaChangeRequest createRequest =
                    new SchemaChangeRequest(ORDERS, new CreateTableEvent(ORDERS, SCHEMA), 0);
            assertThat(send(handler, createRequest).isAccepted()).isTrue();
            handler.flushSuccess(ORDERS, 0, 1);
            SchemaChangeResultResponse createResult = awaitResult(handler);
            assertThat(createResult.getFinishedSchemaChangeEvents())
                    .containsExactly(new CreateTableEvent(ORDERS, SCHEMA));

            // The custom rename event passes through untouched: accepted, applied, reported.
            SchemaChangeRequest renameRequest =
                    new SchemaChangeRequest(
                            ORDERS, new RenameTableEvent(ORDERS, ORDERS_V2, SCHEMA), 0);
            assertThat(send(handler, renameRequest).isAccepted()).isTrue();
            handler.flushSuccess(ORDERS, 0, 1);
            SchemaChangeResultResponse renameResult = awaitResult(handler);
            assertThat(renameResult.getFinishedSchemaChangeEvents())
                    .containsExactly(new RenameTableEvent(ORDERS, ORDERS_V2, SCHEMA));

            // The metadata applier saw the custom event, and the manager now knows the renamed
            // table while keeping the old table id entry.
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
        // An AlterColumnTypeEvent carries only the new types. An applier implementing
        // OldSchemaAwareMetadataApplier must receive the pre-change schema, resolved from the
        // schema manager before the change is applied.
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
            SchemaChangeRequest createRequest =
                    new SchemaChangeRequest(ORDERS, new CreateTableEvent(ORDERS, v1), 0);
            assertThat(send(handler, createRequest).isAccepted()).isTrue();
            handler.flushSuccess(ORDERS, 0, 1);
            awaitResult(handler);

            AlterColumnTypeEvent alter =
                    new AlterColumnTypeEvent(
                            ORDERS, Collections.singletonMap("name", DataTypes.VARCHAR(300)));
            SchemaChangeRequest alterRequest = new SchemaChangeRequest(ORDERS, alter, 0);
            assertThat(send(handler, alterRequest).isAccepted()).isTrue();
            handler.flushSuccess(ORDERS, 0, 1);
            SchemaChangeResultResponse result = awaitResult(handler);
            assertThat(result.getFinishedSchemaChangeEvents()).containsExactly(alter);

            // The seam handed the applier the pre-change schema: the column was VARCHAR(100) before
            // this event, and the manager has since evolved past it.
            assertThat(applier.applied.get(applier.applied.size() - 1)).isEqualTo(alter);
            assertThat(applier.oldSchema).isPresent();
            assertThat(applier.oldSchema.get().getColumn("name"))
                    .hasValueSatisfying(
                            column ->
                                    assertThat(column.getType()).isEqualTo(DataTypes.VARCHAR(100)));
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
            KafkaJsonSchemaRegistryRequestHandler handler, SchemaChangeRequest request)
            throws Exception {
        CompletableFuture<CoordinationResponse> future = new CompletableFuture<>();
        handler.handleSchemaChangeRequest(request, future);
        return CoordinationResponseUtils.unwrap(future.get());
    }

    /** Polls {@code getSchemaChangeResult} until the request reaches {@code FINISHED}. */
    private static SchemaChangeResultResponse awaitResult(
            KafkaJsonSchemaRegistryRequestHandler handler) throws Exception {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
        while (System.currentTimeMillis() < deadline) {
            CompletableFuture<CoordinationResponse> future = new CompletableFuture<>();
            handler.getSchemaChangeResult(future);
            CoordinationResponse response = CoordinationResponseUtils.unwrap(future.get());
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

    /**
     * A {@link MetadataApplier} that additionally implements {@link OldSchemaAwareMetadataApplier}
     * and records the pre-change schema an {@link AlterColumnTypeEvent} was applied against.
     */
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

    /** A minimal {@link OperatorCoordinator.Context} stub; only {@code failJob} is meaningful. */
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
