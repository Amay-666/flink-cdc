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

package org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordinator;

import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.connectors.kafkajson.event.BatchCreateTableEvents;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordination.KafkaJsonSchemaChangeRequest;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordination.KafkaJsonSchemaChangeResultRequest;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordination.SchemaChangeResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeProcessingResponse;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.cdc.runtime.operators.schema.event.CoordinationResponseUtils.wrap;

/**
 * A handler to deal with all requests and events for {@link KafkaJsonSchemaRegistry}.
 *
 * <p>This mirrors the released {@code SchemaRegistryRequestHandler} state machine — {@code IDLE →
 * WAITING_FOR_FLUSH → APPLYING → FINISHED} with {@code BUSY} requests queued by subtask — so that a
 * schema change blocks upstream data flow until every sink subtask has flushed, then applies the
 * DDL and lets the data flow resume. Two deviations make the connector's custom events safe: the
 * derivation is a pure pass-through (no {@code lenientize} switch on {@code getType()}), and {@link
 * #applySchemaChange} gates on {@code instanceof CreateTableEvent} instead of {@code getType()} and
 * drops the {@code acceptsSchemaEvolutionType} check — the connector's metadata applier accepts all
 * events. An {@code AlterColumnTypeEvent} additionally carries no old type, so when the applier
 * implements {@link OldSchemaAwareMetadataApplier} the handler resolves the pre-change schema from
 * {@link KafkaJsonSchemaManager} and passes it along.
 */
public class KafkaJsonSchemaRegistryRequestHandler implements Closeable {
    private static final Logger LOG =
            LoggerFactory.getLogger(KafkaJsonSchemaRegistryRequestHandler.class);

    /** The {@link MetadataApplier} for every table. */
    private final MetadataApplier metadataApplier;
    /** All active sink writers. */
    private final Set<Integer> activeSinkWriters;
    /** Schema manager holding schema for all tables. */
    private final KafkaJsonSchemaManager schemaManager;

    private final KafkaJsonSchemaDerivation schemaDerivation;

    /**
     * Atomic flag indicating if current RequestHandler could accept more schema changes for now.
     */
    private volatile RequestStatus schemaChangeStatus;

    private final Object schemaChangeRequestLock;

    private volatile Throwable currentChangeException;
    private volatile List<SchemaChangeEvent> currentFinishedSchemaChanges;
    private volatile List<SchemaChangeEvent> currentIgnoredSchemaChanges;

    /** Sink writers which have sent flush success events for the request. */
    private final ConcurrentHashMap<Long, Set<Integer>> flushedSinkWriters;

    /** Executor service to execute schema change. */
    private final ExecutorService schemaChangeThreadPool;

    private final SchemaChangeBehavior schemaChangeBehavior;

    private final OperatorCoordinator.Context context;

    private final int parallelism;

    public KafkaJsonSchemaRegistryRequestHandler(
            MetadataApplier metadataApplier,
            KafkaJsonSchemaManager schemaManager,
            KafkaJsonSchemaDerivation schemaDerivation,
            SchemaChangeBehavior schemaChangeBehavior,
            OperatorCoordinator.Context context) {
        this.metadataApplier = metadataApplier;
        this.schemaManager = schemaManager;
        this.schemaDerivation = schemaDerivation;
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.context = context;

        this.activeSinkWriters = ConcurrentHashMap.newKeySet();
        this.flushedSinkWriters = new ConcurrentHashMap<>();
        this.schemaChangeThreadPool = Executors.newSingleThreadExecutor();

        this.currentFinishedSchemaChanges = new ArrayList<>();
        this.currentIgnoredSchemaChanges = new ArrayList<>();

        this.schemaChangeStatus = RequestStatus.IDLE;
        this.schemaChangeRequestLock = new Object();

        // This check is meant to allow migration test pass since we don't have a valid
        // `OperatorCoordinator.Context` in mocked environment.
        this.parallelism = context != null ? context.currentParallelism() : 0;
    }

    /**
     * Handle the {@link KafkaJsonSchemaChangeRequest} and wait for all sink subtasks flushing.
     *
     * @param request the received KafkaJsonSchemaChangeRequest
     */
    public void handleSchemaChangeRequest(
            KafkaJsonSchemaChangeRequest request,
            CompletableFuture<CoordinationResponse> response) {

        // We use nonce to identify each schema change request
        long nonce = request.getNonce();

        synchronized (schemaChangeRequestLock) {
            // Make sure we handle the first request in the pending list to avoid out-of-order
            // waiting and blocks checkpointing mechanism.
            if (schemaChangeStatus == RequestStatus.IDLE) {

                SchemaChangeEvent event = request.getSchemaChangeEvent();

                // A create-table batch is one request and one flush, but every table is applied
                // individually. A restart can replay a batch whose tables were partially created
                // already, so the redundant tables are dropped here, one at a time.
                List<CreateTableEvent> batchCreates = new ArrayList<>();
                if (event instanceof BatchCreateTableEvents) {
                    Set<TableId> seenTables = new HashSet<>();
                    for (CreateTableEvent create :
                            ((BatchCreateTableEvents) event).getCreateTableEvents()) {
                        if (seenTables.add(create.tableId())
                                && !schemaManager.isOriginalSchemaChangeEventRedundant(create)) {
                            batchCreates.add(create);
                        }
                    }
                    if (batchCreates.isEmpty()) {
                        LOG.info(
                                "All tables of create-table batch {} have been addressed before, ignoring it.",
                                event);
                        clearCurrentSchemaChangeRequest(nonce);
                        response.complete(wrap(SchemaChangeResponse.duplicate()));
                        return;
                    }
                } else if (schemaManager.isOriginalSchemaChangeEventRedundant(event)) {
                    // If this schema change event has been requested by another subTask, ignore it.
                    LOG.info("Event {} has been addressed before, ignoring it.", event);
                    clearCurrentSchemaChangeRequest(nonce);
                    LOG.info(
                            "SchemaChangeStatus switched from WAITING_FOR_FLUSH to IDLE for request {} due to duplicated request.",
                            request);
                    response.complete(wrap(SchemaChangeResponse.duplicate()));
                    return;
                }

                if (activeSinkWriters.size() < parallelism) {
                    LOG.info(
                            "Not all active sink writers have been registered. Current {}, expected {}.",
                            activeSinkWriters.size(),
                            parallelism);
                    response.complete(wrap(SchemaChangeResponse.waitingForFlush()));
                    return;
                }

                if (!activeSinkWriters.equals(flushedSinkWriters.get(nonce))) {
                    LOG.info(
                            "Not all active sink writers have completed flush (nonce: {}). Flushed writers: {}, expected: {}.",
                            nonce,
                            flushedSinkWriters.get(nonce),
                            activeSinkWriters);
                    response.complete(wrap(SchemaChangeResponse.waitingForFlush()));
                    return;
                }

                List<SchemaChangeEvent> derivedSchemaChangeEvents;
                if (event instanceof BatchCreateTableEvents) {
                    // The batch's own derivation is the pass-through of the whole batch, which has
                    // no meaning downstream: unfold it into the creates it carries instead.
                    batchCreates.forEach(schemaManager::applyOriginalSchemaChange);
                    derivedSchemaChangeEvents = new ArrayList<>(batchCreates);
                } else {
                    schemaManager.applyOriginalSchemaChange(event);
                    derivedSchemaChangeEvents = calculateDerivedSchemaChangeEvents(event);
                }

                // A guard rather than a live path: this connector's derivation always returns the
                // event itself (no LENIENT mode, no route rules — see KafkaJsonSchemaDerivation).
                // It is kept because the released SchemaOperator is the other side of this protocol
                // and does handle SchemaChangeResponse.ignored().
                if (derivedSchemaChangeEvents.isEmpty()) {
                    LOG.info("Event {} is omitted from sending to downstream, ignoring it.", event);
                    clearCurrentSchemaChangeRequest(nonce);
                    LOG.info(
                            "SchemaChangeStatus switched from WAITING_FOR_FLUSH to IDLE for request {} due to ignored request.",
                            request);

                    response.complete(wrap(SchemaChangeResponse.ignored()));
                    return;
                }

                LOG.info(
                        "SchemaChangeStatus switched from IDLE to APPLYING, other requests will be blocked.");
                // This request has been accepted.
                schemaChangeStatus = RequestStatus.APPLYING;

                response.complete(wrap(SchemaChangeResponse.accepted(derivedSchemaChangeEvents)));

                LOG.info(
                        "All sink subtask have flushed for table {}. Start to apply schema change request {}.",
                        request.getTableId().toString(),
                        request);
                schemaChangeThreadPool.submit(
                        () -> applySchemaChange(request.getTableId(), derivedSchemaChangeEvents));
            } else {
                response.complete(wrap(SchemaChangeResponse.busy()));
            }
        }
    }

    /**
     * Apply the schema change to the external system.
     *
     * @param tableId the table need to change schema
     * @param derivedSchemaChangeEvents list of the schema changes
     */
    private void applySchemaChange(
            TableId tableId, List<SchemaChangeEvent> derivedSchemaChangeEvents) {
        for (SchemaChangeEvent changeEvent : derivedSchemaChangeEvents) {
            if (schemaChangeBehavior == SchemaChangeBehavior.IGNORE
                    && !(changeEvent instanceof CreateTableEvent)) {
                currentIgnoredSchemaChanges.add(changeEvent);
                continue;
            }
            try {
                if (changeEvent instanceof AlterColumnTypeEvent
                        && metadataApplier instanceof OldSchemaAwareMetadataApplier) {
                    // An AlterColumnTypeEvent only carries the new types; an engine that needs the
                    // old (pre-change) types to react — Doris, which cannot shrink a CHAR/VARCHAR
                    // column the way MySQL/TiDB can — gets them from the pre-change evolved schema.
                    // It is resolved before applyEvolvedSchemaChange below, so it still reflects
                    // this event's "before" state.
                    Optional<Schema> oldSchema =
                            schemaManager.getLatestEvolvedSchema(changeEvent.tableId());
                    ((OldSchemaAwareMetadataApplier) metadataApplier)
                            .applyAlterColumnType((AlterColumnTypeEvent) changeEvent, oldSchema);
                } else {
                    metadataApplier.applySchemaChange(changeEvent);
                }
                LOG.info("Applied schema change {} to table {}.", changeEvent, tableId);
                schemaManager.applyEvolvedSchemaChange(changeEvent);
                currentFinishedSchemaChanges.add(changeEvent);
            } catch (Throwable t) {
                LOG.error(
                        "Failed to apply schema change {} to table {}. Caused by: {}",
                        changeEvent,
                        tableId,
                        t);
                if (!shouldIgnoreException(t)) {
                    currentChangeException = t;
                    break;
                } else {
                    LOG.warn(
                            "Failed to apply event {}, but keeps running in tolerant mode. Caused by: {}",
                            changeEvent,
                            t);
                }
            }
        }
        Preconditions.checkState(
                schemaChangeStatus == RequestStatus.APPLYING,
                "Illegal schemaChangeStatus state: should be APPLYING before applySchemaChange finishes, not "
                        + schemaChangeStatus);
        schemaChangeStatus = RequestStatus.FINISHED;
        LOG.info("SchemaChangeStatus switched from APPLYING to FINISHED.");
    }

    /**
     * Register a sink subtask.
     *
     * @param sinkSubtask the sink subtask to register
     */
    public void registerSinkWriter(int sinkSubtask) {
        LOG.info("Register sink subtask {}.", sinkSubtask);
        activeSinkWriters.add(sinkSubtask);
    }

    /**
     * Record flushed sink subtasks after receiving FlushSuccessEvent.
     *
     * @param sinkSubtask the sink subtask succeed flushing
     */
    public void flushSuccess(int sinkSubtask, long nonce) {
        synchronized (schemaChangeRequestLock) {
            if (!flushedSinkWriters.containsKey(nonce)) {
                flushedSinkWriters.put(nonce, ConcurrentHashMap.newKeySet());
            }
            flushedSinkWriters.get(nonce).add(sinkSubtask);
        }
    }

    public void getSchemaChangeResult(
            KafkaJsonSchemaChangeResultRequest request,
            CompletableFuture<CoordinationResponse> response) {
        Preconditions.checkState(
                schemaChangeStatus != RequestStatus.IDLE,
                "Illegal schemaChangeStatus: should not be IDLE before getting schema change request results.");
        if (schemaChangeStatus == RequestStatus.FINISHED) {
            schemaChangeStatus = RequestStatus.IDLE;
            LOG.info(
                    "SchemaChangeStatus switched from FINISHED to IDLE. (nonce: {})",
                    request.getNonce());

            // This request has been finished, return it and prepare for the next request
            List<SchemaChangeEvent> finishedEvents =
                    clearCurrentSchemaChangeRequest(request.getNonce());
            org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeResultResponse
                    resultResponse =
                            new org.apache.flink.cdc.runtime.operators.schema.event
                                    .SchemaChangeResultResponse(finishedEvents);
            response.complete(wrap(resultResponse));
        } else {
            // Still working on schema change request, waiting it
            response.complete(wrap(new SchemaChangeProcessingResponse()));
        }
    }

    @Override
    public void close() throws IOException {
        if (schemaChangeThreadPool != null) {
            schemaChangeThreadPool.shutdown();
        }
    }

    private List<SchemaChangeEvent> calculateDerivedSchemaChangeEvents(SchemaChangeEvent event) {
        // The connector always runs with EVOLVE behavior and no route rules, so the derivation is a
        // pure pass-through. The released LENIENT path (lenientizeSchemaChangeEvent) is omitted
        // because its switch on event.getType() cannot represent the connector's custom events.
        return schemaDerivation.applySchemaChange(event);
    }

    private boolean shouldIgnoreException(Throwable throwable) {
        // In IGNORE mode, will never try to apply schema change events
        // In EVOLVE and LENIENT mode, such failure will not be tolerated
        // In EXCEPTION mode, an exception will be thrown once captured
        return (throwable instanceof UnsupportedSchemaChangeEventException)
                && (schemaChangeBehavior == SchemaChangeBehavior.TRY_EVOLVE);
    }

    private List<SchemaChangeEvent> clearCurrentSchemaChangeRequest(long nonce) {
        if (currentChangeException != null) {
            context.failJob(
                    new RuntimeException("Failed to apply schema change.", currentChangeException));
        }
        List<SchemaChangeEvent> finishedSchemaChanges =
                new ArrayList<>(currentFinishedSchemaChanges);
        currentFinishedSchemaChanges.clear();
        currentIgnoredSchemaChanges.clear();
        currentChangeException = null;
        flushedSinkWriters.remove(nonce);
        return finishedSchemaChanges;
    }

    // Schema change event state could transfer in the following way:
    //
    //  --------
    //  | IDLE | -------------------A
    //  --------                    |
    //     ^                        |
    //      C                       |
    //       \                      v
    //  ------------          ------------
    //  | FINISHED | <-- B -- | APPLYING |
    //  ------------          ------------
    //
    //  A: When a request came to an idling request handler. Only possible when registry is IDLE,
    // and it has collected all FlushEvents from sink writers.
    //  B: When schema change application finishes (successfully or with exceptions)
    //  C: When current schema change request result has been retrieved by SchemaOperator, and be
    // ready for the next request.
    private enum RequestStatus {
        IDLE,
        APPLYING,
        FINISHED
    }
}
