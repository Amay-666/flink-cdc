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

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.runtime.operators.schema.event.FlushSuccessEvent;
import org.apache.flink.cdc.runtime.operators.schema.event.GetEvolvedSchemaRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.GetEvolvedSchemaResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.GetOriginalSchemaRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.GetOriginalSchemaResponse;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeResultRequest;
import org.apache.flink.cdc.runtime.operators.schema.event.SinkWriterRegisterEvent;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.flink.cdc.runtime.operators.schema.event.CoordinationResponseUtils.wrap;

/**
 * The {@link OperatorCoordinator} for the kafka-json schema operator.
 *
 * <p>This mirrors the released {@code SchemaRegistry} of flink-cdc-runtime — same event-loop thread
 * model, same {@link CoordinationRequestHandler} contract and the same flush-acknowledgement /
 * schema-change application flow — but drives the connector's own {@link
 * KafkaJsonSchemaManager}/{@link KafkaJsonSchemaDerivation}/{@link
 * KafkaJsonSchemaRegistryRequestHandler} so that the five custom schema-change events are handled
 * without ever calling {@code getType()}. It also responds to {@link GetEvolvedSchemaRequest} /
 * {@link GetOriginalSchemaRequest}, which the partitioning operator relies on to rebuild its hash
 * functions.
 */
public class KafkaJsonSchemaRegistry
        implements OperatorCoordinator, CoordinationRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonSchemaRegistry.class);

    /** The context of the coordinator. */
    private final OperatorCoordinator.Context context;
    /** The name of the operator this coordinator is associated with. */
    private final String operatorName;

    /** A single-thread executor to handle async execution of the coordinator. */
    private final ExecutorService coordinatorExecutor;

    /** Tracks the subtask failed reason to throw a more meaningful exception in {@link
     * #subtaskReset}. */
    private final Map<Integer, Throwable> failedReasons;

    /** Metadata applier for applying schema changes to external system. */
    private final MetadataApplier metadataApplier;

    private final SchemaChangeBehavior schemaChangeBehavior;

    /** The request handler that handle all requests and events. */
    private KafkaJsonSchemaRegistryRequestHandler requestHandler;

    /** Schema manager for tracking schemas of all tables. */
    private KafkaJsonSchemaManager schemaManager;

    private KafkaJsonSchemaDerivation schemaDerivation;

    /**
     * Current parallelism. Use this to verify if the registry has collected enough flush success
     * events from sink operators.
     */
    private int currentParallelism;

    public KafkaJsonSchemaRegistry(
            String operatorName,
            OperatorCoordinator.Context context,
            ExecutorService coordinatorExecutor,
            MetadataApplier metadataApplier,
            SchemaChangeBehavior schemaChangeBehavior) {
        this.context = context;
        this.coordinatorExecutor = coordinatorExecutor;
        this.operatorName = operatorName;
        this.failedReasons = new HashMap<>();
        this.metadataApplier = metadataApplier;
        this.schemaChangeBehavior = schemaChangeBehavior;
        this.schemaManager = new KafkaJsonSchemaManager(schemaChangeBehavior);
        this.schemaDerivation = new KafkaJsonSchemaDerivation();
        this.requestHandler =
                new KafkaJsonSchemaRegistryRequestHandler(
                        metadataApplier,
                        schemaManager,
                        schemaDerivation,
                        schemaChangeBehavior,
                        context);
    }

    @Override
    public void start() throws Exception {
        LOG.info("Starting KafkaJsonSchemaRegistry for {}.", operatorName);
        this.failedReasons.clear();
        this.currentParallelism = context.currentParallelism();
        LOG.info(
                "Started KafkaJsonSchemaRegistry for {}. Parallelism: {}",
                operatorName,
                currentParallelism);
    }

    @Override
    public void close() throws Exception {
        LOG.info("KafkaJsonSchemaRegistry for {} closed.", operatorName);
        coordinatorExecutor.shutdown();
        requestHandler.close();
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {
        runInEventLoop(
                () -> {
                    try {
                        if (event instanceof FlushSuccessEvent) {
                            FlushSuccessEvent flushSuccessEvent = (FlushSuccessEvent) event;
                            LOG.info(
                                    "Sink subtask {} succeed flushing for table {}.",
                                    flushSuccessEvent.getSubtask(),
                                    flushSuccessEvent.getTableId().toString());
                            requestHandler.flushSuccess(
                                    flushSuccessEvent.getTableId(),
                                    flushSuccessEvent.getSubtask(),
                                    currentParallelism);
                        } else if (event instanceof SinkWriterRegisterEvent) {
                            requestHandler.registerSinkWriter(
                                    ((SinkWriterRegisterEvent) event).getSubtask());
                        } else {
                            throw new FlinkException("Unrecognized Operator Event: " + event);
                        }
                    } catch (Throwable t) {
                        context.failJob(t);
                        throw t;
                    }
                },
                "handling event %s from subTask %d",
                event,
                subtask);
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) {
        // we generate checkpoint in an async thread to not block the JobManager's main thread, the
        // coordinator state might be large if there are many schema changes and monitor many
        // tables.
        runInEventLoop(
                () -> {
                    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            DataOutputStream out = new DataOutputStream(baos)) {
                        // Serialize SchemaManager
                        int schemaManagerSerializerVersion =
                                KafkaJsonSchemaManager.SERIALIZER.getVersion();
                        out.writeInt(schemaManagerSerializerVersion);
                        byte[] serializedSchemaManager =
                                KafkaJsonSchemaManager.SERIALIZER.serialize(schemaManager);
                        out.writeInt(serializedSchemaManager.length);
                        out.write(serializedSchemaManager);
                        // The derivation is stateless (no routing support), so nothing more is
                        // written.
                        resultFuture.complete(baos.toByteArray());
                    } catch (Throwable t) {
                        context.failJob(t);
                        throw t;
                    }
                },
                "taking checkpoint %d",
                checkpointId);
    }

    private void runInEventLoop(
            final ThrowingRunnable<Throwable> action,
            final String actionName,
            final Object... actionNameFormatParameters) {
        coordinatorExecutor.execute(
                () -> {
                    try {
                        action.run();
                    } catch (Throwable t) {
                        // if we have a JVM critical error, promote it immediately, there is a good
                        // chance the logging or job failing will not succeed anymore
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(t);

                        final String actionString =
                                String.format(actionName, actionNameFormatParameters);
                        LOG.error(
                                "Uncaught exception in the KafkaJsonSchemaRegistry for {} while {}. Triggering job failover.",
                                operatorName,
                                actionString,
                                t);
                        context.failJob(t);
                    }
                });
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // do nothing
    }

    @Override
    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        CompletableFuture<CoordinationResponse> responseFuture = new CompletableFuture<>();
        runInEventLoop(
                () -> {
                    try {
                        if (request instanceof SchemaChangeRequest) {
                            SchemaChangeRequest schemaChangeRequest = (SchemaChangeRequest) request;
                            requestHandler.handleSchemaChangeRequest(
                                    schemaChangeRequest, responseFuture);
                        } else if (request instanceof SchemaChangeResultRequest) {
                            requestHandler.getSchemaChangeResult(responseFuture);
                        } else if (request instanceof GetEvolvedSchemaRequest) {
                            handleGetEvolvedSchemaRequest(
                                    ((GetEvolvedSchemaRequest) request), responseFuture);
                        } else if (request instanceof GetOriginalSchemaRequest) {
                            handleGetOriginalSchemaRequest(
                                    (GetOriginalSchemaRequest) request, responseFuture);
                        } else {
                            throw new IllegalArgumentException(
                                    "Unrecognized CoordinationRequest type: " + request);
                        }
                    } catch (Throwable t) {
                        context.failJob(t);
                        throw t;
                    }
                },
                "handling coordination request %s",
                request);
        return responseFuture;
    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
            throws Exception {
        if (checkpointData == null) {
            return;
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(checkpointData);
                DataInputStream in = new DataInputStream(bais)) {
            int schemaManagerSerializerVersion = in.readInt();

            switch (schemaManagerSerializerVersion) {
                case 2:
                    {
                        int length = in.readInt();
                        byte[] serializedSchemaManager = new byte[length];
                        in.readFully(serializedSchemaManager);
                        schemaManager =
                                KafkaJsonSchemaManager.SERIALIZER.deserialize(
                                        schemaManagerSerializerVersion, serializedSchemaManager);
                        schemaDerivation = new KafkaJsonSchemaDerivation();
                        requestHandler =
                                new KafkaJsonSchemaRegistryRequestHandler(
                                        metadataApplier,
                                        schemaManager,
                                        schemaDerivation,
                                        schemaManager.getBehavior(),
                                        context);
                        break;
                    }
                default:
                    throw new IOException(
                            "Unrecognized serialization version " + schemaManagerSerializerVersion);
            }
        } catch (Throwable t) {
            context.failJob(t);
            throw t;
        }
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        Throwable rootCause = failedReasons.get(subtask);
        LOG.error(
                String.format("Subtask %d reset at checkpoint %d.", subtask, checkpointId),
                rootCause);
    }

    @Override
    public void executionAttemptFailed(
            int subtask, int attemptNumber, @Nullable Throwable throwable) {
        failedReasons.put(subtask, throwable);
    }

    @Override
    public void executionAttemptReady(
            int subtask, int attemptNumber, SubtaskGateway subtaskGateway) {
        // do nothing
    }

    private void handleGetEvolvedSchemaRequest(
            GetEvolvedSchemaRequest getEvolvedSchemaRequest,
            CompletableFuture<CoordinationResponse> response) {
        LOG.info("Handling evolved schema request: {}", getEvolvedSchemaRequest);
        int schemaVersion = getEvolvedSchemaRequest.getSchemaVersion();
        TableId tableId = getEvolvedSchemaRequest.getTableId();
        if (schemaVersion == GetEvolvedSchemaRequest.LATEST_SCHEMA_VERSION) {
            response.complete(
                    wrap(
                            new GetEvolvedSchemaResponse(
                                    schemaManager.getLatestEvolvedSchema(tableId).orElse(null))));
        } else {
            try {
                response.complete(
                        wrap(
                                new GetEvolvedSchemaResponse(
                                        schemaManager.getEvolvedSchema(tableId, schemaVersion))));
            } catch (IllegalArgumentException iae) {
                LOG.warn(
                        "Some client is requesting an non-existed evolved schema for table {} with version {}",
                        tableId,
                        schemaVersion);
                response.complete(wrap(new GetEvolvedSchemaResponse(null)));
            }
        }
    }

    private void handleGetOriginalSchemaRequest(
            GetOriginalSchemaRequest getOriginalSchemaRequest,
            CompletableFuture<CoordinationResponse> response) {
        LOG.info("Handling original schema request: {}", getOriginalSchemaRequest);
        int schemaVersion = getOriginalSchemaRequest.getSchemaVersion();
        TableId tableId = getOriginalSchemaRequest.getTableId();
        if (schemaVersion == GetOriginalSchemaRequest.LATEST_SCHEMA_VERSION) {
            response.complete(
                    wrap(
                            new GetOriginalSchemaResponse(
                                    schemaManager.getLatestOriginalSchema(tableId).orElse(null))));
        } else {
            try {
                response.complete(
                        wrap(
                                new GetOriginalSchemaResponse(
                                        schemaManager.getOriginalSchema(tableId, schemaVersion))));
            } catch (IllegalArgumentException iae) {
                LOG.warn(
                        "Some client is requesting an non-existed original schema for table {} with version {}",
                        tableId,
                        schemaVersion);
                response.complete(wrap(new GetOriginalSchemaResponse(null)));
            }
        }
    }

    // --------------------Only visible for test -----------------

    @VisibleForTesting
    public void handleApplyOriginalSchemaChangeEvent(SchemaChangeEvent schemaChangeEvent) {
        schemaManager.applyOriginalSchemaChange(schemaChangeEvent);
    }

    @VisibleForTesting
    public void handleApplyEvolvedSchemaChangeRequest(SchemaChangeEvent schemaChangeEvent) {
        schemaManager.applyEvolvedSchemaChange(schemaChangeEvent);
    }

    /** Returns the current schema manager (for tests). */
    @VisibleForTesting
    public Optional<KafkaJsonSchemaManager> getSchemaManager() {
        return Optional.ofNullable(schemaManager);
    }
}
