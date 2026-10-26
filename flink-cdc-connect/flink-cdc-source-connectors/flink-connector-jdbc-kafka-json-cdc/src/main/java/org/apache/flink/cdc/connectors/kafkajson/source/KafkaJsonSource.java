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

package org.apache.flink.cdc.connectors.kafkajson.source;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfigFactory;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.SplitAssigner;
import org.apache.flink.cdc.connectors.base.source.assigner.StreamSplitAssigner;
import org.apache.flink.cdc.connectors.base.source.assigner.state.HybridPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.assigner.state.StreamPendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderContext;
import org.apache.flink.cdc.connectors.kafkajson.source.assigner.KafkaJsonHybridSplitAssigner;
import org.apache.flink.cdc.connectors.kafkajson.source.assigner.state.KafkaJsonPendingSplitsStateSerializer;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.enumerator.KafkaJsonIncrementalSourceEnumerator;
import org.apache.flink.cdc.connectors.kafkajson.source.reader.KafkaJsonSourceReader;
import org.apache.flink.cdc.connectors.kafkajson.source.reader.external.KafkaJsonIncrementalSourceSplitReader;
import org.apache.flink.cdc.connectors.kafkajson.source.split.KafkaJsonSplitSerializer;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.relational.TableId;

import java.util.List;
import java.util.function.Supplier;

/**
 * The Kafka-json source based on the incremental snapshot framework (FLIP-27 + watermark signal
 * algorithm). It reads the table snapshot through JDBC chunking and consumes the incremental data
 * changes (plus DDL) from Kafka messages produced by external tools (canal, in flatMessage JSON, or
 * a Debezium / TiCDC connector).
 *
 * <p>This class is the skeleton entry point. The {@code createEnumerator} / {@code
 * restoreEnumerator} wiring is inherited from the base framework; {@code createReader} is
 * overridden only to build the {@link KafkaJsonSourceReader}, whose {@code
 * IncrementalSourceReaderWithCommit} base forwards the checkpoint-complete callback to the dialect
 * so the consumed Kafka offsets can be committed to the consumer group (see the reader's javadoc).
 */
@Experimental
public class KafkaJsonSource<T> extends JdbcIncrementalSource<T> {

    /**
     * The split serializer used for the reader's checkpointed splits, the split-assignment RPC and
     * the enumerator state. It replaces the anonymous base serializer to avoid its quadratic
     * full-buffer copies per finished split info (FLINK-38184); the byte format is identical, so
     * checkpoints of either serializer restore through the same read path.
     */
    private final KafkaJsonSplitSerializer kafkaJsonSplitSerializer;

    public KafkaJsonSource(
            JdbcSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema,
            OffsetFactory offsetFactory,
            JdbcDataSourceDialect dataSourceDialect) {
        super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
        this.kafkaJsonSplitSerializer = new KafkaJsonSplitSerializer(offsetFactory);
    }

    @Override
    public SimpleVersionedSerializer<SourceSplitBase> getSplitSerializer() {
        return kafkaJsonSplitSerializer;
    }

    /**
     * FLINK-40697: routes the enumerator checkpoint serializer through the kafkajson serializer,
     * which writes the base v6 format when the release option is off (byte for byte identical to
     * the base serializer) and v7 (base + released flag) when it is on. The base's implementation
     * builds a plain base serializer.
     */
    @Override
    public SimpleVersionedSerializer<PendingSplitsState> getEnumeratorCheckpointSerializer() {
        KafkaJsonSourceConfig sourceConfig = (KafkaJsonSourceConfig) configFactory.create(0);
        return new KafkaJsonPendingSplitsStateSerializer(
                (SourceSplitSerializer) getSplitSerializer(),
                sourceConfig.isReleaseSnapshotMetadataEnabled());
    }

    /**
     * Connector-side port of FLINK-40697: creates the kafkajson assigner and enumerator, which
     * support releasing the snapshot-split metadata once the stream phase starts. Falls through to
     * the base's {@code StreamSplitAssigner} for stream-only reads (no metadata to release).
     */
    @Override
    public SplitEnumerator<SourceSplitBase, PendingSplitsState> createEnumerator(
            SplitEnumeratorContext<SourceSplitBase> enumContext) {
        KafkaJsonSourceConfig sourceConfig = (KafkaJsonSourceConfig) configFactory.create(0);
        final SplitAssigner splitAssigner;
        if (!sourceConfig.getStartupOptions().isStreamOnly()) {
            try {
                final List<TableId> remainingTables =
                        dataSourceDialect.discoverDataCollections(sourceConfig);
                boolean isTableIdCaseSensitive =
                        dataSourceDialect.isDataCollectionIdCaseSensitive(sourceConfig);
                splitAssigner =
                        new KafkaJsonHybridSplitAssigner<>(
                                sourceConfig,
                                enumContext.currentParallelism(),
                                remainingTables,
                                isTableIdCaseSensitive,
                                dataSourceDialect,
                                offsetFactory);
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to discover captured tables for enumerator", e);
            }
        } else {
            splitAssigner = new StreamSplitAssigner(sourceConfig, dataSourceDialect, offsetFactory);
        }

        return new KafkaJsonIncrementalSourceEnumerator(
                enumContext, sourceConfig, splitAssigner, getBoundedness());
    }

    /**
     * Connector-side port of FLINK-40697: restores the kafkajson assigner and enumerator from a
     * checkpoint.
     */
    @Override
    public SplitEnumerator<SourceSplitBase, PendingSplitsState> restoreEnumerator(
            SplitEnumeratorContext<SourceSplitBase> enumContext, PendingSplitsState checkpoint) {
        KafkaJsonSourceConfig sourceConfig = (KafkaJsonSourceConfig) configFactory.create(0);

        final SplitAssigner splitAssigner;
        if (checkpoint instanceof HybridPendingSplitsState) {
            splitAssigner =
                    new KafkaJsonHybridSplitAssigner<>(
                            sourceConfig,
                            enumContext.currentParallelism(),
                            (HybridPendingSplitsState) checkpoint,
                            dataSourceDialect,
                            offsetFactory);
        } else if (checkpoint instanceof StreamPendingSplitsState) {
            splitAssigner =
                    new StreamSplitAssigner(
                            sourceConfig,
                            (StreamPendingSplitsState) checkpoint,
                            dataSourceDialect,
                            offsetFactory);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported restored PendingSplitsState: " + checkpoint);
        }

        return new KafkaJsonIncrementalSourceEnumerator(
                enumContext, sourceConfig, splitAssigner, getBoundedness());
    }

    @Override
    public KafkaJsonSourceReader createReader(SourceReaderContext readerContext) throws Exception {
        // create source config for the given subtask (e.g. unique server id)
        KafkaJsonSourceConfig sourceConfig =
                (KafkaJsonSourceConfig) configFactory.create(readerContext.getIndexOfSubtask());
        FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecords>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        final SourceReaderMetrics sourceReaderMetrics =
                new SourceReaderMetrics(readerContext.metricGroup());

        sourceReaderMetrics.registerMetrics();
        IncrementalSourceReaderContext incrementalSourceReaderContext =
                new IncrementalSourceReaderContext(readerContext);
        Supplier<KafkaJsonIncrementalSourceSplitReader<JdbcSourceConfig>> splitReaderSupplier =
                () ->
                        new KafkaJsonIncrementalSourceSplitReader<>(
                                readerContext.getIndexOfSubtask(),
                                dataSourceDialect,
                                sourceConfig,
                                incrementalSourceReaderContext,
                                snapshotHooks);
        return new KafkaJsonSourceReader(
                elementsQueue,
                splitReaderSupplier,
                createRecordEmitter(sourceConfig, sourceReaderMetrics),
                readerContext.getConfiguration(),
                incrementalSourceReaderContext,
                sourceConfig,
                kafkaJsonSplitSerializer,
                dataSourceDialect);
    }
}
