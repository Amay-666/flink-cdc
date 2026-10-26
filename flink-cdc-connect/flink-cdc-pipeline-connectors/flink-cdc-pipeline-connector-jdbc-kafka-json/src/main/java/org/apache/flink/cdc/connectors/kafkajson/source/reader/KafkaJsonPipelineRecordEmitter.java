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

package org.apache.flink.cdc.connectors.kafkajson.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.dialect.KafkaJsonDialect;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffsetFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.utils.KafkaJsonTableDiscoveryUtils;
import org.apache.flink.cdc.connectors.kafkajson.utils.KafkaJsonSchemaUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import io.debezium.jdbc.JdbcConnection;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isLowWatermarkEvent;

/** The {@link RecordEmitter} implementation for the jdbc-kafka-json pipeline connector. */
public class KafkaJsonPipelineRecordEmitter extends IncrementalSourceRecordEmitter<Event> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonPipelineRecordEmitter.class);

    private final KafkaJsonSourceConfig sourceConfig;

    /**
     * The dialect shared by every schema read of this emitter (the constructor's full-table scan
     * and the per-split low-watermark reads). Its {@code KafkaJsonSchema} caches the {@code
     * TableChange} of each table, so the low-watermark {@code CreateTableEvent} reuses the schema
     * read at job start instead of re-querying the JDBC metadata — the same consistency rationale
     * as the cached {@link #createTableEventCache} at the stream transition: never emit an evolved
     * schema before the corresponding schema change event, and no NPE when a table is renamed or
     * dropped while its snapshot splits are still pending. Tables not in the cache (newly added
     * ones) are read from JDBC on first request.
     */
    private final KafkaJsonDialect dialect;

    // Used when startup mode is initial
    private final Set<TableId> alreadySendCreateTableTables;

    // Used when startup mode is not initial
    private boolean alreadySendCreateTableForStreamSplit = false;
    private final List<CreateTableEvent> createTableEventCache;

    /**
     * FLINK-40102: when enabled, all CreateTableEvents are emitted together on the first record
     * (before any DataChangeEvent) instead of one per table interleaved with the data stream. Each
     * per-table event triggers a downstream FlushEvent that flushes buffered data; in batch mode
     * the flushes hit an empty buffer, so no data files are produced and the data stream is not
     * interrupted. Recommended for hundreds+ tables.
     */
    private final boolean batchEmitCreateTableEventsEnabled;

    private boolean batchEmitted = false;

    public KafkaJsonPipelineRecordEmitter(
            DebeziumDeserializationSchema<Event> debeziumDeserializationSchema,
            SourceReaderMetrics sourceReaderMetrics,
            KafkaJsonSourceConfig sourceConfig) {
        super(
                debeziumDeserializationSchema,
                sourceReaderMetrics,
                sourceConfig.isIncludeSchemaChanges(),
                new KafkaJsonOffsetFactory());
        this.sourceConfig = sourceConfig;
        this.dialect = new KafkaJsonDialect(sourceConfig);
        this.alreadySendCreateTableTables = new HashSet<>();
        this.createTableEventCache = generateCreateTableEvent(sourceConfig);
        this.batchEmitCreateTableEventsEnabled = sourceConfig.isBatchEmitCreateTableEventsEnabled();
    }

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<Event> output, SourceSplitState splitState)
            throws Exception {
        // FLINK-40102 batch mode: emit all CreateTableEvents together on the first record, before
        // any DataChangeEvent. Each per-table event would trigger a downstream FlushEvent that
        // flushes buffered data; in batch mode the flushes hit an empty buffer (no data has flowed
        // yet), so no data files are produced and the data stream is not interrupted.
        if (batchEmitCreateTableEventsEnabled && !batchEmitted) {
            createTableEventCache.forEach(output::collect);
            createTableEventCache.forEach(
                    event -> alreadySendCreateTableTables.add(event.tableId()));
            batchEmitted = true;
        }

        if (isLowWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
            // In Snapshot phase of INITIAL startup mode, we lazily send CreateTableEvent to
            // downstream to avoid checkpoint timeout. In batch mode, all events have already been
            // sent upfront, so the alreadySendCreateTableTables check skips the per-table emission.
            io.debezium.relational.TableId tableId =
                    splitState.asSnapshotSplitState().toSourceSplit().getTableId();
            TableId commonTableId = KafkaJsonSchemaUtils.toCommonTableId(tableId);
            if (!alreadySendCreateTableTables.contains(commonTableId)) {
                try (JdbcConnection jdbc = dialect.openJdbcConnection(sourceConfig)) {
                    sendCreateTableEvent(jdbc, commonTableId, output);
                    alreadySendCreateTableTables.add(commonTableId);
                }
            }
        } else if (splitState.isStreamSplitState() && !alreadySendCreateTableForStreamSplit) {
            alreadySendCreateTableForStreamSplit = true;
            // Emit any schemas not already sent — either per-table during the snapshot phase
            // (INITIAL) or by the upfront batch emission (FLINK-40102). We use the previously
            // cached schemas instead of re-requesting the latest ones because there might be
            // pending schema change events in the queue, and re-requesting could accidentally
            // emit evolved schemas before the corresponding schema change events. In non-batch
            // stream-only mode the set is empty, so everything is emitted.
            createTableEventCache.stream()
                    .filter(event -> !alreadySendCreateTableTables.contains(event.tableId()))
                    .forEach(output::collect);
        }
        super.processElement(element, output, splitState);
    }

    private void sendCreateTableEvent(
            JdbcConnection jdbc, TableId tableId, SourceOutput<Event> output) {
        Schema schema = KafkaJsonSchemaUtils.getTableSchema(jdbc, dialect, tableId);
        output.collect(new CreateTableEvent(tableId, schema));
    }

    private List<CreateTableEvent> generateCreateTableEvent(KafkaJsonSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = dialect.openJdbcConnection(sourceConfig)) {
            List<CreateTableEvent> createTableEventCache = new ArrayList<>();
            // The captured tables may span multiple databases: walk every configured one — the
            // same discovery the dialect's discoverDataCollections uses — so the cache (and with
            // it the stream-only "emit all schemas at once" path and the INITIAL snapshot-to-
            // stream transition backstop) covers them all.
            List<io.debezium.relational.TableId> capturedTableIds = new ArrayList<>();
            for (String database : sourceConfig.getDatabaseList()) {
                capturedTableIds.addAll(
                        KafkaJsonTableDiscoveryUtils.listTables(
                                database, jdbc, sourceConfig.getTableFilters()));
            }
            for (io.debezium.relational.TableId tableId : capturedTableIds) {
                TableId commonTableId = KafkaJsonSchemaUtils.toCommonTableId(tableId);
                createTableEventCache.add(
                        new CreateTableEvent(
                                commonTableId,
                                KafkaJsonSchemaUtils.getTableSchema(jdbc, dialect, commonTableId)));
            }
            return createTableEventCache;
        } catch (SQLException e) {
            throw new RuntimeException("Cannot start emitter to fetch table schema.", e);
        }
    }
}
