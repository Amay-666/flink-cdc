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

package org.apache.flink.cdc.connectors.canal.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.base.options.StartupMode;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.flink.cdc.connectors.canal.source.config.CanalSourceConfig;
import org.apache.flink.cdc.connectors.canal.source.offset.CanalOffsetFactory;
import org.apache.flink.cdc.connectors.canal.source.utils.CanalTableDiscoveryUtils;
import org.apache.flink.cdc.connectors.canal.utils.CanalSchemaUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;

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

/** The {@link RecordEmitter} implementation for pipeline canal connector. */
public class CanalPipelineRecordEmitter extends IncrementalSourceRecordEmitter<Event> {

    private static final Logger LOG = LoggerFactory.getLogger(CanalPipelineRecordEmitter.class);

    private final CanalSourceConfig sourceConfig;

    // Used when startup mode is initial
    private final Set<TableId> alreadySendCreateTableTables;

    // Used when startup mode is not initial
    private boolean alreadySendCreateTableForStreamSplit = false;
    private final List<CreateTableEvent> createTableEventCache;

    public CanalPipelineRecordEmitter(
            DebeziumDeserializationSchema<Event> debeziumDeserializationSchema,
            SourceReaderMetrics sourceReaderMetrics,
            CanalSourceConfig sourceConfig) {
        super(
                debeziumDeserializationSchema,
                sourceReaderMetrics,
                sourceConfig.isIncludeSchemaChanges(),
                new CanalOffsetFactory());
        this.sourceConfig = sourceConfig;
        this.alreadySendCreateTableTables = new HashSet<>();
        this.createTableEventCache = generateCreateTableEvent(sourceConfig);
    }

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<Event> output, SourceSplitState splitState)
            throws Exception {
        if (isLowWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
            // In Snapshot phase of INITIAL startup mode, we lazily send CreateTableEvent to
            // downstream to avoid checkpoint timeout.
            io.debezium.relational.TableId tableId =
                    splitState.asSnapshotSplitState().toSourceSplit().getTableId();
            TableId commonTableId = CanalSchemaUtils.toCommonTableId(tableId);
            if (!alreadySendCreateTableTables.contains(commonTableId)) {
                try (JdbcConnection jdbc = CanalSchemaUtils.openJdbcConnection(sourceConfig)) {
                    sendCreateTableEvent(jdbc, commonTableId, output);
                    alreadySendCreateTableTables.add(commonTableId);
                }
            }
        } else if (splitState.isStreamSplitState() && !alreadySendCreateTableForStreamSplit) {
            alreadySendCreateTableForStreamSplit = true;
            if (sourceConfig.getStartupOptions().startupMode.equals(StartupMode.INITIAL)) {
                // In Snapshot -> Binlog transition of INITIAL startup mode, ensure all table
                // schemas have been sent to downstream. We use previously cached schema instead of
                // re-request latest schema because there might be some pending schema change events
                // in the queue, and that may accidentally emit evolved schema before corresponding
                // schema change events.
                createTableEventCache.stream()
                        .filter(event -> !alreadySendCreateTableTables.contains(event.tableId()))
                        .forEach(output::collect);
            } else {
                // In stream-only mode, we simply emit all schemas at once.
                createTableEventCache.forEach(output::collect);
            }
        }
        super.processElement(element, output, splitState);
    }

    private void sendCreateTableEvent(
            JdbcConnection jdbc, TableId tableId, SourceOutput<Event> output) {
        Schema schema = CanalSchemaUtils.getTableSchema(jdbc, sourceConfig, tableId);
        output.collect(new CreateTableEvent(tableId, schema));
    }

    private List<CreateTableEvent> generateCreateTableEvent(CanalSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = CanalSchemaUtils.openJdbcConnection(sourceConfig)) {
            List<CreateTableEvent> createTableEventCache = new ArrayList<>();
            List<io.debezium.relational.TableId> capturedTableIds =
                    CanalTableDiscoveryUtils.listTables(
                            sourceConfig.getDatabaseList().get(0),
                            jdbc,
                            sourceConfig.getTableFilters());
            for (io.debezium.relational.TableId tableId : capturedTableIds) {
                TableId commonTableId = CanalSchemaUtils.toCommonTableId(tableId);
                createTableEventCache.add(
                        new CreateTableEvent(
                                commonTableId,
                                CanalSchemaUtils.getTableSchema(jdbc, sourceConfig, commonTableId)));
            }
            return createTableEventCache;
        } catch (SQLException e) {
            throw new RuntimeException("Cannot start emitter to fetch table schema.", e);
        }
    }
}
