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

package org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris;

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterColumnCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterTableCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.DropTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.TruncateTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.ddl.DorisDdlBuilder;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.http.DorisHttpClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;

/**
 * The per-subtask Doris writer driven by the released {@code DataSinkWriterOperator}.
 *
 * <p>Holds a local view of each table's schema (evolved by standard and custom schema-change
 * events), converts {@link DataChangeEvent}s to StreamLoad-ready JSON rows via {@link
 * DorisRowConverter}, buffers them per table and flushes via {@link DorisHttpClient#streamLoad}
 * when a buffer fills, when the periodic flush timer fires, on every {@code FlushEvent} (a schema
 * change blocks upstream data until every subtask has flushed — this method is the data landing
 * point of that protocol) and on close.
 *
 * <p>DELETE events map to a batch-delete marker ({@link DorisDdlBuilder#DELETE_SIGN_COLUMN}) on
 * the UNIQUE-model table; INSERT/UPDATE/REPLACE are all upserts keyed by the primary key.
 */
public class DorisSinkWriter implements SinkWriter<Event> {

    private static final Logger LOG = LoggerFactory.getLogger(DorisSinkWriter.class);

    private static final String LABEL_PREFIX = "cdc_";

    private final DorisDataSinkOptions options;
    private final ZoneId pipelineZoneId;
    private final DorisHttpClient httpClient;

    /** Latest schema per table, evolved by the schema-change events flowing downstream. */
    private final Map<TableId, Schema> schemaMaps = new HashMap<>();
    private final Map<TableId, DorisRowConverter> rowConverters = new HashMap<>();
    private final Map<TableId, List<Map<String, Object>>> buffer = new HashMap<>();

    private volatile boolean closed;
    private ScheduledFuture<?> flushTimer;

    public DorisSinkWriter(
            DorisDataSinkOptions options, ZoneId pipelineZoneId, Sink.InitContext initContext) {
        this.options = options;
        this.pipelineZoneId = pipelineZoneId;
        this.httpClient =
                new DorisHttpClient(
                        options.getFenodes(),
                        options.getUsername(),
                        options.getPassword(),
                        options.getMaxRetries());
        schedulePeriodicFlush(initContext);
    }

    @Override
    public void write(Event element, SinkWriter.Context context) throws IOException {
        if (element instanceof DataChangeEvent) {
            writeDataChangeEvent((DataChangeEvent) element);
        } else if (element instanceof SchemaChangeEvent) {
            applySchemaChange((SchemaChangeEvent) element);
        }
        // FlushEvent never reaches the writer: DataSinkWriterOperator intercepts it and calls
        // flush() directly.
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        flushBuffered();
    }

    @Override
    public void close() throws IOException {
        closed = true;
        if (flushTimer != null) {
            flushTimer.cancel(false);
        }
        flushBuffered();
        httpClient.close();
    }

    private void writeDataChangeEvent(DataChangeEvent event) throws IOException {
        TableId tableId = event.tableId();
        Schema schema = schemaMaps.get(tableId);
        DorisRowConverter converter = rowConverters.get(tableId);
        if (schema == null || converter == null) {
            throw new IOException(
                    "Received a data change event for unknown table "
                            + tableId
                            + "; the CreateTableEvent must precede its data.");
        }
        Map<String, Object> row;
        if (event.op() == OperationType.DELETE) {
            row = converter.convert(event.before(), schema);
            if (options.isEnableBatchDelete()) {
                row.put(DorisDdlBuilder.DELETE_SIGN_COLUMN, true);
            }
        } else {
            // INSERT / UPDATE / REPLACE are all upserts in the Doris UNIQUE model.
            row = converter.convert(event.after(), schema);
        }
        List<Map<String, Object>> rows = buffer.computeIfAbsent(tableId, t -> new ArrayList<>());
        rows.add(row);
        if (rows.size() >= options.getBufferSize()) {
            flushTable(tableId);
        }
    }

    private void applySchemaChange(SchemaChangeEvent event) {
        if (event instanceof CreateTableEvent) {
            CreateTableEvent create = (CreateTableEvent) event;
            schemaMaps.put(create.tableId(), create.getSchema());
            rowConverters.put(
                    create.tableId(),
                    new DorisRowConverter(create.getSchema(), pipelineZoneId));
        } else if (event instanceof RenameTableEvent) {
            RenameTableEvent rename = (RenameTableEvent) event;
            // Subsequent data carries the new table id: re-key the local view.
            schemaMaps.remove(rename.getOldTableId());
            rowConverters.remove(rename.getOldTableId());
            schemaMaps.put(rename.getNewTableId(), rename.getSchema());
            rowConverters.put(
                    rename.getNewTableId(),
                    new DorisRowConverter(rename.getSchema(), pipelineZoneId));
            if (buffer.containsKey(rename.getOldTableId())) {
                buffer.put(rename.getNewTableId(), buffer.remove(rename.getOldTableId()));
            }
        } else if (event instanceof DropTableEvent) {
            schemaMaps.remove(event.tableId());
            rowConverters.remove(event.tableId());
            buffer.remove(event.tableId());
        } else if (event instanceof TruncateTableEvent) {
            // The blocking protocol flushes all buffered rows before the truncate DDL is applied
            // and no data flows until the truncate event has been broadcast downstream, so the
            // writer keeps its schema and has nothing to do.
        } else if (event instanceof AlterTableCommentEvent
                || event instanceof AlterColumnCommentEvent) {
            // Comments do not affect row conversion; keep the current schema and converter.
        } else {
            Schema current = schemaMaps.get(event.tableId());
            if (current != null) {
                Schema evolved = SchemaUtils.applySchemaChangeEvent(current, event);
                schemaMaps.put(event.tableId(), evolved);
                rowConverters.put(
                        event.tableId(), new DorisRowConverter(evolved, pipelineZoneId));
            }
        }
    }

    private void flushBuffered() throws IOException {
        for (TableId tableId : new ArrayList<>(buffer.keySet())) {
            flushTable(tableId);
        }
    }

    private void flushTable(TableId tableId) throws IOException {
        List<Map<String, Object>> rows = buffer.remove(tableId);
        if (rows == null || rows.isEmpty()) {
            return;
        }
        httpClient.streamLoad(
                options.mapDatabase(tableId),
                options.mapTable(tableId),
                newLabel(tableId),
                rows);
    }

    private void schedulePeriodicFlush(Sink.InitContext initContext) {
        if (options.getFlushInterval().isZero() || options.getFlushInterval().isNegative()) {
            return;
        }
        ProcessingTimeService timeService = initContext.getProcessingTimeService();
        flushTimer =
                timeService.registerTimer(
                        timeService.getCurrentProcessingTime()
                                + options.getFlushInterval().toMillis(),
                        timestamp -> {
                            if (closed) {
                                return;
                            }
                            try {
                                flushBuffered();
                            } catch (IOException e) {
                                throw new RuntimeException("Failed to flush Doris buffer.", e);
                            }
                            schedulePeriodicFlush(initContext);
                        });
    }

    private String newLabel(TableId tableId) {
        return LABEL_PREFIX
                + sanitize(options.mapDatabase(tableId))
                + "_"
                + sanitize(options.mapTable(tableId))
                + "_"
                + UUID.randomUUID().toString().replace("-", "");
    }

    private static String sanitize(String name) {
        return name.replaceAll("[^a-zA-Z0-9_]", "_");
    }
}
