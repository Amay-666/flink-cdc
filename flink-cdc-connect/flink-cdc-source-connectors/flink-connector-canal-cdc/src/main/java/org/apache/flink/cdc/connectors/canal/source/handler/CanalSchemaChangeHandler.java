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

package org.apache.flink.cdc.connectors.canal.source.handler;

import org.apache.flink.cdc.connectors.canal.source.config.CanalSourceConfig;
import org.apache.flink.cdc.connectors.canal.source.config.CanalSourceOptions.DdlParser;
import org.apache.flink.cdc.connectors.canal.source.ddl.CanalDdlParsedResult;
import org.apache.flink.cdc.connectors.canal.source.ddl.CanalDdlParser;
import org.apache.flink.cdc.connectors.canal.source.ddl.CanalDebeziumDdlParser;
import org.apache.flink.cdc.connectors.canal.source.ddl.CanalDruidDdlParser;
import org.apache.flink.cdc.connectors.canal.source.fetch.CanalSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.canal.source.message.CanalFlatMessage;
import org.apache.flink.cdc.connectors.canal.source.offset.CanalOffset;
import org.apache.flink.cdc.connectors.canal.source.offset.CanalPartition;
import org.apache.flink.cdc.connectors.canal.source.schema.CanalSourceInfo;
import org.apache.flink.cdc.connectors.canal.source.schema.CanalSourceInfoStructMaker;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlTopicSelector;
import io.debezium.document.DocumentWriter;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChangeType;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles the DDL messages of the canal stream: applies the schema change to the shared {@code
 * CanalSchema} — so that subsequent data records of the affected table are decoded with the new
 * schema — and, when {@code include.schema.changes} is enabled, enqueues the Debezium-shaped
 * schema-change {@link SourceRecord} into the shared queue.
 *
 * <p>The schema-change record is built here directly instead of going through {@code
 * JdbcSourceEventDispatcher.dispatchSchemaChangeEvent}: the dispatcher's {@code
 * SchemaChangeEventReceiver} only enqueues the record when {@code
 * CommonConnectorConfig.isSchemaChangesHistoryEnabled()} is true, which the bundled Debezium 1.9.8
 * hard-codes to {@code false} for the MySQL connector. The record mirrors the dispatcher's format so
 * that the base {@code IncrementalSourceRecordEmitter} — which reads the history record for the
 * stream-split schema bookkeeping — consumes it unchanged.
 */
public class CanalSchemaChangeHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CanalSchemaChangeHandler.class);

    private static final String HISTORY_RECORD_FIELD = "historyRecord";
    private static final String SCHEMA_CHANGE_KEY_NAME =
            "io.debezium.connector.canal.SchemaChangeKey";
    private static final String SCHEMA_CHANGE_VALUE_NAME =
            "io.debezium.connector.canal.SchemaChangeValue";

    private static final DocumentWriter DOCUMENT_WRITER = DocumentWriter.defaultWriter();

    private final CanalDdlParser ddlParser;
    private final CanalSourceInfoStructMaker sourceInfoStructMaker;

    public CanalSchemaChangeHandler(CanalSourceConfig sourceConfig) {
        this.ddlParser = createParser(sourceConfig.getDdlParser());
        this.sourceInfoStructMaker =
                new CanalSourceInfoStructMaker(
                        "canal", CanalSourceInfoStructMaker.DEBEZIUM_VERSION,
                        sourceConfig.getDbzConnectorConfig());
    }

    private static CanalDdlParser createParser(DdlParser parserType) {
        switch (parserType) {
            case DEBEZIUM:
                return new CanalDebeziumDdlParser();
            case DRUID:
            default:
                return new CanalDruidDdlParser();
        }
    }

    /**
     * Applies the DDL of the message to the shared schema and enqueues the schema-change record.
     *
     * @param context the fetch task context (shared schema + queue)
     * @param message the canal DDL message
     * @param offset the stream position of the message
     */
    public void handle(
            CanalSourceFetchTaskContext context, CanalFlatMessage message, CanalOffset offset)
            throws IOException, InterruptedException {
        String sql = message.getSql();
        if (sql == null || sql.isEmpty()) {
            return;
        }
        TableId tableId =
                new TableId(
                        message.getDatabase() == null ? "" : message.getDatabase(),
                        null,
                        message.getTable());
        Table currentTable = context.getDatabaseSchema().tableFor(tableId);
        CanalDdlParsedResult result =
                ddlParser.parse(message.getDatabase(), tableId, currentTable, sql);
        if (result == null) {
            LOG.debug("Skipping DDL that does not change the table schema: {}", sql);
            return;
        }
        applySchemaChange(context, result);
        if (context.getSourceConfig().isIncludeSchemaChanges()) {
            enqueueSchemaChange(context, message, offset, result);
        }
    }

    private static void applySchemaChange(
            CanalSourceFetchTaskContext context, CanalDdlParsedResult result) {
        if (result.getType() == TableChangeType.DROP) {
            context.getDatabaseSchema().removeTable(result.getTableId());
        } else {
            context.getDatabaseSchema().registerTable(result.getTable());
        }
    }

    private void enqueueSchemaChange(
            CanalSourceFetchTaskContext context,
            CanalFlatMessage message,
            CanalOffset offset,
            CanalDdlParsedResult result)
            throws IOException, InterruptedException {
        CanalSourceConfig sourceConfig = context.getSourceConfig();
        MySqlConnectorConfig dbzConfig = sourceConfig.getDbzConnectorConfig();

        Schema keySchema =
                SchemaBuilder.struct()
                        .name(SchemaNameAdjuster.create().adjust(SCHEMA_CHANGE_KEY_NAME))
                        .field(HistoryRecord.Fields.DATABASE_NAME, Schema.STRING_SCHEMA)
                        .build();
        Schema valueSchema =
                SchemaBuilder.struct()
                        .name(SchemaNameAdjuster.create().adjust(SCHEMA_CHANGE_VALUE_NAME))
                        .field(HistoryRecord.Fields.SOURCE, sourceInfoStructMaker.schema())
                        .field(HISTORY_RECORD_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                        .build();

        TableChanges tableChanges = new TableChanges();
        if (result.getType() == TableChangeType.ALTER) {
            tableChanges.alter(result.getTable());
        } else if (result.getType() == TableChangeType.CREATE) {
            tableChanges.create(result.getTable());
        } else {
            // the serializer requires a (possibly empty) table for a DROP change
            tableChanges.drop(Table.editor().tableId(result.getTableId()).create());
        }

        Map<String, Object> source = new HashMap<>();
        source.put(AbstractSourceInfo.DATABASE_NAME_KEY, message.getDatabase());
        CanalSourceInfo sourceInfo =
                new CanalSourceInfo(
                        dbzConfig,
                        message.getDatabase(),
                        message.getTable(),
                        offset.getEventTime(),
                        message.getEs(),
                        message.getTs(),
                        SnapshotRecord.FALSE);
        Struct sourceStruct = sourceInfoStructMaker.struct(sourceInfo);

        HistoryRecord historyRecord =
                new HistoryRecord(
                        source,
                        offset.getOffset(),
                        message.getDatabase(),
                        null,
                        message.getSql(),
                        tableChanges);
        String historyStr = DOCUMENT_WRITER.write(historyRecord.document());

        Struct key = new Struct(keySchema);
        key.put(HistoryRecord.Fields.DATABASE_NAME, message.getDatabase());

        Struct value = new Struct(valueSchema);
        value.put(HistoryRecord.Fields.SOURCE, sourceStruct);
        value.put(HISTORY_RECORD_FIELD, historyStr);

        String topic = MySqlTopicSelector.defaultSelector(dbzConfig).getPrimaryTopic();
        SourceRecord record =
                new SourceRecord(
                        new CanalPartition(dbzConfig.getLogicalName()).getSourcePartition(),
                        offset.getOffset(),
                        topic,
                        0,
                        keySchema,
                        key,
                        valueSchema,
                        value);
        context.getQueue().enqueue(new DataChangeEvent(record));
    }

    /** Returns the {@link CanalDdlParser} used by this handler (exposed for unit tests). */
    public CanalDdlParser getDdlParser() {
        return ddlParser;
    }
}
