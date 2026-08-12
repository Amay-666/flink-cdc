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

package org.apache.flink.cdc.connectors.kafkajson.source.handler;

import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.DdlParser;
import org.apache.flink.cdc.connectors.kafkajson.source.ddl.KafkaJsonDdlParsedResult;
import org.apache.flink.cdc.connectors.kafkajson.source.ddl.KafkaJsonDdlParser;
import org.apache.flink.cdc.connectors.kafkajson.source.ddl.KafkaJsonDebeziumDdlParser;
import org.apache.flink.cdc.connectors.kafkajson.source.ddl.KafkaJsonDruidDdlParser;
import org.apache.flink.cdc.connectors.kafkajson.source.ddl.KafkaJsonTableChangeType;
import org.apache.flink.cdc.connectors.kafkajson.source.fetch.KafkaJsonSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonFlatMessage;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonPartition;
import org.apache.flink.cdc.connectors.kafkajson.source.schema.KafkaJsonSourceInfo;
import org.apache.flink.cdc.connectors.kafkajson.source.schema.KafkaJsonSourceInfoStructMaker;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlTopicSelector;
import io.debezium.document.Document;
import io.debezium.document.DocumentWriter;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
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
 * KafkaJsonSchema} — so that subsequent data records of the affected table are decoded with the new
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
public class KafkaJsonSchemaChangeHandler {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonSchemaChangeHandler.class);

    private static final String HISTORY_RECORD_FIELD = "historyRecord";
    private static final String SCHEMA_CHANGE_KEY_NAME =
            "io.debezium.connector.kafkajson.SchemaChangeKey";
    private static final String SCHEMA_CHANGE_VALUE_NAME =
            "io.debezium.connector.canal.SchemaChangeValue";

    /**
     * The custom fields set on the history-record document of a rename schema change. They are read
     * by {@code KafkaJsonEventDeserializer} (in the pipeline module) to rebuild the rename that the
     * Debezium {@code TableChanges.TableChangeType} cannot express: {@code RENAME_TABLE} carries the
     * {@link #NEW_TABLE_ID} of the renamed table, {@code RENAME_COLUMN} only the type marker.
     */
    public static final String TABLE_CHANGE_TYPE = "tableChangeType";
    public static final String TABLE_CHANGE_TYPE_RENAME_TABLE = "RENAME_TABLE";
    public static final String TABLE_CHANGE_TYPE_RENAME_COLUMN = "RENAME_COLUMN";
    public static final String NEW_TABLE_ID = "newTableId";

    private static final DocumentWriter DOCUMENT_WRITER = DocumentWriter.defaultWriter();

    private final KafkaJsonDdlParser ddlParser;
    private final KafkaJsonSourceInfoStructMaker sourceInfoStructMaker;

    public KafkaJsonSchemaChangeHandler(KafkaJsonSourceConfig sourceConfig) {
        this.ddlParser = createParser(sourceConfig.getDdlParser());
        this.sourceInfoStructMaker =
                new KafkaJsonSourceInfoStructMaker(
                        "canal", KafkaJsonSourceInfoStructMaker.DEBEZIUM_VERSION,
                        sourceConfig.getDbzConnectorConfig());
    }

    private static KafkaJsonDdlParser createParser(DdlParser parserType) {
        switch (parserType) {
            case DEBEZIUM:
                return new KafkaJsonDebeziumDdlParser();
            case DRUID:
            default:
                return new KafkaJsonDruidDdlParser();
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
            KafkaJsonSourceFetchTaskContext context, KafkaJsonFlatMessage message, KafkaJsonOffset offset)
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
        KafkaJsonDdlParsedResult result =
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
            KafkaJsonSourceFetchTaskContext context, KafkaJsonDdlParsedResult result) {
        KafkaJsonTableChangeType type = result.getType();
        if (type == KafkaJsonTableChangeType.DROP) {
            context.getDatabaseSchema().removeTable(result.getTableId());
        } else if (type == KafkaJsonTableChangeType.RENAME_TABLE) {
            // The old table is gone; register the renamed table so that subsequent data records of
            // the new table are decoded with the new schema. When the old schema was not observed,
            // the new table cannot be registered (its schema is unknown) but the rename is still
            // announced.
            context.getDatabaseSchema().removeTable(result.getTableId());
            if (result.getNewTable() != null) {
                context.getDatabaseSchema().registerTable(result.getNewTable());
            }
        } else if (result.getNewTable() != null) {
            // CREATE / ALTER / RENAME_COLUMN all leave the affected table under the same id
            context.getDatabaseSchema().registerTable(result.getNewTable());
        }
    }

    private void enqueueSchemaChange(
            KafkaJsonSourceFetchTaskContext context,
            KafkaJsonFlatMessage message,
            KafkaJsonOffset offset,
            KafkaJsonDdlParsedResult result)
            throws IOException, InterruptedException {
        KafkaJsonSourceConfig sourceConfig = context.getSourceConfig();
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
        KafkaJsonTableChangeType type = result.getType();
        if (type == KafkaJsonTableChangeType.CREATE) {
            tableChanges.create(result.getNewTable());
        } else if (type == KafkaJsonTableChangeType.ALTER
                || type == KafkaJsonTableChangeType.RENAME_COLUMN) {
            // The Debezium history format carries only the post-change schema in an ALTER
            // TableChange. The pipeline deserializer derives the column-level events by diffing the
            // old and the new schema, so it needs both images: snapshot tables announce their schema
            // as CreateTableEvents via JDBC (bypassing the schema-change stream), so the deserializer
            // never observed their CREATE and cannot diff an ALTER on its own. Carry the pre-change
            // schema as a leading ALTER change — the deserializer processes the changes in order, so
            // the leading change primes its registry and the trailing change is diffed against it.
            if (result.getOldTable() != null) {
                tableChanges.alter(result.getOldTable());
            }
            tableChanges.alter(result.getNewTable());
        } else if (type == KafkaJsonTableChangeType.RENAME_TABLE) {
            // The schema of the renamed table travels as a CREATE change of the new table id; the
            // rename itself is carried by the custom history-record fields below.
            if (result.getNewTable() != null) {
                tableChanges.create(result.getNewTable());
            }
        } else {
            // the serializer requires a (possibly empty) table for a DROP change
            tableChanges.drop(Table.editor().tableId(result.getTableId()).create());
        }

        Map<String, Object> source = new HashMap<>();
        source.put(AbstractSourceInfo.DATABASE_NAME_KEY, message.getDatabase());
        KafkaJsonSourceInfo sourceInfo =
                new KafkaJsonSourceInfo(
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
        Document historyDocument = historyRecord.document();
        if (type == KafkaJsonTableChangeType.RENAME_TABLE) {
            historyDocument.set(TABLE_CHANGE_TYPE, TABLE_CHANGE_TYPE_RENAME_TABLE);
            if (result.getNewTableId() != null) {
                historyDocument.set(NEW_TABLE_ID, result.getNewTableId().toString());
            }
        } else if (type == KafkaJsonTableChangeType.RENAME_COLUMN) {
            historyDocument.set(TABLE_CHANGE_TYPE, TABLE_CHANGE_TYPE_RENAME_COLUMN);
        }
        String historyStr = DOCUMENT_WRITER.write(historyDocument);

        Struct key = new Struct(keySchema);
        key.put(HistoryRecord.Fields.DATABASE_NAME, message.getDatabase());

        Struct value = new Struct(valueSchema);
        value.put(HistoryRecord.Fields.SOURCE, sourceStruct);
        value.put(HISTORY_RECORD_FIELD, historyStr);

        String topic = MySqlTopicSelector.defaultSelector(dbzConfig).getPrimaryTopic();
        SourceRecord record =
                new SourceRecord(
                        new KafkaJsonPartition(dbzConfig.getLogicalName()).getSourcePartition(),
                        offset.getOffset(),
                        topic,
                        0,
                        keySchema,
                        key,
                        valueSchema,
                        value);
        context.getQueue().enqueue(new DataChangeEvent(record));
    }

    /** Returns the {@link KafkaJsonDdlParser} used by this handler (exposed for unit tests). */
    public KafkaJsonDdlParser getDdlParser() {
        return ddlParser;
    }
}
