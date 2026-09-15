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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.kafkajson.event.DropTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent.TableRename;
import org.apache.flink.cdc.connectors.kafkajson.event.TruncateTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.serializer.KafkaJsonEventTypeInfo;
import org.apache.flink.cdc.connectors.kafkajson.source.handler.KafkaJsonSchemaChangeHandler;
import org.apache.flink.cdc.connectors.kafkajson.utils.KafkaJsonSchemaUtils;
import org.apache.flink.cdc.connectors.kafkajson.utils.SchemaChangeUtil;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.cdc.debezium.event.DebeziumSchemaDataTypeInference;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.getHistoryRecord;

/** Event deserializer for {@link KafkaJsonDataSource}. */
@Internal
public class KafkaJsonEventDeserializer extends DebeziumEventDeserializationSchema {

    private static final long serialVersionUID = 1L;

    /**
     * The name of the schema change event key; it must match the key name {@link
     * KafkaJsonSchemaChangeHandler} uses when it serializes schema change events.
     */
    public static final String SCHEMA_CHANGE_EVENT_KEY_NAME =
            "io.debezium.connector.kafka.json.SchemaChangeKey";

    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();

    private final boolean includeSchemaChanges;

    /**
     * The table registry used to diff the old and the new table schema of an {@code ALTER} DDL. It
     * is primed by the {@code CREATE} schema change events in the stream and by the pre-change
     * schema that the canal DDL handler attaches as a leading {@code ALTER} change (snapshot tables
     * announce their {@code CREATE} as {@code CreateTableEvent}s via JDBC, so their schema-change
     * stream never contains a {@code CREATE}).
     */
    private transient Tables tables;

    public KafkaJsonEventDeserializer(
            DebeziumChangelogMode changelogMode, boolean includeSchemaChanges) {
        super(new DebeziumSchemaDataTypeInference(), changelogMode);
        this.includeSchemaChanges = includeSchemaChanges;
    }

    @Override
    protected List<SchemaChangeEvent> deserializeSchemaChangeRecord(SourceRecord record) {
        if (includeSchemaChanges) {
            if (tables == null) {
                tables = new Tables();
            }
            try {
                HistoryRecord historyRecord = getHistoryRecord(record);
                if (isRenameTableChange(historyRecord)) {
                    return handleRenameTable(historyRecord);
                }
                if (isTruncateTableChange(historyRecord)) {
                    return handleTruncateTable(record, historyRecord);
                }
                Array tableChanges =
                        historyRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
                TableChanges changes = TABLE_CHANGE_SERIALIZER.deserialize(tableChanges, true);
                String sql =
                        historyRecord.document().getString(HistoryRecord.Fields.DDL_STATEMENTS);
                List<SchemaChangeEvent> events = new ArrayList<>();
                for (TableChanges.TableChange tableChange : changes) {
                    events.addAll(convertTableChange(tableChange, sql));
                }
                return events;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to parse the schema change : " + record, e);
            }
        }
        return Collections.emptyList();
    }

    @Override
    protected boolean isDataChangeRecord(SourceRecord record) {
        Schema valueSchema = record.valueSchema();
        Struct value = (Struct) record.value();
        return value != null
                && valueSchema != null
                && valueSchema.field(Envelope.FieldName.OPERATION) != null
                && value.getString(Envelope.FieldName.OPERATION) != null;
    }

    @Override
    protected boolean isSchemaChangeRecord(SourceRecord record) {
        Schema keySchema = record.keySchema();
        return keySchema != null && SCHEMA_CHANGE_EVENT_KEY_NAME.equalsIgnoreCase(keySchema.name());
    }

    @Override
    protected TableId getTableId(SourceRecord record) {
        Struct value = (Struct) record.value();
        Struct source = value.getStruct("source");
        String database = source.getString(AbstractSourceInfo.DATABASE_NAME_KEY);
        String table = source.getString(AbstractSourceInfo.TABLE_NAME_KEY);
        return TableId.tableId(database, table);
    }

    @Override
    protected Map<String, String> getMetadata(SourceRecord record) {
        return Collections.emptyMap();
    }

    /**
     * The produced type carries the connector's own {@link KafkaJsonEventTypeInfo}, so the stream
     * is (de)serialized by {@link
     * org.apache.flink.cdc.connectors.kafkajson.serializer.KafkaJsonEventSerializer} which knows
     * how to handle {@link RenameTableEvent} and {@link TruncateTableEvent}. Without this override
     * the stream would use the released {@code EventTypeInfo}/{@code EventSerializer}, which reject
     * the new event types.
     */
    @Override
    public TypeInformation<Event> getProducedType() {
        return new KafkaJsonEventTypeInfo();
    }

    /**
     * Returns whether the schema-change record announces a table rename (a {@code RENAME_TABLE}).
     */
    private static boolean isRenameTableChange(HistoryRecord historyRecord) {
        return KafkaJsonSchemaChangeHandler.TABLE_CHANGE_TYPE_RENAME_TABLE.equals(
                historyRecord.document().getString(KafkaJsonSchemaChangeHandler.TABLE_CHANGE_TYPE));
    }

    /**
     * Rebuilds the {@link RenameTableEvent} of a {@code RENAME_TABLE} schema-change record.
     *
     * <p>Both table ids of every pair come from the custom history-record fields the DDL handler
     * attached, never from the record's {@code source}: a canal message announces the
     * <b>post</b>-rename table name, so a pair rebuilt from the source would read {@code old ==
     * new}, and a Debezium schema-change record announces no table name at all. The schema of each
     * renamed table travels as a CREATE change under its new table id, in the same order as the
     * pairs.
     */
    private List<SchemaChangeEvent> handleRenameTable(HistoryRecord historyRecord)
            throws IOException {
        Array pairsArray =
                historyRecord.document().getArray(KafkaJsonSchemaChangeHandler.RENAME_PAIRS);
        if (pairsArray == null || pairsArray.isEmpty()) {
            return Collections.emptyList();
        }
        List<io.debezium.relational.Table> newTables = findCreatedTables(historyRecord);
        List<TableRename> pairs = new ArrayList<>(pairsArray.size());
        for (int i = 0; i < pairsArray.size(); i++) {
            Document pairDocument = pairsArray.get(i).asDocument();
            TableId oldTableId =
                    readTableId(pairDocument, KafkaJsonSchemaChangeHandler.OLD_TABLE_ID);
            TableId newTableId =
                    readTableId(pairDocument, KafkaJsonSchemaChangeHandler.NEW_TABLE_ID);
            if (oldTableId == null || newTableId == null) {
                continue;
            }
            io.debezium.relational.Table newTable = i < newTables.size() ? newTables.get(i) : null;
            // Keep the internal registry in sync: the old table is gone, the new one registered.
            tables.removeTable(KafkaJsonSchemaUtils.toDbzTableId(oldTableId));
            if (newTable != null) {
                tables.overwriteTable(newTable);
            }
            pairs.add(
                    new TableRename(
                            oldTableId,
                            newTableId,
                            newTable == null
                                    ? org.apache.flink.cdc.common.schema.Schema.newBuilder().build()
                                    : KafkaJsonSchemaUtils.toSchema(newTable)));
        }
        if (pairs.isEmpty()) {
            return Collections.emptyList();
        }
        String sql = historyRecord.document().getString(HistoryRecord.Fields.DDL_STATEMENTS);
        return Collections.singletonList(new RenameTableEvent(pairs, sql));
    }

    /** Reads one table id of a rename-pair document, or {@code null} when the field is absent. */
    @Nullable
    private static TableId readTableId(Document pairDocument, String field) {
        if (pairDocument == null) {
            return null;
        }
        String value = pairDocument.getString(field);
        return value == null
                ? null
                : KafkaJsonSchemaUtils.toCommonTableId(io.debezium.relational.TableId.parse(value));
    }

    /**
     * Returns the tables of every CREATE change of the history record, in order: a rename carries
     * one per pair, under the pair's new table id.
     */
    private List<io.debezium.relational.Table> findCreatedTables(HistoryRecord historyRecord)
            throws IOException {
        Array tableChanges = historyRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
        if (tableChanges == null) {
            return Collections.emptyList();
        }
        TableChanges changes = TABLE_CHANGE_SERIALIZER.deserialize(tableChanges, true);
        List<io.debezium.relational.Table> created = new ArrayList<>();
        for (TableChanges.TableChange change : changes) {
            if (change.getType() == TableChanges.TableChangeType.CREATE) {
                created.add(change.getTable());
            }
        }
        return created;
    }

    /**
     * Returns whether the schema-change record announces a table truncate (a {@code
     * TRUNCATE_TABLE}).
     */
    private static boolean isTruncateTableChange(HistoryRecord historyRecord) {
        return KafkaJsonSchemaChangeHandler.TABLE_CHANGE_TYPE_TRUNCATE_TABLE.equals(
                historyRecord.document().getString(KafkaJsonSchemaChangeHandler.TABLE_CHANGE_TYPE));
    }

    /**
     * Rebuilds the {@link TruncateTableEvent} of a {@code TRUNCATE_TABLE} schema-change record. The
     * table id comes from the record's {@code source}; the table schema is retrieved from the
     * internal registry so that downstream consumers can clear per-table state associated with the
     * truncated table.
     */
    private List<SchemaChangeEvent> handleTruncateTable(
            SourceRecord record, HistoryRecord historyRecord) {
        TableId tableId = getTableId(record);
        io.debezium.relational.Table table =
                tables.forTable(KafkaJsonSchemaUtils.toDbzTableId(tableId));
        org.apache.flink.cdc.common.schema.Schema schema =
                table == null
                        ? org.apache.flink.cdc.common.schema.Schema.newBuilder().build()
                        : KafkaJsonSchemaUtils.toSchema(table);
        String sql = historyRecord.document().getString(HistoryRecord.Fields.DDL_STATEMENTS);
        return Collections.singletonList(new TruncateTableEvent(tableId, schema, sql));
    }

    private List<SchemaChangeEvent> convertTableChange(
            TableChanges.TableChange tableChange, String sql) {
        TableId tableId = KafkaJsonSchemaUtils.toCommonTableId(tableChange.getId());
        switch (tableChange.getType()) {
            case CREATE:
                io.debezium.relational.Table table = tableChange.getTable();
                tables.overwriteTable(table);
                return Collections.singletonList(
                        new CreateTableEvent(tableId, KafkaJsonSchemaUtils.toSchema(table)));
            case ALTER:
                io.debezium.relational.Table newTable = tableChange.getTable();
                io.debezium.relational.Table oldTable = tables.forTable(tableChange.getId());
                tables.overwriteTable(newTable);
                if (oldTable == null) {
                    // The registry does not contain the old schema, so the column-level changes
                    // cannot be derived. Skip the event rather than emitting a CreateTableEvent
                    // for an existing table, which the downstream SchemaManager rejects.
                    return Collections.emptyList();
                }
                // todo: we should introduce ddl parser to handle SchemaChangeEvent, now we just
                // diff the table columns.
                return SchemaChangeUtil.inferMinimalSchemaChanges(
                        tableId, oldTable.columns(), newTable.columns());
            case DROP:
                // The DROP table change from the source handler carries an empty stub table (the
                // serializer requires a table), so the pre-drop schema is read from the registry
                // like the truncate handler does.
                io.debezium.relational.Table droppedTable = tables.forTable(tableChange.getId());
                tables.removeTable(tableChange.getId());
                org.apache.flink.cdc.common.schema.Schema dropSchema =
                        droppedTable == null
                                ? org.apache.flink.cdc.common.schema.Schema.newBuilder().build()
                                : KafkaJsonSchemaUtils.toSchema(droppedTable);
                return Collections.singletonList(new DropTableEvent(tableId, dropSchema, sql));
            default:
                return Collections.emptyList();
        }
    }
}
