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
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.serializer.KafkaJsonEventTypeInfo;
import org.apache.flink.cdc.connectors.kafkajson.source.handler.KafkaJsonSchemaChangeHandler;
import org.apache.flink.cdc.connectors.kafkajson.utils.KafkaJsonSchemaUtils;
import org.apache.flink.cdc.connectors.kafkajson.utils.KafkaJsonTypeUtils;
import org.apache.flink.cdc.debezium.event.DebeziumEventDeserializationSchema;
import org.apache.flink.cdc.debezium.event.DebeziumSchemaDataTypeInference;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.document.Array;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.getHistoryRecord;

/** Event deserializer for {@link KafkaJsonDataSource}. */
@Internal
public class KafkaJsonEventDeserializer extends DebeziumEventDeserializationSchema {

    private static final long serialVersionUID = 1L;

    public static final String SCHEMA_CHANGE_EVENT_KEY_NAME =
            "io.debezium.connector.kafkajson.SchemaChangeKey";

    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();

    private final boolean includeSchemaChanges;

    /**
     * The table registry used to diff the old and the new table schema of an {@code ALTER} DDL. It
     * is only primed by the {@code CREATE} schema change events in the stream, so an {@code ALTER}
     * of a table whose {@code CREATE} was not observed (e.g. the table existed before the job
     * started and no subsequent {@code CREATE} was emitted) cannot be diffed and is skipped.
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
                    return handleRenameTable(record, historyRecord);
                }
                Array tableChanges =
                        historyRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
                TableChanges changes = TABLE_CHANGE_SERIALIZER.deserialize(tableChanges, true);
                List<SchemaChangeEvent> events = new ArrayList<>();
                for (TableChanges.TableChange tableChange : changes) {
                    events.addAll(convertTableChange(tableChange));
                }
                return events;
            } catch (IOException e) {
                throw new IllegalStateException(
                        "Failed to parse the schema change : " + record, e);
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
     * The produced type carries the connector's own {@link KafkaJsonEventTypeInfo}, so the stream is
     * (de)serialized by {@link org.apache.flink.cdc.connectors.kafkajson.serializer.KafkaJsonEventSerializer}
     * which knows how to handle {@link RenameTableEvent}. Without this override the stream would use
     * the released {@code EventTypeInfo}/{@code EventSerializer}, which reject the new event type.
     */
    @Override
    public TypeInformation<Event> getProducedType() {
        return new KafkaJsonEventTypeInfo();
    }

    /** Returns whether the schema-change record announces a table rename (a {@code RENAME_TABLE}). */
    private static boolean isRenameTableChange(HistoryRecord historyRecord) {
        return KafkaJsonSchemaChangeHandler.TABLE_CHANGE_TYPE_RENAME_TABLE.equals(
                historyRecord
                        .document()
                        .getString(KafkaJsonSchemaChangeHandler.TABLE_CHANGE_TYPE));
    }

    /**
     * Rebuilds the {@link RenameTableEvent} of a {@code RENAME_TABLE} schema-change record. The old
     * table id comes from the record's {@code source}, the new table id and the renamed schema come
     * from the custom history-record fields the canal DDL handler attached.
     */
    private List<SchemaChangeEvent> handleRenameTable(
            SourceRecord record, HistoryRecord historyRecord) throws IOException {
        String newTableIdStr =
                historyRecord.document().getString(KafkaJsonSchemaChangeHandler.NEW_TABLE_ID);
        if (newTableIdStr == null) {
            return Collections.emptyList();
        }
        TableId oldTableId = getTableId(record);
        TableId newTableId =
                KafkaJsonSchemaUtils.toCommonTableId(
                        io.debezium.relational.TableId.parse(newTableIdStr));
        // The schema of the renamed table travels as the single CREATE change of the history record.
        io.debezium.relational.Table newTable = findCreatedTable(historyRecord);
        // Keep the internal registry in sync: the old table is gone, the new table registered.
        tables.removeTable(KafkaJsonSchemaUtils.toDbzTableId(oldTableId));
        if (newTable != null) {
            tables.overwriteTable(newTable);
        }
        String sql = historyRecord.document().getString(HistoryRecord.Fields.DDL_STATEMENTS);
        org.apache.flink.cdc.common.schema.Schema schema =
                newTable == null
                        ? org.apache.flink.cdc.common.schema.Schema.newBuilder().build()
                        : KafkaJsonSchemaUtils.toSchema(newTable);
        return Collections.singletonList(
                new RenameTableEvent(oldTableId, newTableId, schema, sql));
    }

    private io.debezium.relational.Table findCreatedTable(HistoryRecord historyRecord)
            throws IOException {
        Array tableChanges = historyRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
        if (tableChanges == null) {
            return null;
        }
        TableChanges changes = TABLE_CHANGE_SERIALIZER.deserialize(tableChanges, true);
        for (TableChanges.TableChange change : changes) {
            if (change.getType() == TableChanges.TableChangeType.CREATE) {
                return change.getTable();
            }
        }
        return null;
    }

    private List<SchemaChangeEvent> convertTableChange(
            TableChanges.TableChange tableChange) {
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
                return diffTable(tableId, oldTable, newTable);
            case DROP:
                tables.removeTable(tableChange.getId());
                return Collections.emptyList();
            default:
                return Collections.emptyList();
        }
    }

    /**
     * Diffs the old and the new table and produces the column-level schema change events. A column
     * that is gone in the new table but re-appears at the same position with the same type under a
     * different name is treated as a rename ({@link RenameColumnEvent}) instead of a {@code DROP}+
     * {@code ADD} pair.
     */
    private List<SchemaChangeEvent> diffTable(
            TableId tableId,
            io.debezium.relational.Table oldTable,
            io.debezium.relational.Table newTable) {
        Map<String, io.debezium.relational.Column> oldColumns =
                oldTable.columns().stream()
                        .collect(
                                Collectors.toMap(
                                        io.debezium.relational.Column::name, column -> column));

        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        Map<String, DataType> alteredColumns = new HashMap<>();
        Map<String, String> renamedColumns = new HashMap<>();
        Set<String> usedOldColumns = new HashSet<>();
        for (io.debezium.relational.Column newColumn : newTable.columns()) {
            io.debezium.relational.Column oldColumn = oldColumns.get(newColumn.name());
            if (oldColumn != null) {
                usedOldColumns.add(newColumn.name());
                if (!sameColumnType(oldColumn, newColumn)) {
                    alteredColumns.put(newColumn.name(), KafkaJsonTypeUtils.fromDbzColumn(newColumn));
                }
                continue;
            }
            io.debezium.relational.Column renameSource =
                    findRenameSource(oldTable, newTable, newColumn, usedOldColumns);
            if (renameSource != null) {
                renamedColumns.put(renameSource.name(), newColumn.name());
                usedOldColumns.add(renameSource.name());
            } else {
                addedColumns.add(
                        new AddColumnEvent.ColumnWithPosition(
                                KafkaJsonSchemaUtils.toColumn(newColumn)));
            }
        }
        List<String> droppedColumns =
                oldTable.columns().stream()
                        .map(io.debezium.relational.Column::name)
                        .filter(oldName -> !usedOldColumns.contains(oldName))
                        .collect(Collectors.toList());

        List<SchemaChangeEvent> events = new ArrayList<>();
        if (!renamedColumns.isEmpty()) {
            events.add(new RenameColumnEvent(tableId, renamedColumns));
        }
        if (!addedColumns.isEmpty()) {
            events.add(new AddColumnEvent(tableId, addedColumns));
        }
        if (!droppedColumns.isEmpty()) {
            events.add(new DropColumnEvent(tableId, droppedColumns));
        }
        if (!alteredColumns.isEmpty()) {
            events.add(new AlterColumnTypeEvent(tableId, alteredColumns));
        }
        return events;
    }

    /**
     * Returns the old column a new column with no same-name counterpart was renamed from: the old
     * column at the same position whose type matches, whose name no longer exists in the new table
     * and which is not already the source of another rename. {@code null} when the new column is a
     * genuine addition.
     */
    private static io.debezium.relational.Column findRenameSource(
            io.debezium.relational.Table oldTable,
            io.debezium.relational.Table newTable,
            io.debezium.relational.Column newColumn,
            Set<String> usedOldColumns) {
        List<io.debezium.relational.Column> oldColumns = oldTable.columns();
        int position = newColumn.position();
        if (position < 1 || position > oldColumns.size()) {
            return null;
        }
        io.debezium.relational.Column candidate = oldColumns.get(position - 1);
        if (candidate == null
                || usedOldColumns.contains(candidate.name())
                || newTable.columnWithName(candidate.name()) != null) {
            return null;
        }
        return sameColumnType(candidate, newColumn) ? candidate : null;
    }

    private static boolean sameColumnType(
            io.debezium.relational.Column a, io.debezium.relational.Column b) {
        return KafkaJsonTypeUtils.fromDbzColumn(a).equals(KafkaJsonTypeUtils.fromDbzColumn(b));
    }
}
