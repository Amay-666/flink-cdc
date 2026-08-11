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

package org.apache.flink.cdc.connectors.canal.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.canal.utils.CanalSchemaUtils;
import org.apache.flink.cdc.connectors.canal.utils.CanalTypeUtils;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.getHistoryRecord;

/** Event deserializer for {@link CanalDataSource}. */
@Internal
public class CanalEventDeserializer extends DebeziumEventDeserializationSchema {

    private static final long serialVersionUID = 1L;

    public static final String SCHEMA_CHANGE_EVENT_KEY_NAME =
            "io.debezium.connector.canal.SchemaChangeKey";

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

    public CanalEventDeserializer(
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

    private List<SchemaChangeEvent> convertTableChange(
            TableChanges.TableChange tableChange) {
        TableId tableId = CanalSchemaUtils.toCommonTableId(tableChange.getId());
        switch (tableChange.getType()) {
            case CREATE:
                io.debezium.relational.Table table = tableChange.getTable();
                tables.overwriteTable(table);
                return Collections.singletonList(
                        new CreateTableEvent(tableId, CanalSchemaUtils.toSchema(table)));
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

    /** Diffs the old and the new table and produces the column-level schema change events. */
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
        for (io.debezium.relational.Column newColumn : newTable.columns()) {
            io.debezium.relational.Column oldColumn = oldColumns.get(newColumn.name());
            if (oldColumn == null) {
                addedColumns.add(
                        new AddColumnEvent.ColumnWithPosition(
                                CanalSchemaUtils.toColumn(newColumn)));
            } else if (!CanalTypeUtils.fromDbzColumn(oldColumn)
                    .equals(CanalTypeUtils.fromDbzColumn(newColumn))) {
                alteredColumns.put(newColumn.name(), CanalTypeUtils.fromDbzColumn(newColumn));
            }
        }
        List<String> droppedColumns =
                oldTable.columns().stream()
                        .map(io.debezium.relational.Column::name)
                        .filter(
                                oldName ->
                                        newTable.columns().stream()
                                                .noneMatch(c -> c.name().equals(oldName)))
                        .collect(Collectors.toList());

        List<SchemaChangeEvent> events = new ArrayList<>();
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
}
