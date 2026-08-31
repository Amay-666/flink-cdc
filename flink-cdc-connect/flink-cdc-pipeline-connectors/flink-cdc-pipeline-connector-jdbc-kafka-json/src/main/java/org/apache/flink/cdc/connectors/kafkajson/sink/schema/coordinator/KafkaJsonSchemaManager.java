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

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.MetadataColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterColumnCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterTableCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.DropTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.TruncateTableEvent;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.schema.SchemaSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.flink.cdc.common.utils.Preconditions.checkArgument;

/**
 * Schema manager of the kafka-json connector that tracks the original and evolved schema of every
 * table.
 *
 * <p>This mirrors the released {@code SchemaManager} of flink-cdc-runtime — same structure (two
 * {@code TableId → versioned schemas} maps plus the behavior), same checkpoint wire format — but
 * dispatches on {@code instanceof} for <em>all</em> ten schema-change events instead of the released
 * {@code getType()} switch, so the connector's five custom events (rename/drop/truncate/comment)
 * are handled safely:
 *
 * <ul>
 *   <li>{@link RenameTableEvent} registers the schema under the new table id and <b>keeps</b> the
 *       old table id entry. The released {@code SchemaOperator} refreshes its caches under the old
 *       table id after processing the event (it derives the id from {@code tableId()} of the
 *       rename), so removing the old entry would make that refresh fail.
 *   <li>{@link DropTableEvent} keeps the entry (idempotent {@code DROP TABLE IF EXISTS}); {@link
 *       TruncateTableEvent} leaves the schema untouched.
 *   <li>{@link AlterTableCommentEvent} / {@link AlterColumnCommentEvent} rebuild the schema with the
 *       new comment(s).
 * </ul>
 */
public class KafkaJsonSchemaManager {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonSchemaManager.class);
    private static final int INITIAL_SCHEMA_VERSION = 0;
    private static final int VERSIONS_TO_KEEP = 3;
    private final SchemaChangeBehavior behavior;

    // Serializer for checkpointing
    public static final Serializer SERIALIZER = new Serializer();

    // Schema management
    private final Map<TableId, SortedMap<Integer, Schema>> originalSchemas;

    // Schema management
    private final Map<TableId, SortedMap<Integer, Schema>> evolvedSchemas;

    public KafkaJsonSchemaManager() {
        evolvedSchemas = new HashMap<>();
        originalSchemas = new HashMap<>();
        behavior = SchemaChangeBehavior.EVOLVE;
    }

    public KafkaJsonSchemaManager(SchemaChangeBehavior behavior) {
        evolvedSchemas = new HashMap<>();
        originalSchemas = new HashMap<>();
        this.behavior = behavior;
    }

    public KafkaJsonSchemaManager(
            Map<TableId, SortedMap<Integer, Schema>> originalSchemas,
            Map<TableId, SortedMap<Integer, Schema>> evolvedSchemas,
            SchemaChangeBehavior behavior) {
        this.evolvedSchemas = evolvedSchemas;
        this.originalSchemas = originalSchemas;
        this.behavior = behavior;
    }

    public SchemaChangeBehavior getBehavior() {
        return behavior;
    }

    /**
     * Checks if the given schema change event has been applied already. If so, it will be ignored to
     * avoid sending duplicate evolved schema change events to the sink metadata applier. Unlike the
     * released {@code SchemaManager}, this dispatches on {@code instanceof} and therefore also
     * understands the connector's five custom events.
     */
    public final boolean isOriginalSchemaChangeEventRedundant(SchemaChangeEvent event) {
        if (event instanceof CreateTableEvent) {
            return getLatestOriginalSchema(event.tableId()).isPresent();
        } else if (event instanceof AddColumnEvent) {
            AddColumnEvent addColumnEvent = (AddColumnEvent) event;
            Optional<Schema> latestSchema = getLatestOriginalSchema(event.tableId());
            if (!latestSchema.isPresent()) {
                return false;
            }
            List<Column> existedColumns = latestSchema.get().getColumns();
            for (AddColumnEvent.ColumnWithPosition column : addColumnEvent.getAddedColumns()) {
                if (!existedColumns.contains(column.getAddColumn())) {
                    return false;
                }
            }
            return true;
        } else if (event instanceof AlterColumnTypeEvent) {
            AlterColumnTypeEvent alterColumnTypeEvent = (AlterColumnTypeEvent) event;
            Optional<Schema> latestSchema = getLatestOriginalSchema(event.tableId());
            if (!latestSchema.isPresent()) {
                return false;
            }
            Schema schema = latestSchema.get();
            for (Map.Entry<String, DataType> entry :
                    alterColumnTypeEvent.getTypeMapping().entrySet()) {
                if (!schema.getColumn(entry.getKey()).isPresent()
                        || !schema.getColumn(entry.getKey())
                                .get()
                                .getType()
                                .equals(entry.getValue())) {
                    return false;
                }
            }
            return true;
        } else if (event instanceof DropColumnEvent) {
            DropColumnEvent dropColumnEvent = (DropColumnEvent) event;
            Optional<Schema> latestSchema = getLatestOriginalSchema(event.tableId());
            if (!latestSchema.isPresent()) {
                return false;
            }
            List<String> existedColumnNames = latestSchema.get().getColumnNames();
            return dropColumnEvent.getDroppedColumnNames().stream()
                    .noneMatch(existedColumnNames::contains);
        } else if (event instanceof RenameColumnEvent) {
            RenameColumnEvent renameColumnEvent = (RenameColumnEvent) event;
            Optional<Schema> latestSchema = getLatestOriginalSchema(event.tableId());
            if (!latestSchema.isPresent()) {
                return false;
            }
            List<String> existedColumnNames = latestSchema.get().getColumnNames();
            for (Map.Entry<String, String> entry :
                    renameColumnEvent.getNameMapping().entrySet()) {
                if (existedColumnNames.contains(entry.getKey())
                        || !existedColumnNames.contains(entry.getValue())) {
                    return false;
                }
            }
            return true;
        } else if (event instanceof RenameTableEvent) {
            // Applied once the renamed table id has been registered
            return getLatestOriginalSchema(((RenameTableEvent) event).getNewTableId()).isPresent();
        } else if (event instanceof DropTableEvent) {
            // Drop is re-playable: DROP TABLE IF EXISTS is idempotent, and the schema entry is kept
            return false;
        } else if (event instanceof TruncateTableEvent) {
            // Truncate does not change the schema and may be re-executed safely
            return false;
        } else if (event instanceof AlterTableCommentEvent) {
            Optional<Schema> latestSchema = getLatestOriginalSchema(event.tableId());
            return latestSchema
                    .map(schema -> Objects.equals(schema.comment(), ((AlterTableCommentEvent) event).getComment()))
                    .orElse(false);
        } else if (event instanceof AlterColumnCommentEvent) {
            Optional<Schema> latestSchema = getLatestOriginalSchema(event.tableId());
            if (!latestSchema.isPresent()) {
                return false;
            }
            Map<String, String> commentMapping = ((AlterColumnCommentEvent) event).getCommentMapping();
            for (Map.Entry<String, String> entry : commentMapping.entrySet()) {
                Optional<Column> column = latestSchema.get().getColumn(entry.getKey());
                if (!column.isPresent() || !Objects.equals(column.get().getComment(), entry.getValue())) {
                    return false;
                }
            }
            return true;
        } else {
            throw new RuntimeException("Unknown schema change event: " + event);
        }
    }

    public final boolean schemaExists(
            Map<TableId, SortedMap<Integer, Schema>> schemaMap, TableId tableId) {
        return schemaMap.containsKey(tableId) && !schemaMap.get(tableId).isEmpty();
    }

    public final boolean originalSchemaExists(TableId tableId) {
        return schemaExists(originalSchemas, tableId);
    }

    public final boolean evolvedSchemaExists(TableId tableId) {
        return schemaExists(evolvedSchemas, tableId);
    }

    /** Get the latest evolved schema of the specified table. */
    public Optional<Schema> getLatestEvolvedSchema(TableId tableId) {
        return getLatestSchemaVersion(evolvedSchemas, tableId)
                .map(version -> evolvedSchemas.get(tableId).get(version));
    }

    /** Get the latest original schema of the specified table. */
    public Optional<Schema> getLatestOriginalSchema(TableId tableId) {
        return getLatestSchemaVersion(originalSchemas, tableId)
                .map(version -> originalSchemas.get(tableId).get(version));
    }

    /** Get schema at the specified version of a table. */
    public Schema getEvolvedSchema(TableId tableId, int version) {
        checkArgument(
                evolvedSchemas.containsKey(tableId),
                "Unable to find evolved schema for table \"%s\"",
                tableId);
        SortedMap<Integer, Schema> versionedSchemas = evolvedSchemas.get(tableId);
        checkArgument(
                versionedSchemas.containsKey(version),
                "Schema version %s does not exist for table \"%s\"",
                version,
                tableId);
        return versionedSchemas.get(version);
    }

    /** Get schema at the specified version of a table. */
    public Schema getOriginalSchema(TableId tableId, int version) {
        checkArgument(
                originalSchemas.containsKey(tableId),
                "Unable to find original schema for table \"%s\"",
                tableId);
        SortedMap<Integer, Schema> versionedSchemas = originalSchemas.get(tableId);
        checkArgument(
                versionedSchemas.containsKey(version),
                "Schema version %s does not exist for table \"%s\"",
                version,
                tableId);
        return versionedSchemas.get(version);
    }

    /** Apply a schema change to the original schema of the affected table. */
    public void applyOriginalSchemaChange(SchemaChangeEvent schemaChangeEvent) {
        applySchemaChange(originalSchemas, schemaChangeEvent);
    }

    /** Apply a schema change to the evolved schema of the affected table. */
    public void applyEvolvedSchemaChange(SchemaChangeEvent schemaChangeEvent) {
        applySchemaChange(evolvedSchemas, schemaChangeEvent);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaJsonSchemaManager that = (KafkaJsonSchemaManager) o;
        return Objects.equals(originalSchemas, that.originalSchemas)
                && Objects.equals(evolvedSchemas, that.evolvedSchemas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(originalSchemas, evolvedSchemas);
    }

    // -------------------------------- Helper functions -------------------------------------

    private void applySchemaChange(
            Map<TableId, SortedMap<Integer, Schema>> schemaMap, SchemaChangeEvent event) {
        if (event instanceof CreateTableEvent) {
            handleCreateTableEvent(schemaMap, (CreateTableEvent) event);
            return;
        }
        if (event instanceof RenameTableEvent) {
            // Register the renamed schema under the new table id and keep the old table id entry
            // (the released SchemaOperator refreshes its per-table caches under the old id after
            // processing a rename).
            RenameTableEvent renameTableEvent = (RenameTableEvent) event;
            LOG.info("Handling schema change event: {}", event);
            registerNewSchema(schemaMap, renameTableEvent.getNewTableId(), renameTableEvent.getSchema());
            return;
        }
        if (event instanceof DropTableEvent || event instanceof TruncateTableEvent) {
            // The schema is left unchanged: the drop/truncate DDL is idempotent, and the schema
            // entry is kept so that the SchemaOperator can still refresh its caches afterwards.
            LOG.info("Handling schema change event: {}", event);
            return;
        }
        Optional<Schema> optionalSchema = getLatestSchema(schemaMap, event.tableId());
        checkArgument(
                optionalSchema.isPresent(),
                "Unable to apply SchemaChangeEvent for table \"%s\" without existing schema",
                event.tableId());
        LOG.info("Handling schema change event: {}", event);
        Schema evolvedSchema = optionalSchema.get();
        if (event instanceof AlterTableCommentEvent) {
            registerNewSchema(
                    schemaMap,
                    event.tableId(),
                    rebuildWithComment(
                            evolvedSchema, ((AlterTableCommentEvent) event).getComment()));
        } else if (event instanceof AlterColumnCommentEvent) {
            registerNewSchema(
                    schemaMap,
                    event.tableId(),
                    rebuildWithColumnComments(
                            evolvedSchema, ((AlterColumnCommentEvent) event).getCommentMapping()));
        } else {
            // Standard events: AddColumn / AlterColumnType / DropColumn / RenameColumn
            registerNewSchema(
                    schemaMap,
                    event.tableId(),
                    SchemaUtils.applySchemaChangeEvent(evolvedSchema, event));
        }
    }

    /** Rebuilds a schema with the same columns, keys and options but a new table comment. */
    private Schema rebuildWithComment(Schema schema, String newComment) {
        return Schema.newBuilder()
                .setColumns(schema.getColumns())
                .primaryKey(schema.primaryKeys())
                .partitionKey(schema.partitionKeys())
                .options(schema.options())
                .comment(newComment)
                .build();
    }

    /** Rebuilds a schema applying the given column comment updates. */
    private Schema rebuildWithColumnComments(Schema schema, Map<String, String> commentMapping) {
        List<Column> columns = new ArrayList<>(schema.getColumns().size());
        for (Column column : schema.getColumns()) {
            if (!commentMapping.containsKey(column.getName())) {
                columns.add(column);
                continue;
            }
            String newComment = commentMapping.get(column.getName());
            if (column.isPhysical()) {
                columns.add(
                        Column.physicalColumn(
                                column.getName(),
                                column.getType(),
                                newComment,
                                column.getDefaultValueExpression()));
            } else {
                MetadataColumn metadataColumn = (MetadataColumn) column;
                columns.add(
                        Column.metadataColumn(
                                column.getName(),
                                column.getType(),
                                metadataColumn.getMetadataKey(),
                                newComment));
            }
        }
        return schema.copy(columns);
    }

    private Optional<Schema> getLatestSchema(
            final Map<TableId, SortedMap<Integer, Schema>> schemaMap, TableId tableId) {
        return getLatestSchemaVersion(schemaMap, tableId)
                .map(version -> schemaMap.get(tableId).get(version));
    }

    private Optional<Integer> getLatestSchemaVersion(
            final Map<TableId, SortedMap<Integer, Schema>> schemaMap, TableId tableId) {
        if (!schemaMap.containsKey(tableId)) {
            return Optional.empty();
        }
        try {
            return Optional.of(schemaMap.get(tableId).lastKey());
        } catch (NoSuchElementException e) {
            return Optional.empty();
        }
    }

    private void handleCreateTableEvent(
            final Map<TableId, SortedMap<Integer, Schema>> schemaMap, CreateTableEvent event) {
        checkArgument(
                !schemaExists(schemaMap, event.tableId()),
                "Unable to apply CreateTableEvent to an existing schema for table \"%s\"",
                event.tableId());
        LOG.info("Handling schema change event: {}", event);
        registerNewSchema(schemaMap, event.tableId(), event.getSchema());
    }

    private void registerNewSchema(
            final Map<TableId, SortedMap<Integer, Schema>> schemaMap,
            TableId tableId,
            Schema newSchema) {
        if (schemaExists(schemaMap, tableId)) {
            SortedMap<Integer, Schema> versionedSchemas = schemaMap.get(tableId);
            Integer latestVersion = versionedSchemas.lastKey();
            versionedSchemas.put(latestVersion + 1, newSchema);
            if (versionedSchemas.size() > VERSIONS_TO_KEEP) {
                versionedSchemas.remove(versionedSchemas.firstKey());
            }
        } else {
            TreeMap<Integer, Schema> versionedSchemas = new TreeMap<>();
            versionedSchemas.put(INITIAL_SCHEMA_VERSION, newSchema);
            schemaMap.putIfAbsent(tableId, versionedSchemas);
        }
    }

    /** Serializer for {@link KafkaJsonSchemaManager}. */
    public static class Serializer implements SimpleVersionedSerializer<KafkaJsonSchemaManager> {

        /**
         * Update history: from Version 3.0.0, set to 0, from version 3.1.1, updated to 1, from
         * version 3.2.0, updated to 2.
         */
        public static final int CURRENT_VERSION = 2;

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public byte[] serialize(KafkaJsonSchemaManager schemaManager) throws IOException {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream out = new DataOutputStream(baos)) {
                serializeSchemaMap(schemaManager.evolvedSchemas, out);
                serializeSchemaMap(schemaManager.originalSchemas, out);
                out.writeUTF(schemaManager.getBehavior().name());
                return baos.toByteArray();
            }
        }

        private static void serializeSchemaMap(
                Map<TableId, SortedMap<Integer, Schema>> schemaMap, DataOutputStream out)
                throws IOException {
            TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
            SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;
            // Number of tables
            out.writeInt(schemaMap.size());
            for (Map.Entry<TableId, SortedMap<Integer, Schema>> tableSchema :
                    schemaMap.entrySet()) {
                // Table ID
                TableId tableId = tableSchema.getKey();
                tableIdSerializer.serialize(tableId, new DataOutputViewStreamWrapper(out));

                // Schema with versions
                SortedMap<Integer, Schema> versionedSchemas = tableSchema.getValue();
                out.writeInt(versionedSchemas.size());
                for (Map.Entry<Integer, Schema> versionedSchema : versionedSchemas.entrySet()) {
                    // Version
                    Integer version = versionedSchema.getKey();
                    out.writeInt(version);
                    // Schema
                    Schema schema = versionedSchema.getValue();
                    schemaSerializer.serialize(schema, new DataOutputViewStreamWrapper(out));
                }
            }
        }

        @Override
        public KafkaJsonSchemaManager deserialize(int version, byte[] serialized)
                throws IOException {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                    DataInputStream in = new DataInputStream(bais)) {
                switch (version) {
                    case 0:
                    case 1:
                        {
                            Map<TableId, SortedMap<Integer, Schema>> schemas =
                                    deserializeSchemaMap(version, in);
                            // In legacy mode, original schema and evolved schema never differs
                            return new KafkaJsonSchemaManager(
                                    schemas, schemas, SchemaChangeBehavior.EVOLVE);
                        }
                    case 2:
                        {
                            Map<TableId, SortedMap<Integer, Schema>> evolvedSchemas =
                                    deserializeSchemaMap(version, in);
                            Map<TableId, SortedMap<Integer, Schema>> originalSchemas =
                                    deserializeSchemaMap(version, in);
                            SchemaChangeBehavior behavior =
                                    SchemaChangeBehavior.valueOf(in.readUTF());
                            return new KafkaJsonSchemaManager(
                                    originalSchemas, evolvedSchemas, behavior);
                        }
                    default:
                        throw new RuntimeException("Unknown serialize version: " + version);
                }
            }
        }

        private static Map<TableId, SortedMap<Integer, Schema>> deserializeSchemaMap(
                int version, DataInputStream in) throws IOException {
            TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
            SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;
            // Total schema length
            int numTables = in.readInt();
            Map<TableId, SortedMap<Integer, Schema>> tableSchemas = new HashMap<>(numTables);
            for (int i = 0; i < numTables; i++) {
                // Table ID
                TableId tableId = tableIdSerializer.deserialize(new DataInputViewStreamWrapper(in));
                // Schema with versions
                int numVersions = in.readInt();
                SortedMap<Integer, Schema> versionedSchemas = new TreeMap<>(Integer::compareTo);
                for (int j = 0; j < numVersions; j++) {
                    // Version
                    int schemaVersion = in.readInt();
                    Schema schema =
                            schemaSerializer.deserialize(
                                    version, new DataInputViewStreamWrapper(in));
                    versionedSchemas.put(schemaVersion, schema);
                }
                tableSchemas.put(tableId, versionedSchemas);
            }
            return tableSchemas;
        }
    }
}
