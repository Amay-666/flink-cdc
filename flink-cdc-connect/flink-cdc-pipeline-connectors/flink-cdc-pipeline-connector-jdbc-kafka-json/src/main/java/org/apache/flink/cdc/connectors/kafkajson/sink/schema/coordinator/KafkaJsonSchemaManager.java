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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.utils.Preconditions.checkArgument;

/**
 * Schema manager of the kafka-json connector that tracks the original and evolved schema of every
 * table.
 *
 * <p>This mirrors the released {@code SchemaManager} of flink-cdc-runtime — same structure (two
 * {@code TableId → versioned schemas} maps plus the behavior), and it reuses the released {@code
 * SchemaSerializer} for the schemas it writes — but dispatches on {@code instanceof} for
 * <em>all</em> ten schema-change events instead of the released {@code getType()} switch, so the
 * connector's five custom events (rename/drop/truncate/comment) are handled safely:
 *
 * <ul>
 *   <li>{@link RenameTableEvent} registers the schema under the new table id and <b>keeps</b> the
 *       old table id entry. The released {@code SchemaOperator} refreshes its caches under the old
 *       table id after processing the event (it derives the id from {@code tableId()} of the
 *       rename), so removing the old entry would make that refresh fail. The old table id is
 *       recorded in {@link #removedTables}, because no table carries it any more.
 *   <li>{@link DropTableEvent} keeps the entry (idempotent {@code DROP TABLE IF EXISTS}) and
 *       records the table id in {@link #removedTables} as well; {@link TruncateTableEvent} leaves
 *       the schema untouched.
 *   <li>{@link AlterTableCommentEvent} / {@link AlterColumnCommentEvent} rebuild the schema with
 *       the new comment(s).
 * </ul>
 *
 * <p>The kept entries of {@link #removedTables} are what make the first two events dangerous for
 * the redundancy check: an entry outlives its table, so a {@code CREATE TABLE} — or a rename back
 * onto that id — would be discarded as a duplicate request while the external system has no such
 * table. {@link #removedTables} is therefore consulted by the redundancy check and is part of the
 * checkpointed state.
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

    /**
     * Table ids that no table carries any more: {@code DROP TABLE} has been applied for them, or
     * the table was renamed away to a new id. Their schema {@link #originalSchemas} / {@link
     * #evolvedSchemas} entries are kept (see the class javadoc), so this set is what tells the
     * redundancy check that the id is free again and a later {@code CREATE TABLE} — or a rename
     * back onto it — has to be applied instead of being discarded. Part of the checkpointed state:
     * without it a failover would restore a manager that swallows the re-creation again.
     */
    private final Set<TableId> removedTables;

    public KafkaJsonSchemaManager() {
        evolvedSchemas = new HashMap<>();
        originalSchemas = new HashMap<>();
        removedTables = new HashSet<>();
        behavior = SchemaChangeBehavior.EVOLVE;
    }

    public KafkaJsonSchemaManager(SchemaChangeBehavior behavior) {
        evolvedSchemas = new HashMap<>();
        originalSchemas = new HashMap<>();
        removedTables = new HashSet<>();
        this.behavior = behavior;
    }

    public KafkaJsonSchemaManager(
            Map<TableId, SortedMap<Integer, Schema>> originalSchemas,
            Map<TableId, SortedMap<Integer, Schema>> evolvedSchemas,
            SchemaChangeBehavior behavior) {
        this(originalSchemas, evolvedSchemas, new HashSet<>(), behavior);
    }

    public KafkaJsonSchemaManager(
            Map<TableId, SortedMap<Integer, Schema>> originalSchemas,
            Map<TableId, SortedMap<Integer, Schema>> evolvedSchemas,
            Set<TableId> removedTables,
            SchemaChangeBehavior behavior) {
        this.evolvedSchemas = evolvedSchemas;
        this.originalSchemas = originalSchemas;
        this.removedTables = removedTables;
        this.behavior = behavior;
    }

    public SchemaChangeBehavior getBehavior() {
        return behavior;
    }

    /**
     * Checks if the given schema change event has been applied already. If so, it will be ignored
     * to avoid sending duplicate evolved schema change events to the sink metadata applier. Unlike
     * the released {@code SchemaManager}, this dispatches on {@code instanceof} and therefore also
     * understands the connector's five custom events.
     */
    public final boolean isOriginalSchemaChangeEventRedundant(SchemaChangeEvent event) {
        if (event instanceof CreateTableEvent) {
            // A CREATE TABLE for an id that is free again is never redundant: the entry of the
            // removed table is still around, but the table itself is gone.
            return getLatestOriginalSchema(event.tableId()).isPresent()
                    && !removedTables.contains(event.tableId());
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
            for (Map.Entry<String, String> entry : renameColumnEvent.getNameMapping().entrySet()) {
                if (existedColumnNames.contains(entry.getKey())
                        || !existedColumnNames.contains(entry.getValue())) {
                    return false;
                }
            }
            return true;
        } else if (event instanceof RenameTableEvent) {
            // Applied once every renamed table id has been registered. An id that is free again
            // still has an entry, so it is checked against the removed set as well.
            List<RenameTableEvent.TableRename> pairs = ((RenameTableEvent) event).getPairs();
            Set<TableId> oldTableIds =
                    pairs.stream()
                            .map(RenameTableEvent.TableRename::getOldTableId)
                            .collect(Collectors.toSet());
            for (RenameTableEvent.TableRename pair : pairs) {
                TableId newTableId = pair.getNewTableId();
                // A name that the same statement renames away is occupied before and after it —
                // an `a TO b, b TO a` swap leaves both names holding a table, so both targets
                // already have a registered schema and "already registered" says nothing about
                // whether this rename was applied.
                if (oldTableIds.contains(newTableId)
                        || !getLatestOriginalSchema(newTableId).isPresent()
                        || removedTables.contains(newTableId)) {
                    return false;
                }
            }
            return true;
        } else if (event instanceof DropTableEvent) {
            // Drop is re-playable: DROP TABLE IF EXISTS is idempotent, and the schema entry is kept
            return false;
        } else if (event instanceof TruncateTableEvent) {
            // Truncate does not change the schema and may be re-executed safely
            return false;
        } else if (event instanceof AlterTableCommentEvent) {
            Optional<Schema> latestSchema = getLatestOriginalSchema(event.tableId());
            return latestSchema
                    .map(
                            schema ->
                                    Objects.equals(
                                            schema.comment(),
                                            ((AlterTableCommentEvent) event).getComment()))
                    .orElse(false);
        } else if (event instanceof AlterColumnCommentEvent) {
            Optional<Schema> latestSchema = getLatestOriginalSchema(event.tableId());
            if (!latestSchema.isPresent()) {
                return false;
            }
            Map<String, String> commentMapping =
                    ((AlterColumnCommentEvent) event).getCommentMapping();
            for (Map.Entry<String, String> entry : commentMapping.entrySet()) {
                Optional<Column> column = latestSchema.get().getColumn(entry.getKey());
                if (!column.isPresent()
                        || !Objects.equals(column.get().getComment(), entry.getValue())) {
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
        applySchemaChange(originalSchemas, schemaChangeEvent, false);
    }

    /**
     * Apply a schema change to the evolved schema of the affected table. The request handler always
     * applies the original schema first and the evolved one last, so the evolved apply is the one
     * that consumes the removed-table markers of the event (see {@link #removedTables}).
     */
    public void applyEvolvedSchemaChange(SchemaChangeEvent schemaChangeEvent) {
        applySchemaChange(evolvedSchemas, schemaChangeEvent, true);
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
                && Objects.equals(evolvedSchemas, that.evolvedSchemas)
                && Objects.equals(removedTables, that.removedTables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(originalSchemas, evolvedSchemas, removedTables);
    }

    // -------------------------------- Helper functions -------------------------------------

    private void applySchemaChange(
            Map<TableId, SortedMap<Integer, Schema>> schemaMap,
            SchemaChangeEvent event,
            boolean lastApply) {
        if (event instanceof CreateTableEvent) {
            handleCreateTableEvent(schemaMap, (CreateTableEvent) event, lastApply);
            return;
        }
        if (event instanceof RenameTableEvent) {
            // Register every renamed schema under its new table id and keep the old table id
            // entries (the released SchemaOperator refreshes its per-table caches under the first
            // old id after processing a rename).
            RenameTableEvent renameTableEvent = (RenameTableEvent) event;
            LOG.info("Handling schema change event: {}", event);
            List<RenameTableEvent.TableRename> pairs = renameTableEvent.getPairs();
            Set<TableId> newTableIds =
                    pairs.stream()
                            .map(RenameTableEvent.TableRename::getNewTableId)
                            .collect(Collectors.toSet());
            for (RenameTableEvent.TableRename pair : pairs) {
                // The old id is free again: no table carries it any more, so a later CREATE TABLE
                // of that name — or a rename back onto it — must be applied rather than discarded.
                // A name another pair of the same statement renames onto is the exception: a swap
                // leaves it occupied, and marking it removed would discard what that pair
                // registers for it.
                if (!newTableIds.contains(pair.getOldTableId())) {
                    removedTables.add(pair.getOldTableId());
                }
            }
            for (RenameTableEvent.TableRename pair : pairs) {
                if (resetRemovedTable(
                        schemaMap, pair.getNewTableId(), pair.getSchema(), lastApply)) {
                    continue;
                }
                registerNewSchema(schemaMap, pair.getNewTableId(), pair.getSchema());
            }
            return;
        }
        if (event instanceof DropTableEvent) {
            // The schema is left unchanged: the drop DDL is idempotent, and the entry is kept so
            // that the released SchemaOperator can still refresh its caches afterwards. The table
            // is recorded as removed, which is what lets a later CREATE TABLE of the same name be
            // applied instead of being discarded as a duplicate.
            LOG.info("Handling schema change event: {}", event);
            removedTables.add(event.tableId());
            return;
        }
        if (event instanceof TruncateTableEvent) {
            // Truncate does not change the schema; the entry is kept for the same reason as above.
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
            registerCommentChange(
                    schemaMap,
                    event.tableId(),
                    evolvedSchema,
                    rebuildWithComment(
                            evolvedSchema, ((AlterTableCommentEvent) event).getComment()));
        } else if (event instanceof AlterColumnCommentEvent) {
            registerCommentChange(
                    schemaMap,
                    event.tableId(),
                    evolvedSchema,
                    rebuildWithColumnComments(
                            event.tableId(),
                            evolvedSchema,
                            ((AlterColumnCommentEvent) event).getCommentMapping()));
        } else {
            // Standard events: AddColumn / AlterColumnType / DropColumn / RenameColumn
            registerNewSchema(
                    schemaMap,
                    event.tableId(),
                    SchemaUtils.applySchemaChangeEvent(evolvedSchema, event));
        }
    }

    /**
     * Registers the schema a comment event produced, unless it is identical to the one already
     * registered.
     *
     * <p>A comment event can leave the schema untouched — a comment that repeats the current one,
     * or a column comment naming a column the table does not have (see {@link
     * #rebuildWithColumnComments}). Registering such a schema would still consume one of the
     * {@value #VERSIONS_TO_KEEP} retained versions and eventually evict a version that does differ,
     * which a checkpoint restart then restores from that older state. The event itself stays
     * reproducible, so replaying it against the unchanged version is harmless.
     */
    private void registerCommentChange(
            final Map<TableId, SortedMap<Integer, Schema>> schemaMap,
            TableId tableId,
            Schema currentSchema,
            Schema newSchema) {
        if (currentSchema.equals(newSchema)) {
            LOG.warn(
                    "Schema change for table \"{}\" produced the schema that is already "
                            + "registered; the schema version is left untouched so that no "
                            + "retained version is evicted.",
                    tableId);
            return;
        }
        registerNewSchema(schemaMap, tableId, newSchema);
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
    private Schema rebuildWithColumnComments(
            TableId tableId, Schema schema, Map<String, String> commentMapping) {
        for (String columnName : commentMapping.keySet()) {
            if (!schema.getColumn(columnName).isPresent()) {
                // The events reaching a coordinator are derived from a before/after column diff,
                // which only ever pairs columns present on both sides, so an unknown name means the
                // event did not come from that path. Surface it: the loop below drops the update
                // while the DDL builder still emits it, and Doris then rejects the statement — the
                // warning is what connects the two.
                LOG.warn(
                        "Column comment change names column \"{}\" of table \"{}\", which the "
                                + "current schema does not have; the comment is ignored here.",
                        columnName,
                        tableId);
            }
        }
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
            final Map<TableId, SortedMap<Integer, Schema>> schemaMap,
            CreateTableEvent event,
            boolean lastApply) {
        LOG.info("Handling schema change event: {}", event);
        if (resetRemovedTable(schemaMap, event.tableId(), event.getSchema(), lastApply)) {
            LOG.info(
                    "Table {} had been removed; its schema history was reset by this re-creation.",
                    event.tableId());
            return;
        }
        checkArgument(
                !schemaExists(schemaMap, event.tableId()),
                "Unable to apply CreateTableEvent to an existing schema for table \"%s\"",
                event.tableId());
        registerNewSchema(schemaMap, event.tableId(), event.getSchema());
    }

    /**
     * Replaces the schema entry of a table id that is free again with a fresh history holding only
     * {@code newSchema}. The stale entry cannot simply be removed: the released {@code
     * SchemaOperator} refreshes its caches under that table id right after the event and fails the
     * job when no schema can be found for it.
     *
     * @param lastApply whether this is the last apply of the event. Both the original and the
     *     evolved map have to see the marker, so it is only consumed on the last apply.
     * @return whether the table id was marked as removed (and its history was therefore reset)
     */
    private boolean resetRemovedTable(
            final Map<TableId, SortedMap<Integer, Schema>> schemaMap,
            TableId tableId,
            Schema newSchema,
            boolean lastApply) {
        if (!removedTables.contains(tableId)) {
            return false;
        }
        if (lastApply) {
            removedTables.remove(tableId);
        }
        SortedMap<Integer, Schema> versionedSchemas = new TreeMap<>();
        versionedSchemas.put(INITIAL_SCHEMA_VERSION, newSchema);
        schemaMap.put(tableId, versionedSchemas);
        return true;
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
         * Format version this connector writes. Versions 0-2 are the released {@code SchemaManager}
         * formats (0 and 1 hold a single schema map, 2 splits it into evolved and original) and are
         * read for compatibility only — this connector has never written either, and {@link
         * #deserialize} keeps the branches so a manager restored from a released connector's
         * checkpoint still loads. Version 3 appends the set of removed tables, which the released
         * format has no field for — dropping it would restore a schema manager that swallows a
         * re-created table's CREATE TABLE event after a failover.
         */
        public static final int CURRENT_VERSION = 3;

        /**
         * Highest format version the released {@code SchemaSerializer} understands. Kept here
         * because the released field is private, and because a manager version above it must not be
         * forwarded to the schema serializer (see {@link #schemaSerializerVersion(int)}).
         */
        private static final int SCHEMA_SERIALIZER_VERSION = 2;

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
                // Table ids that are free again (dropped, or renamed away) and have not been taken
                // by a new table since.
                TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
                out.writeInt(schemaManager.removedTables.size());
                for (TableId removedTable : schemaManager.removedTables) {
                    tableIdSerializer.serialize(removedTable, new DataOutputViewStreamWrapper(out));
                }
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
                    case 3:
                        {
                            Map<TableId, SortedMap<Integer, Schema>> evolvedSchemas =
                                    deserializeSchemaMap(version, in);
                            Map<TableId, SortedMap<Integer, Schema>> originalSchemas =
                                    deserializeSchemaMap(version, in);
                            SchemaChangeBehavior behavior =
                                    SchemaChangeBehavior.valueOf(in.readUTF());
                            Set<TableId> removedTables = new HashSet<>();
                            if (version >= 3) {
                                TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
                                int numRemovedTables = in.readInt();
                                for (int i = 0; i < numRemovedTables; i++) {
                                    removedTables.add(
                                            tableIdSerializer.deserialize(
                                                    new DataInputViewStreamWrapper(in)));
                                }
                            }
                            return new KafkaJsonSchemaManager(
                                    originalSchemas, evolvedSchemas, removedTables, behavior);
                        }
                    default:
                        throw new RuntimeException("Unknown serialize version: " + version);
                }
            }
        }

        private static Map<TableId, SortedMap<Integer, Schema>> deserializeSchemaMap(
                int managerVersion, DataInputStream in) throws IOException {
            TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
            SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;
            int schemaSerializerVersion = schemaSerializerVersion(managerVersion);
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
                                    schemaSerializerVersion, new DataInputViewStreamWrapper(in));
                    versionedSchemas.put(schemaVersion, schema);
                }
                tableSchemas.put(tableId, versionedSchemas);
            }
            return tableSchemas;
        }

        /**
         * Returns the {@code SchemaSerializer} format the schema payloads of a manager version were
         * written in. The released manager numbered them alongside its own version (0/1/2) and
         * handed its version to the schema serializer, but the released schema serializer knows no
         * version above {@value #SCHEMA_SERIALIZER_VERSION} — so manager version 3, which only
         * appended the removed-table set, writes and reads its schemas in that same format.
         */
        private static int schemaSerializerVersion(int managerVersion) {
            return Math.min(managerVersion, SCHEMA_SERIALIZER_VERSION);
        }
    }
}
