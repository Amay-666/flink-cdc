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

package org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.ddl;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterColumnCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterTableCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.DropTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.TruncateTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.http.DorisHttpClient;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.alert.SchemaEvolutionAlert;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.alert.SchemaEvolutionAlertHandler;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordinator.OldSchemaAwareMetadataApplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Applies schema changes to Doris over HTTP.
 *
 * <p>Runs on the JobManager inside the schema-evolution coordinator. Every event — the five
 * standard ones plus the connector's five custom ones — is dispatched by {@code instanceof} to
 * {@link DorisDdlBuilder}, and the resulting statements are executed through a single {@link
 * DorisHttpClient} held for the applier's lifetime (see {@link #httpClient}). All event types are
 * accepted (no {@code acceptsSchemaEvolutionType} narrowing), matching the coordinator's
 * pass-through derivation.
 *
 * <p>Implements {@link OldSchemaAwareMetadataApplier}: when the coordinator applies an {@link
 * AlterColumnTypeEvent} it also resolves the pre-change schema (an event only carries the new
 * types), which lets this applier recognise a primary-key column and a capacity reduction.
 *
 * <h2>Schema-change pre-check</h2>
 *
 * <p>The coordinator's schema state is checkpointed, but a DDL is executed before its round is
 * committed. After a failover the schema registry is restored to the last completed checkpoint, so
 * every round applied after it is delivered again. Some of those DDLs are not idempotent — a second
 * {@code ADD COLUMN} makes Doris reject the statement and fail the job, replayed {@code DROP
 * COLUMN}/{@code RENAME} fail because their source is gone, and a replayed {@code DROP TABLE}
 * followed by {@code CREATE TABLE} (when {@code sink.is-drop-table-before-create} is set) would
 * discard the data loaded since the checkpoint. Before executing such a change the applier
 * therefore reads the target table's current columns ({@link DorisMetadataInspector}) and skips the
 * operations Doris already reflects. The pre-check is controlled by {@code
 * sink.schema-change.pre-check} (default on) and is a no-op when disabled.
 *
 * <p>Skipping has two grades, matching what a user needs to know:
 *
 * <ul>
 *   <li><b>Duplicate</b> — the DDL was already applied (the column exists, the rename's target is
 *       already in place, the table's type is already the target). Skipping is idempotent and is
 *       logged at INFO; it never reaches an alert handler.
 *   <li><b>Non-idempotent skip</b> — applying the change is impossible and the target schema stays
 *       diverged: a {@code MODIFY COLUMN} on a key column, a capacity reduction, a rename whose
 *       source and target are both missing. These are logged at WARN and delivered to the
 *       configured {@link SchemaEvolutionAlertHandler}.
 * </ul>
 *
 * <p>Alerts are delivered directly from this applier (rather than through an operator side output,
 * which the JobManager-side coordinator has no access to). Delivery is at-least-once across
 * checkpoint restores: a round that did not commit is replayed, and with it its alerts. Each alert
 * carries a deterministic id for receiver-side deduplication; a failed handler call is logged and
 * swallowed so that alerting can never fail the data pipeline.
 *
 * <p>After executing a {@code MODIFY COLUMN} DDL (which triggers a heavy, asynchronous schema
 * change in Doris), the applier delegates to {@link DorisSchemaChangeMonitor} to poll {@code SHOW
 * ALTER TABLE COLUMN} until the job reaches a final state. This ensures Group Commit writes are not
 * rejected when data flow resumes. The DDL execution and the SHOW polling share the same {@code
 * DorisHttpClient} instance.
 */
public class DorisMetadataApplier implements MetadataApplier, OldSchemaAwareMetadataApplier {

    private static final Logger LOG = LoggerFactory.getLogger(DorisMetadataApplier.class);

    private final DorisDataSinkOptions options;
    private final DorisDdlBuilder ddlBuilder;
    private final DorisSchemaChangeMonitor schemaChangeMonitor;
    private final DorisMetadataInspector metadataInspector;

    /**
     * User alert handler, loaded on first use from {@code sink.schema-change.alert-handler}.
     *
     * <p>{@code transient}: the applier is serialized with the schema-registry state, and the
     * handler is a user object that need not be serializable; the deserialized copy loads it again.
     * {@code alertHandlerLoaded} separates "not loaded yet" from "loaded, none configured/failed"
     * so a missing class is reported once instead of on every alert.
     */
    private transient SchemaEvolutionAlertHandler alertHandler;

    private transient boolean alertHandlerLoaded;

    /**
     * The client shared by every schema change this applier applies.
     *
     * <p>One instance for the applier's lifetime instead of one per event: a {@link
     * DorisHttpClient} owns its own OkHttp connection pool, so a client created per event opens a
     * fresh TCP connection for every DDL and closes it again right after (its {@code close()} shuts
     * the dispatcher down and evicts the pool). That cost is invisible for a handful of events but
     * is paid hundreds of times when a snapshot creates hundreds of tables in one round. Schema
     * changes are applied serially on the coordinator's apply thread, so one client is never used
     * concurrently.
     *
     * <p>{@code transient}: this applier is part of the schema registry state that Flink serializes
     * to the JobManager, and an OkHttp client is not serializable. It is created on first use, so
     * the deserialized copy gets its own client. Not closed explicitly — the applier has no
     * lifecycle hook — but its footprint stays bounded: OkHttp returns idle connections to its pool
     * and closes them after its keep-alive expires, and its dispatcher threads time out when idle.
     */
    private transient DorisHttpClient httpClient;

    public DorisMetadataApplier(DorisDataSinkOptions options) {
        this.options = options;
        this.ddlBuilder = new DorisDdlBuilder(options);
        this.schemaChangeMonitor = new DorisSchemaChangeMonitor(options);
        this.metadataInspector = new DorisMetadataInspector(options);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent)
            throws SchemaEvolveException {
        try {
            if (schemaChangeEvent instanceof CreateTableEvent) {
                applyCreateTable((CreateTableEvent) schemaChangeEvent);
            } else if (schemaChangeEvent instanceof AddColumnEvent) {
                applyAddColumn((AddColumnEvent) schemaChangeEvent);
            } else if (schemaChangeEvent instanceof DropColumnEvent) {
                applyDropColumn((DropColumnEvent) schemaChangeEvent);
            } else if (schemaChangeEvent instanceof RenameColumnEvent) {
                applyRenameColumn((RenameColumnEvent) schemaChangeEvent);
            } else if (schemaChangeEvent instanceof AlterColumnTypeEvent) {
                // The coordinator resolves the old schema for appliers implementing
                // OldSchemaAwareMetadataApplier and calls applyAlterColumnType instead; this path
                // is the degraded case (no pre-change schema registered yet).
                applyAlterColumnType((AlterColumnTypeEvent) schemaChangeEvent, Optional.empty());
            } else if (schemaChangeEvent instanceof RenameTableEvent) {
                applyRenameTable((RenameTableEvent) schemaChangeEvent);
            } else if (schemaChangeEvent instanceof TruncateTableEvent) {
                applyTruncateTable((TruncateTableEvent) schemaChangeEvent);
            } else if (schemaChangeEvent instanceof AlterColumnCommentEvent) {
                applyAlterColumnComment((AlterColumnCommentEvent) schemaChangeEvent);
            } else if (schemaChangeEvent instanceof AlterTableCommentEvent) {
                applyAlterTableComment((AlterTableCommentEvent) schemaChangeEvent);
            } else {
                applySqls(schemaChangeEvent, buildSqls(schemaChangeEvent), httpClient());
            }
        } catch (SchemaEvolveException e) {
            throw e;
        } catch (Exception e) {
            // Pass the cause on: without it the report of a failed DDL has no stack trace and the
            // reason (a serializer error, a malformed SQL string, an NPE) is unrecoverable.
            throw new SchemaEvolveException(schemaChangeEvent, e.getMessage(), e);
        }
    }

    // ------------------------------------------------------------------------
    //  Per-event application with pre-checks
    // ------------------------------------------------------------------------

    /**
     * A plain {@code CREATE TABLE IF NOT EXISTS} is idempotent, so a replay is harmless. Only
     * {@code sink.is-drop-table-before-create} needs protection: its SQL starts with {@code DROP
     * TABLE IF EXISTS}, and replaying that after a restore would discard everything written since
     * the checkpoint. When the table already exists with every column the event would create, the
     * pair is skipped instead of dropping the table.
     */
    private void applyCreateTable(CreateTableEvent event) throws SchemaEvolveException {
        if (!options.isDropTableBeforeCreate() || !isPreCheckEnabled()) {
            applySqls(event, ddlBuilder.buildCreateTableSql(event), httpClient());
            return;
        }
        DorisHttpClient client = httpClient();
        DorisTableMetadata metadata = inspect(event, client);
        List<String> sqls = ddlBuilder.buildCreateTableSql(event);
        if (!metadata.tableExists()) {
            applySqls(event, sqls, client);
            return;
        }
        List<String> expected = ddlBuilder.expectedColumnNames(event);
        List<String> missing = new ArrayList<>();
        for (String column : expected) {
            if (!metadata.hasColumn(column)) {
                missing.add(column);
            }
        }
        if (missing.isEmpty()) {
            skip(
                    event,
                    SchemaEvolutionAlert.Level.INFO,
                    SchemaEvolutionAlert.Kind.DUPLICATE,
                    null,
                    "table already exists with every column of the replayed CREATE TABLE;"
                            + " skipping the configured DROP+CREATE so the restore keeps the data"
                            + " loaded since the checkpoint",
                    sqls.get(0));
            return;
        }
        // The table exists but does not match the event: dropping it could destroy data that the
        // event schema knows nothing about (for example columns added after this CREATE TABLE).
        // Replaying DROP+CREATE would also re-create an older schema, so refuse and surface it.
        skip(
                event,
                SchemaEvolutionAlert.Level.WARNING,
                SchemaEvolutionAlert.Kind.INCONSISTENT_STATE,
                null,
                "table exists but lacks the columns "
                        + missing
                        + " of the replayed CREATE TABLE; refusing to drop it and skipping the"
                        + " event. Drop the table manually if it is safe to rebuild it.",
                sqls.get(0));
    }

    /**
     * Skips added columns that already exist. Doris rejects a duplicate {@code ADD COLUMN}, and the
     * duplicate is usually a replayed round; the column being present (with any type) means the ADD
     * is effectively applied, so this is an idempotent skip and not an alert.
     *
     * <p>A type difference is worth a sentence in the log but not a WARNING: the source may have
     * changed the type in a later event, whose own replay (or the first application) settles it.
     */
    private void applyAddColumn(AddColumnEvent event) throws SchemaEvolveException {
        if (!isPreCheckEnabled()) {
            applySqls(event, ddlBuilder.buildAddColumnSql(event), httpClient());
            return;
        }
        DorisHttpClient client = httpClient();
        DorisTableMetadata metadata = inspect(event, client);
        if (!metadata.tableExists()) {
            skipMissingTable(event, "ALTER TABLE ADD COLUMN");
            return;
        }
        List<AddColumnEvent.ColumnWithPosition> pending = new ArrayList<>();
        for (AddColumnEvent.ColumnWithPosition added : event.getAddedColumns()) {
            Column column = added.getAddColumn();
            String name = column.getName();
            if (!metadata.hasColumn(name)) {
                pending.add(added);
                continue;
            }
            String currentType = metadata.normalizedType(name).orElse("");
            String targetType =
                    DorisMetadataInspector.normalizeType(
                            ddlBuilder.dorisType(column.getType(), metadata.isKeyColumn(name)));
            String detail =
                    currentType.equals(targetType)
                            ? ""
                            : " (the existing column has type "
                                    + currentType
                                    + ", the event would add "
                                    + targetType
                                    + " — a later type change accounts for the difference)";
            skip(
                    event,
                    SchemaEvolutionAlert.Level.INFO,
                    SchemaEvolutionAlert.Kind.DUPLICATE,
                    name,
                    "column already exists; skipping the replayed ADD COLUMN" + detail,
                    null);
        }
        if (pending.isEmpty()) {
            return;
        }
        AddColumnEvent filtered = new AddColumnEvent(event.tableId(), pending);
        applySqls(filtered, ddlBuilder.buildAddColumnSql(filtered), client);
    }

    /** Skips dropping a column that is already gone — Doris rejects an unknown column. */
    private void applyDropColumn(DropColumnEvent event) throws SchemaEvolveException {
        if (!isPreCheckEnabled()) {
            applySqls(event, ddlBuilder.buildDropColumnSql(event), httpClient());
            return;
        }
        DorisHttpClient client = httpClient();
        DorisTableMetadata metadata = inspect(event, client);
        if (!metadata.tableExists()) {
            skipMissingTable(event, "ALTER TABLE DROP COLUMN");
            return;
        }
        List<String> pending = new ArrayList<>();
        for (String column : event.getDroppedColumnNames()) {
            if (metadata.hasColumn(column)) {
                pending.add(column);
            } else {
                skip(
                        event,
                        SchemaEvolutionAlert.Level.INFO,
                        SchemaEvolutionAlert.Kind.DUPLICATE,
                        column,
                        "column is already absent; skipping the replayed DROP COLUMN",
                        null);
            }
        }
        if (pending.isEmpty()) {
            return;
        }
        DropColumnEvent filtered = new DropColumnEvent(event.tableId(), pending);
        applySqls(filtered, ddlBuilder.buildDropColumnSql(filtered), client);
    }

    /**
     * Resolves each rename against the current columns: the target already exists and the source is
     * gone means the rename was applied (skip, idempotent); both present, or both missing, is a
     * state the statement cannot reconcile and Doris would reject (alert and skip).
     */
    private void applyRenameColumn(RenameColumnEvent event) throws SchemaEvolveException {
        if (!isPreCheckEnabled()) {
            applySqls(event, ddlBuilder.buildRenameColumnSql(event), httpClient());
            return;
        }
        DorisHttpClient client = httpClient();
        DorisTableMetadata metadata = inspect(event, client);
        if (!metadata.tableExists()) {
            skipMissingTable(event, "ALTER TABLE RENAME COLUMN");
            return;
        }
        Map<String, String> pending = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : event.getNameMapping().entrySet()) {
            String from = entry.getKey();
            String to = entry.getValue();
            if (Objects.equals(from, to)) {
                continue;
            }
            boolean fromExists = metadata.hasColumn(from);
            boolean toExists = metadata.hasColumn(to);
            if (fromExists && !toExists) {
                pending.put(from, to);
            } else if (!fromExists && toExists) {
                skip(
                        event,
                        SchemaEvolutionAlert.Level.INFO,
                        SchemaEvolutionAlert.Kind.DUPLICATE,
                        from,
                        "column `"
                                + from
                                + "` is already renamed to `"
                                + to
                                + "`; skipping the replayed RENAME COLUMN",
                        null);
            } else if (fromExists) {
                skip(
                        event,
                        SchemaEvolutionAlert.Level.WARNING,
                        SchemaEvolutionAlert.Kind.COLUMN_CONFLICT,
                        from,
                        "both `"
                                + from
                                + "` and its rename target `"
                                + to
                                + "` exist in Doris; the rename is ambiguous and Doris would"
                                + " reject it, so it is skipped. Reconcile the two columns"
                                + " manually.",
                        null);
            } else {
                skip(
                        event,
                        SchemaEvolutionAlert.Level.WARNING,
                        SchemaEvolutionAlert.Kind.MISSING_COLUMN,
                        from,
                        "neither `"
                                + from
                                + "` nor its rename target `"
                                + to
                                + "` exists in Doris, so the rename can neither be applied nor"
                                + " considered done.",
                        null);
            }
        }
        if (pending.isEmpty()) {
            return;
        }
        RenameColumnEvent filtered = new RenameColumnEvent(event.tableId(), pending);
        applySqls(filtered, ddlBuilder.buildRenameColumnSql(filtered), client);
    }

    /**
     * Applies a column type change with the pre-change schema, skipping:
     *
     * <ul>
     *   <li>key columns — Doris only supports a narrow subset of key-column type changes and
     *       rejects the rest; the user asked for these to be skipped and alerted;
     *   <li>capacity reductions — {@link DorisDdlBuilder} already refuses them;
     *   <li>columns whose Doris type is already the target — a replayed round;
     *   <li>columns missing from Doris.
     * </ul>
     *
     * <p>What remains goes through the direct {@code MODIFY COLUMN}, with the temp-column fallback
     * for types Doris cannot convert in place.
     */
    @Override
    public void applyAlterColumnType(AlterColumnTypeEvent event, Optional<Schema> oldSchema)
            throws SchemaEvolveException {
        Map<String, DataType> pending = new LinkedHashMap<>();
        for (Map.Entry<String, DataType> entry : event.getTypeMapping().entrySet()) {
            String column = entry.getKey();
            DataType newType = entry.getValue();
            Optional<DataType> oldType =
                    oldSchema.flatMap(schema -> schema.getColumn(column)).map(Column::getType);
            if (isSourceKeyColumn(column, oldSchema)) {
                skip(
                        event,
                        SchemaEvolutionAlert.Level.WARNING,
                        SchemaEvolutionAlert.Kind.MODIFY_KEY_COLUMN,
                        column,
                        "column is part of the primary key; Doris does not support this type"
                                + " change on a key column, so the MODIFY COLUMN is skipped and"
                                + " the Doris column keeps its type "
                                + oldType.map(Object::toString).orElse("(unknown)")
                                + " instead of "
                                + newType
                                + ".",
                        null);
                continue;
            }
            if (DorisDdlBuilder.isCapacityReduction(oldType.orElse(null), newType)) {
                skip(
                        event,
                        SchemaEvolutionAlert.Level.WARNING,
                        SchemaEvolutionAlert.Kind.CAPACITY_REDUCTION,
                        column,
                        "capacity reduction from "
                                + oldType.get()
                                + " to "
                                + newType
                                + " is rejected by Doris; the wider column is kept and already"
                                + " accepts the narrower data.",
                        null);
                continue;
            }
            pending.put(column, newType);
        }
        if (pending.isEmpty()) {
            return;
        }

        DorisHttpClient client = httpClient();
        if (isPreCheckEnabled()) {
            DorisTableMetadata metadata = inspect(event, client);
            if (!metadata.tableExists()) {
                skipMissingTable(event, "ALTER TABLE MODIFY COLUMN");
                return;
            }
            Iterator<Map.Entry<String, DataType>> iterator = pending.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, DataType> entry = iterator.next();
                String column = entry.getKey();
                if (!metadata.hasColumn(column)) {
                    iterator.remove();
                    skip(
                            event,
                            SchemaEvolutionAlert.Level.WARNING,
                            SchemaEvolutionAlert.Kind.MISSING_COLUMN,
                            column,
                            "column does not exist in Doris, so the MODIFY COLUMN cannot be"
                                    + " applied.",
                            null);
                    continue;
                }
                if (metadata.isKeyColumn(column)) {
                    iterator.remove();
                    skip(
                            event,
                            SchemaEvolutionAlert.Level.WARNING,
                            SchemaEvolutionAlert.Kind.MODIFY_KEY_COLUMN,
                            column,
                            "Doris marks the column as a key column; its type change would be"
                                    + " rejected, so the MODIFY COLUMN is skipped.",
                            null);
                    continue;
                }
                String currentType = metadata.normalizedType(column).orElse("");
                String targetType =
                        DorisMetadataInspector.normalizeType(
                                ddlBuilder.dorisType(
                                        entry.getValue(), metadata.isKeyColumn(column)));
                if (currentType.equals(targetType)) {
                    iterator.remove();
                    skip(
                            event,
                            SchemaEvolutionAlert.Level.INFO,
                            SchemaEvolutionAlert.Kind.DUPLICATE,
                            column,
                            "column already has the target type "
                                    + targetType
                                    + "; skipping the replayed MODIFY COLUMN",
                            null);
                }
            }
            if (pending.isEmpty()) {
                return;
            }
        }

        AlterColumnTypeEvent filtered = new AlterColumnTypeEvent(event.tableId(), pending);
        List<String> sqls = ddlBuilder.buildAlterColumnTypeSql(filtered, oldSchema);
        if (sqls.isEmpty()) {
            return;
        }
        try {
            try {
                applySqls(filtered, sqls, client);
            } catch (SchemaEvolveException e) {
                // Direct MODIFY COLUMN failed — Doris cannot convert between these types in
                // place (MySQL/TiDB can, so the source DDL already committed). Fall back to
                // the temp-column strategy: add, cast, drop, rename. Each DDL is executed
                // and waited on individually: Doris rejects overlapping ALTER jobs, and
                // SHOW ALTER TABLE COLUMN only exposes the latest job's state, so issuing
                // them back-to-back would leave earlier jobs unchecked.
                List<String> fallbackSqls =
                        ddlBuilder.buildAlterColumnTypeFallbackSql(filtered, oldSchema);
                if (fallbackSqls.isEmpty()) {
                    throw e;
                }
                LOG.warn(
                        "Direct MODIFY COLUMN failed on {}, falling back to the temp-column strategy: {}",
                        event.tableId(),
                        e.getMessage());
                for (String sql : fallbackSqls) {
                    try {
                        LOG.info("Applying fallback Doris DDL [{}]: {}", event.tableId(), sql);
                        client.executeSql(options.mapDatabase(event.tableId()), sql);
                    } catch (Exception ex) {
                        // A replayed fallback that was interrupted between its steps leaves a
                        // partial state (the temp column exists, or the original column is
                        // already gone). The pre-check above cannot see inside the four-step
                        // sequence, so surface the inconsistency instead of retrying blindly.
                        throw new SchemaEvolveException(
                                event, "Fallback DDL failed: " + ex.getMessage(), ex);
                    }
                    schemaChangeMonitor.waitForSchemaChangeCompletion(client, event);
                }
            }
            schemaChangeMonitor.waitForSchemaChangeCompletion(client, event);
        } catch (SchemaEvolveException e) {
            // already carries its message in getExceptionMessage() (getMessage() is null);
            // re-wrapping it would hide the reason
            throw e;
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    /**
     * Renames tables, tolerating a replay.
     *
     * <p>When every old name still exists, the event has not been applied at all (or is a name
     * swap/cycle, which only makes sense as a whole) and the original event is applied — the
     * builder orders the statements and breaks cycles. When every rename is already reflected
     * (sources gone, targets present) the event is skipped as a duplicate. A mixed state cannot be
     * reconciled reliably: the pending renames are applied and the rest is alerted.
     */
    private void applyRenameTable(RenameTableEvent event) throws SchemaEvolveException {
        if (!isPreCheckEnabled()) {
            applySqls(event, ddlBuilder.buildRenameTableSql(event), httpClient());
            return;
        }
        DorisHttpClient client = httpClient();
        List<RenameTableEvent.TableRename> pairs = new ArrayList<>();
        for (RenameTableEvent.TableRename pair : event.getPairs()) {
            if (!sameMappedName(pair.getOldTableId(), pair.getNewTableId())) {
                pairs.add(pair);
            }
        }
        if (pairs.isEmpty()) {
            return;
        }
        // Existence of the pair's old and new table, in pair order; the later passes reuse it
        // instead of querying the same tables again.
        List<boolean[]> existence = new ArrayList<>();
        Set<String> oldNames = new HashSet<>();
        for (RenameTableEvent.TableRename pair : pairs) {
            oldNames.add(mappedKey(pair.getOldTableId()));
        }
        boolean allOldExist = true;
        boolean allApplied = true;
        // A target name that exists is not a conflict when another rename of this event frees it
        // (a swap a→b, b→a, or a longer cycle).
        boolean everyExistingTargetIsFreed = true;
        for (RenameTableEvent.TableRename pair : pairs) {
            boolean oldExists = inspect(event, pair.getOldTableId(), client).tableExists();
            boolean newExists = inspect(event, pair.getNewTableId(), client).tableExists();
            existence.add(new boolean[] {oldExists, newExists});
            allOldExist &= oldExists;
            allApplied &= !oldExists && newExists;
            if (newExists) {
                everyExistingTargetIsFreed &= oldNames.contains(mappedKey(pair.getNewTableId()));
            }
        }
        if (allOldExist && everyExistingTargetIsFreed) {
            // First application, or a swap/cycle: only the whole event is meaningful.
            applySqls(event, ddlBuilder.buildRenameTableSql(event), client);
            return;
        }
        if (allApplied) {
            skip(
                    event,
                    SchemaEvolutionAlert.Level.INFO,
                    SchemaEvolutionAlert.Kind.DUPLICATE,
                    null,
                    "every table of the rename is already in its target place; skipping the"
                            + " replayed RENAME TABLE",
                    null);
            return;
        }
        // Mixed state: schedule every rename whose source still exists and whose target is free
        // now, or becomes free after another rename of the event runs. The fixed point matters for
        // a chain (b→c then a→b): a→b is only runnable once b→c has moved b aside. A rename whose
        // target stays occupied by a table this event will not move is alerted instead of being
        // sent to Doris, which would reject it and fail the job.
        boolean[] scheduled = new boolean[pairs.size()];
        Set<String> freeingNames = new HashSet<>();
        boolean changed = true;
        while (changed) {
            changed = false;
            for (int i = 0; i < pairs.size(); i++) {
                if (scheduled[i] || !existence.get(i)[0]) {
                    continue;
                }
                String target = mappedKey(pairs.get(i).getNewTableId());
                if (!existence.get(i)[1] || freeingNames.contains(target)) {
                    scheduled[i] = true;
                    freeingNames.add(mappedKey(pairs.get(i).getOldTableId()));
                    changed = true;
                }
            }
        }
        List<RenameTableEvent.TableRename> pending = new ArrayList<>();
        for (int i = 0; i < pairs.size(); i++) {
            RenameTableEvent.TableRename pair = pairs.get(i);
            boolean oldExists = existence.get(i)[0];
            boolean newExists = existence.get(i)[1];
            if (scheduled[i]) {
                pending.add(pair);
            } else if (oldExists) {
                skip(
                        event,
                        SchemaEvolutionAlert.Level.WARNING,
                        SchemaEvolutionAlert.Kind.INCONSISTENT_STATE,
                        null,
                        "both `"
                                + pair.getOldTableId()
                                + "` and its rename target `"
                                + pair.getNewTableId()
                                + "` exist in Doris; the rename cannot be applied and is"
                                + " skipped.",
                        null);
            } else if (!newExists) {
                skip(
                        event,
                        SchemaEvolutionAlert.Level.WARNING,
                        SchemaEvolutionAlert.Kind.MISSING_TABLE,
                        null,
                        "neither `"
                                + pair.getOldTableId()
                                + "` nor its rename target `"
                                + pair.getNewTableId()
                                + "` exists in Doris; the rename can neither be applied nor"
                                + " considered done.",
                        null);
            }
        }
        if (pending.isEmpty()) {
            return;
        }
        RenameTableEvent filtered = new RenameTableEvent(pending, event.getSql());
        applySqls(filtered, ddlBuilder.buildRenameTableSql(filtered), client);
    }

    /**
     * A truncated table is empty, so re-running the statement is harmless; the only replay that
     * fails is one whose table has since been dropped, which the pre-check turns into a skip.
     */
    private void applyTruncateTable(TruncateTableEvent event) throws SchemaEvolveException {
        if (!isPreCheckEnabled()) {
            applySqls(event, ddlBuilder.buildTruncateTableSql(event), httpClient());
            return;
        }
        DorisHttpClient client = httpClient();
        if (!inspect(event, client).tableExists()) {
            skip(
                    event,
                    SchemaEvolutionAlert.Level.INFO,
                    SchemaEvolutionAlert.Kind.DUPLICATE,
                    null,
                    "table no longer exists, so there is nothing left to truncate; skipping the"
                            + " replayed TRUNCATE TABLE",
                    null);
            return;
        }
        applySqls(event, ddlBuilder.buildTruncateTableSql(event), client);
    }

    /**
     * A column comment is part of the column definition; replaying the same comment is harmless,
     * but the statement fails on a column (or table) that no longer exists. A replay that found the
     * comment already in place is skipped as a duplicate.
     */
    private void applyAlterColumnComment(AlterColumnCommentEvent event)
            throws SchemaEvolveException {
        if (!isPreCheckEnabled()) {
            applySqls(event, ddlBuilder.buildAlterColumnCommentSql(event), httpClient());
            return;
        }
        DorisHttpClient client = httpClient();
        DorisTableMetadata metadata = inspect(event, client);
        if (!metadata.tableExists()) {
            skipMissingTable(event, "ALTER TABLE MODIFY COLUMN COMMENT");
            return;
        }
        Map<String, String> pending = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : event.getCommentMapping().entrySet()) {
            String column = entry.getKey();
            String comment = entry.getValue();
            // The builder skips a null comment (removing a comment is not expressible as an
            // ALTER); keep that behaviour here so the pre-check does not report it as applied.
            if (comment == null) {
                continue;
            }
            DorisTableMetadata.ColumnMetadata current = metadata.column(column);
            if (current == null) {
                skip(
                        event,
                        SchemaEvolutionAlert.Level.WARNING,
                        SchemaEvolutionAlert.Kind.MISSING_COLUMN,
                        column,
                        "column does not exist in Doris, so the comment change cannot be applied.",
                        null);
                continue;
            }
            String currentComment = current.getComment() == null ? "" : current.getComment();
            if (Objects.equals(currentComment, comment)) {
                skip(
                        event,
                        SchemaEvolutionAlert.Level.INFO,
                        SchemaEvolutionAlert.Kind.DUPLICATE,
                        column,
                        "column already carries the requested comment; skipping the replayed"
                                + " comment change",
                        null);
                continue;
            }
            pending.put(column, comment);
        }
        if (pending.isEmpty()) {
            return;
        }
        AlterColumnCommentEvent filtered = new AlterColumnCommentEvent(event.tableId(), pending);
        applySqls(filtered, ddlBuilder.buildAlterColumnCommentSql(filtered), client);
    }

    /** {@code DROP TABLE IF EXISTS} is already idempotent; a table comment change is too. */
    private void applyAlterTableComment(AlterTableCommentEvent event) throws SchemaEvolveException {
        if (isPreCheckEnabled() && !inspect(event, httpClient()).tableExists()) {
            skipMissingTable(event, "ALTER TABLE MODIFY COMMENT");
            return;
        }
        applySqls(event, ddlBuilder.buildAlterTableCommentSql(event), httpClient());
    }

    // ------------------------------------------------------------------------
    //  Execution helpers
    // ------------------------------------------------------------------------

    private void applySqls(SchemaChangeEvent event, List<String> sqls, DorisHttpClient client)
            throws SchemaEvolveException {
        try {
            for (String sql : sqls) {
                LOG.info("Applying Doris DDL [{}]: {}", event.tableId(), sql);
                client.executeSql(options.mapDatabase(event.tableId()), sql);
            }
        } catch (Exception e) {
            // Pass the cause on: without it the report of a failed DDL has no stack trace.
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private List<String> buildSqls(SchemaChangeEvent event) {
        if (event instanceof CreateTableEvent) {
            return ddlBuilder.buildCreateTableSql((CreateTableEvent) event);
        } else if (event instanceof AddColumnEvent) {
            return ddlBuilder.buildAddColumnSql((AddColumnEvent) event);
        } else if (event instanceof DropColumnEvent) {
            return ddlBuilder.buildDropColumnSql((DropColumnEvent) event);
        } else if (event instanceof RenameColumnEvent) {
            return ddlBuilder.buildRenameColumnSql((RenameColumnEvent) event);
        } else if (event instanceof AlterColumnTypeEvent) {
            return ddlBuilder.buildAlterColumnTypeSql((AlterColumnTypeEvent) event);
        } else if (event instanceof RenameTableEvent) {
            return ddlBuilder.buildRenameTableSql((RenameTableEvent) event);
        } else if (event instanceof DropTableEvent) {
            return ddlBuilder.buildDropTableSql((DropTableEvent) event);
        } else if (event instanceof TruncateTableEvent) {
            return ddlBuilder.buildTruncateTableSql((TruncateTableEvent) event);
        } else if (event instanceof AlterTableCommentEvent) {
            return ddlBuilder.buildAlterTableCommentSql((AlterTableCommentEvent) event);
        } else if (event instanceof AlterColumnCommentEvent) {
            return ddlBuilder.buildAlterColumnCommentSql((AlterColumnCommentEvent) event);
        }
        throw new UnsupportedSchemaChangeEventException(event);
    }

    private boolean isPreCheckEnabled() {
        return options.isSchemaChangePreCheckEnabled();
    }

    private DorisTableMetadata inspect(SchemaChangeEvent event, DorisHttpClient client)
            throws SchemaEvolveException {
        return inspect(event, event.tableId(), client);
    }

    private DorisTableMetadata inspect(
            SchemaChangeEvent event, TableId tableId, DorisHttpClient client)
            throws SchemaEvolveException {
        try {
            return metadataInspector.describe(client, tableId);
        } catch (IOException e) {
            // Propagate: a failed metadata read must not be mistaken for "nothing applied yet",
            // which would re-run non-idempotent DDLs.
            throw new SchemaEvolveException(
                    event,
                    "Failed to read the Doris metadata of "
                            + tableId
                            + " for the schema-change pre-check: "
                            + e.getMessage(),
                    e);
        }
    }

    private void skipMissingTable(SchemaChangeEvent event, String operation) {
        skip(
                event,
                SchemaEvolutionAlert.Level.WARNING,
                SchemaEvolutionAlert.Kind.MISSING_TABLE,
                null,
                "the target table does not exist in Doris, so " + operation + " cannot be applied.",
                null);
    }

    /**
     * Records a deliberately skipped change. INFO skips (duplicates) are logged and go no further;
     * WARNING skips are also delivered to the configured alert handler.
     */
    private void skip(
            SchemaChangeEvent event,
            SchemaEvolutionAlert.Level level,
            SchemaEvolutionAlert.Kind kind,
            @Nullable String column,
            String message,
            @Nullable String skippedSql) {
        SchemaEvolutionAlert alert =
                new SchemaEvolutionAlert(
                        level,
                        kind,
                        event.tableId(),
                        event.getClass().getSimpleName(),
                        column,
                        message,
                        skippedSql);
        if (level == SchemaEvolutionAlert.Level.INFO) {
            LOG.info(
                    "Schema evolution skipped on {} [{}]: {}",
                    event.tableId(),
                    alert.getAlertId(),
                    message);
            return;
        }
        LOG.warn(
                "Schema evolution skipped on {} [{}]: {}",
                event.tableId(),
                alert.getAlertId(),
                message);
        deliver(alert);
    }

    private void deliver(SchemaEvolutionAlert alert) {
        SchemaEvolutionAlertHandler handler = alertHandler();
        if (handler == null) {
            return;
        }
        try {
            handler.onAlert(alert);
        } catch (Exception e) {
            // Alert delivery must never fail the pipeline: the skip has already been decided, and
            // the log line above still records it.
            LOG.warn(
                    "Schema-evolution alert handler failed to deliver alert {}; the alert is"
                            + " logged above and the pipeline continues.",
                    alert.getAlertId(),
                    e);
        }
    }

    private synchronized SchemaEvolutionAlertHandler alertHandler() {
        if (!alertHandlerLoaded) {
            alertHandlerLoaded = true;
            String className = options.getSchemaChangeAlertHandler();
            if (className == null || className.trim().isEmpty()) {
                LOG.info(
                        "No {} configured: non-idempotent schema-change skips will only be"
                                + " logged.",
                        DorisDataSinkOptions.SCHEMA_CHANGE_ALERT_HANDLER.key());
            } else {
                alertHandler = loadAlertHandler(className.trim());
            }
        }
        return alertHandler;
    }

    @Nullable
    private static SchemaEvolutionAlertHandler loadAlertHandler(String className) {
        try {
            Class<?> clazz =
                    Class.forName(className, true, Thread.currentThread().getContextClassLoader());
            if (!SchemaEvolutionAlertHandler.class.isAssignableFrom(clazz)) {
                LOG.error(
                        "Configured schema-evolution alert handler {} does not implement {};"
                                + " alerts will only be logged.",
                        className,
                        SchemaEvolutionAlertHandler.class.getName());
                return null;
            }
            return (SchemaEvolutionAlertHandler) clazz.getDeclaredConstructor().newInstance();
        } catch (Throwable t) {
            LOG.error(
                    "Could not instantiate the schema-evolution alert handler {}; alerts will only"
                            + " be logged. The class must be on the JobManager classpath and have"
                            + " a public no-argument constructor.",
                    className,
                    t);
            return null;
        }
    }

    private static boolean isSourceKeyColumn(String column, Optional<Schema> oldSchema) {
        return oldSchema
                .map(
                        schema ->
                                schema.primaryKeys() != null
                                        && schema.primaryKeys().contains(column))
                .orElse(false);
    }

    private boolean sameMappedName(TableId left, TableId right) {
        return mappedKey(left).equals(mappedKey(right));
    }

    /** The mapped {@code database.table} name, for matching renames against each other. */
    private String mappedKey(TableId tableId) {
        return options.mapDatabase(tableId) + "." + options.mapTable(tableId);
    }

    /**
     * Returns the applier's client, creating it on first use — the applier is serialized to the
     * JobManager, so the field is null until the deserialized copy first needs to reach Doris.
     */
    private synchronized DorisHttpClient httpClient() {
        if (httpClient == null) {
            httpClient =
                    new DorisHttpClient(
                            options.getFenodes(),
                            options.getUsername(),
                            options.getPassword(),
                            options.getMaxRetries(),
                            options.getStreamLoadProperties());
        }
        return httpClient;
    }
}
