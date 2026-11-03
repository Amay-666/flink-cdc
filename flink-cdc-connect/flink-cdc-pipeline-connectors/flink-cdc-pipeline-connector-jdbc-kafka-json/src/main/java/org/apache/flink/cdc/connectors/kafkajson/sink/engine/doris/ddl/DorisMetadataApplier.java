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
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterColumnCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterTableCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.DropTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.TruncateTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.http.DorisHttpClient;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordinator.OldSchemaAwareMetadataApplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

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
 * types), which lets {@link DorisDdlBuilder} recognise a {@code CHAR}/{@code VARCHAR} shrink — a
 * change Doris rejects but MySQL/TiDB allows — and skip it with a warning.
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
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent)
            throws SchemaEvolveException {
        try {
            applySqls(schemaChangeEvent, buildSqls(schemaChangeEvent), httpClient());
        } catch (SchemaEvolveException e) {
            throw e;
        } catch (Exception e) {
            // Pass the cause on: without it the report of a failed DDL has no stack trace and the
            // reason (a serializer error, a malformed SQL string, an NPE) is unrecoverable.
            throw new SchemaEvolveException(schemaChangeEvent, e.getMessage(), e);
        }
    }

    @Override
    public void applyAlterColumnType(AlterColumnTypeEvent event, Optional<Schema> oldSchema)
            throws SchemaEvolveException {
        // The old schema lets the builder recognise a CHAR/VARCHAR length reduction and skip it
        // (Doris rejects shrinking such a column; see DorisDdlBuilder.buildAlterColumnTypeSql).
        List<String> sqls = ddlBuilder.buildAlterColumnTypeSql(event, oldSchema);
        if (sqls.isEmpty()) {
            return;
        }

        DorisHttpClient client = httpClient();
        try {
            try {
                applySqls(event, sqls, client);
            } catch (SchemaEvolveException e) {
                // Direct MODIFY COLUMN failed — Doris cannot convert between these types in
                // place (MySQL/TiDB can, so the source DDL already committed). Fall back to
                // the temp-column strategy: add, cast, drop, rename. Each DDL is executed
                // and waited on individually: Doris rejects overlapping ALTER jobs, and
                // SHOW ALTER TABLE COLUMN only exposes the latest job's state, so issuing
                // them back-to-back would leave earlier jobs unchecked.
                List<String> fallbackSqls =
                        ddlBuilder.buildAlterColumnTypeFallbackSql(event, oldSchema);
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
                        throw new SchemaEvolveException(
                                event, "Fallback DDL failed: " + ex.getMessage(), ex);
                    }
                    // returns immediately for light DDLs (no pending job); a CANCELLED state
                    // here is a system issue, not a type conversion issue — it fails
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
