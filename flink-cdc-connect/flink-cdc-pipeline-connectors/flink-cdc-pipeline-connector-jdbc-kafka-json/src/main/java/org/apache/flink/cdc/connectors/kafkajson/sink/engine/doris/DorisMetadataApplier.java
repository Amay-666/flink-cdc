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

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterColumnCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterTableCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.DropTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.event.TruncateTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.ddl.DorisDdlBuilder;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.http.DorisHttpClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Applies schema changes to Doris over HTTP.
 *
 * <p>Runs on the JobManager inside the schema-evolution coordinator. Every event — the five
 * standard ones plus the connector's five custom ones — is dispatched by {@code instanceof} to
 * {@link DorisDdlBuilder}, and the resulting statements are executed through {@link
 * DorisHttpClient#executeSql}. All event types are accepted (no {@code acceptsSchemaEvolutionType}
 * narrowing), matching the coordinator's pass-through derivation.
 *
 * <p>The HTTP client is created lazily because this object is serialized from the client to the
 * JobManager; {@link okhttp3.OkHttpClient} is not serializable.
 */
public class DorisMetadataApplier implements MetadataApplier {

    private static final Logger LOG = LoggerFactory.getLogger(DorisMetadataApplier.class);

    private final DorisDataSinkOptions options;
    private final DorisDdlBuilder ddlBuilder;
    private transient DorisHttpClient httpClient;

    public DorisMetadataApplier(DorisDataSinkOptions options) {
        this.options = options;
        this.ddlBuilder = new DorisDdlBuilder(options);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) throws SchemaEvolveException {
        try {
            List<String> sqls = buildSqls(schemaChangeEvent);
            for (String sql : sqls) {
                LOG.info(
                        "Applying Doris DDL [{}]: {}",
                        schemaChangeEvent.tableId(),
                        sql);
                client().executeSql(options.mapDatabase(schemaChangeEvent.tableId()), sql);
            }
        } catch (Exception e) {
            throw new SchemaEvolveException(schemaChangeEvent, e.getMessage(), null);
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

    private synchronized DorisHttpClient client() {
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
