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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.utils.KafkaJsonSchemaUtils;

import javax.annotation.Nullable;

import java.util.List;

/** {@link MetadataAccessor} for {@link KafkaJsonDataSource}. */
@Internal
public class KafkaJsonMetadataAccessor implements MetadataAccessor {

    private final KafkaJsonSourceConfig sourceConfig;

    public KafkaJsonMetadataAccessor(KafkaJsonSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    /**
     * Always throw {@link UnsupportedOperationException} because Canal does not support namespace.
     */
    @Override
    public List<String> listNamespaces() {
        throw new UnsupportedOperationException("List namespace is not supported by Canal.");
    }

    /**
     * List all database from MySQL.
     *
     * @param namespace This parameter is ignored because Canal does not support namespace.
     * @return The list of database
     */
    @Override
    public List<String> listSchemas(@Nullable String namespace) {
        return KafkaJsonSchemaUtils.listDatabases(sourceConfig);
    }

    /**
     * List tables from MySQL.
     *
     * @param namespace This parameter is ignored because Canal does not support namespace.
     * @param dbName The database to list tables from. If null, list tables from all databases.
     * @return The list of {@link TableId}s.
     */
    @Override
    public List<TableId> listTables(@Nullable String namespace, @Nullable String dbName) {
        return KafkaJsonSchemaUtils.listTables(sourceConfig, dbName);
    }

    /**
     * Get the {@link Schema} of the given table.
     *
     * @param tableId The {@link TableId} of the given table.
     * @return The {@link Schema} of the table.
     */
    @Override
    public Schema getTableSchema(TableId tableId) {
        return KafkaJsonSchemaUtils.getTableSchema(sourceConfig, tableId);
    }
}
