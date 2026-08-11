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

package org.apache.flink.cdc.connectors.canal.source.schema;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.AbstractSourceInfoStructMaker;
import io.debezium.connector.SourceInfoStructMaker;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Builds the {@code source} struct of the Canal source records.
 *
 * <p>The schema mirrors the MySQL connector's common fields ({@code version}/{@code connector}/
 * {@code name}/{@code ts_ms}/{@code snapshot}/{@code db}) and additionally exposes the canal
 * {@code es}/{@code ts} timestamps. {@code ts_ms} always carries the configured event time so that
 * the record timestamp and the offset ordering stay consistent.
 */
public class CanalSourceInfoStructMaker implements SourceInfoStructMaker<CanalSourceInfo> {

    /** The Debezium version embedded in the {@code source} struct. */
    public static final String DEBEZIUM_VERSION = "1.9.8.Final";

    private final String connector;
    private final String version;
    private final String serverName;
    private final Schema schema;

    public CanalSourceInfoStructMaker(
            String connector, String version, CommonConnectorConfig connectorConfig) {
        this.connector = connector;
        this.version = version;
        this.serverName = connectorConfig.getLogicalName();
        this.schema =
                SchemaBuilder.struct()
                        .name("io.debezium.connector.canal.Source")
                        .field(AbstractSourceInfo.DEBEZIUM_VERSION_KEY, Schema.STRING_SCHEMA)
                        .field(AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY, Schema.STRING_SCHEMA)
                        .field(AbstractSourceInfo.SERVER_NAME_KEY, Schema.STRING_SCHEMA)
                        .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                        .field(
                                AbstractSourceInfo.SNAPSHOT_KEY,
                                AbstractSourceInfoStructMaker.SNAPSHOT_RECORD_SCHEMA)
                        .field(AbstractSourceInfo.DATABASE_NAME_KEY, Schema.STRING_SCHEMA)
                        .field(AbstractSourceInfo.TABLE_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                        .field("es", Schema.OPTIONAL_INT64_SCHEMA)
                        .field("ts", Schema.OPTIONAL_INT64_SCHEMA)
                        .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(CanalSourceInfo sourceInfo) {
        Struct ret =
                new Struct(schema)
                        .put(AbstractSourceInfo.DEBEZIUM_VERSION_KEY, version)
                        .put(AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY, connector)
                        .put(AbstractSourceInfo.SERVER_NAME_KEY, serverName)
                        .put(AbstractSourceInfo.TIMESTAMP_KEY, sourceInfo.timestamp().toEpochMilli())
                        .put(AbstractSourceInfo.DATABASE_NAME_KEY, sourceInfo.database());
        if (sourceInfo.table() != null) {
            ret.put(AbstractSourceInfo.TABLE_NAME_KEY, sourceInfo.table());
        }
        sourceInfo.snapshot().toSource(ret);
        if (sourceInfo.getExecuteTime() > 0) {
            ret.put("es", sourceInfo.getExecuteTime());
        }
        if (sourceInfo.getSendTime() > 0) {
            ret.put("ts", sourceInfo.getSendTime());
        }
        return ret;
    }
}
