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

package org.apache.flink.cdc.connectors.canal.source.config;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/** The configuration for the Canal source. */
public class CanalSourceConfig extends JdbcSourceConfig {

    private static final long serialVersionUID = 1L;

    private final int subtaskId;
    private final String kafkaBootstrapServers;
    private final String kafkaGroupId;
    private final List<String> kafkaTopics;
    private final CanalSourceOptions.MessageFormat messageFormat;
    private final CanalSourceOptions.EventTime eventTime;
    private final CanalSourceOptions.BoundaryMode boundaryMode;
    private final CanalSourceOptions.KafkaStartupMode kafkaStartupMode;
    private final CanalSourceOptions.DdlParser ddlParser;
    private final Properties kafkaProperties;

    public CanalSourceConfig(
            int subtaskId,
            StartupOptions startupOptions,
            List<String> databaseList,
            @Nullable List<String> schemaList,
            List<String> tableList,
            int splitSize,
            int splitMetaGroupSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            boolean includeSchemaChanges,
            boolean closeIdleReaders,
            Properties dbzProperties,
            Configuration dbzConfiguration,
            String driverClassName,
            String hostname,
            int port,
            String username,
            String password,
            int fetchSize,
            String serverTimeZone,
            Duration connectTimeout,
            int connectMaxRetries,
            int connectionPoolSize,
            @Nullable String chunkKeyColumn,
            boolean skipSnapshotBackfill,
            boolean isScanNewlyAddedTableEnabled,
            String kafkaBootstrapServers,
            String kafkaGroupId,
            List<String> kafkaTopics,
            CanalSourceOptions.MessageFormat messageFormat,
            CanalSourceOptions.EventTime eventTime,
            CanalSourceOptions.BoundaryMode boundaryMode,
            CanalSourceOptions.KafkaStartupMode kafkaStartupMode,
            CanalSourceOptions.DdlParser ddlParser,
            Properties kafkaProperties) {
        super(
                startupOptions,
                databaseList,
                schemaList,
                tableList,
                splitSize,
                splitMetaGroupSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                closeIdleReaders,
                dbzProperties,
                dbzConfiguration,
                driverClassName,
                hostname,
                port,
                username,
                password,
                fetchSize,
                serverTimeZone,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                chunkKeyColumn,
                skipSnapshotBackfill,
                isScanNewlyAddedTableEnabled);
        this.subtaskId = subtaskId;
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.kafkaGroupId = kafkaGroupId;
        this.kafkaTopics = kafkaTopics;
        this.messageFormat = messageFormat;
        this.eventTime = eventTime;
        this.boundaryMode = boundaryMode;
        this.kafkaStartupMode = kafkaStartupMode;
        this.ddlParser = ddlParser;
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * Returns the {@code subtaskId} value.
     *
     * @return subtask id
     */
    public int getSubtaskId() {
        return subtaskId;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getKafkaGroupId() {
        return kafkaGroupId;
    }

    public List<String> getKafkaTopics() {
        return kafkaTopics;
    }

    public CanalSourceOptions.MessageFormat getMessageFormat() {
        return messageFormat;
    }

    public CanalSourceOptions.EventTime getEventTime() {
        return eventTime;
    }

    public CanalSourceOptions.BoundaryMode getBoundaryMode() {
        return boundaryMode;
    }

    public CanalSourceOptions.KafkaStartupMode getKafkaStartupMode() {
        return kafkaStartupMode;
    }

    public CanalSourceOptions.DdlParser getDdlParser() {
        return ddlParser;
    }

    public Properties getKafkaProperties() {
        return kafkaProperties;
    }

    /**
     * Returns the minimal Debezium MySQL connector config used by the {@code JdbcSourceEventDispatcher}
     * (logical name, schema name adjuster, source info struct maker, table filters). Since canal is
     * bound to MySQL, we reuse {@link MySqlConnectorConfig} built from the light Debezium
     * configuration instead of implementing a full connector config.
     */
    @Override
    public MySqlConnectorConfig getDbzConnectorConfig() {
        return new MySqlConnectorConfig(getDbzConfiguration());
    }
}
