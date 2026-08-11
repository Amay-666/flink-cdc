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

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfigFactory;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.configuration.ConfigOption;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnector;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Factory to create {@link CanalSourceConfig}. */
public class CanalSourceConfigFactory extends JdbcSourceConfigFactory {

    private static final long serialVersionUID = 1L;

    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

    private String kafkaBootstrapServers;
    private String kafkaGroupId;
    private List<String> kafkaTopics;
    private CanalSourceOptions.MessageFormat messageFormat =
            enumDefault(CanalSourceOptions.MESSAGE_FORMAT, CanalSourceOptions.MessageFormat.class);
    private CanalSourceOptions.EventTime eventTime =
            enumDefault(CanalSourceOptions.EVENT_TIME, CanalSourceOptions.EventTime.class);
    private CanalSourceOptions.BoundaryMode boundaryMode =
            enumDefault(CanalSourceOptions.BOUNDARY_MODE, CanalSourceOptions.BoundaryMode.class);
    private CanalSourceOptions.KafkaStartupMode kafkaStartupMode =
            enumDefault(
                    CanalSourceOptions.KAFKA_STARTUP_MODE,
                    CanalSourceOptions.KafkaStartupMode.class);
    private CanalSourceOptions.DdlParser ddlParser =
            enumDefault(CanalSourceOptions.CANAL_DDL_PARSER, CanalSourceOptions.DdlParser.class);
    private Properties kafkaProperties = new Properties();

    /**
     * Resolves the kebab-case default value of a {@code stringType()} canal option into its enum.
     * Keeps the config option default and the field default in sync.
     */
    private static <T extends Enum<T>> T enumDefault(
            ConfigOption<String> option, Class<T> enumType) {
        return Arrays.stream(enumType.getEnumConstants())
                .filter(
                        c ->
                                c.toString()
                                        .equalsIgnoreCase(option.defaultValue().replace('-', '_')))
                .findFirst()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "Invalid default value: " + option.defaultValue()));
    }

    public CanalSourceConfigFactory() {
        this.port = CanalSourceOptions.CANAL_MYSQL_PORT.defaultValue();
    }

    /** Creates a new {@link CanalSourceConfig} for the given subtask {@code subtaskId}. */
    @Override
    public CanalSourceConfig create(int subtaskId) {
        Properties props = new Properties();
        // Debezium MySQL connector config is only used as a light carrier for logical name /
        // table filters / schema name adjuster. Snapshot reading is done by our own JDBC code,
        // so the actual database connection properties still need to be present.
        props.setProperty("connector.class", MySqlConnector.class.getCanonicalName());
        props.setProperty("database.server.name", "canal_cdc_source");
        props.setProperty("database.hostname", checkNotNull(hostname));
        props.setProperty("database.port", String.valueOf(port));
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));
        if (databaseList != null) {
            props.setProperty("database.include.list", String.join(",", databaseList));
        }
        if (tableList != null) {
            props.setProperty("table.include.list", String.join(",", tableList));
        }
        props.setProperty("include.schema.changes", String.valueOf(includeSchemaChanges));

        // override the user-defined debezium properties
        if (dbzProperties != null) {
            props.putAll(dbzProperties);
        }

        Configuration dbzConfiguration = Configuration.from(props);
        return new CanalSourceConfig(
                subtaskId,
                startupOptions,
                databaseList,
                Collections.emptyList(),
                tableList,
                splitSize,
                splitMetaGroupSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                closeIdleReaders,
                props,
                dbzConfiguration,
                JDBC_DRIVER,
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
                scanNewlyAddedTableEnabled,
                kafkaBootstrapServers,
                kafkaGroupId,
                kafkaTopics,
                messageFormat,
                eventTime,
                boundaryMode,
                kafkaStartupMode,
                ddlParser,
                kafkaProperties);
    }

    /** The Kafka bootstrap servers that external tools (canal) write the messages to. */
    public CanalSourceConfigFactory kafkaBootstrapServers(String bootstrapServers) {
        this.kafkaBootstrapServers = bootstrapServers;
        return this;
    }

    /** The Kafka consumer group id. */
    public CanalSourceConfigFactory kafkaGroupId(String groupId) {
        this.kafkaGroupId = groupId;
        return this;
    }

    /** The Kafka topics that carry the incremental messages. */
    public CanalSourceConfigFactory kafkaTopics(String... topics) {
        this.kafkaTopics = Arrays.asList(topics);
        return this;
    }

    /** The message format of the Kafka messages. */
    public CanalSourceConfigFactory messageFormat(CanalSourceOptions.MessageFormat messageFormat) {
        this.messageFormat = messageFormat;
        return this;
    }

    /** The timestamp field used as the offset event time. */
    public CanalSourceConfigFactory eventTime(CanalSourceOptions.EventTime eventTime) {
        this.eventTime = eventTime;
        return this;
    }

    /** The boundary handling mode of the full->incremental switch. */
    public CanalSourceConfigFactory boundaryMode(CanalSourceOptions.BoundaryMode boundaryMode) {
        this.boundaryMode = boundaryMode;
        return this;
    }

    /** The Kafka consumer startup mode for stream-only scenarios. */
    public CanalSourceConfigFactory kafkaStartupMode(
            CanalSourceOptions.KafkaStartupMode kafkaStartupMode) {
        this.kafkaStartupMode = kafkaStartupMode;
        return this;
    }

    /** The DDL parser implementation. */
    public CanalSourceConfigFactory ddlParser(CanalSourceOptions.DdlParser ddlParser) {
        this.ddlParser = ddlParser;
        return this;
    }

    /** Arbitrary Kafka consumer properties. */
    public CanalSourceConfigFactory kafkaProperties(Properties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
        return this;
    }

    // ---------------------------------------------------------------------------------------
    // Override the base fluent setters with covariant return types so that chained calls keep
    // the CanalSourceConfigFactory type.
    // ---------------------------------------------------------------------------------------

    @Override
    public CanalSourceConfigFactory hostname(String hostname) {
        return (CanalSourceConfigFactory) super.hostname(hostname);
    }

    @Override
    public CanalSourceConfigFactory port(int port) {
        return (CanalSourceConfigFactory) super.port(port);
    }

    @Override
    public CanalSourceConfigFactory databaseList(String... databaseList) {
        return (CanalSourceConfigFactory) super.databaseList(databaseList);
    }

    @Override
    public CanalSourceConfigFactory tableList(String... tableList) {
        return (CanalSourceConfigFactory) super.tableList(tableList);
    }

    @Override
    public CanalSourceConfigFactory username(String username) {
        return (CanalSourceConfigFactory) super.username(username);
    }

    @Override
    public CanalSourceConfigFactory password(String password) {
        return (CanalSourceConfigFactory) super.password(password);
    }

    @Override
    public CanalSourceConfigFactory serverTimeZone(String timeZone) {
        return (CanalSourceConfigFactory) super.serverTimeZone(timeZone);
    }

    @Override
    public CanalSourceConfigFactory splitSize(int splitSize) {
        return (CanalSourceConfigFactory) super.splitSize(splitSize);
    }

    @Override
    public CanalSourceConfigFactory splitMetaGroupSize(int splitMetaGroupSize) {
        return (CanalSourceConfigFactory) super.splitMetaGroupSize(splitMetaGroupSize);
    }

    @Override
    public CanalSourceConfigFactory distributionFactorUpper(double distributionFactorUpper) {
        return (CanalSourceConfigFactory) super.distributionFactorUpper(distributionFactorUpper);
    }

    @Override
    public CanalSourceConfigFactory distributionFactorLower(double distributionFactorLower) {
        return (CanalSourceConfigFactory) super.distributionFactorLower(distributionFactorLower);
    }

    @Override
    public CanalSourceConfigFactory fetchSize(int fetchSize) {
        return (CanalSourceConfigFactory) super.fetchSize(fetchSize);
    }

    @Override
    public CanalSourceConfigFactory connectTimeout(Duration connectTimeout) {
        return (CanalSourceConfigFactory) super.connectTimeout(connectTimeout);
    }

    @Override
    public CanalSourceConfigFactory connectionPoolSize(int connectionPoolSize) {
        return (CanalSourceConfigFactory) super.connectionPoolSize(connectionPoolSize);
    }

    @Override
    public CanalSourceConfigFactory connectMaxRetries(int connectMaxRetries) {
        return (CanalSourceConfigFactory) super.connectMaxRetries(connectMaxRetries);
    }

    @Override
    public CanalSourceConfigFactory includeSchemaChanges(boolean includeSchemaChanges) {
        return (CanalSourceConfigFactory) super.includeSchemaChanges(includeSchemaChanges);
    }

    @Override
    public CanalSourceConfigFactory debeziumProperties(Properties properties) {
        return (CanalSourceConfigFactory) super.debeziumProperties(properties);
    }

    @Override
    public CanalSourceConfigFactory chunkKeyColumn(String chunkKeyColumn) {
        return (CanalSourceConfigFactory) super.chunkKeyColumn(chunkKeyColumn);
    }

    @Override
    public CanalSourceConfigFactory startupOptions(StartupOptions startupOptions) {
        return (CanalSourceConfigFactory) super.startupOptions(startupOptions);
    }

    @Override
    public CanalSourceConfigFactory closeIdleReaders(boolean closeIdleReaders) {
        return (CanalSourceConfigFactory) super.closeIdleReaders(closeIdleReaders);
    }

    @Override
    public CanalSourceConfigFactory skipSnapshotBackfill(boolean skipSnapshotBackfill) {
        return (CanalSourceConfigFactory) super.skipSnapshotBackfill(skipSnapshotBackfill);
    }

    @Override
    public CanalSourceConfigFactory scanNewlyAddedTableEnabled(boolean scanNewlyAddedTableEnabled) {
        return (CanalSourceConfigFactory)
                super.scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled);
    }
}
