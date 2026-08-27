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

package org.apache.flink.cdc.connectors.kafkajson.source.config;

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

/** Factory to create {@link KafkaJsonSourceConfig}. */
public class KafkaJsonSourceConfigFactory extends JdbcSourceConfigFactory {

    private static final long serialVersionUID = 1L;

    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

    private String kafkaBootstrapServers;
    private String kafkaGroupId;
    private List<String> kafkaTopics;
    private KafkaJsonSourceOptions.MessageFormat messageFormat =
            enumDefault(
                    KafkaJsonSourceOptions.MESSAGE_FORMAT,
                    KafkaJsonSourceOptions.MessageFormat.class);
    private KafkaJsonSourceOptions.DatabaseType databaseType =
            enumDefault(
                    KafkaJsonSourceOptions.DATABASE_TYPE,
                    KafkaJsonSourceOptions.DatabaseType.class);
    private KafkaJsonSourceOptions.EventTime eventTime =
            enumDefault(KafkaJsonSourceOptions.EVENT_TIME, KafkaJsonSourceOptions.EventTime.class);
    private KafkaJsonSourceOptions.BoundaryMode boundaryMode =
            enumDefault(
                    KafkaJsonSourceOptions.BOUNDARY_MODE,
                    KafkaJsonSourceOptions.BoundaryMode.class);
    private KafkaJsonSourceOptions.KafkaStartupMode kafkaStartupMode =
            enumDefault(
                    KafkaJsonSourceOptions.KAFKA_STARTUP_MODE,
                    KafkaJsonSourceOptions.KafkaStartupMode.class);
    private KafkaJsonSourceOptions.DdlParser ddlParser =
            enumDefault(
                    KafkaJsonSourceOptions.CANAL_DDL_PARSER,
                    KafkaJsonSourceOptions.DdlParser.class);
    private Properties kafkaProperties = new Properties();

    /**
     * Resolves the kebab-case default value of a {@code stringType()} canal option into its enum.
     * Keeps the config option default and the field default in sync.
     */
    private static <T extends Enum<T>> T enumDefault(
            ConfigOption<String> option, Class<T> enumType) {
        return Arrays.stream(enumType.getEnumConstants())
                .filter(c -> c.toString().equalsIgnoreCase(option.defaultValue().replace('-', '_')))
                .findFirst()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "Invalid default value: " + option.defaultValue()));
    }

    public KafkaJsonSourceConfigFactory() {
        this.port = KafkaJsonSourceOptions.CANAL_MYSQL_PORT.defaultValue();
    }

    /** Creates a new {@link KafkaJsonSourceConfig} for the given subtask {@code subtaskId}. */
    @Override
    public KafkaJsonSourceConfig create(int subtaskId) {
        // Extension seam: the 'canal' flatMessage format and the 'debezium' envelope format are both
        // implemented (see docs/DEBEZIUM_PLAN.md); the MySQL source and 'tidb' (an alias that
        // reuses the MySQL-compatible JDBC/dialect path, since TiDB speaks the MySQL wire protocol)
        // are implemented. Declared-but-unimplemented combinations fail fast here, at job setup,
        // instead of surfacing later as a confusing error deep inside the read pipeline.
        if (databaseType != KafkaJsonSourceOptions.DatabaseType.MYSQL
                && databaseType != KafkaJsonSourceOptions.DatabaseType.TIDB) {
            throw new IllegalArgumentException(
                    "scan.database.type="
                            + databaseType
                            + " is declared but not implemented; only 'mysql' and 'tidb' are supported in this version.");
        }
        Properties props = new Properties();
        // Debezium MySQL connector config is only used as a light carrier for logical name /
        // table filters / schema name adjuster. Snapshot reading is done by our own JDBC code,
        // so the actual database connection properties still need to be present.
        props.setProperty("connector.class", MySqlConnector.class.getCanonicalName());
        props.setProperty("database.server.name", "kafka_json_cdc_source");
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
        return new KafkaJsonSourceConfig(
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
                databaseType,
                eventTime,
                boundaryMode,
                kafkaStartupMode,
                ddlParser,
                kafkaProperties);
    }

    /** The Kafka bootstrap servers that external tools (canal) write the messages to. */
    public KafkaJsonSourceConfigFactory kafkaBootstrapServers(String bootstrapServers) {
        this.kafkaBootstrapServers = bootstrapServers;
        return this;
    }

    /** The Kafka consumer group id. */
    public KafkaJsonSourceConfigFactory kafkaGroupId(String groupId) {
        this.kafkaGroupId = groupId;
        return this;
    }

    /** The Kafka topics that carry the incremental messages. */
    public KafkaJsonSourceConfigFactory kafkaTopics(String... topics) {
        this.kafkaTopics = Arrays.asList(topics);
        return this;
    }

    /** The message format of the Kafka messages. */
    public KafkaJsonSourceConfigFactory messageFormat(
            KafkaJsonSourceOptions.MessageFormat messageFormat) {
        this.messageFormat = messageFormat;
        return this;
    }

    /** The database type of the source. */
    public KafkaJsonSourceConfigFactory databaseType(
            KafkaJsonSourceOptions.DatabaseType databaseType) {
        this.databaseType = databaseType;
        return this;
    }

    /** The timestamp field used as the offset event time. */
    public KafkaJsonSourceConfigFactory eventTime(KafkaJsonSourceOptions.EventTime eventTime) {
        this.eventTime = eventTime;
        return this;
    }

    /** The boundary handling mode of the full->incremental switch. */
    public KafkaJsonSourceConfigFactory boundaryMode(
            KafkaJsonSourceOptions.BoundaryMode boundaryMode) {
        this.boundaryMode = boundaryMode;
        return this;
    }

    /** The Kafka consumer startup mode for stream-only scenarios. */
    public KafkaJsonSourceConfigFactory kafkaStartupMode(
            KafkaJsonSourceOptions.KafkaStartupMode kafkaStartupMode) {
        this.kafkaStartupMode = kafkaStartupMode;
        return this;
    }

    /** The DDL parser implementation. */
    public KafkaJsonSourceConfigFactory ddlParser(KafkaJsonSourceOptions.DdlParser ddlParser) {
        this.ddlParser = ddlParser;
        return this;
    }

    /** Arbitrary Kafka consumer properties. */
    public KafkaJsonSourceConfigFactory kafkaProperties(Properties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
        return this;
    }

    // ---------------------------------------------------------------------------------------
    // Override the base fluent setters with covariant return types so that chained calls keep
    // the KafkaJsonSourceConfigFactory type.
    // ---------------------------------------------------------------------------------------

    @Override
    public KafkaJsonSourceConfigFactory hostname(String hostname) {
        return (KafkaJsonSourceConfigFactory) super.hostname(hostname);
    }

    @Override
    public KafkaJsonSourceConfigFactory port(int port) {
        return (KafkaJsonSourceConfigFactory) super.port(port);
    }

    @Override
    public KafkaJsonSourceConfigFactory databaseList(String... databaseList) {
        return (KafkaJsonSourceConfigFactory) super.databaseList(databaseList);
    }

    @Override
    public KafkaJsonSourceConfigFactory tableList(String... tableList) {
        return (KafkaJsonSourceConfigFactory) super.tableList(tableList);
    }

    @Override
    public KafkaJsonSourceConfigFactory username(String username) {
        return (KafkaJsonSourceConfigFactory) super.username(username);
    }

    @Override
    public KafkaJsonSourceConfigFactory password(String password) {
        return (KafkaJsonSourceConfigFactory) super.password(password);
    }

    @Override
    public KafkaJsonSourceConfigFactory serverTimeZone(String timeZone) {
        return (KafkaJsonSourceConfigFactory) super.serverTimeZone(timeZone);
    }

    @Override
    public KafkaJsonSourceConfigFactory splitSize(int splitSize) {
        return (KafkaJsonSourceConfigFactory) super.splitSize(splitSize);
    }

    @Override
    public KafkaJsonSourceConfigFactory splitMetaGroupSize(int splitMetaGroupSize) {
        return (KafkaJsonSourceConfigFactory) super.splitMetaGroupSize(splitMetaGroupSize);
    }

    @Override
    public KafkaJsonSourceConfigFactory distributionFactorUpper(double distributionFactorUpper) {
        return (KafkaJsonSourceConfigFactory)
                super.distributionFactorUpper(distributionFactorUpper);
    }

    @Override
    public KafkaJsonSourceConfigFactory distributionFactorLower(double distributionFactorLower) {
        return (KafkaJsonSourceConfigFactory)
                super.distributionFactorLower(distributionFactorLower);
    }

    @Override
    public KafkaJsonSourceConfigFactory fetchSize(int fetchSize) {
        return (KafkaJsonSourceConfigFactory) super.fetchSize(fetchSize);
    }

    @Override
    public KafkaJsonSourceConfigFactory connectTimeout(Duration connectTimeout) {
        return (KafkaJsonSourceConfigFactory) super.connectTimeout(connectTimeout);
    }

    @Override
    public KafkaJsonSourceConfigFactory connectionPoolSize(int connectionPoolSize) {
        return (KafkaJsonSourceConfigFactory) super.connectionPoolSize(connectionPoolSize);
    }

    @Override
    public KafkaJsonSourceConfigFactory connectMaxRetries(int connectMaxRetries) {
        return (KafkaJsonSourceConfigFactory) super.connectMaxRetries(connectMaxRetries);
    }

    @Override
    public KafkaJsonSourceConfigFactory includeSchemaChanges(boolean includeSchemaChanges) {
        return (KafkaJsonSourceConfigFactory) super.includeSchemaChanges(includeSchemaChanges);
    }

    @Override
    public KafkaJsonSourceConfigFactory debeziumProperties(Properties properties) {
        return (KafkaJsonSourceConfigFactory) super.debeziumProperties(properties);
    }

    @Override
    public KafkaJsonSourceConfigFactory chunkKeyColumn(String chunkKeyColumn) {
        return (KafkaJsonSourceConfigFactory) super.chunkKeyColumn(chunkKeyColumn);
    }

    @Override
    public KafkaJsonSourceConfigFactory startupOptions(StartupOptions startupOptions) {
        // The base factory only whitelists INITIAL / SNAPSHOT / LATEST_OFFSET. A pure streaming
        // read from the beginning of the Kafka log — startup-options=earliest, no snapshot — is a
        // supported mode of this connector (the streaming consumer then seeks per
        // scan.kafka.startup.mode), so allow it alongside the base set.
        switch (startupOptions.startupMode) {
            case INITIAL:
            case SNAPSHOT:
            case LATEST_OFFSET:
            case EARLIEST_OFFSET:
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported startup mode: " + startupOptions.startupMode);
        }
        this.startupOptions = startupOptions;
        return this;
    }

    @Override
    public KafkaJsonSourceConfigFactory closeIdleReaders(boolean closeIdleReaders) {
        return (KafkaJsonSourceConfigFactory) super.closeIdleReaders(closeIdleReaders);
    }

    @Override
    public KafkaJsonSourceConfigFactory skipSnapshotBackfill(boolean skipSnapshotBackfill) {
        return (KafkaJsonSourceConfigFactory) super.skipSnapshotBackfill(skipSnapshotBackfill);
    }

    @Override
    public KafkaJsonSourceConfigFactory scanNewlyAddedTableEnabled(
            boolean scanNewlyAddedTableEnabled) {
        return (KafkaJsonSourceConfigFactory)
                super.scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled);
    }
}
