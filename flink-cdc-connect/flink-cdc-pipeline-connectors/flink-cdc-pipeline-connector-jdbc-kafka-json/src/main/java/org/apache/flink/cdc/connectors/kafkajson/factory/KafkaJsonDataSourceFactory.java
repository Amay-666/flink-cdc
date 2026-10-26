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

package org.apache.flink.cdc.connectors.kafkajson.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSource;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions;
import org.apache.flink.cdc.connectors.kafkajson.utils.KafkaJsonSchemaUtils;
import org.apache.flink.table.api.ValidationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.CANAL_DDL_PARSER;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.CHUNK_META_GROUP_SIZE;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.CONNECTION_POOL_SIZE;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.CONNECT_MAX_RETRIES;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.CONNECT_TIMEOUT;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.DATABASE_TYPE;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.EVENT_TIME;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.KAFKA_BOOTSTRAP_SERVERS;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.KAFKA_GROUP_ID;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.KAFKA_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.MESSAGE_FORMAT;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.PORT;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_METADATA_RELEASE_ENABLED;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.SCAN_KAFKA_TOPICS;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.SCHEMA_CHANGE_ENABLED;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.SERVER_TIME_ZONE;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.USERNAME;
import static org.apache.flink.cdc.debezium.table.DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX;
import static org.apache.flink.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;
import static org.apache.flink.cdc.debezium.utils.JdbcUrlUtils.PROPERTIES_PREFIX;
import static org.apache.flink.util.Preconditions.checkState;

/** A {@link Factory} to create {@link KafkaJsonDataSource}. */
@Internal
public class KafkaJsonDataSourceFactory implements DataSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonDataSourceFactory.class);

    public static final String IDENTIFIER = "jdbc-kafka-json-cdc";

    @Override
    public DataSource createDataSource(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(
                        PROPERTIES_PREFIX,
                        DEBEZIUM_OPTIONS_PREFIX,
                        KafkaJsonSourceOptions.KAFKA_PROPERTIES_PREFIX);

        final Configuration config = context.getFactoryConfiguration();
        String hostname = config.get(HOSTNAME);
        int port = config.get(PORT);

        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String tables = config.get(TABLES);

        ZoneId serverTimeZone = getServerTimeZone(config);
        StartupOptions startupOptions = getStartupOptions(config);

        boolean includeSchemaChanges = config.get(SCHEMA_CHANGE_ENABLED);

        int fetchSize = config.get(SCAN_SNAPSHOT_FETCH_SIZE);
        int splitSize = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int splitMetaGroupSize = config.get(CHUNK_META_GROUP_SIZE);

        double distributionFactorUpper = config.get(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        double distributionFactorLower = config.get(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);

        boolean closeIdleReaders = config.get(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);

        Duration connectTimeout = config.get(CONNECT_TIMEOUT);
        int connectMaxRetries = config.get(CONNECT_MAX_RETRIES);
        int connectionPoolSize = config.get(CONNECTION_POOL_SIZE);
        boolean scanNewlyAddedTableEnabled = config.get(SCAN_NEWLY_ADDED_TABLE_ENABLED);
        boolean releaseSnapshotMetadataEnabled =
                config.get(SCAN_INCREMENTAL_SNAPSHOT_METADATA_RELEASE_ENABLED);
        if (releaseSnapshotMetadataEnabled && scanNewlyAddedTableEnabled) {
            throw new ValidationException(
                    "scan.incremental.snapshot.metadata.release.enabled cannot be enabled"
                            + " together with scan.newly-added-table.enabled: the"
                            + " newly-added-table flow needs the snapshot split metadata that"
                            + " would be released.");
        }

        validateIntegerOption(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, splitSize, 1);
        validateIntegerOption(CHUNK_META_GROUP_SIZE, splitMetaGroupSize, 1);
        validateIntegerOption(SCAN_SNAPSHOT_FETCH_SIZE, fetchSize, 1);
        validateIntegerOption(CONNECTION_POOL_SIZE, connectionPoolSize, 1);
        validateIntegerOption(CONNECT_MAX_RETRIES, connectMaxRetries, 0);
        validateDistributionFactorUpper(distributionFactorUpper);
        validateDistributionFactorLower(distributionFactorLower);

        String kafkaBootstrapServers = config.getOptional(KAFKA_BOOTSTRAP_SERVERS).orElse(null);
        String kafkaGroupId = config.getOptional(KAFKA_GROUP_ID).orElse(null);
        String kafkaTopicsOption = config.getOptional(SCAN_KAFKA_TOPICS).orElse(null);
        List<String> kafkaTopics =
                kafkaTopicsOption == null
                        ? Collections.emptyList()
                        : getKafkaTopics(kafkaTopicsOption);
        validateKafkaOptions(startupOptions, kafkaTopics, kafkaBootstrapServers);
        Map<String, String> configMap = config.toMap();
        KafkaJsonSourceOptions.MessageFormat messageFormat =
                getEnumOption(config, MESSAGE_FORMAT, KafkaJsonSourceOptions.MessageFormat.class);
        KafkaJsonSourceOptions.EventTime eventTime =
                getEnumOption(config, EVENT_TIME, KafkaJsonSourceOptions.EventTime.class);
        KafkaJsonSourceOptions.KafkaStartupMode kafkaStartupMode =
                getEnumOption(
                        config, KAFKA_STARTUP_MODE, KafkaJsonSourceOptions.KafkaStartupMode.class);
        KafkaJsonSourceOptions.DdlParser ddlParser =
                getEnumOption(config, CANAL_DDL_PARSER, KafkaJsonSourceOptions.DdlParser.class);
        KafkaJsonSourceOptions.DatabaseType databaseType =
                getEnumOption(config, DATABASE_TYPE, KafkaJsonSourceOptions.DatabaseType.class);
        Properties kafkaProperties = getKafkaProperties(configMap);

        KafkaJsonSourceConfigFactory configFactory =
                new KafkaJsonSourceConfigFactory()
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .startupOptions(startupOptions)
                        .serverTimeZone(serverTimeZone.getId())
                        .fetchSize(fetchSize)
                        .splitSize(splitSize)
                        .splitMetaGroupSize(splitMetaGroupSize)
                        .distributionFactorLower(distributionFactorLower)
                        .distributionFactorUpper(distributionFactorUpper)
                        .connectTimeout(connectTimeout)
                        .connectMaxRetries(connectMaxRetries)
                        .connectionPoolSize(connectionPoolSize)
                        .closeIdleReaders(closeIdleReaders)
                        .includeSchemaChanges(includeSchemaChanges)
                        .scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled)
                        .debeziumProperties(getDebeziumProperties(configMap))
                        .kafkaBootstrapServers(kafkaBootstrapServers)
                        .kafkaGroupId(kafkaGroupId)
                        .kafkaTopics(kafkaTopics.toArray(new String[0]))
                        .messageFormat(messageFormat)
                        .databaseType(databaseType)
                        .eventTime(eventTime)
                        .kafkaStartupMode(kafkaStartupMode)
                        .ddlParser(ddlParser)
                        .kafkaProperties(kafkaProperties)
                        .releaseSnapshotMetadata(releaseSnapshotMetadataEnabled);

        String chunkKeyColumn = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        if (chunkKeyColumn != null) {
            configFactory.chunkKeyColumn(chunkKeyColumn);
        }

        Selectors selectors = new Selectors.SelectorsBuilder().includeTables(tables).build();
        List<String> capturedTables = getTableList(configFactory.create(0), selectors);
        if (capturedTables.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot find any table by the option 'tables' = " + tables);
        }
        configureCapturedTables(configFactory, capturedTables);

        LOG.info(
                "Properties for the jdbc-kafka-json data source: "
                        + "{hostname={}, port={}, tables={}, kafka-bootstrap-servers={}, "
                        + "kafka-topics={}, scan.startup.mode={}, message-format={}, ddl-parser={}}",
                hostname,
                port,
                tables,
                kafkaBootstrapServers,
                kafkaTopics,
                startupOptions.startupMode,
                messageFormat,
                ddlParser);

        return new KafkaJsonDataSource(configFactory);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(TABLES);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PORT);
        options.add(SERVER_TIME_ZONE);
        options.add(SCHEMA_CHANGE_ENABLED);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        options.add(CHUNK_META_GROUP_SIZE);
        options.add(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        options.add(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        options.add(CONNECT_TIMEOUT);
        options.add(CONNECT_MAX_RETRIES);
        options.add(CONNECTION_POOL_SIZE);
        options.add(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        options.add(SCAN_NEWLY_ADDED_TABLE_ENABLED);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_METADATA_RELEASE_ENABLED);
        options.add(KAFKA_GROUP_ID);
        options.add(MESSAGE_FORMAT);
        options.add(EVENT_TIME);
        options.add(KAFKA_STARTUP_MODE);
        options.add(CANAL_DDL_PARSER);
        // must be registered here, otherwise FactoryHelper.validateExcept rejects it as an
        // unsupported option and createDataSource fails for users setting scan.database.type=tidb
        options.add(DATABASE_TYPE);
        // Optional so that a snapshot-only pipeline (scan.startup.mode=snapshot) can run as a
        // pure JDBC dump without Kafka; every other mode must configure both, validated in
        // createDataSource
        options.add(KAFKA_BOOTSTRAP_SERVERS);
        options.add(SCAN_KAFKA_TOPICS);
        return options;
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_SNAPSHOT = "snapshot";
    private static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";

    private static StartupOptions getStartupOptions(Configuration config) {
        String modeString = config.get(SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();
            case SCAN_STARTUP_MODE_VALUE_SNAPSHOT:
                return StartupOptions.snapshot();
            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();
            case SCAN_STARTUP_MODE_VALUE_EARLIEST:
                return StartupOptions.earliest();
            default:
                // 'specific-offset' (binlog file/pos) and 'timestamp' have no canal counterpart:
                // the stream offset is the canal event time, not a binlog position or epoch millis
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s, %s], but was: %s",
                                SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_SNAPSHOT,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                SCAN_STARTUP_MODE_VALUE_EARLIEST,
                                modeString));
        }
    }

    private static List<String> getTableList(
            KafkaJsonSourceConfig sourceConfig, Selectors selectors) {
        return KafkaJsonSchemaUtils.listTables(sourceConfig, null).stream()
                .filter(selectors::isMatch)
                .map(TableId::toString)
                .collect(Collectors.toList());
    }

    /**
     * Wires the captured tables (in {@code database.table} form, as returned by {@link
     * #getTableList}) into the source config factory. The tables may span multiple databases: every
     * distinct database is forwarded to {@code databaseList} — the snapshot discovery walks all of
     * them and the comma-joined {@code database.include.list} filters the stream phase. The
     * databases keep the discovery order (a {@link LinkedHashSet}), so the split enumeration is
     * deterministic across job submissions.
     */
    @VisibleForTesting
    public static void configureCapturedTables(
            KafkaJsonSourceConfigFactory configFactory, List<String> capturedTables) {
        Set<String> databases =
                capturedTables.stream()
                        .map(table -> table.substring(0, table.indexOf('.')))
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        configFactory.databaseList(databases.toArray(new String[0]));
        configFactory.tableList(capturedTables.toArray(new String[0]));
    }

    private static List<String> getKafkaTopics(String topics) {
        return Arrays.stream(topics.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    /**
     * The Kafka options are optional only for a snapshot-only pipeline ({@code
     * scan.startup.mode=snapshot}): it runs as a pure JDBC dump — the watermark probe
     * short-circuits to the INITIAL offset and the stream fetch task completes its no-op stream
     * split without opening a Kafka connection. Every other mode consumes the incremental changes
     * from Kafka, so both the topics and the bootstrap servers are required, and they must be
     * configured together.
     */
    private static void validateKafkaOptions(
            StartupOptions startupOptions, List<String> topics, String bootstrapServers) {
        if (topics.isEmpty()) {
            if (!startupOptions.isSnapshotOnly()) {
                throw new ValidationException(
                        "Missing required options: [scan.kafka.topics, "
                                + "properties.kafka.bootstrap.servers]. Only a snapshot-only "
                                + "pipeline ('scan.startup.mode'='snapshot') may omit them and "
                                + "run as a pure JDBC dump.");
            }
        } else if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            throw new ValidationException(
                    "Incomplete Kafka configuration: 'scan.kafka.topics' is set but "
                            + "'properties.kafka.bootstrap.servers' is missing; configure both.");
        }
    }

    /**
     * Resolves a {@code stringType()} connector option into its enum. Unlike {@link
     * org.apache.flink.configuration.ConfigurationUtils#convertToEnum}, both the kebab-case values
     * (e.g. {@code at-least-once}) and the underscore enum names are accepted.
     */
    private static <T extends Enum<T>> T getEnumOption(
            Configuration config, ConfigOption<String> option, Class<T> enumType) {
        String value = config.get(option);
        for (T constant : enumType.getEnumConstants()) {
            if (constant.toString().equalsIgnoreCase(value.replace('-', '_'))) {
                return constant;
            }
        }
        throw new ValidationException(
                String.format(
                        "Invalid value '%s' for option '%s'. Expected one of: [%s]",
                        value,
                        option.key(),
                        Arrays.stream(enumType.getEnumConstants())
                                .map(Enum::toString)
                                .collect(Collectors.joining(", "))));
    }

    private static Properties getKafkaProperties(Map<String, String> configMap) {
        Properties kafkaProperties = new Properties();
        configMap.forEach(
                (key, value) -> {
                    if (key.startsWith(KafkaJsonSourceOptions.KAFKA_PROPERTIES_PREFIX)) {
                        kafkaProperties.setProperty(
                                key.substring(
                                        KafkaJsonSourceOptions.KAFKA_PROPERTIES_PREFIX.length()),
                                value);
                    }
                });
        return kafkaProperties;
    }

    /** Replaces the default timezone placeholder with session timezone, if applicable. */
    private static ZoneId getServerTimeZone(Configuration config) {
        final String serverTimeZone = config.get(SERVER_TIME_ZONE);
        if (serverTimeZone != null) {
            return ZoneId.of(serverTimeZone);
        } else {
            LOG.warn(
                    "{} is not set, which might cause data inconsistencies for time-related fields.",
                    SERVER_TIME_ZONE.key());
            return ZoneId.systemDefault();
        }
    }

    /** Checks the value of given integer option is valid. */
    private void validateIntegerOption(
            ConfigOption<Integer> option, int optionValue, int exclusiveMin) {
        checkState(
                optionValue > exclusiveMin,
                String.format(
                        "The value of option '%s' must larger than %d, but is %d",
                        option.key(), exclusiveMin, optionValue));
    }

    /** Checks the value of given evenly distribution factor upper bound is valid. */
    private void validateDistributionFactorUpper(double distributionFactorUpper) {
        checkState(
                distributionFactorUpper >= 1.0d,
                String.format(
                        "The value of option '%s' must larger than or equals %s, but is %s",
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.key(),
                        1.0d,
                        distributionFactorUpper));
    }

    /** Checks the value of given evenly distribution factor lower bound is valid. */
    private void validateDistributionFactorLower(double distributionFactorLower) {
        checkState(
                distributionFactorLower >= 0.0d && distributionFactorLower <= 1.0d,
                String.format(
                        "The value of option '%s' must between %s and %s inclusively, but is %s",
                        CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.key(),
                        0.0d,
                        1.0d,
                        distributionFactorLower));
    }
}
