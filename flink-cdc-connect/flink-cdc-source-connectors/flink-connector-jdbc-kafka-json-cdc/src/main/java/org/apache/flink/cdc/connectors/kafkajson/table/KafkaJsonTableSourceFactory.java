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

package org.apache.flink.cdc.connectors.kafkajson.table;

import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.cdc.connectors.base.options.SourceOptions;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions;
import org.apache.flink.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.cdc.debezium.utils.JdbcUrlUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;
import static org.apache.flink.cdc.debezium.utils.ResolvedSchemaUtils.getPhysicalSchema;

/** Factory for creating configured instance of {@link KafkaJsonTableSource}. */
public class KafkaJsonTableSourceFactory implements DynamicTableSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonTableSourceFactory.class);

    private static final String IDENTIFIER = "jdbc-kafka-json-cdc";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(
                DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX,
                JdbcUrlUtils.PROPERTIES_PREFIX,
                KafkaJsonSourceOptions.KAFKA_PROPERTIES_PREFIX);

        final ReadableConfig config = helper.getOptions();
        String hostname = config.get(JdbcSourceOptions.HOSTNAME);
        String username = config.get(JdbcSourceOptions.USERNAME);
        String password = config.get(JdbcSourceOptions.PASSWORD);
        String databaseName = config.get(JdbcSourceOptions.DATABASE_NAME);
        validateRegex(JdbcSourceOptions.DATABASE_NAME.key(), databaseName);
        String tableName = config.get(JdbcSourceOptions.TABLE_NAME);
        validateRegex(JdbcSourceOptions.TABLE_NAME.key(), tableName);
        int port = config.get(KafkaJsonSourceOptions.CANAL_MYSQL_PORT);
        ZoneId serverTimeZone = getServerTimeZone(config);
        ResolvedSchema physicalSchema =
                getPhysicalSchema(context.getCatalogTable().getResolvedSchema());
        StartupOptions startupOptions = getStartupOptions(config);

        int splitSize = config.get(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int splitMetaGroupSize = config.get(SourceOptions.CHUNK_META_GROUP_SIZE);
        int fetchSize = config.get(SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE);
        Duration connectTimeout = config.get(JdbcSourceOptions.CONNECT_TIMEOUT);
        int connectMaxRetries = config.get(JdbcSourceOptions.CONNECT_MAX_RETRIES);
        int connectionPoolSize = config.get(JdbcSourceOptions.CONNECTION_POOL_SIZE);
        String chunkKeyColumn =
                config.getOptional(JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN)
                        .orElse(null);
        boolean skipSnapshotBackfill =
                config.get(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        boolean closeIdleReaders =
                config.get(SourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        boolean scanNewlyAddedTableEnabled =
                config.get(SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED);

        String kafkaBootstrapServers = config.get(KafkaJsonSourceOptions.KAFKA_BOOTSTRAP_SERVERS);
        String kafkaGroupId = config.getOptional(KafkaJsonSourceOptions.KAFKA_GROUP_ID).orElse(null);
        List<String> kafkaTopics = getKafkaTopics(config.get(KafkaJsonSourceOptions.SCAN_KAFKA_TOPICS));
        Map<String, String> options = context.getCatalogTable().getOptions();
        KafkaJsonSourceOptions.MessageFormat messageFormat =
                getEnumOption(
                        options,
                        KafkaJsonSourceOptions.MESSAGE_FORMAT,
                        KafkaJsonSourceOptions.MessageFormat.class);
        KafkaJsonSourceOptions.EventTime eventTime =
                getEnumOption(
                        options, KafkaJsonSourceOptions.EVENT_TIME, KafkaJsonSourceOptions.EventTime.class);
        KafkaJsonSourceOptions.BoundaryMode boundaryMode =
                getEnumOption(
                        options,
                        KafkaJsonSourceOptions.BOUNDARY_MODE,
                        KafkaJsonSourceOptions.BoundaryMode.class);
        KafkaJsonSourceOptions.KafkaStartupMode kafkaStartupMode =
                getEnumOption(
                        options,
                        KafkaJsonSourceOptions.KAFKA_STARTUP_MODE,
                        KafkaJsonSourceOptions.KafkaStartupMode.class);
        KafkaJsonSourceOptions.DdlParser ddlParser =
                getEnumOption(
                        options,
                        KafkaJsonSourceOptions.CANAL_DDL_PARSER,
                        KafkaJsonSourceOptions.DdlParser.class);
        KafkaJsonSourceOptions.DatabaseType databaseType =
                getEnumOption(
                        options,
                        KafkaJsonSourceOptions.DATABASE_TYPE,
                        KafkaJsonSourceOptions.DatabaseType.class);
        Properties kafkaProperties = getKafkaProperties(options);

        LOG.info(
                "Properties for the Canal table source: "
                        + "{hostname={}, port={}, database-name={}, table-name={}, kafka-bootstrap-servers={}, "
                        + "kafka-topics={}, scan.startup.mode={}, message-format={}, ddl-parser={}}",
                hostname,
                port,
                databaseName,
                tableName,
                kafkaBootstrapServers,
                kafkaTopics,
                startupOptions.startupMode,
                messageFormat,
                ddlParser);

        return new KafkaJsonTableSource(
                physicalSchema,
                port,
                hostname,
                databaseName,
                tableName,
                username,
                password,
                serverTimeZone,
                getDebeziumProperties(context.getCatalogTable().getOptions()),
                startupOptions,
                splitSize,
                splitMetaGroupSize,
                fetchSize,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                chunkKeyColumn,
                skipSnapshotBackfill,
                closeIdleReaders,
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

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(JdbcSourceOptions.HOSTNAME);
        options.add(JdbcSourceOptions.USERNAME);
        options.add(JdbcSourceOptions.PASSWORD);
        options.add(JdbcSourceOptions.DATABASE_NAME);
        options.add(JdbcSourceOptions.TABLE_NAME);
        options.add(KafkaJsonSourceOptions.KAFKA_BOOTSTRAP_SERVERS);
        options.add(KafkaJsonSourceOptions.SCAN_KAFKA_TOPICS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(KafkaJsonSourceOptions.CANAL_MYSQL_PORT);
        options.add(KafkaJsonSourceOptions.SERVER_TIME_ZONE);
        options.add(SourceOptions.SCAN_STARTUP_MODE);
        options.add(SourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(SourceOptions.CHUNK_META_GROUP_SIZE);
        options.add(SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        options.add(JdbcSourceOptions.CONNECT_TIMEOUT);
        options.add(JdbcSourceOptions.CONNECTION_POOL_SIZE);
        options.add(JdbcSourceOptions.CONNECT_MAX_RETRIES);
        options.add(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        options.add(SourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        options.add(SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED);
        options.add(KafkaJsonSourceOptions.KAFKA_GROUP_ID);
        options.add(KafkaJsonSourceOptions.MESSAGE_FORMAT);
        options.add(KafkaJsonSourceOptions.DATABASE_TYPE);
        options.add(KafkaJsonSourceOptions.EVENT_TIME);
        options.add(KafkaJsonSourceOptions.BOUNDARY_MODE);
        options.add(KafkaJsonSourceOptions.KAFKA_STARTUP_MODE);
        options.add(KafkaJsonSourceOptions.CANAL_DDL_PARSER);
        return options;
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_SNAPSHOT = "snapshot";
    private static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

    private static StartupOptions getStartupOptions(ReadableConfig config) {
        String modeString = config.get(SourceOptions.SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();
            case SCAN_STARTUP_MODE_VALUE_SNAPSHOT:
                return StartupOptions.snapshot();
            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();
            case SCAN_STARTUP_MODE_VALUE_EARLIEST:
                return StartupOptions.earliest();
            case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
                return StartupOptions.timestamp(
                        config.get(SourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS));
            default:
                // 'specific-offset' (binlog file/pos) has no canal counterpart: the stream offset is
                // the canal event time, which 'timestamp' already covers
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s, %s, %s], but was: %s",
                                SourceOptions.SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_SNAPSHOT,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                SCAN_STARTUP_MODE_VALUE_EARLIEST,
                                SCAN_STARTUP_MODE_VALUE_TIMESTAMP,
                                modeString));
        }
    }

    private static List<String> getKafkaTopics(String topics) {
        return Arrays.stream(topics.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    /**
     * Resolves a {@code stringType()} canal option (read from the raw table options) into its enum.
     * Unlike {@link org.apache.flink.configuration.ConfigurationUtils#convertToEnum}, both the
     * kebab-case SQL values (e.g. {@code at-least-once}) and the underscore enum names are
     * accepted.
     */
    private static <T extends Enum<T>> T getEnumOption(
            Map<String, String> options, ConfigOption<String> option, Class<T> enumType) {
        String value = options.get(option.key());
        if (value == null) {
            value = option.defaultValue();
        }
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

    private static Properties getKafkaProperties(Map<String, String> tableOptions) {
        Properties kafkaProperties = new Properties();
        tableOptions.forEach(
                (key, value) -> {
                    if (key.startsWith(KafkaJsonSourceOptions.KAFKA_PROPERTIES_PREFIX)) {
                        kafkaProperties.setProperty(
                                key.substring(KafkaJsonSourceOptions.KAFKA_PROPERTIES_PREFIX.length()),
                                value);
                    }
                });
        return kafkaProperties;
    }

    /**
     * Checks the given regular expression's syntax is valid.
     *
     * @param optionName the option name of the regex
     * @param regex The regular expression to be checked
     * @throws ValidationException If the expression's syntax is invalid
     */
    private static void validateRegex(String optionName, String regex) {
        try {
            Pattern.compile(regex);
        } catch (Exception e) {
            throw new ValidationException(
                    String.format("The %s '%s' is not a valid regular expression", optionName, regex),
                    e);
        }
    }

    /** Replaces the default timezone placeholder with session timezone, if applicable. */
    private static ZoneId getServerTimeZone(ReadableConfig config) {
        final String serverTimeZone = config.get(KafkaJsonSourceOptions.SERVER_TIME_ZONE);
        if (serverTimeZone != null) {
            return ZoneId.of(serverTimeZone);
        } else {
            LOG.warn(
                    "{} is not set, which might cause data inconsistencies for time-related fields.",
                    KafkaJsonSourceOptions.SERVER_TIME_ZONE.key());
            final String sessionTimeZone = config.get(TableConfigOptions.LOCAL_TIME_ZONE);
            final ZoneId zoneId =
                    TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(sessionTimeZone)
                            ? ZoneId.systemDefault()
                            : ZoneId.of(sessionTimeZone);

            return zoneId;
        }
    }
}
