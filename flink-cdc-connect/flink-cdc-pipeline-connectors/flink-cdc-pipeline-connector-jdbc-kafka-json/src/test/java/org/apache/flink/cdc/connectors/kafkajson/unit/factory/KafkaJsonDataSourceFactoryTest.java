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

package org.apache.flink.cdc.connectors.kafkajson.unit.factory;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.connectors.kafkajson.factory.KafkaJsonDataSourceFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.table.api.ValidationException;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.KAFKA_BOOTSTRAP_SERVERS;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.SCAN_KAFKA_TOPICS;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSourceOptions.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link KafkaJsonDataSourceFactory}. Only the option-validation paths are covered
 * here (they do not need a running MySQL/Kafka); the full data-source creation is exercised by the
 * integration tests.
 */
public class KafkaJsonDataSourceFactoryTest {

    private final KafkaJsonDataSourceFactory factory = new KafkaJsonDataSourceFactory();

    @Test
    public void testIdentifier() {
        assertThat(factory.identifier()).isEqualTo("jdbc-kafka-json-cdc");
    }

    @Test
    public void testRequiredOptions() {
        List<String> requireKeys =
                factory.requiredOptions().stream()
                        .map(ConfigOption::key)
                        .sorted()
                        .collect(Collectors.toList());
        // the Kafka options (scan.kafka.topics, properties.kafka.bootstrap.servers) are
        // optional: a snapshot-only pipeline may omit them and run as a pure JDBC dump
        assertThat(requireKeys)
                .containsExactly("hostname", "password", "tables", "username");
    }

    /** Every non-snapshot mode needs the Kafka options: rejected before any JDBC discovery. */
    @Test
    public void testNonSnapshotModeWithoutKafkaOptionsIsRejected() {
        Map<String, String> options = baseOptions();
        options.remove(SCAN_KAFKA_TOPICS.key());
        options.remove(KAFKA_BOOTSTRAP_SERVERS.key());
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("scan.kafka.topics");
    }

    /** Topics without bootstrap servers can never be consumed: rejected at job setup. */
    @Test
    public void testTopicsWithoutBootstrapServersIsRejected() {
        Map<String, String> options = baseOptions();
        options.remove(KAFKA_BOOTSTRAP_SERVERS.key());
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Incomplete Kafka configuration");
    }

    @Test
    public void testLackRequireOption() {
        Map<String, String> options = baseOptions();
        for (String requireKey :
                factory.requiredOptions().stream()
                        .map(ConfigOption::key)
                        .collect(Collectors.toList())) {
            Map<String, String> remainingOptions = new HashMap<>(options);
            remainingOptions.remove(requireKey);
            Factory.Context context = new MockContext(Configuration.fromMap(remainingOptions));

            assertThatThrownBy(() -> factory.createDataSource(context))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(
                            String.format(
                                    "One or more required options are missing.\n\n"
                                            + "Missing required options are:\n\n"
                                            + "%s",
                                    requireKey));
        }
    }

    @Test
    public void testUnsupportedOption() {
        Map<String, String> options = baseOptions();
        options.put("unsupported_key", "unsupported_value");
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Unsupported options found for 'jdbc-kafka-json-cdc'.\n\n"
                                + "Unsupported options:\n\n"
                                + "unsupported_key");
    }

    @Test
    public void testDatabaseTypeOptionIsRegistered() {
        // DATABASE_TYPE (scan.database.type) is read by createDataSource for the TiDB snapshot
        // boundary (TSO); it must be in optionalOptions() or FactoryHelper.validateExcept rejects
        // it as unsupported and createDataSource fails for users setting scan.database.type=tidb.
        assertThat(factory.optionalOptions())
                .extracting(ConfigOption::key)
                .contains("scan.database.type");
    }

    @Test
    public void testInvalidStartupMode() {
        Map<String, String> options = baseOptions();
        options.put(SCAN_STARTUP_MODE.key(), "specific-offset");
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Invalid value for option 'scan.startup.mode'");
    }

    @Test
    public void testTimestampStartupModeIsRejected() {
        // the timestamp startup mode was removed: a "timestamp" value now falls into the
        // invalid-enum rejection instead of the dedicated timestamp-millis validation
        Map<String, String> options = baseOptions();
        options.put(SCAN_STARTUP_MODE.key(), "timestamp");
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Invalid value for option 'scan.startup.mode'");
    }

    @Test
    public void testInvalidEnumOption() {
        Map<String, String> options = baseOptions();
        options.put("scan.message.format", "unknown-format");
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Invalid value 'unknown-format' for option 'scan.message.format'");
    }

    @Test
    public void testInvalidChunkSize() {
        Map<String, String> options = baseOptions();
        options.put(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.key(), "0");
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "The value of option 'scan.incremental.snapshot.chunk.size' must larger than 1");
    }

    /**
     * Tables may span multiple databases: the factory no longer rejects them — every distinct
     * database is forwarded to databaseList (the source's snapshot discovery walks all of them),
     * not just the first, and the include lists carry the full set into the stream phase.
     */
    @Test
    public void testCapturedTablesMaySpanMultipleDatabases() {
        KafkaJsonSourceConfigFactory configFactory =
                new KafkaJsonSourceConfigFactory()
                        .hostname("localhost")
                        .username("root")
                        .password("x")
                        .kafkaBootstrapServers("localhost:9092")
                        .kafkaTopics("topic");

        KafkaJsonDataSourceFactory.configureCapturedTables(
                configFactory, Arrays.asList("db1.orders", "db1.users", "db2.shipments"));

        KafkaJsonSourceConfig config = configFactory.create(0);
        assertThat(config.getDatabaseList()).containsExactly("db1", "db2");
        assertThat(config.getTableList())
                .containsExactly("db1.orders", "db1.users", "db2.shipments");
        assertThat(config.getDbzConfiguration().getString("database.include.list"))
                .isEqualTo("db1,db2");
    }

    private static Map<String, String> baseOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), "localhost");
        options.put(USERNAME.key(), "root");
        options.put(PASSWORD.key(), "password");
        options.put(TABLES.key(), "test.\\\\.*");
        options.put(KAFKA_BOOTSTRAP_SERVERS.key(), "localhost:9092");
        options.put(SCAN_KAFKA_TOPICS.key(), "test_topic");
        return options;
    }

    private static class MockContext implements Factory.Context {

        private final Configuration factoryConfiguration;

        MockContext(Configuration factoryConfiguration) {
            this.factoryConfiguration = factoryConfiguration;
        }

        @Override
        public Configuration getFactoryConfiguration() {
            return factoryConfiguration;
        }

        @Override
        public Configuration getPipelineConfiguration() {
            return null;
        }

        @Override
        public ClassLoader getClassLoader() {
            return getClass().getClassLoader();
        }
    }
}
