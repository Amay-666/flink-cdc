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

package org.apache.flink.cdc.connectors.canal.factory;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.table.api.ValidationException;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.canal.source.CanalDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.canal.source.CanalDataSourceOptions.KAFKA_BOOTSTRAP_SERVERS;
import static org.apache.flink.cdc.connectors.canal.source.CanalDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.canal.source.CanalDataSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static org.apache.flink.cdc.connectors.canal.source.CanalDataSourceOptions.SCAN_KAFKA_TOPICS;
import static org.apache.flink.cdc.connectors.canal.source.CanalDataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.canal.source.CanalDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.canal.source.CanalDataSourceOptions.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link CanalDataSourceFactory}. Only the option-validation paths are covered here
 * (they do not need a running MySQL/Kafka); the full data-source creation is exercised by the
 * integration tests.
 */
public class CanalDataSourceFactoryTest {

    private final CanalDataSourceFactory factory = new CanalDataSourceFactory();

    @Test
    public void testIdentifier() {
        assertThat(factory.identifier()).isEqualTo("canal-cdc");
    }

    @Test
    public void testRequiredOptions() {
        List<String> requireKeys =
                factory.requiredOptions().stream()
                        .map(ConfigOption::key)
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(requireKeys)
                .containsExactly(
                        "hostname",
                        "password",
                        "properties.kafka.bootstrap.servers",
                        "scan.kafka.topics",
                        "tables",
                        "username");
    }

    @Test
    public void testLackRequireOption() {
        Map<String, String> options = baseOptions();
        for (String requireKey : factory.requiredOptions().stream()
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
                        "Unsupported options found for 'canal-cdc'.\n\n"
                                + "Unsupported options:\n\n"
                                + "unsupported_key");
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
