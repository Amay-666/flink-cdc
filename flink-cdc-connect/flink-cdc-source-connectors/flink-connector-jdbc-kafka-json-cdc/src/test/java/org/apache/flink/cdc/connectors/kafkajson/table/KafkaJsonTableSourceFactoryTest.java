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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link KafkaJsonTableSource} created by {@link KafkaJsonTableSourceFactory}. */
class KafkaJsonTableSourceFactoryTest {

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("id", DataTypes.BIGINT().notNull()),
                            Column.physical("name", DataTypes.STRING().notNull()),
                            Column.physical("count", DataTypes.DECIMAL(38, 18))),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

    private static final ResolvedSchema SCHEMA_WITH_METADATA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("id", DataTypes.BIGINT().notNull()),
                            Column.metadata("time", DataTypes.TIMESTAMP_LTZ(3), "op_ts", true),
                            Column.metadata(
                                    "database_name", DataTypes.STRING(), "database_name", true)),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

    private static final String MY_LOCALHOST = "localhost";
    private static final String MY_USERNAME = "flinkuser";
    private static final String MY_PASSWORD = "flinkpw";
    private static final String MY_DATABASE = "myDB";
    private static final String MY_TABLE = "myTable";
    private static final String MY_KAFKA_BOOTSTRAP = "localhost:9092";
    private static final String MY_KAFKA_TOPICS = "canal_data";

    @Test
    void testCommonProperties() {
        Map<String, String> properties = getAllOptions();

        DynamicTableSource actualSource = createTableSource(properties);
        KafkaJsonTableSource expectedSource =
                new KafkaJsonTableSource(
                        SCHEMA,
                        KafkaJsonSourceOptions.CANAL_MYSQL_PORT.defaultValue(),
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.systemDefault(),
                        new Properties(),
                        StartupOptions.initial(),
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue(),
                        SourceOptions.CHUNK_META_GROUP_SIZE.defaultValue(),
                        SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE.defaultValue(),
                        JdbcSourceOptions.CONNECT_TIMEOUT.defaultValue(),
                        JdbcSourceOptions.CONNECT_MAX_RETRIES.defaultValue(),
                        JdbcSourceOptions.CONNECTION_POOL_SIZE.defaultValue(),
                        null,
                        SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.defaultValue(),
                        SourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED.defaultValue(),
                        SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED.defaultValue(),
                        MY_KAFKA_BOOTSTRAP,
                        null,
                        Collections.singletonList(MY_KAFKA_TOPICS),
                        KafkaJsonSourceOptions.MessageFormat.CANAL,
                        KafkaJsonSourceOptions.DatabaseType.MYSQL,
                        KafkaJsonSourceOptions.EventTime.ES,
                        KafkaJsonSourceOptions.BoundaryMode.EXACTLY_ONCE,
                        KafkaJsonSourceOptions.KafkaStartupMode.EARLIEST,
                        KafkaJsonSourceOptions.DdlParser.DRUID,
                        new Properties());

        assertEquals(expectedSource, actualSource);
    }

    @Test
    void testOptionalProperties() {
        Map<String, String> options = getAllOptions();
        options.put("port", "3307");
        options.put("server-time-zone", "Asia/Shanghai");
        options.put("properties.kafka.group.id", "group-1");
        options.put("scan.message.format", "debezium");
        options.put("scan.message.event-time", "ts");
        options.put("scan.boundary.mode", "at-least-once");
        options.put("scan.kafka.startup.mode", "latest");
        options.put("scan.ddl.parser", "debezium");
        options.put("scan.incremental.snapshot.chunk.size", "8000");
        options.put("chunk-meta.group.size", "3000");
        options.put("scan.snapshot.fetch.size", "100");
        options.put("scan.incremental.snapshot.chunk.key-column", "id");
        options.put("connect.timeout", "45s");
        options.put("scan.incremental.snapshot.backfill.skip", "true");
        options.put("scan.incremental.close-idle-reader.enabled", "true");
        options.put("scan.newly-added-table.enabled", "true");
        options.put("debezium.snapshot.mode", "never");
        options.put("scan.kafka.properties.max.poll.records", "500");

        DynamicTableSource actualSource = createTableSource(options);
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty("snapshot.mode", "never");
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("max.poll.records", "500");
        KafkaJsonTableSource expectedSource =
                new KafkaJsonTableSource(
                        SCHEMA,
                        3307,
                        MY_LOCALHOST,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        ZoneId.of("Asia/Shanghai"),
                        dbzProperties,
                        StartupOptions.initial(),
                        8000,
                        3000,
                        100,
                        Duration.ofSeconds(45),
                        JdbcSourceOptions.CONNECT_MAX_RETRIES.defaultValue(),
                        JdbcSourceOptions.CONNECTION_POOL_SIZE.defaultValue(),
                        "id",
                        true,
                        true,
                        true,
                        MY_KAFKA_BOOTSTRAP,
                        "group-1",
                        Collections.singletonList(MY_KAFKA_TOPICS),
                        KafkaJsonSourceOptions.MessageFormat.DEBEZIUM,
                        KafkaJsonSourceOptions.DatabaseType.MYSQL,
                        KafkaJsonSourceOptions.EventTime.TS,
                        KafkaJsonSourceOptions.BoundaryMode.AT_LEAST_ONCE,
                        KafkaJsonSourceOptions.KafkaStartupMode.LATEST,
                        KafkaJsonSourceOptions.DdlParser.DEBEZIUM,
                        kafkaProperties);

        assertEquals(expectedSource, actualSource);
    }

    @Test
    void testTimestampStartupMode() {
        Map<String, String> options = getAllOptions();
        options.put("scan.startup.mode", "timestamp");
        options.put("scan.startup.timestamp-millis", "1585723984000");

        KafkaJsonTableSource source = (KafkaJsonTableSource) createTableSource(options);
        assertEquals(StartupOptions.timestamp(1585723984000L), source.getStartupOptions());
    }

    @Test
    void testSnapshotStartupMode() {
        Map<String, String> options = getAllOptions();
        options.put("scan.startup.mode", "snapshot");

        KafkaJsonTableSource source = (KafkaJsonTableSource) createTableSource(options);
        assertEquals(StartupOptions.snapshot(), source.getStartupOptions());
    }

    @Test
    void testLatestStartupMode() {
        Map<String, String> options = getAllOptions();
        options.put("scan.startup.mode", "latest-offset");

        KafkaJsonTableSource source = (KafkaJsonTableSource) createTableSource(options);
        assertEquals(StartupOptions.latest(), source.getStartupOptions());
    }

    @Test
    void testSpecificOffsetStartupModeIsRejected() {
        Map<String, String> options = getAllOptions();
        options.put("scan.startup.mode", "specific-offset");

        // 'specific-offset' (binlog file/pos) has no canal counterpart
        ValidationException e =
                assertThrows(ValidationException.class, () -> createTableSource(options));
        assertTrue(
                getRootCauseMessage(e).contains("Invalid value for option 'scan.startup.mode'"));
    }

    @Test
    void testInvalidDatabaseNameRegex() {
        Map<String, String> options = getAllOptions();
        options.put("database-name", "([");

        ValidationException e =
                assertThrows(ValidationException.class, () -> createTableSource(options));
        assertTrue(e.getMessage().contains("database-name"));
    }

    @Test
    void testMissingRequiredOptions() {
        Map<String, String> options = getAllOptions();
        options.remove("scan.kafka.topics");

        ValidationException e =
                assertThrows(ValidationException.class, () -> createTableSource(options));
        assertTrue(getRootCauseMessage(e).contains("scan.kafka.topics"));
    }

    @Test
    void testMetadata() {
        Map<String, String> options = getAllOptions();

        DynamicTableSource actualSource = createTableSource(SCHEMA_WITH_METADATA, options);
        assertTrue(actualSource instanceof KafkaJsonTableSource);
        KafkaJsonTableSource tableSource = (KafkaJsonTableSource) actualSource;

        Map<String, DataType> readableMetadata = tableSource.listReadableMetadata();
        assertTrue(readableMetadata.containsKey("table_name"));
        assertTrue(readableMetadata.containsKey("database_name"));
        assertTrue(readableMetadata.containsKey("op_ts"));
        assertTrue(readableMetadata.containsKey("es"));
        assertTrue(readableMetadata.containsKey("ts"));
        assertTrue(readableMetadata.containsKey("row_kind"));

        tableSource.applyReadableMetadata(
                Arrays.asList("table_name", "op_ts"),
                SCHEMA_WITH_METADATA.toSourceRowDataType());

        // copy() keeps the mutable metadata state
        KafkaJsonTableSource copied = (KafkaJsonTableSource) tableSource.copy();
        assertEquals(tableSource, copied);
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "jdbc-kafka-json-cdc");
        options.put("hostname", MY_LOCALHOST);
        options.put("database-name", MY_DATABASE);
        options.put("table-name", MY_TABLE);
        options.put("username", MY_USERNAME);
        options.put("password", MY_PASSWORD);
        options.put("properties.kafka.bootstrap.servers", MY_KAFKA_BOOTSTRAP);
        options.put("scan.kafka.topics", MY_KAFKA_TOPICS);
        return options;
    }

    private static DynamicTableSource createTableSource(Map<String, String> options) {
        return createTableSource(SCHEMA, options);
    }

    private static DynamicTableSource createTableSource(
            ResolvedSchema schema, Map<String, String> options) {
        return FactoryMocks.createTableSource(schema, options);
    }

    /**
     * {@code FactoryUtil} wraps any exception thrown while creating a table source in its own {@link
     * ValidationException} ("Unable to create a source ..."), so assertions must walk down to the
     * root cause.
     */
    private static String getRootCauseMessage(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getMessage();
    }
}
