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

package org.apache.flink.cdc.connectors.kafkajson.testutils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.kafkajson.factory.KafkaJsonDataSourceFactory;
import org.apache.flink.cdc.connectors.kafkajson.serializer.KafkaJsonEventTypeInfo;
import org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSource;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.TestLogger;

import org.junit.Assume;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * Base class for the KafkaJson source integration tests.
 *
 * <p>Every container created by a subclass joins the shared {@link #NETWORK} so that sibling
 * containers (canal-server, TiDB PD/TiKV/TiCDC, ...) can reach each other over docker network
 * aliases. The Flink MiniCluster runs on the host JVM and therefore talks to the MySQL/TiDB/Kafka
 * containers through their host-mapped ports, exactly like {@code MySqlPipelineITCase} does.
 *
 * <p>Test flow (mirrors the high-watermark semantics of the connector): the source is started with
 * an <em>empty</em> Kafka topic, so the snapshot high watermark is {@code INITIAL_OFFSET(-1)}.
 * Increment messages are written <em>after</em> the snapshot events are collected, so every stream
 * message carries an event time strictly greater than the high watermark and survives the
 * exactly-once boundary.
 */
public abstract class KafkaJsonSourceTestBase extends TestLogger {

    protected static final Logger LOG = LoggerFactory.getLogger(KafkaJsonSourceTestBase.class);

    protected static final int DEFAULT_PARALLELISM = 1;

    /**
     * Credentials of the {@code mysqluser} account created by {@code docker/setup.sql} (all
     * privileges), mirroring {@code MySqlPipelineITCase}. The container's root account is {@code
     * flinkuser}/{@code flinkpw}; the source and {@code UniqueDatabase} connect as {@code
     * mysqluser}, and canal-server tails the binlog as {@code flinkuser}.
     */
    public static final String TEST_USER = "mysqluser";

    public static final String TEST_PASSWORD = "mysqlpw";

    /** Shared docker network; containers reach each other via network aliases. */
    protected static final Network NETWORK = Network.newNetwork();

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .build());

    /**
     * Call from a subclass {@code @BeforeClass}: silently skips the whole test class when Docker is
     * unavailable (e.g. CI without a docker daemon) instead of failing the build.
     */
    protected static void checkDockerAvailable() {
        Assume.assumeTrue(
                "Docker is not available; skipping integration test.",
                DockerClientFactory.instance().isDockerAvailable());
    }

    /**
     * Creates a MySQL container with the binlog/replication setup required by the snapshot reader
     * and canal. Returned as {@link KafkaJsonMySqlContainer} so callers can pin a fixed host port
     * (see {@link KafkaJsonMySqlContainer#withFixedExposedPort}).
     */
    protected static KafkaJsonMySqlContainer createMySqlContainer(MySqlVersion version) {
        // withConfigurationOverride/withSetupSQL/withDatabaseName/... are declared on the released
        // MySqlContainer to return MySqlContainer, so the chain's static type is MySqlContainer and
        // the concrete KafkaJsonMySqlContainer requires an explicit cast.
        return (KafkaJsonMySqlContainer)
                new KafkaJsonMySqlContainer(version)
                        .withConfigurationOverride("docker/server-gtids/my.cnf")
                        .withSetupSQL("docker/setup.sql")
                        .withDatabaseName("flink-test")
                        .withUsername("flinkuser")
                        .withPassword("flinkpw")
                        .withNetwork(NETWORK)
                        .withNetworkAliases("mysql")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
    }

    /**
     * Builds a {@link KafkaJsonSourceConfigFactory} pointed at the given database server, with the
     * options the integration tests rely on (initial snapshot, exactly-once boundary, canal message
     * format). {@code tablePattern} is a regex matched against {@code database.table}; pass e.g.
     * {@code "customers"} for a single table.
     */
    protected static KafkaJsonSourceConfigFactory buildConfigFactory(
            String hostname,
            int port,
            String username,
            String password,
            String databaseName,
            String tablePattern,
            String kafkaBootstrapServers,
            String kafkaTopic) {
        return buildConfigFactory(
                hostname,
                port,
                username,
                password,
                databaseName,
                tablePattern,
                kafkaBootstrapServers,
                kafkaTopic,
                KafkaJsonSourceOptions.DatabaseType.MYSQL);
    }

    /**
     * Variant of {@link #buildConfigFactory} with an explicit database type (e.g. {@code tidb}).
     */
    protected static KafkaJsonSourceConfigFactory buildConfigFactory(
            String hostname,
            int port,
            String username,
            String password,
            String databaseName,
            String tablePattern,
            String kafkaBootstrapServers,
            String kafkaTopic,
            KafkaJsonSourceOptions.DatabaseType databaseType) {
        return new KafkaJsonSourceConfigFactory()
                .hostname(hostname)
                .port(port)
                .username(username)
                .password(password)
                .databaseList(databaseName)
                .tableList(databaseName + "\\.(" + tablePattern + ")")
                .startupOptions(StartupOptions.initial())
                .serverTimeZone("UTC")
                .includeSchemaChanges(true)
                .kafkaBootstrapServers(kafkaBootstrapServers)
                .kafkaGroupId("kafka-json-test-" + UUID.randomUUID())
                .kafkaTopics(kafkaTopic)
                .messageFormat(KafkaJsonSourceOptions.MessageFormat.CANAL)
                .databaseType(databaseType)
                .eventTime(KafkaJsonSourceOptions.EventTime.ES)
                .boundaryMode(KafkaJsonSourceOptions.BoundaryMode.EXACTLY_ONCE)
                .kafkaStartupMode(KafkaJsonSourceOptions.KafkaStartupMode.EARLIEST)
                .ddlParser(KafkaJsonSourceOptions.DdlParser.DRUID);
    }

    /**
     * Starts the source over the given config factory on the given environment and returns the
     * event iterator. The environment must have been created with the desired parallelism,
     * checkpointing and restart strategy before calling this method.
     */
    protected static CloseableIterator<Event> runSource(
            KafkaJsonSourceConfigFactory configFactory, StreamExecutionEnvironment env)
            throws Exception {
        KafkaJsonDataSource dataSource = new KafkaJsonDataSource(configFactory);
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) dataSource.getEventSourceProvider();
        return env.fromSource(
                        sourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        KafkaJsonDataSourceFactory.IDENTIFIER,
                        new KafkaJsonEventTypeInfo())
                .executeAndCollect();
    }

    /** Configures the given environment for a deterministic single-parallelism source run. */
    protected static void configureEnv(StreamExecutionEnvironment env) {
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(2000);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    /**
     * Builds canal flatMessage JSON for INSERT 105, UPDATE 101 and DELETE 102 on {@code customers}.
     * The same wire format the {@link CanalServerContainer} produces, so the simulated chains are a
     * deterministic reference for the real chains.
     */
    protected static List<String> simulatedCanalMessages(String databaseName) {
        long baseEventTime = 1700000001000L;
        return Arrays.asList(
                insertMessage(databaseName, "105", "user_5", "Chengdu", baseEventTime),
                updateMessage(
                        databaseName,
                        "101",
                        "user_1",
                        "Hangzhou",
                        "Shanghai",
                        baseEventTime + 1000),
                deleteMessage(databaseName, "102", "user_2", "Beijing", baseEventTime + 2000));
    }

    private static String insertMessage(
            String db, String id, String name, String address, long eventTime) {
        return String.format(
                "{\"data\":[{\"id\":\"%s\",\"name\":\"%s\",\"address\":\"%s\"}],"
                        + "\"database\":\"%s\",\"es\":%d,\"id\":1,\"isDdl\":false,"
                        + "\"mysqlType\":{\"id\":\"int\",\"name\":\"varchar(255)\",\"address\":\"varchar(255)\"},"
                        + "\"old\":null,\"pkNames\":[\"id\"],\"sql\":\"\","
                        + "\"sqlType\":{\"id\":4,\"name\":12,\"address\":12},\"table\":\"customers\","
                        + "\"ts\":%d,\"type\":\"INSERT\"}",
                id, name, address, db, eventTime, eventTime);
    }

    private static String updateMessage(
            String db, String id, String name, String address, String oldAddress, long eventTime) {
        return String.format(
                "{\"data\":[{\"id\":\"%s\",\"name\":\"%s\",\"address\":\"%s\"}],"
                        + "\"database\":\"%s\",\"es\":%d,\"id\":2,\"isDdl\":false,"
                        + "\"mysqlType\":{\"id\":\"int\",\"name\":\"varchar(255)\",\"address\":\"varchar(255)\"},"
                        + "\"old\":[{\"address\":\"%s\"}],\"pkNames\":[\"id\"],\"sql\":\"\","
                        + "\"sqlType\":{\"id\":4,\"name\":12,\"address\":12},\"table\":\"customers\","
                        + "\"ts\":%d,\"type\":\"UPDATE\"}",
                id, name, address, db, eventTime, oldAddress, eventTime);
    }

    private static String deleteMessage(
            String db, String id, String name, String address, long eventTime) {
        return String.format(
                "{\"data\":[{\"id\":\"%s\",\"name\":\"%s\",\"address\":\"%s\"}],"
                        + "\"database\":\"%s\",\"es\":%d,\"id\":3,\"isDdl\":false,"
                        + "\"mysqlType\":{\"id\":\"int\",\"name\":\"varchar(255)\",\"address\":\"varchar(255)\"},"
                        + "\"old\":null,\"pkNames\":[\"id\"],\"sql\":\"\","
                        + "\"sqlType\":{\"id\":4,\"name\":12,\"address\":12},\"table\":\"customers\","
                        + "\"ts\":%d,\"type\":\"DELETE\"}",
                id, name, address, db, eventTime, eventTime);
    }

    /**
     * Collects {@code size} non-{@link CreateTableEvent} events from the iterator, appending any
     * CreateTableEvent seen along the way to {@code createTableSink}. The caller can then assert
     * the sink is non-empty, since the connector does not guarantee how many CreateTableEvents a
     * table produces.
     */
    protected static List<Event> fetchDataEvents(
            Iterator<Event> iter, int size, List<CreateTableEvent> createTableSink) {
        List<Event> result = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Event event = iter.next();
            if (event instanceof CreateTableEvent) {
                createTableSink.add((CreateTableEvent) event);
            } else {
                result.add(event);
                size--;
            }
        }
        return result;
    }

    /**
     * Row type of {@code customers}: {@code id INT NOT NULL, name VARCHAR(255) NOT NULL, address
     * VARCHAR(255)} — identical on MySQL and TiDB.
     */
    protected static BinaryRecordDataGenerator snapshotRowGenerator() {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(255).notNull(),
                            DataTypes.VARCHAR(255)
                        },
                        new String[] {"id", "name", "address"});
        return new BinaryRecordDataGenerator(rowType);
    }

    /** Expected snapshot events: INSERT 101-104 (the 4 pre-existing rows of {@code customers}). */
    protected static List<Event> expectedSnapshotEvents(String dbName) {
        TableId tableId = TableId.tableId(dbName, "customers");
        BinaryRecordDataGenerator generator = snapshotRowGenerator();
        return Arrays.asList(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    101,
                                    BinaryStringData.fromString("user_1"),
                                    BinaryStringData.fromString("Shanghai")
                                })),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    102,
                                    BinaryStringData.fromString("user_2"),
                                    BinaryStringData.fromString("Beijing")
                                })),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    103,
                                    BinaryStringData.fromString("user_3"),
                                    BinaryStringData.fromString("Hangzhou")
                                })),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    104,
                                    BinaryStringData.fromString("user_4"),
                                    BinaryStringData.fromString("Shenzhen")
                                })));
    }

    /**
     * Expected stream events after the incremental messages (INSERT 105, UPDATE 101, DELETE 102).
     * The UPDATE before image is the complete old row: canal's {@code old} carries only the changed
     * columns and the unchanged ones keep their (identical) values from {@code data}.
     */
    protected static List<Event> expectedStreamEvents(String dbName) {
        TableId tableId = TableId.tableId(dbName, "customers");
        BinaryRecordDataGenerator generator = snapshotRowGenerator();
        return Arrays.asList(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    105,
                                    BinaryStringData.fromString("user_5"),
                                    BinaryStringData.fromString("Chengdu")
                                })),
                DataChangeEvent.updateEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    101,
                                    BinaryStringData.fromString("user_1"),
                                    BinaryStringData.fromString("Shanghai")
                                }),
                        generator.generate(
                                new Object[] {
                                    101,
                                    BinaryStringData.fromString("user_1"),
                                    BinaryStringData.fromString("Hangzhou")
                                })),
                DataChangeEvent.deleteEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    102,
                                    BinaryStringData.fromString("user_2"),
                                    BinaryStringData.fromString("Beijing")
                                })));
    }
}
