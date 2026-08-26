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

package org.apache.flink.cdc.connectors.kafkajson.source;

import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.testutils.CanalServerContainer;
import org.apache.flink.cdc.connectors.kafkajson.testutils.KafkaJsonSourceTestBase;
import org.apache.flink.cdc.connectors.kafkajson.testutils.KafkaUtil;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real end-to-end MySQL + canal-server chain: a real {@code canal/canal-server:v1.1.8} tails the
 * MySQL binlog and writes flatMessage JSON to Kafka, and the connector snapshots the table and then
 * consumes the incremental changes, including a DDL (schema change).
 *
 * <p>This validates that the connector's wire-format contract (the fields {@link
 * org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonFlatMessage} parses) matches
 * what canal actually emits. DML is executed only after the snapshot phase has finished, so the
 * incremental messages survive the exactly-once boundary.
 */
public class MySqlCanalChainITCase extends KafkaJsonSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlCanalChainITCase.class);

    protected static final MySqlContainer MYSQL8 = createMySqlContainer(MySqlVersion.V8_0);
    protected static final KafkaContainer KAFKA = KafkaUtil.createKafkaContainer(LOG, NETWORK);

    @BeforeClass
    public static void startContainers() {
        checkDockerAvailable();
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL8, KAFKA)).join();
        LOG.info("Containers are started.");
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        KAFKA.stop();
        MYSQL8.stop();
        LOG.info("Containers are stopped.");
    }

    @Test(timeout = 300_000)
    public void testRealCanalServerChain() throws Exception {
        UniqueDatabase database = new UniqueDatabase(MYSQL8, "customers", TEST_USER, TEST_PASSWORD);
        database.createAndInitialize();
        String topic = "canal-real-" + UUID.randomUUID();

        // 1) start canal-server pointing at MySQL and the Kafka topic
        CanalServerContainer canal =
                new CanalServerContainer(database.getDatabaseName(), topic, NETWORK, LOG);
        canal.start();
        canal.waitUntilStarted();
        // let the instance finish registering as a slave and start tailing the binlog
        Thread.sleep(3_000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureEnv(env);
        KafkaJsonSourceConfigFactory configFactory =
                buildConfigFactory(
                        database.getHost(),
                        database.getDatabasePort(),
                        TEST_USER,
                        TEST_PASSWORD,
                        database.getDatabaseName(),
                        "customers",
                        KAFKA.getBootstrapServers(),
                        topic);
        CloseableIterator<Event> events = runSource(configFactory, env);

        List<CreateTableEvent> createTables = new ArrayList<>();

        // 2) snapshot: the 4 pre-existing rows
        List<Event> snapshot = fetchDataEvents(events, 4, createTables);

        // Let the snapshot phase finish (high-watermark capture + stream split assignment).
        Thread.sleep(5_000);

        // 3) incremental DML -> canal -> Kafka -> connector
        String dbName = database.getDatabaseName();
        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "INSERT INTO `%s`.`customers` (id, name, address) VALUES (105, 'user_5', 'Chengdu')",
                            dbName));
            statement.execute(
                    String.format(
                            "UPDATE `%s`.`customers` SET address='Hangzhou' WHERE id=101", dbName));
            statement.execute(String.format("DELETE FROM `%s`.`customers` WHERE id=102", dbName));
        }
        // give canal time to tail the binlog and produce to Kafka before the connector consumes
        Thread.sleep(5_000);
        List<Event> stream = fetchDataEvents(events, 3, createTables);

        // 4) DDL -> canal -> Kafka -> connector
        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format(
                            "ALTER TABLE `%s`.`customers` ADD COLUMN email VARCHAR(255)", dbName));
        }
        List<Event> schemaChanges = fetchDataEvents(events, 1, createTables);

        assertThat(createTables).isNotEmpty();

        TableId tableId = TableId.tableId(dbName, "customers");
        BinaryRecordDataGenerator generator = snapshotRowGenerator();
        assertThat(snapshot)
                .containsExactlyInAnyOrder(
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

        assertThat(stream)
                .containsExactly(
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

        // DDL: the connector parses the canal DDL message and emits an AddColumnEvent
        assertThat(schemaChanges).hasSize(1);
        assertThat(schemaChanges.get(0)).isInstanceOf(AddColumnEvent.class);
        SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) schemaChanges.get(0);
        assertThat(schemaChangeEvent.tableId()).isEqualTo(tableId);
        AddColumnEvent addColumnEvent = (AddColumnEvent) schemaChangeEvent;
        assertThat(addColumnEvent.getAddedColumns())
                .anyMatch(c -> c.getAddColumn().getName().equals("email"));
    }
}
