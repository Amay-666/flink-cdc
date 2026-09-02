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

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.kafkajson.infra.DebeziumConnectContainer;
import org.apache.flink.cdc.connectors.kafkajson.infra.KafkaJsonSourceTestBase;
import org.apache.flink.cdc.connectors.kafkajson.infra.KafkaUtil;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real end-to-end MySQL + Debezium chain: a real {@code debezium/connect:1.9} worker reads the
 * MySQL binlog and writes the {@code {schema, payload}} SourceRecord JSON envelope to Kafka, and
 * the connector snapshots the table and then consumes the incremental changes.
 *
 * <p>This validates the connector's Debezium wire-format contract against what Debezium 1.9
 * actually emits (the fork embeds Debezium {@code 1.9.8.Final}). The timing avoids a
 * double-snapshot race: the source is started only after the Debezium snapshot has fully drained to
 * Kafka (all N {@code op:r} records visible), so the Debezium snapshot records fall before the
 * stream boundary and the DML executed after the source snapshot produces exactly the M incremental
 * events.
 */
public class DebeziumCdcChainITCase extends KafkaJsonSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumCdcChainITCase.class);

    protected static final MySqlContainer MYSQL8 = createMySqlContainer(MySqlVersion.V8_0);
    protected static final KafkaContainer KAFKA = KafkaUtil.createKafkaContainer(LOG, NETWORK);
    protected static final DebeziumConnectContainer DEBEZIUM =
            new DebeziumConnectContainer(NETWORK, LOG);

    @BeforeClass
    public static void startContainers() {
        checkDockerAvailable();
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL8, KAFKA, DEBEZIUM)).join();
        LOG.info("Containers are started.");
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        DEBEZIUM.stop();
        KAFKA.stop();
        MYSQL8.stop();
        LOG.info("Containers are stopped.");
    }

    @Test(timeout = 300_000)
    public void testRealDebeziumChain() throws Exception {
        UniqueDatabase database = new UniqueDatabase(MYSQL8, "customers", TEST_USER, TEST_PASSWORD);
        database.createAndInitialize();
        String topicPrefix = "debezium" + UUID.randomUUID().toString().replace("-", "");
        String topic = topicPrefix + "." + database.getDatabaseName() + ".customers";
        String bootstrapServers = KAFKA.getBootstrapServers();

        // 1) register the connector: Debezium snapshots the table and then tails the binlog
        DEBEZIUM.createMySqlConnector(
                "debezium-" + UUID.randomUUID(),
                "mysql",
                MySqlContainer.MYSQL_PORT,
                TEST_USER,
                TEST_PASSWORD,
                database.getDatabaseName(),
                "customers",
                topicPrefix);

        // 2) deterministic "Debezium snapshot finished" signal: all 4 op:r records are in Kafka
        waitForDebeziumSnapshot(bootstrapServers, topic, 4);

        // 3) start the source: its JDBC snapshot reads the same 4 rows, and the stream boundary
        // lands after the Debezium snapshot records, so those are not re-emitted as stream events
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
                                bootstrapServers,
                                topic)
                        // the only difference from the canal chain: the incremental format
                        .messageFormat(KafkaJsonSourceOptions.MessageFormat.DEBEZIUM);
        CloseableIterator<Event> events = runSource(configFactory, env);

        List<CreateTableEvent> createTables = new ArrayList<>();
        List<Event> snapshot = fetchDataEvents(events, 4, createTables);

        // Let the snapshot phase finish (high-watermark capture + stream split assignment).
        Thread.sleep(5_000);

        // 4) incremental DML -> binlog -> Debezium -> Kafka -> connector
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
        // give Debezium time to pick up the binlog and produce to Kafka before the connector
        // consumes
        Thread.sleep(5_000);
        List<Event> stream = fetchDataEvents(events, 3, createTables);

        assertThat(createTables).isNotEmpty();
        assertThat(snapshot)
                .containsExactlyInAnyOrder(expectedSnapshotEvents(dbName).toArray(new Event[0]));
        assertThat(stream).containsExactly(expectedStreamEvents(dbName).toArray(new Event[0]));
    }

    /**
     * Waits until the Debezium snapshot has written the expected number of {@code op:r} records to
     * the topic. The topic is drained from the beginning (never consumed), so once {@code
     * expectedCount} records are visible the snapshot is complete and the source can be started.
     */
    private static void waitForDebeziumSnapshot(
            String bootstrapServers, String topic, int expectedCount) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 180_000;
        while (System.currentTimeMillis() < deadline) {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            // the topic may not exist yet; keep the metadata lookup short per poll
            props.put("default.api.timeout.ms", "5000");
            List<ConsumerRecord<byte[], byte[]>> records =
                    KafkaUtil.drainAllRecordsFromTopic(topic, props);
            LOG.info(
                    "Debezium snapshot: topic {} has {}/{} records.",
                    topic,
                    records.size(),
                    expectedCount);
            if (records.size() >= expectedCount) {
                return;
            }
            Thread.sleep(1_000);
        }
        throw new IllegalStateException(
                "Debezium did not finish the snapshot (expected "
                        + expectedCount
                        + " records on "
                        + topic
                        + ") within 180s");
    }
}
