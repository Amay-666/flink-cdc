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
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions;
import org.apache.flink.cdc.connectors.kafkajson.testutils.KafkaJsonSourceTestBase;
import org.apache.flink.cdc.connectors.kafkajson.testutils.KafkaUtil;
import org.apache.flink.cdc.connectors.kafkajson.testutils.TiDBCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.lifecycle.Startables;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simulated-producer baseline for the TiDB chain: the connector snapshots a real TiDB v8.5 cluster
 * over the MySQL wire protocol ({@code scan.database.type=tidb}) and consumes canal flatMessage
 * JSON written by a test-side producer as the incremental stream.
 *
 * <p>This empirically validates the assumption that the snapshot path (JDBC metadata discovery +
 * MySQL-compatible chunk queries) works against TiDB. The incremental half is identical to the
 * {@link KafkaJsonSimulatedChainITCase} baseline, so a green run proves the only difference between
 * the two chains — the TiDB snapshot — behaves exactly like MySQL.
 */
public class TiDBSnapshotSimulatedITCase extends KafkaJsonSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TiDBSnapshotSimulatedITCase.class);

    protected static final TiDBCluster TIDB = new TiDBCluster(NETWORK, LOG);
    protected static final KafkaContainer KAFKA = KafkaUtil.createKafkaContainer(LOG, NETWORK);

    @BeforeClass
    public static void startContainers() {
        checkDockerAvailable();
        LOG.info("Starting TiDB cluster and Kafka...");
        TIDB.start();
        Startables.deepStart(Stream.of(KAFKA)).join();
        LOG.info("Containers are started.");
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        KAFKA.stop();
        TIDB.stop();
        LOG.info("Containers are stopped.");
    }

    @Test(timeout = 300_000)
    public void testTiDBSnapshotWithSimulatedMessages() throws Exception {
        String dbName = "tidb_sim_" + UUID.randomUUID().toString().replace("-", "");
        initDatabase(dbName);

        String topic = "tidb-simulated-" + UUID.randomUUID();
        String bootstrapServers = KAFKA.getBootstrapServers();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureEnv(env);
        KafkaJsonSourceConfigFactory configFactory =
                buildConfigFactory(
                        TIDB.getHost(),
                        TIDB.getMappedPort(),
                        TiDBCluster.TIDB_USER,
                        TiDBCluster.TIDB_PASSWORD,
                        dbName,
                        "customers",
                        bootstrapServers,
                        topic,
                        KafkaJsonSourceOptions.DatabaseType.TIDB);
        CloseableIterator<Event> events = runSource(configFactory, env);

        List<CreateTableEvent> createTables = new ArrayList<>();

        // 1) snapshot: the 4 pre-existing rows
        List<Event> snapshot = fetchDataEvents(events, 4, createTables);

        // Let the snapshot phase finish (high-watermark capture + stream split assignment) before
        // writing, so the messages are not absorbed into the snapshot high watermark.
        Thread.sleep(5_000);

        // 2) simulated canal incremental messages, written after the snapshot
        KafkaUtil.produce(bootstrapServers, topic, simulatedCanalMessages(dbName));

        // 3) stream: the 3 messages, in order
        List<Event> stream = fetchDataEvents(events, 3, createTables);

        assertThat(createTables).isNotEmpty();
        assertThat(snapshot)
                .containsExactlyInAnyOrder(expectedSnapshotEvents(dbName).toArray(new Event[0]));
        assertThat(stream).containsExactly(expectedStreamEvents(dbName).toArray(new Event[0]));
    }

    /** Creates {@code customers} with the same 4 rows as the MySQL baseline, on TiDB. */
    private static void initDatabase(String dbName) throws Exception {
        TIDB.execute("CREATE DATABASE IF NOT EXISTS `" + dbName + "`");
        TIDB.execute(
                String.format(
                        "CREATE TABLE `%s`.`customers` ("
                                + "id INT NOT NULL, name VARCHAR(255) NOT NULL, "
                                + "address VARCHAR(255), PRIMARY KEY (id))",
                        dbName));
        for (Object[] row :
                new Object[][] {
                    {101, "user_1", "Shanghai"},
                    {102, "user_2", "Beijing"},
                    {103, "user_3", "Hangzhou"},
                    {104, "user_4", "Shenzhen"}
                }) {
            TIDB.execute(
                    String.format(
                            "INSERT INTO `%s`.`customers` (id, name, address) VALUES (%d, '%s', '%s')",
                            dbName, row[0], row[1], row[2]));
        }
    }
}
