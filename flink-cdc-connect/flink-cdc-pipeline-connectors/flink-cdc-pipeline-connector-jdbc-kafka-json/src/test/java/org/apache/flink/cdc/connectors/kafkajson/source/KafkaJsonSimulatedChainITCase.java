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
import org.apache.flink.cdc.connectors.kafkajson.testutils.KafkaJsonSourceTestBase;
import org.apache.flink.cdc.connectors.kafkajson.testutils.KafkaUtil;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
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
 * Simulated-producer baseline for the MySQL chain: a real MySQL snapshot is read by the source, then
 * canal flatMessage JSON (INSERT/UPDATE/DELETE) is written to Kafka by a test-side producer and
 * consumed as the incremental stream.
 *
 * <p>This is the deterministic reference for the {@link MySqlCanalChainITCase}: it exercises the
 * same wire format and the same snapshot-to-stream transition, without depending on canal-server's
 * startup timing. Messages are produced only <em>after</em> the snapshot events have been collected,
 * so every message survives the exactly-once boundary (empty topic at snapshot start → high
 * watermark {@code -1}).
 */
public class KafkaJsonSimulatedChainITCase extends KafkaJsonSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonSimulatedChainITCase.class);

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

    @Test(timeout = 240_000)
    public void testSimulatedCanalMessages() throws Exception {
        UniqueDatabase database = new UniqueDatabase(MYSQL8, "customers", TEST_USER, TEST_PASSWORD);
        database.createAndInitialize();
        String topic = "canal-simulated-" + UUID.randomUUID();
        String bootstrapServers = KAFKA.getBootstrapServers();

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
                        topic);
        CloseableIterator<Event> events = runSource(configFactory, env);

        List<CreateTableEvent> createTables = new ArrayList<>();

        // 1) snapshot: the 4 pre-existing rows
        List<Event> snapshot = fetchDataEvents(events, 4, createTables);

        // Let the snapshot phase finish (high-watermark capture + stream split assignment) before
        // writing, so the messages are not absorbed into the snapshot high watermark.
        Thread.sleep(5_000);

        // 2) simulated canal incremental messages, written after the snapshot
        KafkaUtil.produce(
                bootstrapServers, topic, simulatedCanalMessages(database.getDatabaseName()));

        // 3) stream: the 3 messages, in order
        List<Event> stream = fetchDataEvents(events, 3, createTables);

        assertThat(createTables).isNotEmpty();

        String dbName = database.getDatabaseName();
        assertThat(snapshot)
                .containsExactlyInAnyOrder(expectedSnapshotEvents(dbName).toArray(new Event[0]));
        assertThat(stream).containsExactly(expectedStreamEvents(dbName).toArray(new Event[0]));
    }
}
