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
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.kafkajson.infra.CanalServerContainer;
import org.apache.flink.cdc.connectors.kafkajson.infra.KafkaJsonSourceTestBase;
import org.apache.flink.cdc.connectors.kafkajson.infra.KafkaUtil;
import org.apache.flink.cdc.connectors.kafkajson.reconcile.CustomerRow;
import org.apache.flink.cdc.connectors.kafkajson.reconcile.EventArchive;
import org.apache.flink.cdc.connectors.kafkajson.reconcile.EventCollector;
import org.apache.flink.cdc.connectors.kafkajson.reconcile.Ledger;
import org.apache.flink.cdc.connectors.kafkajson.reconcile.LedgerVerifier;
import org.apache.flink.cdc.connectors.kafkajson.reconcile.LedgerVerifier.Violation;
import org.apache.flink.cdc.connectors.kafkajson.reconcile.WorkloadDriver;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
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
 * End-to-end "no duplicate, no loss" verification over a real MySQL + canal-server + Kafka chain,
 * using real binlog-produced data.
 *
 * <p>Flow: the {@link WorkloadDriver} seeds 50 rows (pre-snapshot baseline) and records them in the
 * ground-truth {@link Ledger}; the connector snapshots them. After the snapshot phase has finished,
 * the driver executes 30 random DML operations through the real canal chain, recording each in the
 * ledger. The connector's output is archived per primary key ({@link EventArchive}) and reconciled
 * against the ledger by {@link LedgerVerifier}:
 *
 * <ul>
 *   <li>each post-snapshot operation must be emitted exactly once, in order, with identical values;
 *   <li>replaying the events must converge to the source table's actual final state.
 * </ul>
 *
 * <p>A violation in any of these is a duplicate, a loss, or a value error.
 */
public class KafkaJsonExactlyOnceITCase extends KafkaJsonSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonExactlyOnceITCase.class);

    /** Number of pre-snapshot rows; one snapshot split on this table. */
    private static final int INITIAL_ROWS = 50;

    /** Random DML operations executed after the snapshot phase (40/40/20 insert/update/delete). */
    private static final int POST_OPS = 30;

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
    public void testExactlyOnceWithRealCanalChain() throws Exception {
        String dbName = "exactly_once_" + UUID.randomUUID().toString().replace("-", "");
        String topic = "canal-exactly-once-" + UUID.randomUUID();
        long seed = System.currentTimeMillis();

        Ledger ledger = new Ledger();
        // Connect to the MySQL server without a database; initSchema() creates the unique one.
        String jdbcUrl = MYSQL8.getJdbcUrl("");

        CanalServerContainer canal = null;
        try (WorkloadDriver driver =
                new WorkloadDriver(
                        jdbcUrl, TEST_USER, TEST_PASSWORD, dbName, "customers", ledger, seed)) {
            driver.initSchema();
            driver.insertInitialRows(INITIAL_ROWS);

            canal = new CanalServerContainer(dbName, topic, NETWORK, LOG);
            canal.start();
            canal.waitUntilStarted();
            // let the instance finish registering as a slave before any DML matters
            Thread.sleep(3_000);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            configureEnv(env);
            KafkaJsonSourceConfigFactory configFactory =
                    buildConfigFactory(
                            MYSQL8.getHost(),
                            MYSQL8.getDatabasePort(),
                            TEST_USER,
                            TEST_PASSWORD,
                            dbName,
                            "customers",
                            KAFKA.getBootstrapServers(),
                            topic);
            CloseableIterator<Event> events = runSource(configFactory, env);

            EventCollector collector = new EventCollector(events);
            Thread collectorThread = new Thread(collector, "event-collector");
            collectorThread.start();

            try {
                EventArchive archive = new EventArchive();
                List<CreateTableEvent> createTables = new ArrayList<>();

                // 1) snapshot phase: exactly the INITIAL_ROWS pre-seeded rows
                int snapshotSeen = 0;
                long snapshotDeadline = System.currentTimeMillis() + 120_000;
                while (snapshotSeen < INITIAL_ROWS
                        && System.currentTimeMillis() < snapshotDeadline) {
                    Event event = pollOrFail(collector, 5_000);
                    if (event instanceof CreateTableEvent) {
                        createTables.add((CreateTableEvent) event);
                    } else if (event instanceof DataChangeEvent) {
                        archive.add((DataChangeEvent) event);
                        snapshotSeen++;
                    }
                }
                assertThat(snapshotSeen)
                        .as("snapshot phase must deliver exactly the pre-seeded rows")
                        .isEqualTo(INITIAL_ROWS);
                assertThat(createTables).isNotEmpty();

                // let the snapshot split complete (HIGH capture + stream split assignment) so that
                // writes from now on are emitted as dedicated stream events
                Thread.sleep(5_000);

                // 2) post-snapshot DML through the real canal chain
                driver.runRandomDml(POST_OPS);

                // 3) drain until the stream has been quiet for a while
                drainUntilQuiet(collector, archive, 120_000, 8_000);

                // 4) reconcile ground truth against what the connector emitted
                List<CustomerRow> sourceFinal = driver.querySourceFinalState();
                List<Violation> violations =
                        LedgerVerifier.verify(ledger, archive, driver.expectedModel(), sourceFinal);
                LOG.info(
                        "ledger entries={}, archived events={}, expected final rows={}, source final rows={}",
                        ledger.size(),
                        archive.size(),
                        driver.expectedModel().size(),
                        sourceFinal.size());
                LOG.info("violations: {}", violations);
                assertThat(violations)
                        .as("no duplicate / no loss / converged final state")
                        .isEmpty();
            } finally {
                events.close();
                collectorThread.join(10_000);
            }
        } finally {
            if (canal != null) {
                canal.stop();
            }
        }
    }

    /** Polls one event, surfacing a job failure as a test failure instead of a silent timeout. */
    private static Event pollOrFail(EventCollector collector, long timeoutMs)
            throws InterruptedException {
        Event event = collector.poll(timeoutMs);
        if (event == null && collector.failure() != null) {
            throw new IllegalStateException(
                    "Connector job failed while collecting events.", collector.failure());
        }
        return event;
    }

    /**
     * Drains the collector into the archive until no event arrives for {@code quietMs} or {@code
     * maxWaitMs} elapses, whichever comes first.
     */
    private static void drainUntilQuiet(
            EventCollector collector, EventArchive archive, long maxWaitMs, long quietMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        long lastEventAt = System.currentTimeMillis();
        while (System.currentTimeMillis() < deadline) {
            Event event = collector.poll(1_000);
            if (event == null) {
                if (collector.failure() != null) {
                    throw new IllegalStateException(
                            "Connector job failed while draining.", collector.failure());
                }
                if (System.currentTimeMillis() - lastEventAt > quietMs) {
                    LOG.info("Stream quiet for {} ms; drain complete.", quietMs);
                    return;
                }
                continue;
            }
            lastEventAt = System.currentTimeMillis();
            if (event instanceof CreateTableEvent) {
                LOG.info("Late CreateTableEvent during drain: {}", event);
            } else if (event instanceof DataChangeEvent) {
                archive.add((DataChangeEvent) event);
            }
        }
        LOG.warn(
                "Drain hit maxWaitMs={} without going quiet; continuing with what was collected.",
                maxWaitMs);
    }
}
