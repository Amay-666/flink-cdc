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
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.infra.CanalServerContainer;
import org.apache.flink.cdc.connectors.kafkajson.infra.KafkaJsonSourceTestBase;
import org.apache.flink.cdc.connectors.kafkajson.infra.KafkaUtil;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
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
 * Real end-to-end MySQL + canal-server chain, {@code RENAME TABLE} leg: a table renamed while the
 * job runs must keep delivering its changes under the new name.
 *
 * <p>This is the regression test for the bug the copied stream fetcher fixes: the emission rule
 * classifies a record against the finished snapshot splits of its table, and those keep the
 * pre-rename name, so every change of a renamed table used to be dropped from the rename on — a
 * loss that a restart could not repair, because the state it is decided from is rebuilt from the
 * checkpoint.
 *
 * <p>Both halves of the fix are exercised: the data keeps flowing with the schema-change switch
 * <em>off</em> (the default, where the connector's own state must still learn the new name), and
 * with it <em>on</em> (where the DDL additionally reaches downstream as a {@link
 * RenameTableEvent}).
 */
public class MySqlCanalRenameITCase extends KafkaJsonSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlCanalRenameITCase.class);

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

    /**
     * The production default: {@code include.schema.changes=false}. The DDL never reaches
     * downstream, and the table's changes must still arrive under the new name — the state the
     * emission rule consults has to learn the new name from the connector's own bookkeeping.
     */
    @Test(timeout = 300_000)
    public void testRenamedTableKeepsEmittingWithoutSchemaChanges() throws Exception {
        runRenameScenario(false);
    }

    /** With the switch on, the same flow plus the {@link RenameTableEvent} downstream. */
    @Test(timeout = 300_000)
    public void testRenamedTableKeepsEmittingWithSchemaChanges() throws Exception {
        runRenameScenario(true);
    }

    private void runRenameScenario(boolean includeSchemaChanges) throws Exception {
        UniqueDatabase database = new UniqueDatabase(MYSQL8, "customers", TEST_USER, TEST_PASSWORD);
        database.createAndInitialize();
        String dbName = database.getDatabaseName();
        String topic = "canal-rename-" + UUID.randomUUID();

        CanalServerContainer canal = new CanalServerContainer(dbName, topic, NETWORK, LOG);
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
                                dbName,
                                // Only the name the table has when the job is configured. Listing
                                // the names it is renamed to would hide the very bug this test is
                                // for: the emission rule admits a table that is in the filter but
                                // that no finished snapshot split covers, on the grounds that it is
                                // newly added, so a renamed table whose new name is in the filter
                                // never reaches the rule that used to drop it
                                "customers",
                                KAFKA.getBootstrapServers(),
                                topic)
                        .includeSchemaChanges(includeSchemaChanges);
        CloseableIterator<Event> events = runSource(configFactory, env);

        try {
            List<CreateTableEvent> createTables = new ArrayList<>();

            // 1) snapshot: the 4 pre-existing rows of `customers`
            assertThat(fetchDataEvents(events, 4, createTables)).hasSize(4);
            // let the snapshot phase finish (high-watermark capture + stream split assignment)
            Thread.sleep(5_000);

            // 2) the rename itself
            execute(
                    database,
                    String.format(
                            "RENAME TABLE `%s`.`customers` TO `%s`.`vip_customers`",
                            dbName, dbName));
            if (includeSchemaChanges) {
                assertRenameEvent(
                        nextNonCreateTableEvent(events, createTables),
                        dbName,
                        "customers",
                        "vip_customers");
            }

            // 3) DML on the renamed table: this is what the bug dropped entirely
            TableId renamed = TableId.tableId(dbName, "vip_customers");
            execute(
                    database,
                    String.format(
                            "INSERT INTO `%s`.`vip_customers` (id, name, address)"
                                    + " VALUES (105, 'user_5', 'Chengdu')",
                            dbName));
            execute(
                    database,
                    String.format(
                            "UPDATE `%s`.`vip_customers` SET address='Hangzhou' WHERE id=101",
                            dbName));
            execute(
                    database,
                    String.format("DELETE FROM `%s`.`vip_customers` WHERE id=102", dbName));

            BinaryRecordDataGenerator generator = snapshotRowGenerator();
            List<Event> stream = fetchDataEvents(events, 3, createTables);
            if (!includeSchemaChanges) {
                // the DDL must not be forwarded: the first thing downstream sees after the rename
                // is the data change of the renamed table
                assertThat(stream.get(0)).isInstanceOf(DataChangeEvent.class);
            }
            assertThat(stream)
                    .containsExactly(
                            DataChangeEvent.insertEvent(
                                    renamed,
                                    generator.generate(
                                            new Object[] {
                                                105,
                                                BinaryStringData.fromString("user_5"),
                                                BinaryStringData.fromString("Chengdu")
                                            })),
                            DataChangeEvent.updateEvent(
                                    renamed,
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
                                    renamed,
                                    generator.generate(
                                            new Object[] {
                                                102,
                                                BinaryStringData.fromString("user_2"),
                                                BinaryStringData.fromString("Beijing")
                                            })));

            // 4) a second rename of the same table, to show the bookkeeping follows each DDL
            execute(
                    database,
                    String.format(
                            "RENAME TABLE `%s`.`vip_customers` TO `%s`.`orders_archive`",
                            dbName, dbName));
            if (includeSchemaChanges) {
                assertRenameEvent(
                        nextNonCreateTableEvent(events, createTables),
                        dbName,
                        "vip_customers",
                        "orders_archive");
            }
            execute(
                    database,
                    String.format(
                            "INSERT INTO `%s`.`orders_archive` (id, name, address)"
                                    + " VALUES (106, 'user_6', 'Wuhan')",
                            dbName));
            List<Event> afterSecondRename = fetchDataEvents(events, 1, createTables);
            assertThat(afterSecondRename)
                    .containsExactly(
                            DataChangeEvent.insertEvent(
                                    TableId.tableId(dbName, "orders_archive"),
                                    generator.generate(
                                            new Object[] {
                                                106,
                                                BinaryStringData.fromString("user_6"),
                                                BinaryStringData.fromString("Wuhan")
                                            })));
        } finally {
            events.close();
            canal.stop();
        }
    }

    private static void assertRenameEvent(
            Event event, String dbName, String oldTable, String newTable) {
        assertThat(event).isInstanceOf(RenameTableEvent.class);
        RenameTableEvent rename = (RenameTableEvent) event;
        assertThat(rename.getOldTableId()).isEqualTo(TableId.tableId(dbName, oldTable));
        assertThat(rename.getNewTableId()).isEqualTo(TableId.tableId(dbName, newTable));
    }

    /**
     * Returns the next event that is not a {@link CreateTableEvent} (the connector decides on its
     * own how many of those a table produces, so tests skip them rather than count them).
     */
    private static Event nextNonCreateTableEvent(
            CloseableIterator<Event> events, List<CreateTableEvent> createTableSink) {
        while (events.hasNext()) {
            Event event = events.next();
            if (event instanceof CreateTableEvent) {
                createTableSink.add((CreateTableEvent) event);
            } else {
                return event;
            }
        }
        throw new IllegalStateException("The event stream ended before the expected event.");
    }

    /** Runs one statement against the source database as the test user. */
    private static void execute(UniqueDatabase database, String sql) throws Exception {
        try (Connection connection = database.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
