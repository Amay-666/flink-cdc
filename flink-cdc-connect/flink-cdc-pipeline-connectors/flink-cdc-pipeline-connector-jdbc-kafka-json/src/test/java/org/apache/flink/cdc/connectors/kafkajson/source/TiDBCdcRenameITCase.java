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
import org.apache.flink.cdc.connectors.kafkajson.infra.KafkaJsonSourceTestBase;
import org.apache.flink.cdc.connectors.kafkajson.infra.KafkaUtil;
import org.apache.flink.cdc.connectors.kafkajson.infra.TiCDCServer;
import org.apache.flink.cdc.connectors.kafkajson.infra.TiDBCluster;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions;
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
 * Real end-to-end TiDB + TiCDC chain, {@code RENAME TABLE} leg. The same scenario as {@link
 * MySqlCanalRenameITCase} against the other supported producer, because the two disagree on the
 * message: a canal-server message announces the <em>new</em> table name and carries the statement
 * in {@code sql}, and so does TiCDC's — which is the shape the rename path parses its pairs from.
 *
 * <p>What makes this a different test from the MySQL one rather than a copy: TiCDC writes the DDL
 * asynchronously behind its own schema-version bookkeeping, and it is the producer whose table-name
 * mapping for a renamed table is known to lag its DDL, so a job that keeps emitting the table's
 * records here is exactly the property the fix has to hold for.
 */
public class TiDBCdcRenameITCase extends KafkaJsonSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TiDBCdcRenameITCase.class);

    protected static final TiDBCluster TIDB = new TiDBCluster(NETWORK, LOG);
    protected static final KafkaContainer KAFKA = KafkaUtil.createKafkaContainer(LOG, NETWORK);
    protected static final TiCDCServer TICDC = new TiCDCServer(NETWORK, LOG);

    @BeforeClass
    public static void startContainers() {
        checkDockerAvailable();
        LOG.info("Starting containers...");
        TIDB.start();
        Startables.deepStart(Stream.of(KAFKA)).join();
        TICDC.start();
        LOG.info("Containers are started.");
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        TICDC.stop();
        KAFKA.stop();
        TIDB.stop();
        LOG.info("Containers are stopped.");
    }

    /**
     * The production default: {@code include.schema.changes=false}. The DDL never reaches
     * downstream, and the table's changes must still arrive under the new name.
     */
    @Test(timeout = 420_000)
    public void testRenamedTableKeepsEmittingWithoutSchemaChanges() throws Exception {
        runRenameScenario(false);
    }

    /** With the switch on, the same flow plus the {@link RenameTableEvent} downstream. */
    @Test(timeout = 420_000)
    public void testRenamedTableKeepsEmittingWithSchemaChanges() throws Exception {
        runRenameScenario(true);
    }

    private void runRenameScenario(boolean includeSchemaChanges) throws Exception {
        String dbName = "ticdc_rename_" + UUID.randomUUID().toString().replace("-", "");
        initDatabase(dbName);
        String topic = "ticdc-rename-" + UUID.randomUUID();

        // TiCDC tails the cluster and publishes canal-json to the topic. The two test methods
        // share one TiCDC server and the changefeed id is fixed, so the previous test's
        // changefeed has to be released first: it is still writing to its own topic.
        TICDC.removeChangefeed();
        TICDC.createChangefeed(topic);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureEnv(env);
        KafkaJsonSourceConfigFactory configFactory =
                buildConfigFactory(
                                TIDB.getHost(),
                                TIDB.getMappedPort(),
                                TiDBCluster.TIDB_USER,
                                TiDBCluster.TIDB_PASSWORD,
                                dbName,
                                // only the name the table has when the job is configured:
                                // listing the names it will be renamed to would let the renamed
                                // table through on the rule that admits a newly added table,
                                // and never reach the rule this test is about
                                "customers",
                                KAFKA.getBootstrapServers(),
                                topic,
                                KafkaJsonSourceOptions.DatabaseType.TIDB)
                        .includeSchemaChanges(includeSchemaChanges);
        CloseableIterator<Event> events = runSource(configFactory, env);

        try {
            List<CreateTableEvent> createTables = new ArrayList<>();

            // 1) snapshot: the 4 pre-existing rows of `customers`
            assertThat(fetchDataEvents(events, 4, createTables)).hasSize(4);
            // let the snapshot phase finish (high-watermark capture + stream split assignment)
            Thread.sleep(5_000);

            // 2) the rename itself
            TIDB.execute(
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
            try (Connection connection = TIDB.getJdbcConnection(dbName);
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        "INSERT INTO `vip_customers` (id, name, address)"
                                + " VALUES (105, 'user_5', 'Chengdu')");
                statement.execute("UPDATE `vip_customers` SET address='Hangzhou' WHERE id=101");
                statement.execute("DELETE FROM `vip_customers` WHERE id=102");
            }

            BinaryRecordDataGenerator generator = snapshotRowGenerator();
            List<Event> stream = fetchDataEvents(events, 3, createTables);
            if (!includeSchemaChanges) {
                // the DDL must not be forwarded: the first thing downstream sees after the rename
                // is the data change of the renamed table
                assertThat(stream.get(0)).isInstanceOf(DataChangeEvent.class);
            }
            assertThat(stream).hasSize(3);
            // INSERT 105
            assertThat(stream.get(0))
                    .isEqualTo(
                            DataChangeEvent.insertEvent(
                                    renamed,
                                    generator.generate(
                                            new Object[] {
                                                105,
                                                BinaryStringData.fromString("user_5"),
                                                BinaryStringData.fromString("Chengdu")
                                            })));
            // UPDATE 101 -> address: Shanghai -> Hangzhou, on the renamed table
            assertThat(stream.get(1)).isInstanceOf(DataChangeEvent.class);
            DataChangeEvent update = (DataChangeEvent) stream.get(1);
            assertThat(update.tableId()).isEqualTo(renamed);
            assertThat(update.after())
                    .isEqualTo(
                            generator.generate(
                                    new Object[] {
                                        101,
                                        BinaryStringData.fromString("user_1"),
                                        BinaryStringData.fromString("Hangzhou")
                                    }));
            assertThat(update.before()).isNotNull();
            assertThat(update.before().isNullAt(2)).isFalse();
            assertThat(update.before().getString(2))
                    .isEqualTo(BinaryStringData.fromString("Shanghai"));
            // DELETE 102
            assertThat(stream.get(2))
                    .isEqualTo(
                            DataChangeEvent.deleteEvent(
                                    renamed,
                                    generator.generate(
                                            new Object[] {
                                                102,
                                                BinaryStringData.fromString("user_2"),
                                                BinaryStringData.fromString("Beijing")
                                            })));

            // 4) a second rename of the same table, to show the bookkeeping follows each DDL
            TIDB.execute(
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
            TIDB.execute(
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
