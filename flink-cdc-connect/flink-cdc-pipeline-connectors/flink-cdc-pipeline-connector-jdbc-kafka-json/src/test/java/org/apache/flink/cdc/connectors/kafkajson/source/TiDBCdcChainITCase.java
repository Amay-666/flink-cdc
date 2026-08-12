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
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions;
import org.apache.flink.cdc.connectors.kafkajson.testutils.KafkaJsonSourceTestBase;
import org.apache.flink.cdc.connectors.kafkajson.testutils.KafkaUtil;
import org.apache.flink.cdc.connectors.kafkajson.testutils.TiCDCServer;
import org.apache.flink.cdc.connectors.kafkajson.testutils.TiDBCluster;
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
 * Real end-to-end TiDB + TiCDC chain: a real {@code pingcap/ticdc:v8.5.1} captures the TiDB
 * cluster's row changes and writes them to Kafka in the {@code canal-json} protocol, and the
 * connector snapshots the table (over the MySQL wire protocol, {@code scan.database.type=tidb}) and
 * then consumes the incremental changes.
 *
 * <p>This is the empirical test for the open compatibility risk: whether TiCDC's {@code canal-json}
 * envelope matches the parser's contract. The snapshot half is validated by {@link
 * TiDBSnapshotSimulatedITCase}; here the focus is the live TiCDC stream. DML is executed only after
 * the snapshot phase has finished, so the incremental messages survive the exactly-once boundary.
 *
 * <p>Note on the UPDATE before image: canal's {@code old} carries only the changed columns, while
 * TiCDC may carry the full row. The assertion below pins what must hold in either case (the changed
 * column is present) and lets the first run decide whether the connector reproduces the partial or
 * the full form.
 */
public class TiDBCdcChainITCase extends KafkaJsonSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TiDBCdcChainITCase.class);

    protected static final TiDBCluster TIDB = new TiDBCluster(NETWORK, LOG);
    protected static final KafkaContainer KAFKA = KafkaUtil.createKafkaContainer(LOG, NETWORK);
    protected static final TiCDCServer TICDC = new TiCDCServer(NETWORK, LOG);

    @BeforeClass
    public static void startContainers() {
        checkDockerAvailable();
        LOG.info("Starting TiDB cluster, TiCDC and Kafka...");
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

    @Test(timeout = 420_000)
    public void testRealTiCDCChain() throws Exception {
        String dbName = "tidb_cdc_" + UUID.randomUUID().toString().replace("-", "");
        initDatabase(dbName);

        String topic = "ticdc-canal-" + UUID.randomUUID();
        String bootstrapServers = KAFKA.getBootstrapServers();

        // 1) TiCDC tails the cluster and writes canal-json to the topic; wait until the changefeed
        //    reports normal before starting the source. The changefeed sinks to the in-network
        //    broker (kafka:9092), while the source below consumes from the host-mapped bootstrap.
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
                        "customers",
                        bootstrapServers,
                        topic,
                        KafkaJsonSourceOptions.DatabaseType.TIDB);
        CloseableIterator<Event> events = runSource(configFactory, env);

        List<CreateTableEvent> createTables = new ArrayList<>();

        // 2) snapshot: the 4 pre-existing rows
        List<Event> snapshot = fetchDataEvents(events, 4, createTables);

        // Let the snapshot phase finish (high-watermark capture + stream split assignment).
        Thread.sleep(5_000);

        // 3) incremental DML in TiDB -> TiCDC -> Kafka -> connector
        try (Connection connection = TIDB.getJdbcConnection(dbName);
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "INSERT INTO `customers` (id, name, address) VALUES (105, 'user_5', 'Chengdu')");
            statement.execute(
                    "UPDATE `customers` SET address='Hangzhou' WHERE id=101");
            statement.execute("DELETE FROM `customers` WHERE id=102");
        }
        List<Event> stream = fetchDataEvents(events, 3, createTables);

        assertThat(createTables).isNotEmpty();
        assertThat(snapshot)
                .containsExactlyInAnyOrder(expectedSnapshotEvents(dbName).toArray(new Event[0]));

        // 4) stream: INSERT 105, UPDATE 101, DELETE 102, in order.
        assertThat(stream).hasSize(3);
        TableId tableId = TableId.tableId(dbName, "customers");
        BinaryRecordDataGenerator generator = snapshotRowGenerator();

        // INSERT 105
        assertThat(stream.get(0))
                .isEqualTo(
                        DataChangeEvent.insertEvent(
                                tableId,
                                generator.generate(
                                        new Object[] {
                                            105,
                                            BinaryStringData.fromString("user_5"),
                                            BinaryStringData.fromString("Chengdu")
                                        })));

        // UPDATE 101 -> address: Shanghai -> Hangzhou. The changed column must be in the before
        // image; whether id/name are also present (full-row old) or null (canal partial old) is
        // settled by the actual TiCDC output on the first run.
        assertThat(stream.get(1)).isInstanceOf(DataChangeEvent.class);
        DataChangeEvent update = (DataChangeEvent) stream.get(1);
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
        assertThat(update.before().getString(2)).isEqualTo(BinaryStringData.fromString("Shanghai"));

        // DELETE 102
        assertThat(stream.get(2))
                .isEqualTo(
                        DataChangeEvent.deleteEvent(
                                tableId,
                                generator.generate(
                                        new Object[] {
                                            102,
                                            BinaryStringData.fromString("user_2"),
                                            BinaryStringData.fromString("Beijing")
                                        })));
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
