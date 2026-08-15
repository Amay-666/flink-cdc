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

package org.apache.flink.cdc.connectors.kafkajson.infra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.stream.Stream;

/**
 * A minimal TiDB cluster (PD + TiKV + TiDB) built from the official {@code pingcap} v8.5.1 images,
 * adapted from {@code TiDBTestBase} of the tidb-cdc connector.
 *
 * <p>Unlike the tidb-cdc base, no host port is mapped for PD/TiKV: they only need to be reachable
 * from inside the docker network. Only TiDB's 4000 port is exposed so the Flink MiniCluster (on the
 * host JVM) can reach it over the MySQL-compatible wire protocol, which is exactly the path the
 * {@code scan.database.type=tidb} alias reuses. The {@code DnsCacheManipulator} hack of the
 * original is not needed here: nothing on the host resolves the internal aliases.
 */
public class TiDBCluster {

    private static final Logger LOG = LoggerFactory.getLogger(TiDBCluster.class);

    public static final String PD_SERVICE_NAME = "pd0";
    public static final String TIKV_SERVICE_NAME = "tikv0";
    public static final String TIDB_SERVICE_NAME = "tidb0";

    public static final String TIDB_USER = "root";
    public static final String TIDB_PASSWORD = "";

    private static final String PD_IMAGE = "pingcap/pd:v8.5.1";
    private static final String TIKV_IMAGE = "pingcap/tikv:v8.5.1";
    private static final String TIDB_IMAGE = "pingcap/tidb:v8.5.1";

    private static final int TIDB_PORT = 4000;

    private final GenericContainer<?> pd;
    private final GenericContainer<?> tikv;
    private final GenericContainer<?> tidb;

    public TiDBCluster(Network network, Logger logger) {
        pd =
                new GenericContainer<>(PD_IMAGE)
                        .withCommand(
                                "--name=pd0",
                                "--client-urls=http://0.0.0.0:2379",
                                "--peer-urls=http://0.0.0.0:2380",
                                "--advertise-client-urls=http://pd0:2379",
                                "--advertise-peer-urls=http://pd0:2380",
                                "--initial-cluster=pd0=http://pd0:2380",
                                "--data-dir=/data/pd0",
                                "--log-level=error")
                        .withNetwork(network)
                        .withNetworkAliases(PD_SERVICE_NAME)
                        .withStartupTimeout(Duration.ofSeconds(120))
                        .withLogConsumer(new Slf4jLogConsumer(logger));
        tikv =
                new GenericContainer<>(TIKV_IMAGE)
                        .withCommand(
                                "--addr=0.0.0.0:20160",
                                "--advertise-addr=tikv0:20160",
                                "--data-dir=/data/tikv0",
                                "--pd=pd0:2379",
                                "--log-level=error")
                        .withNetwork(network)
                        .dependsOn(pd)
                        .withNetworkAliases(TIKV_SERVICE_NAME)
                        .withStartupTimeout(Duration.ofSeconds(120))
                        .withLogConsumer(new Slf4jLogConsumer(logger));
        tidb =
                new GenericContainer<>(TIDB_IMAGE)
                        .withExposedPorts(TIDB_PORT)
                        .withCommand(
                                "--store=tikv",
                                "--path=pd0:2379",
                                "--advertise-address=tidb0",
                                "-L",
                                "error")
                        .withNetwork(network)
                        .dependsOn(tikv)
                        .withNetworkAliases(TIDB_SERVICE_NAME)
                        .withStartupTimeout(Duration.ofSeconds(120))
                        .withLogConsumer(new Slf4jLogConsumer(logger));
    }

    public void start() {
        LOG.info("Starting TiDB cluster (pd/tikv/tidb v8.5.1)...");
        Startables.deepStart(Stream.of(pd, tikv, tidb)).join();
        waitForJdbcReady();
        LOG.info("TiDB cluster is started.");
    }

    /**
     * TiDB is MySQL-protocol compatible, but its engine takes a moment before it actually accepts
     * queries after the container reports ready. Polls {@code SELECT 1} until it succeeds.
     */
    private void waitForJdbcReady() {
        long deadline = System.currentTimeMillis() + 90_000;
        SQLException lastError = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                try (Connection connection = getJdbcConnection("");
                        Statement statement = connection.createStatement()) {
                    statement.execute("SELECT 1");
                }
                LOG.info("TiDB is ready to accept queries.");
                return;
            } catch (SQLException e) {
                lastError = e;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for TiDB.", ie);
                }
            }
        }
        throw new IllegalStateException(
                "TiDB did not accept queries within 90s: " + lastError, lastError);
    }

    public void stop() {
        Stream.of(tidb, tikv, pd).forEach(GenericContainer::stop);
    }

    /** Host address of TiDB as seen from the Flink MiniCluster (host JVM). */
    public String getHost() {
        return tidb.getHost();
    }

    /** Host-mapped port of TiDB's MySQL-protocol listener. */
    public int getMappedPort() {
        return tidb.getMappedPort(TIDB_PORT);
    }

    // sslMode=DISABLED: same rationale as the connector's pool URL — avoid cert-validity failures
    // when the WSL2 clock drifts between container boot and connection. allowPublicKeyRetrieval is
    // harmless here (TiDB's root uses the native password plugin) but needed if a caching_sha2
    // account is ever used.
    public String getJdbcUrl() {
        return "jdbc:mysql://" + getHost() + ":" + getMappedPort();
    }

    public String getJdbcUrl(String databaseName) {
        return "jdbc:mysql://"
                + getHost()
                + ":"
                + getMappedPort()
                + "/"
                + databaseName
                + "?sslMode=DISABLED&allowPublicKeyRetrieval=true";
    }

    public Connection getJdbcConnection(String databaseName) throws SQLException {
        return DriverManager.getConnection(getJdbcUrl(databaseName), TIDB_USER, TIDB_PASSWORD);
    }

    /** Executes a single SQL statement against TiDB as root. */
    public void execute(String sql) throws SQLException {
        try (Connection connection = getJdbcConnection("");
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
