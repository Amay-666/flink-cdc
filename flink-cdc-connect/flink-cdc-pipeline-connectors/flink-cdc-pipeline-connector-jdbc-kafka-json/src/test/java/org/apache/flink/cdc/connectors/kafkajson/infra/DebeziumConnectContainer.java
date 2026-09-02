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
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.io.IOException;
import java.time.Duration;

/**
 * A {@code debezium/connect:1.9} Kafka Connect worker that reads the MySQL binlog and writes the
 * Debezium SourceRecord JSON to Kafka.
 *
 * <p>The fork embeds Debezium {@code 1.9.8.Final} (root pom {@code <debezium.version>}), so the 1.9
 * series image is used to keep the wire format aligned. The JsonConverter has schemas enabled, so
 * every record carries the {@code {schema, payload}} envelope that {@link
 * org.apache.flink.cdc.connectors.kafkajson.source.message.debezium.DebeziumMessageParser} unwraps
 * — the exact wire format the real chain must prove the connector consumes.
 *
 * <p>The image entrypoint maps {@code CONNECT_*}-prefixed environment variables into {@code
 * connect-distributed.properties}; {@code BOOTSTRAP_SERVERS}, {@code GROUP_ID} and the three
 * storage topics are translated by the entrypoint itself. Connectors are registered through the
 * Connect REST API (reachable at {@code localhost:8083} inside the container) with {@code curl}.
 */
public class DebeziumConnectContainer extends GenericContainer<DebeziumConnectContainer> {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumConnectContainer.class);

    public static final String IMAGE = "debezium/connect:1.9";

    private static final int REST_PORT = 8083;
    private static final int REST_READY_TIMEOUT_SECONDS = 120;
    private static final int CONNECTOR_STARTUP_TIMEOUT_SECONDS = 120;

    public DebeziumConnectContainer(Network network, Logger logger) {
        super(IMAGE);
        withNetwork(network);
        withNetworkAliases("debezium");
        withExposedPorts(REST_PORT);
        // Kafka Connect worker: bootstrap servers plus the three internal topics
        withEnv("BOOTSTRAP_SERVERS", "kafka:9092");
        withEnv("GROUP_ID", "debezium-test");
        withEnv("CONFIG_STORAGE_TOPIC", "debezium-connect-configs");
        withEnv("OFFSET_STORAGE_TOPIC", "debezium-connect-offsets");
        withEnv("STATUS_STORAGE_TOPIC", "debezium-connect-status");
        // JsonConverter with schemas enabled -> every record is a {schema, payload} envelope,
        // exactly
        // the wire format the connector parses.
        withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "true");
        withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "true");
        withStartupTimeout(Duration.ofSeconds(120));
        withLogConsumer(new Slf4jLogConsumer(logger));
    }

    /**
     * Registers a MySQL source connector and waits until it reports {@code RUNNING}. The connector
     * then snapshots the {@code databaseName} tables matching {@code tablePattern} (one {@code
     * op:r} record per row) and tails the binlog for subsequent changes.
     *
     * <p>{@code topicPrefix} is the {@code database.server.name} of Debezium 1.9 (the {@code
     * topic.prefix} key only exists in 2.x), so records land on {@code
     * topicPrefix.databaseName.tableName}.
     */
    public void createMySqlConnector(
            String connectorName,
            String mysqlHost,
            int mysqlPort,
            String mysqlUser,
            String mysqlPassword,
            String databaseName,
            String tablePattern,
            String topicPrefix)
            throws IOException, InterruptedException {
        waitUntilRestReady();
        String payload =
                String.format(
                        "{"
                                + "\"name\":\"%s\","
                                + "\"config\":{"
                                + "\"connector.class\":\"io.debezium.connector.mysql.MySqlConnector\","
                                + "\"database.hostname\":\"%s\","
                                + "\"database.port\":\"%d\","
                                + "\"database.user\":\"%s\","
                                + "\"database.password\":\"%s\","
                                + "\"database.server.name\":\"%s\","
                                + "\"database.include.list\":\"%s\","
                                + "\"table.include.list\":\"%s\","
                                + "\"database.history.kafka.bootstrap.servers\":\"kafka:9092\","
                                + "\"database.history.kafka.topic\":\"dbhistory\","
                                + "\"snapshot.mode\":\"initial\","
                                + "\"tombstones.on.delete\":\"false\""
                                + "}}",
                        connectorName,
                        mysqlHost,
                        mysqlPort,
                        mysqlUser,
                        mysqlPassword,
                        topicPrefix,
                        databaseName,
                        databaseName + "." + tablePattern);
        LOG.info(
                "Registering Debezium connector {} -> topic prefix {}.",
                connectorName,
                topicPrefix);
        Container.ExecResult result =
                execInContainer(
                        "curl",
                        "-s",
                        "-X",
                        "POST",
                        "http://localhost:" + REST_PORT + "/connectors",
                        "-H",
                        "Content-Type: application/json",
                        "-d",
                        payload);
        if (result.getExitCode() != 0
                || result.getStdout() == null
                || result.getStdout().contains("error_code")) {
            throw new IllegalStateException(
                    "Failed to register Debezium connector "
                            + connectorName
                            + ": "
                            + result.getStdout()
                            + " "
                            + result.getStderr()
                            + "\nlogs:\n"
                            + getLogs());
        }
        waitUntilConnectorRunning(connectorName);
    }

    /**
     * Waits until the Connect REST API answers and the MySQL connector plugin is loaded, so the
     * connector registration does not race the worker startup.
     */
    private void waitUntilRestReady() throws IOException, InterruptedException {
        long deadline = System.currentTimeMillis() + REST_READY_TIMEOUT_SECONDS * 1000L;
        while (System.currentTimeMillis() < deadline) {
            Container.ExecResult result =
                    execInContainer(
                            "curl", "-s", "http://localhost:" + REST_PORT + "/connector-plugins");
            if (result.getExitCode() == 0
                    && result.getStdout() != null
                    && result.getStdout().contains("io.debezium.connector.mysql.MySqlConnector")) {
                LOG.info("Debezium Connect REST API is ready.");
                return;
            }
            Thread.sleep(1_000);
        }
        throw new IllegalStateException(
                "Debezium Connect REST API did not become ready within "
                        + REST_READY_TIMEOUT_SECONDS
                        + "s\nlogs:\n"
                        + getLogs());
    }

    private void waitUntilConnectorRunning(String connectorName)
            throws IOException, InterruptedException {
        long deadline = System.currentTimeMillis() + CONNECTOR_STARTUP_TIMEOUT_SECONDS * 1000L;
        while (System.currentTimeMillis() < deadline) {
            Container.ExecResult result =
                    execInContainer(
                            "curl",
                            "-s",
                            "http://localhost:"
                                    + REST_PORT
                                    + "/connectors/"
                                    + connectorName
                                    + "/status");
            String status = result.getStdout();
            if (status != null && status.contains("\"state\":\"RUNNING\"")) {
                LOG.info("Debezium connector {} is RUNNING.", connectorName);
                return;
            }
            if (status != null && status.contains("\"state\":\"FAILED\"")) {
                throw new IllegalStateException(
                        "Debezium connector "
                                + connectorName
                                + " FAILED: "
                                + status
                                + "\nlogs:\n"
                                + getLogs());
            }
            Thread.sleep(1_000);
        }
        throw new IllegalStateException(
                "Debezium connector "
                        + connectorName
                        + " did not reach RUNNING within "
                        + CONNECTOR_STARTUP_TIMEOUT_SECONDS
                        + "s\nlogs:\n"
                        + getLogs());
    }
}
