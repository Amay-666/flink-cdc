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
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.time.Duration;

/**
 * A {@code pingcap/ticdc:v8.5.1} container that captures the TiDB cluster's row changes and writes
 * them to Kafka in the {@code canal-json} protocol — the exact wire format {@link
 * org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonSource} is designed to parse.
 *
 * <p>This is the empirical test for the open compatibility risk: if TiCDC's {@code canal-json}
 * envelope does not match the parser's contract, the {@code TiDBCdcChainITCase} fails loudly here
 * instead of being silently ignored.
 *
 * <p>The container joins the test network as {@code ticdc}, reaches PD as {@code pd0:2379} and
 * Kafka as {@code kafka:9092} via docker network aliases. Changefeeds are managed through the
 * {@code cdc cli} binary that ships in the image, invoked over {@code execInContainer}.
 */
public class TiCDCServer {

    private static final Logger LOG = LoggerFactory.getLogger(TiCDCServer.class);

    public static final String IMAGE = "pingcap/ticdc:v8.5.1";
    public static final String SERVER_ALIAS = "ticdc";

    private static final int SERVER_PORT = 8300;
    private static final String CHANGEFEED_ID = "kafka-json-cdc";

    private final GenericContainer<?> container;

    public TiCDCServer(Network network, Logger logger) {
        container =
                new GenericContainer<>(IMAGE)
                        .withCommand(
                                "/cdc",
                                "server",
                                "--addr=0.0.0.0:" + SERVER_PORT,
                                // TiCDC requires a concrete IP (it rejects 0.0.0.0 and hostnames).
                                // 127.0.0.1 is sufficient: the only client is the in-container cli
                                // (see runCli), and in this single-capture setup nothing outside
                                // the
                                // container needs to reach the advertise address.
                                "--advertise-addr=127.0.0.1:" + SERVER_PORT,
                                "--pd=http://pd0:2379",
                                "--log-level=error")
                        .withNetwork(network)
                        .withNetworkAliases(SERVER_ALIAS)
                        .withStartupTimeout(Duration.ofSeconds(120))
                        .withLogConsumer(new Slf4jLogConsumer(logger));
    }

    public void start() {
        LOG.info("Starting TiCDC server (v8.5.1)...");
        container.start();
        LOG.info("TiCDC server is started.");
    }

    public void stop() {
        container.stop();
    }

    public GenericContainer<?> getContainer() {
        return container;
    }

    /**
     * Waits until the TiCDC server's HTTP API answers, then creates a changefeed that streams the
     * TiDB cluster's changes to the given Kafka topic as {@code canal-json} and waits until the
     * changefeed reports a {@code normal} state.
     *
     * <p>The sink must point at the in-network broker address ({@code kafka:9092}, the alias + the
     * {@code BROKER} listener of the testcontainers Kafka container), <em>not</em> the host-mapped
     * bootstrap: TiCDC runs inside the docker network and cannot reach {@code localhost} ports.
     */
    public void createChangefeed(String topic) throws Exception {
        waitForServerReady();
        // enable-tidb-extension=true makes TiCDC also emit TIDB_WATERMARK marker events (plus the
        // _tidb field on DML); the connector must drop them so the chain exercises the watermark
        // filter end to end.
        String sinkUri =
                "kafka://kafka:9092/"
                        + topic
                        + "?protocol=canal-json&kafka-version=2.6.0"
                        + "&replication-factor=1&max-message-bytes=10485760&partition-num=1"
                        + "&enable-tidb-extension=true";
        ExecResult result =
                runCli(
                        "changefeed create --server=http://127.0.0.1:"
                                + SERVER_PORT
                                + " --sink-uri=\""
                                + sinkUri
                                + "\" --changefeed-id="
                                + CHANGEFEED_ID);
        LOG.info(
                "changefeed create exit={} stdout={} stderr={}",
                result.getExitCode(),
                result.getStdout(),
                result.getStderr());
        if (result.getExitCode() != 0) {
            throw new IllegalStateException(
                    "Failed to create changefeed: " + result.getStdout() + result.getStderr());
        }
        waitForChangefeedNormal();
        LOG.info("changefeed {} is normal; TiCDC writes {} as canal-json.", CHANGEFEED_ID, topic);
    }

    private void waitForServerReady() throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            ExecResult result = runCli("changefeed list --server=http://127.0.0.1:" + SERVER_PORT);
            if (result.getExitCode() == 0) {
                return;
            }
            Thread.sleep(1000);
        }
        throw new IllegalStateException(
                "TiCDC server did not answer within 60s: " + container.getLogs());
    }

    private void waitForChangefeedNormal() throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            ExecResult result =
                    runCli(
                            "changefeed query --server=http://127.0.0.1:"
                                    + SERVER_PORT
                                    + " --changefeed-id="
                                    + CHANGEFEED_ID);
            if (result.getExitCode() == 0 && result.getStdout().contains("\"state\": \"normal\"")) {
                return;
            }
            Thread.sleep(1000);
        }
        throw new IllegalStateException(
                "changefeed " + CHANGEFEED_ID + " did not become normal: " + container.getLogs());
    }

    private ExecResult runCli(String args) throws Exception {
        return container.execInContainer("/bin/sh", "-c", "/cdc cli " + args);
    }
}
