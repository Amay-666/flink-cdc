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

import java.time.Duration;

/**
 * A {@code canal/canal-server:v1.1.8} container that tails the binlog of a MySQL-compatible source
 * (MySQL or PolarDB-X, whose GalaxyCDC serves a MySQL-compatible binlog) and writes canal flatMessage
 * JSON to a Kafka topic.
 *
 * <p>The image's entrypoint script rewrites {@code canal.*} environment variables into {@code
 * canal.properties} and {@code instance.properties}, so the whole configuration is expressed
 * through environment variables. The container joins the test network as {@code canal} and reaches
 * the source and Kafka ({@code kafka:9092}) via docker network aliases.
 *
 * <p>Only tables matching {@code <databaseName>\..*} are captured (the database name carries the
 * unique per-run suffix), and only messages for the configured topic are produced.
 */
public class CanalServerContainer extends GenericContainer<CanalServerContainer> {

    private static final Logger LOG = LoggerFactory.getLogger(CanalServerContainer.class);

    public static final String IMAGE = "canal/canal-server:v1.1.8";

    private static final int STARTUP_TIMEOUT_SECONDS = 90;

    /** Defaults for the MySQL source ({@code flinkuser}/{@code flinkpw} on {@code mysql:3306}). */
    public CanalServerContainer(
            String databaseName, String kafkaTopic, Network network, Logger logger) {
        this(databaseName, kafkaTopic, "mysql:3306", "flinkuser", "flinkpw", network, logger);
    }

    /**
     * Tails the binlog of a MySQL-compatible source reachable at {@code masterAddress} (e.g. {@code
     * mysql:3306} or {@code polardbx:8527}) with the given credentials.
     */
    public CanalServerContainer(
            String databaseName,
            String kafkaTopic,
            String masterAddress,
            String dbUsername,
            String dbPassword,
            Network network,
            Logger logger) {
        super(IMAGE);
        withNetwork(network);
        withNetworkAliases("canal");
        // mq mode: write flatMessage JSON (the wire format KafkaJsonSource parses) to Kafka
        withEnv("canal.serverMode", "kafka");
        withEnv("canal.mq.servers", "kafka:9092");
        withEnv("canal.mq.topic", kafkaTopic);
        withEnv("canal.mq.flatMessage", "true");
        // instance: replicate from the source with a slave id distinct from its server-id
        withEnv("canal.instance.mysql.slaveId", "123456");
        withEnv("canal.instance.master.address", masterAddress);
        withEnv("canal.instance.dbUsername", dbUsername);
        withEnv("canal.instance.dbPassword", dbPassword);
        withEnv("canal.instance.connectionCharset", "UTF-8");
        withEnv("canal.instance.filter.regex", databaseName + "\\..*");
        withEnv("canal.instance.tsdb.enable", "false");
        withStartupTimeout(Duration.ofSeconds(120));
        withLogConsumer(new Slf4jLogConsumer(logger));
    }

    /**
     * Waits until the canal server reports a successful start. After this returns the instance has
     * connected to MySQL as a replication slave and is tailing the binlog, so DML executed from now
     * on will be captured.
     */
    public void waitUntilStarted() throws InterruptedException {
        long deadline = System.currentTimeMillis() + STARTUP_TIMEOUT_SECONDS * 1000L;
        while (System.currentTimeMillis() < deadline) {
            if (getLogs().contains("start canal successful")) {
                LOG.info("canal-server started successfully.");
                return;
            }
            Thread.sleep(1000);
        }
        throw new IllegalStateException(
                "canal-server did not start within " + STARTUP_TIMEOUT_SECONDS + "s: " + getLogs());
    }
}
