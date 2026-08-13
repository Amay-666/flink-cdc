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

package org.apache.flink.cdc.connectors.kafkajson.example;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.testutils.CanalServerContainer;
import org.apache.flink.cdc.connectors.kafkajson.testutils.KafkaJsonMySqlContainer;
import org.apache.flink.cdc.connectors.kafkajson.testutils.KafkaJsonSourceTestBase;
import org.apache.flink.cdc.connectors.kafkajson.testutils.KafkaUtil;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.lifecycle.Startables;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Manual end-to-end harness that boots a real {@code MySQL + canal-server + Kafka} stack and lets you
 * write data yourself, watching every event the connector emits in real time.
 *
 * <p><b>Why a {@code main()}, not a JUnit test:</b> the harness must run for as long as you keep
 * writing data, so it never completes on its own. A plain {@code main()} in the test sources is
 * runnable from IDEA (right-click the class -&gt; Run 'MySqlCanalManualHarness.main()', the test
 * classpath is on the run configuration automatically) and is never picked up by surefire/failsafe,
 * unlike an {@code *ITCase}.
 *
 * <p><b>Windows connection:</b> this machine runs Docker natively inside WSL2 in mirrored networking
 * mode, so Windows and WSL share the loopback: Windows can reach the containers through {@code
 * localhost:3306} / {@code localhost:9092} (and, as a fallback, the WSL VM IP printed in the banner).
 * Both MySQL ports are pinned with fixed host ports, so your Windows SQL client configuration stays
 * stable across runs.
 *
 * <p><b>Flow:</b> create {@code manualdb.customers} with 4 seed rows -&gt; start canal tailing the
 * binlog to topic {@code manual-topic} -&gt; run the connector source (JDBC snapshot + Kafka
 * increment) -&gt; print the 4 snapshot inserts -&gt; then print every incremental event as you write
 * SQL from Windows, until you stop the program. Exactly mirrors the {@code MySqlCanalChainITCase}
 * wire format, but fixed database/topic names so your ad-hoc SQL is stable.
 *
 * <p>Stop the run by clicking the red square in IDEA. The containers are removed automatically when
 * the JVM exits.
 */
public class MySqlCanalManualHarness extends KafkaJsonSourceTestBase {

    /** Fixed database and topic so the SQL you type from Windows is stable across runs. */
    private static final String DB_NAME = "manualdb";

    private static final String TABLE = "customers";
    private static final String TOPIC = "manual-topic";

    /** Fixed host ports: Windows connects to {@code localhost:3306} / {@code localhost:9092}. */
    private static final int MYSQL_PORT = 3306;

    private static final int KAFKA_PORT = 9092;

    /** Row renderers keyed by table id, built from the CreateTableEvent schema. */
    private static final Map<TableId, List<RecordData.FieldGetter>> FIELD_GETTERS = new HashMap<>();

    private static final Map<TableId, List<String>> COLUMN_NAMES = new HashMap<>();

    private static final SimpleDateFormat TIME = new SimpleDateFormat("HH:mm:ss");

    public static void main(String[] args) throws Exception {
        checkDockerAvailableOrExit();

        KafkaJsonMySqlContainer mysql =
                createMySqlContainer(MySqlVersion.V8_0).withFixedExposedPort(MYSQL_PORT, MYSQL_PORT);
        KafkaContainer kafka = KafkaUtil.createKafkaContainer(LOG, NETWORK, KAFKA_PORT);

        LOG.info("Starting MySQL + Kafka containers...");
        Startables.deepStart(Stream.of(mysql, kafka)).join();
        LOG.info("Containers started.");

        printConnectionBanner();

        createDatabase(mysql);

        CanalServerContainer canal = new CanalServerContainer(DB_NAME, TOPIC, NETWORK, LOG);
        LOG.info("Starting canal-server (tails the binlog of {} into topic {})...", DB_NAME, TOPIC);
        canal.start();
        canal.waitUntilStarted();
        // let the instance finish registering as a slave and start tailing the binlog
        Thread.sleep(3_000);
        LOG.info("canal-server is tailing the binlog; start writing SQL from Windows now.");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureEnv(env);
        KafkaJsonSourceConfigFactory configFactory =
                buildConfigFactory(
                        "localhost",
                        MYSQL_PORT,
                        TEST_USER,
                        TEST_PASSWORD,
                        DB_NAME,
                        TABLE,
                        kafka.getBootstrapServers(),
                        TOPIC);

        LOG.info(
                "Starting the connector source (JDBC snapshot, then consuming {} from Kafka)...",
                TOPIC);
        CloseableIterator<Event> events = runSource(configFactory, env);

        int dataEvents = 0;
        boolean snapshotDone = false;
        while (events.hasNext()) {
            Event event = events.next();
            if (event instanceof DataChangeEvent) {
                dataEvents++;
            }
            printEvent(event);
            if (!snapshotDone && dataEvents >= 4) {
                snapshotDone = true;
                banner(
                        "快照阶段完成（4 行已读入）。之后在 Windows 写入的 SQL 会走 canal -> Kafka -> 连接器增量链路。",
                        "可试：INSERT/UPDATE/DELETE，或 ALTER TABLE ADD COLUMN xxx ...（看 SchemaChangeEvent）。",
                        "要复现“快照期间写入的 UPDATE 被增量再次下发”的重复场景：在启动后立刻写入 UPDATE（快照读取很快，多试几次）。");
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------

    private static void checkDockerAvailableOrExit() {
        if (!org.testcontainers.DockerClientFactory.instance().isDockerAvailable()) {
            System.out.println(
                    "Docker 不可用。请先在 WSL 里启动原生 docker（systemctl start docker），再重试。");
            System.exit(1);
        }
    }

    private static void createDatabase(KafkaJsonMySqlContainer mysql) throws Exception {
        // clean slate on every run: drop/recreate manualdb, then seed 4 rows (the same table the
        // integration tests use). This happens before canal starts, so the seed rows are captured
        // by the JDBC snapshot rather than replayed through the change log.
        try (Connection connection =
                        DriverManager.getConnection(mysql.getJdbcUrl(), TEST_USER, TEST_PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP DATABASE IF EXISTS `" + DB_NAME + "`");
            statement.execute("CREATE DATABASE `" + DB_NAME + "`");
            statement.execute("USE `" + DB_NAME + "`");
            statement.execute(
                    "CREATE TABLE `"
                            + TABLE
                            + "` (id INT NOT NULL, name VARCHAR(255) NOT NULL, "
                            + "address VARCHAR(255), PRIMARY KEY (id))");
            statement.execute(
                    "INSERT INTO `"
                            + TABLE
                            + "` (id, name, address) VALUES "
                            + "(101, 'user_1', 'Shanghai'), (102, 'user_2', 'Beijing'), "
                            + "(103, 'user_3', 'Hangzhou'), (104, 'user_4', 'Shenzhen')");
        }
    }

    private static void printConnectionBanner() {
        banner(
                "在 Windows 上连接 MySQL（二选一）：",
                "  A) localhost:3306   <-- 推荐，WSL2 镜像网络共享回环",
                "  B) " + wslIp() + ":3306   <-- WSL 虚拟机 IP（如果 A 不通）",
                "  账号1: mysqluser / mysqlpw   （全部权限，连接器/JDBC 用它）",
                "  账号2: flinkuser / flinkpw   （canal 复制账号）",
                "  库: " + DB_NAME + "   表: " + TABLE + "   （表已建好，初始 4 行）",
                "  数据库连接串示例: jdbc:mysql://localhost:3306/" + DB_NAME,
                "",
                "Kafka: localhost:9092   topic: " + TOPIC + "   （canal flatMessage JSON）");
    }

    /** Prints a centered banner inside {@code ========} rules. */
    private static void banner(String... lines) {
        int width = 0;
        for (String line : lines) {
            width = Math.max(width, line.length());
        }
        String rule = "=".repeat(width + 4);
        System.out.println();
        System.out.println(rule);
        for (String line : lines) {
            System.out.println("  " + line);
        }
        System.out.println(rule);
        System.out.flush();
    }

    private static void printEvent(Event event) {
        if (event instanceof CreateTableEvent) {
            rememberSchema((CreateTableEvent) event);
            System.out.printf("[%s] Schema  CREATE TABLE %s%n", TIME.format(new Date()), event);
        } else if (event instanceof DataChangeEvent) {
            DataChangeEvent dce = (DataChangeEvent) event;
            System.out.printf(
                    "[%s] %-6s %s  before=%s  after=%s%n",
                    TIME.format(new Date()),
                    dce.op(),
                    dce.tableId(),
                    renderRow(dce.tableId(), dce.before()),
                    renderRow(dce.tableId(), dce.after()));
        } else if (event instanceof SchemaChangeEvent) {
            System.out.printf("[%s] Schema  %s%n", TIME.format(new Date()), event);
        } else {
            System.out.printf("[%s] %s%n", TIME.format(new Date()), event);
        }
        System.out.flush();
    }

    private static void rememberSchema(CreateTableEvent event) {
        Schema schema = event.getSchema();
        List<Column> columns = schema.getColumns();
        List<RecordData.FieldGetter> getters = new ArrayList<>();
        List<String> names = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            getters.add(RecordData.createFieldGetter(column.getType(), i));
            names.add(column.getName());
        }
        FIELD_GETTERS.put(event.tableId(), getters);
        COLUMN_NAMES.put(event.tableId(), names);
    }

    private static String renderRow(TableId tableId, RecordData row) {
        if (row == null) {
            return "null";
        }
        List<RecordData.FieldGetter> getters = FIELD_GETTERS.get(tableId);
        List<String> names = COLUMN_NAMES.get(tableId);
        if (getters == null) {
            return String.valueOf(row);
        }
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < getters.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(names.get(i)).append("=").append(getters.get(i).getFieldOrNull(row));
        }
        return sb.append("}").toString();
    }

    /**
     * Best-effort WSL VM IP (first non-loopback IPv4 that is not a docker bridge), for the Windows
     * fallback connection. Docker bridge interfaces ({@code docker0}, {@code veth*}, {@code br-*})
     * are skipped: their addresses are only visible inside the WSL VM and would be useless to a
     * Windows client.
     */
    private static String wslIp() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                String name = networkInterface.getName();
                if (!networkInterface.isUp()
                        || networkInterface.isLoopback()
                        || name.startsWith("docker")
                        || name.startsWith("veth")
                        || name.startsWith("br-")) {
                    continue;
                }
                Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address instanceof Inet4Address && !address.isLoopbackAddress()) {
                        return address.getHostAddress();
                    }
                }
            }
        } catch (SocketException ignored) {
            // fall through
        }
        return "?";
    }
}
