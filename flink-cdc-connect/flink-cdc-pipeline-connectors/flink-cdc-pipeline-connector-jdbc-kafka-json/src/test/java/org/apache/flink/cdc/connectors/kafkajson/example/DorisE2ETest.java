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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.KafkaJsonDataSinkBuilder;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkDialect;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSource;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.DatabaseType;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.DdlParser;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.EventTime;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.MessageFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.TestLogger;

import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * End-to-end integration test that exercises the full pipeline: TiDB (MySQL protocol) → Kafka →
 * Flink CDC → Doris.
 *
 * <p>Each step is a separate {@code @Test} method (ordered by name via {@link FixMethodOrder}), so
 * failures can be isolated to a specific operation. The {@code @BeforeClass} creates the table and
 * starts the Flink job; the first test method verifies the initial data has landed in Doris.
 *
 * <p>Prerequisites: TiDB, Doris, Kafka, and the TiCDC/canal pipeline must all be running, and
 * {@code e2e/env.yaml} must be provisioned from {@code e2e/env.yaml.template}. Without that
 * configuration the class is skipped rather than failed, so an ordinary {@code mvn test} still
 * passes on a machine that has no such services.
 */
@Category({})
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DorisE2ETest extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(DorisE2ETest.class);

    // ──────────────────────── Connection config ────────────────────────
    //
    // Provisioned from e2e/env.yaml on the test classpath (local and git-ignored; see the committed
    // e2e/env.yaml.template); run against a different environment with
    // -De2e.config=<file-or-resource> (see E2eConfig's javadoc). The fields are assigned in
    // setup(): when no environment is configured the whole class is skipped there.

    private static String mysqlHost;
    private static int mysqlPort;
    private static String mysqlUser;
    private static String mysqlPass;
    private static String mysqlDb;

    private static String dorisFeHttp;
    private static String dorisFeQueryHost;
    private static int dorisFeQueryPort;
    private static String dorisUser;
    private static String dorisPass;
    private static String dorisDb;

    private static String kafkaServers;
    private static String kafkaGroup;
    private static String kafkaTopic;

    // ──────────────────────── Test table names ────────────────────────
    //
    // A single table carries the whole E2E: the full type matrix (34 typed columns) is also the
    // subject of every schema-change step (UPDATE, DELETE, ADD/RENAME/MODIFY/DROP COLUMN, the
    // ENUM list replacement, TABLE RENAME, TRUNCATE and DROP) — the exotic types experience the
    // complete lifecycle, and the schema-change paths run against a realistic wide table.

    private static final String MATRIX_TABLE = "type_matrix";
    private static final String MATRIX_TABLE_RENAMED = "type_matrix_renamed";
    private static final String MATRIX_TABLE_LIKE = "type_matrix_like";

    private static String dorisMatrixTable;
    private static String dorisMatrixTableRenamed;
    private static String dorisMatrixTableLike;

    /** The columns of the {@code type_matrix} table, in declaration order. */
    private static final String[] TYPE_MATRIX_COLUMNS = {
        "id",
        "c_tinyint",
        "c_tinyint_u",
        "c_smallint",
        "c_smallint_u",
        "c_mediumint",
        "c_mediumint_u",
        "c_int",
        "c_int_u",
        "c_bigint",
        "c_bigint_u",
        "c_float",
        "c_double",
        "c_decimal",
        "c_bit1",
        "c_bit8",
        "c_bool",
        "c_date",
        "c_time",
        "c_time_p",
        "c_datetime",
        "c_datetime_p3",
        "c_datetime_p6",
        "c_timestamp",
        "c_year",
        "c_json",
        "c_enum",
        "c_set",
        "c_char",
        "c_varchar",
        "c_text",
        "c_binary",
        "c_varbinary",
        "c_blob"
    };

    // ──────────────────────── Timing ────────────────────────

    private static long pollIntervalMs;
    private static long pollTimeoutMs;

    // ──────────────────────── JDBC connections ────────────────────────

    private static Connection mysqlConn;
    private static Connection dorisConn;

    @BeforeClass
    public static void setup() throws Exception {
        Optional<E2eConfig> config = E2eConfig.find();
        Assume.assumeTrue(
                "E2E environment not configured: copy src/test/resources/e2e/env.yaml.template "
                        + "to e2e/env.yaml (git-ignored) and fill in the endpoints, or pass "
                        + "-De2e.config=<file-or-resource>",
                config.isPresent());
        E2eConfig env = config.get();

        mysqlHost = env.getString("mysql.host");
        mysqlPort = env.getInt("mysql.port");
        mysqlUser = env.getString("mysql.user");
        mysqlPass = env.getString("mysql.password");
        mysqlDb = env.getString("mysql.database");

        dorisFeHttp = env.getString("doris.feHttp");
        dorisFeQueryHost = env.getString("doris.queryHost");
        dorisFeQueryPort = env.getInt("doris.queryPort");
        dorisUser = env.getString("doris.user");
        dorisPass = env.getString("doris.password");
        dorisDb = env.getString("doris.database");

        kafkaServers = env.getString("kafka.servers");
        kafkaGroup = env.getString("kafka.group");
        kafkaTopic = env.getString("kafka.topic");

        dorisMatrixTable = mysqlDb + "_" + MATRIX_TABLE;
        dorisMatrixTableRenamed = mysqlDb + "_" + MATRIX_TABLE_RENAMED;
        dorisMatrixTableLike = mysqlDb + "_" + MATRIX_TABLE_LIKE;

        pollIntervalMs = env.getInt("timing.pollIntervalMs");
        pollTimeoutMs = env.getInt("timing.pollTimeoutMs");

        mysqlConn =
                DriverManager.getConnection(
                        String.format(
                                "jdbc:mysql://%s:%d/%s?useSSL=false&allowMultiQueries=true",
                                mysqlHost, mysqlPort, mysqlDb),
                        mysqlUser,
                        mysqlPass);
        dorisConn =
                DriverManager.getConnection(
                        String.format(
                                "jdbc:mysql://%s:%d/%s?useSSL=false&allowMultiQueries=true",
                                dorisFeQueryHost, dorisFeQueryPort, dorisDb),
                        dorisUser,
                        dorisPass);
        LOG.info("Connected to MySQL and Doris");

        // Create the table BEFORE starting Flink so the enumerator discovers it: the single
        // type matrix table carries the whole E2E (see the constants section).
        LOG.info("=== Setup: CREATE TABLE + INSERT row 1000 (before Flink) ===");
        executeMySql(resource("e2e/step01-init-mysql.sql"));
        executeDorisSql(resource("e2e/step01-init-doris.sql"));

        // Start Flink job in a background thread.
        new Thread(() -> new DorisE2ETest().runFlinkJob()).start();
        LOG.info("Flink job started in background thread");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (mysqlConn != null) {
            mysqlConn.close();
        }
        if (dorisConn != null) {
            dorisConn.close();
        }
    }

    // ──────────────────────── Test steps ────────────────────────

    /** Step 1: the snapshot row 1000 (the JDBC path, all 34 typed columns). */
    @Test
    public void test01SnapshotRow() throws Exception {
        LOG.info("=== Step 1: Full type matrix, snapshot row 1000 (JDBC path) ===");
        waitForDoris(
                dorisMatrixTable,
                "SELECT * FROM `%s` ORDER BY `id`",
                rs -> {
                    List<Map<String, String>> rows = tryCollectRows(rs, TYPE_MATRIX_COLUMNS);
                    assertThat(rows).hasSize(1);
                    assertTypeMatrixRow1000(rows.get(0));
                });
    }

    /** Step 2: the streaming row 2000 (the TiCDC canal-json path, all 34 typed columns). */
    @Test
    public void test02StreamRow() throws Exception {
        LOG.info("=== Step 2: Full type matrix, stream row 2000 (canal path) ===");
        executeMySql(resource("e2e/step02-stream-insert-mysql.sql"));
        waitForDoris(
                dorisMatrixTable,
                "SELECT * FROM `%s` ORDER BY `id`",
                rs -> {
                    List<Map<String, String>> rows = tryCollectRows(rs, TYPE_MATRIX_COLUMNS);
                    assertThat(rows).hasSize(2);
                    assertTypeMatrixRow2000(rows.get(1));
                });
    }

    /** Step 3: UPDATE typed columns + DELETE the stream row. */
    @Test
    public void test03UpdateDelete() throws Exception {
        LOG.info("=== Step 3: UPDATE + DELETE on typed columns ===");
        executeMySql(resource("e2e/step03-update-delete-mysql.sql"));
        waitForDoris(
                dorisMatrixTable,
                "SELECT `id`, `c_varchar`, `c_tinyint`, `c_decimal` FROM `%s` WHERE `id` = 1000",
                rs -> {
                    List<Map<String, String>> rows =
                            tryCollectRows(rs, "id", "c_varchar", "c_tinyint", "c_decimal");
                    assertThat(rows).hasSize(1);
                    assertThat(rows.get(0)).containsEntry("c_varchar", "Updated via canal");
                    assertThat(rows.get(0)).containsEntry("c_tinyint", "42");
                    assertThat(rows.get(0)).containsEntry("c_decimal", "999.9900");
                });
        waitForDoris(
                dorisMatrixTable,
                "SELECT `id` FROM `%s` WHERE `id` = 2000",
                rs -> {
                    List<Map<String, String>> rows = tryCollectRows(rs, "id");
                    assertThat(rows).isEmpty(); // the stream row is deleted
                });
    }

    /** Step 4: ADD COLUMN on the wide matrix table. */
    @Test
    public void test04AddColumn() throws Exception {
        LOG.info("=== Step 4: ADD COLUMN ===");
        executeMySql(resource("e2e/step04-add-column-mysql.sql"));
        waitForDorisColumn(dorisMatrixTable, "extra");
        waitForDoris(
                dorisMatrixTable,
                "SELECT `id`, `extra` FROM `%s` WHERE `id` = 3000",
                rs -> {
                    List<Map<String, String>> rows = tryCollectRows(rs, "id", "extra");
                    assertThat(rows).hasSize(1);
                    assertThat(rows.get(0)).containsEntry("extra", "brand-new");
                });
    }

    /** Step 5: RENAME COLUMN c_char → c_char2. */
    @Test
    public void test05RenameColumn() throws Exception {
        LOG.info("=== Step 5: RENAME COLUMN ===");
        executeMySql(resource("e2e/step05-rename-column-mysql.sql"));
        waitForDorisColumn(dorisMatrixTable, "c_char2");
        waitForDoris(
                dorisMatrixTable,
                "SELECT `id`, `c_char2` FROM `%s` WHERE `id` = 3000",
                rs -> {
                    List<Map<String, String>> rows = tryCollectRows(rs, "id", "c_char2");
                    assertThat(rows).hasSize(1);
                    assertThat(rows.get(0)).containsEntry("c_char2", "ZZTop");
                });
    }

    /**
     * Step 6: MODIFY COLUMN — the ENUM list is REPLACED (MySQL's whole-list semantics): 'a','b','c'
     * becomes 'a','b','c','d'. Row 4000 stores the new label 'd'; TiCDC renders its 1-based index
     * "4", which the connector must map back using the NEW list (the DDL text carried it) —
     * arriving in Doris as 'd'.
     */
    @Test
    public void test06ModifyEnum() throws Exception {
        LOG.info("=== Step 6: MODIFY COLUMN (the ENUM list replacement) ===");
        executeMySql(resource("e2e/step06-modify-enum-mysql.sql"));
        waitForDoris(
                dorisMatrixTable,
                "SELECT `id`, `c_enum` FROM `%s` WHERE `id` = 4000",
                rs -> {
                    List<Map<String, String>> rows = tryCollectRows(rs, "id", "c_enum");
                    assertThat(rows).hasSize(1);
                    // the new label 'd' — mapped through the REPLACED list, not the old one
                    // (the old list would have rendered index 4 as out-of-range garbage)
                    assertThat(rows.get(0)).containsEntry("c_enum", "d");
                });
    }

    /**
     * Step 7: CREATE TABLE LIKE + column type change fallback — the DDL text carries no column
     * definitions, so the handler resolves the schema (in-memory store first, JDBC target
     * fallback). Row 7000 is written into the new table, verified in Doris, then deleted and the
     * deletion verified. Row 8000 carries a numeric c_text, then c_text changes from TEXT to INT —
     * Doris cannot convert STRING to INT in place, so the applier falls back to the temp-column
     * swap (add, cast, drop, rename); the cast value 12345 must survive.
     */
    @Test
    public void test07CreateTableLike() throws Exception {
        LOG.info("=== Step 7: CREATE TABLE LIKE + fallback ===");
        executeMySql(resource("e2e/step07-create-like-mysql.sql"));
        waitForDoris(
                dorisMatrixTableLike,
                "SELECT `id`, `c_varchar` FROM `%s` WHERE `id` = 7000",
                rs -> {
                    List<Map<String, String>> rows = tryCollectRows(rs, "id", "c_varchar");
                    assertThat(rows).hasSize(1);
                    assertThat(rows.get(0)).containsEntry("c_varchar", "like-created");
                });
        // the fallback (temp-column swap) must convert c_text to INT with the cast value
        waitForDoris(
                dorisMatrixTableLike,
                "SELECT `id`, `c_text` FROM `%s` WHERE `id` = 8000",
                rs -> {
                    List<Map<String, String>> rows = tryCollectRows(rs, "id", "c_text");
                    assertThat(rows).hasSize(1);
                    assertThat(rows.get(0)).containsEntry("c_text", "12345");
                });
        executeMySql("DELETE FROM `" + MATRIX_TABLE_LIKE + "` WHERE `id` = 7000");
        executeMySql("DELETE FROM `" + MATRIX_TABLE_LIKE + "` WHERE `id` = 8000");
        waitForDoris(
                dorisMatrixTableLike,
                "SELECT COUNT(*) FROM `%s`",
                rs -> {
                    try {
                        String columnLabel = rs.getMetaData().getColumnLabel(1);
                        List<Map<String, String>> rows = tryCollectRows(rs, columnLabel);
                        assertThat(rows).hasSize(1);
                        assertThat(Integer.parseInt(rows.get(0).get(columnLabel))).isEqualTo(0);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /** Step 8: RENAME TABLE type_matrix → type_matrix_renamed. */
    @Test
    public void test08RenameTable() throws Exception {
        LOG.info("=== Step 8: RENAME TABLE ===");
        executeMySql(resource("e2e/step08-rename-table-mysql.sql"));
        waitForDoris(
                dorisMatrixTableRenamed,
                "SELECT `id`, `c_varchar` FROM `%s` WHERE `id` = 5000",
                rs -> {
                    List<Map<String, String>> rows = tryCollectRows(rs, "id", "c_varchar");
                    assertThat(rows).hasSize(1);
                    assertThat(rows.get(0)).containsEntry("c_varchar", "renamed-row");
                });
    }

    /** Step 9: DROP COLUMN c_text. */
    @Test
    public void test09DropColumn() throws Exception {
        LOG.info("=== Step 9: DROP COLUMN ===");
        executeMySql(resource("e2e/step09-drop-column-mysql.sql"));
        waitForDorisColumnAbsence(dorisMatrixTableRenamed, "c_text");
        waitForDoris(
                dorisMatrixTableRenamed,
                "SELECT `id`, `c_varchar` FROM `%s` WHERE `id` = 6000",
                rs -> {
                    List<Map<String, String>> rows = tryCollectRows(rs, "id", "c_varchar");
                    assertThat(rows).hasSize(1);
                    assertThat(rows.get(0)).containsEntry("c_varchar", "after-drop");
                });
    }

    /** Step 10: TRUNCATE TABLE type_matrix_renamed. */
    @Test
    public void test10TruncateTable() throws Exception {
        LOG.info("=== Step 10: TRUNCATE TABLE ===");
        executeMySql(resource("e2e/step10-truncate-mysql.sql"));
        waitForDoris(
                dorisMatrixTableRenamed,
                "SELECT COUNT(*) FROM `%s`",
                rs -> {
                    try {
                        String columnLabel = rs.getMetaData().getColumnLabel(1);
                        List<Map<String, String>> rows = tryCollectRows(rs, columnLabel);
                        assertThat(rows).hasSize(1);
                        List<String> tables =
                                rows.stream()
                                        .map(row -> row.get(columnLabel))
                                        .collect(Collectors.toList());
                        assertThat(Integer.parseInt(tables.get(0))).isEqualTo(0);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /** Step 11: DROP TABLE type_matrix_renamed. */
    @Test
    public void test11DropTable() throws Exception {
        LOG.info("=== Step 11: DROP TABLE ===");
        executeMySql(resource("e2e/step11-cleanup-mysql.sql"));
        waitForDoris(
                dorisDb,
                "SHOW TABLES FROM `%s`",
                rs -> {
                    try {
                        String columnLabel = rs.getMetaData().getColumnLabel(1);
                        List<Map<String, String>> rows = tryCollectRows(rs, columnLabel);
                        List<String> tables =
                                rows.stream()
                                        .map(row -> row.get(columnLabel))
                                        .collect(Collectors.toList());
                        assertThat(tables)
                                .doesNotContain(
                                        dorisMatrixTable,
                                        dorisMatrixTableRenamed,
                                        dorisMatrixTableLike);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
        LOG.info("=== All steps passed! Pipeline is working correctly. ===");
    }

    private static void assertTypeMatrixRow1000(Map<String, String> row) {
        assertThat(row.get("id")).isEqualTo("1000");
        // integers: unsigned widening per KafkaJsonColumnMeta
        assertThat(row.get("c_tinyint")).isEqualTo("-128");
        assertThat(row.get("c_tinyint_u")).isEqualTo("200"); // TINYINT UNSIGNED -> SMALLINT
        assertThat(row.get("c_smallint")).isEqualTo("-32768");
        assertThat(row.get("c_smallint_u")).isEqualTo("65535"); // -> INT
        assertThat(row.get("c_mediumint")).isEqualTo("-8388608");
        assertThat(row.get("c_mediumint_u")).isEqualTo("16777215"); // -> INT
        assertThat(row.get("c_int")).isEqualTo("-2147483648");
        assertThat(row.get("c_int_u")).isEqualTo("4294967295"); // -> BIGINT
        assertThat(row.get("c_bigint")).isEqualTo("-9223372036854775808");
        // BIGINT UNSIGNED -> DECIMAL(20,0): the full 20-digit value survives end-to-end
        assertThat(row.get("c_bigint_u")).isEqualTo("18446744073709551615");
        // approximate numbers: compared with tolerance, the textual rendering varies
        assertThat(Double.parseDouble(row.get("c_float"))).isCloseTo(1.25, within(1e-6));
        assertThat(Double.parseDouble(row.get("c_double")))
                .isCloseTo(3.141592653589793, within(1e-12));
        assertThat(row.get("c_decimal")).isEqualTo("123456789.1234");
        // BIT(1) -> BOOLEAN, rendered as "1" or "true" depending on the driver
        assertThat(row.get("c_bit1")).matches("(?i)1|true");
        // BIT(8) 170 = 0xAA: one byte, base64-encoded by the stream-load JSON writer
        assertThat(row.get("c_bit8")).isEqualTo("qg==");
        assertThat(row.get("c_bool")).matches("(?i)1|true");
        assertThat(row.get("c_date")).isEqualTo("2026-01-31");
        assertThat(row.get("c_time")).isEqualTo("13:45:01");
        // TIME(3): the row converter renders "HH:mm:ss", the fraction is not carried
        assertThat(row.get("c_time_p")).isEqualTo("13:45:01");
        assertThat(row.get("c_datetime")).isEqualTo("2026-01-31 10:20:30");
        assertThat(row.get("c_datetime_p3")).isEqualTo("2026-01-31 10:20:30.123");
        // DATETIME(6) microseconds survive end-to-end (the epoch-micros conversion)
        assertThat(row.get("c_datetime_p6")).isEqualTo("2026-01-31 10:20:30.123456");
        assertThat(row.get("c_timestamp")).isEqualTo("2026-01-31 10:20:30");
        assertThat(row.get("c_year")).isEqualTo("2026");
        // MySQL/TiDB normalizes the stored JSON: a space follows each colon and comma
        assertThat(row.get("c_json")).isEqualTo("{\"k\": 1}");
        assertThat(row.get("c_enum")).isEqualTo("b");
        // SET stays the comma-joined label string (the official Doris MySQL mapping)
        assertThat(row.get("c_set")).isEqualTo("x,z");
        assertThat(row.get("c_char")).isEqualTo("hello");
        assertThat(row.get("c_varchar")).isEqualTo("你好🌍");
        assertThat(row.get("c_text")).isEqualTo("text value");
        // binary columns: base64 text in the STRING column
        assertThat(row.get("c_binary")).isEqualTo("AQIDBAWqu8w=");
        assertThat(row.get("c_varbinary")).isEqualTo("yv66vg==");
        assertThat(row.get("c_blob")).isEqualTo("3q2+7w==");
    }

    private static void assertTypeMatrixRow2000(Map<String, String> row) {
        assertThat(row.get("id")).isEqualTo("2000");
        assertThat(row.get("c_tinyint")).isEqualTo("127");
        assertThat(row.get("c_tinyint_u")).isEqualTo("255"); // TINYINT UNSIGNED max
        assertThat(row.get("c_smallint")).isEqualTo("32767");
        assertThat(row.get("c_smallint_u")).isEqualTo("1");
        assertThat(row.get("c_mediumint")).isEqualTo("8388607");
        assertThat(row.get("c_mediumint_u")).isEqualTo("1");
        assertThat(row.get("c_int")).isEqualTo("2147483647");
        assertThat(row.get("c_int_u")).isEqualTo("1");
        assertThat(row.get("c_bigint")).isEqualTo("9223372036854775807"); // Long max
        assertThat(row.get("c_bigint_u")).isEqualTo("1");
        assertThat(Double.parseDouble(row.get("c_float"))).isCloseTo(2.5, within(1e-6));
        assertThat(Double.parseDouble(row.get("c_double")))
                .isCloseTo(2.718281828459045, within(1e-12));
        assertThat(row.get("c_decimal")).isEqualTo("-99999.9999"); // negative decimal
        assertThat(row.get("c_bit1")).matches("(?i)0|false");
        // BIT(8) b'00000001' = 1 = 0x01
        assertThat(row.get("c_bit8")).isEqualTo("AQ==");
        assertThat(row.get("c_bool")).matches("(?i)0|false");
        assertThat(row.get("c_date")).isEqualTo("2026-02-28");
        assertThat(row.get("c_time")).isEqualTo("23:59:59");
        assertThat(row.get("c_time_p")).isEqualTo("00:00:01"); // .999 fraction dropped
        assertThat(row.get("c_datetime")).isEqualTo("2026-02-28 23:59:59");
        assertThat(row.get("c_datetime_p3")).isEqualTo("2026-02-28 23:59:59.999");
        assertThat(row.get("c_datetime_p6")).isEqualTo("2026-02-28 23:59:59.999999");
        assertThat(row.get("c_timestamp")).isEqualTo("2026-02-28 23:59:59");
        assertThat(row.get("c_year")).isEqualTo("1999");
        assertThat(row.get("c_json")).isEqualTo("{\"arr\": [1, 2]}");
        assertThat(row.get("c_enum")).isEqualTo("c");
        assertThat(row.get("c_set")).isEqualTo("y");
        assertThat(row.get("c_char")).isEqualTo("wor");
        assertThat(row.get("c_varchar")).isEqualTo("🎉"); // 4-byte utf8mb4 char
        assertThat(row.get("c_text")).isEqualTo("stream text");
        assertThat(row.get("c_binary")).isEqualTo("Dw8PDw8PDw8=");
        assertThat(row.get("c_varbinary")).isEqualTo("qw==");
        assertThat(row.get("c_blob")).isEqualTo("AA==");
    }

    // ──────────────────────── Flink job ────────────────────────

    private void runFlinkJob() {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.enableCheckpointing(6_000L);
            env.setParallelism(1);

            KafkaJsonSourceConfigFactory sourceConfig = getKafkaJsonSourceConfigFactory();

            Configuration sinkConfig = new Configuration();
            sinkConfig.set(DorisDataSinkOptions.FENODES, dorisFeHttp);
            sinkConfig.set(DorisDataSinkOptions.USERNAME, dorisUser);
            sinkConfig.set(DorisDataSinkOptions.PASSWORD, dorisPass);
            Map<String, String> tableProperties = new HashMap<>();
            tableProperties.put("replication_allocation", "tag.location.group_a: 3");
            sinkConfig.set(DorisDataSinkOptions.TABLE_PROPERTIES, tableProperties);
            sinkConfig.set(DorisDataSinkOptions.TABLE_BUCKETS, 8);
            DorisDataSinkOptions sinkOptions =
                    new DorisDataSinkOptions(sinkConfig)
                            .withTableMapping(t -> t.getSchemaName() + "_" + t.getTableName())
                            .withDatabaseMapping(t -> dorisDb);

            KafkaJsonDataSinkBuilder sinkBuilder =
                    new KafkaJsonDataSinkBuilder(
                            new DorisDataSinkDialect(sinkOptions, ZoneId.of("Asia/Shanghai")));

            FlinkSourceProvider provider =
                    (FlinkSourceProvider)
                            new KafkaJsonDataSource(sourceConfig).getEventSourceProvider();

            DataStream<Event> source =
                    env.fromSource(
                            provider.getSource(), WatermarkStrategy.noWatermarks(), "e2e-source");

            sinkBuilder.build(
                    source, Duration.ofSeconds(30), SchemaChangeBehavior.EVOLVE, "Asia/Shanghai");

            env.execute("e2e-tidb-to-doris");
        } catch (Exception e) {
            LOG.error("Flink job failed", e);
        }
    }

    private static @NotNull KafkaJsonSourceConfigFactory getKafkaJsonSourceConfigFactory() {
        KafkaJsonSourceConfigFactory sourceConfig = new KafkaJsonSourceConfigFactory();
        sourceConfig.hostname(mysqlHost);
        sourceConfig.port(mysqlPort);
        sourceConfig.username(mysqlUser);
        sourceConfig.password(mysqlPass);
        sourceConfig.databaseList(mysqlDb);
        sourceConfig.tableList(
                mysqlDb
                        + "."
                        + MATRIX_TABLE
                        + "|"
                        + MATRIX_TABLE_RENAMED
                        + "|"
                        + MATRIX_TABLE_LIKE);
        sourceConfig.databaseType(DatabaseType.TIDB);
        sourceConfig.eventTime(EventTime.TIDB_TSO);
        sourceConfig.includeSchemaChanges(true);
        sourceConfig.kafkaBootstrapServers(kafkaServers);
        sourceConfig.kafkaGroupId(kafkaGroup);
        sourceConfig.kafkaTopics(kafkaTopic);
        sourceConfig.startupOptions(StartupOptions.initial());
        sourceConfig.serverTimeZone("Asia/Shanghai");
        sourceConfig.messageFormat(MessageFormat.CANAL);
        sourceConfig.ddlParser(DdlParser.DRUID);
        sourceConfig.releaseSnapshotMetadata(true);
        sourceConfig.scanNewlyAddedTableEnabled(false);
        return sourceConfig;
    }

    // ──────────────────────── Helper methods ────────────────────────

    private static String resource(String path) throws Exception {
        try (InputStream in = DorisE2ETest.class.getClassLoader().getResourceAsStream(path)) {
            assertThat(in).as("Resource not found: %s", path).isNotNull();
            return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.joining("\n"));
        }
    }

    private static void executeMySql(String sql) throws SQLException {
        try (Statement stmt = mysqlConn.createStatement()) {
            LOG.info("MySQL executing SQL ({} chars)", sql.length());
            stmt.execute(sql);
        }
    }

    private static void executeDorisSql(String sql) throws SQLException {
        try (Statement stmt = dorisConn.createStatement()) {
            LOG.info("Doris executing SQL ({} chars)", sql.length());
            stmt.execute(sql);
        } catch (Exception e) {
            LOG.error("Doris executing SQL failed ({} chars)", sql.length(), e);
        }
    }

    private void waitForDoris(
            String dorisTable, String sqlFormat, java.util.function.Consumer<ResultSet> verifier) {
        String sql = String.format(sqlFormat, dorisTable);
        long deadline = System.currentTimeMillis() + pollTimeoutMs;
        int attempt = 0;
        while (System.currentTimeMillis() < deadline) {
            attempt++;
            try (Statement stmt = dorisConn.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {
                verifier.accept(rs);
                LOG.info("Doris verification passed on attempt {} for: {}", attempt, sql);
                return;
            } catch (AssertionError | SQLException e) {
                if (attempt <= 3 || attempt % 10 == 0) {
                    LOG.info(
                            "Doris verification pending (attempt {}): {} — {}",
                            attempt,
                            sql,
                            e.getMessage());
                }
                try {
                    Thread.sleep(pollIntervalMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for Doris", ie);
                }
            }
        }
        throw new AssertionError(
                "Timed out after " + pollTimeoutMs + "ms waiting for Doris query: " + sql);
    }

    private void waitForDorisColumn(String dorisTable, String columnName) {
        waitForDoris(
                dorisTable,
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                        + "WHERE TABLE_SCHEMA = '"
                        + dorisDb
                        + "' AND TABLE_NAME = '%s' AND COLUMN_NAME = '"
                        + columnName
                        + "'",
                rs -> {
                    try {
                        assertThat(rs.next()).as("Column %s should exist", columnName).isTrue();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private void waitForDorisColumnAbsence(String dorisTable, String columnName) {
        long deadline = System.currentTimeMillis() + pollTimeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (Statement stmt = dorisConn.createStatement();
                    ResultSet rs =
                            stmt.executeQuery(
                                    String.format(
                                            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                                                    + "WHERE TABLE_SCHEMA = '"
                                                    + dorisDb
                                                    + "' AND TABLE_NAME = '%s' AND COLUMN_NAME = '"
                                                    + columnName
                                                    + "'",
                                            dorisTable))) {
                if (!rs.next()) {
                    LOG.info("Column {} is absent from {} as expected", columnName, dorisTable);
                    return;
                }
            } catch (SQLException e) {
                // table might not exist yet — keep waiting
            }
            try {
                Thread.sleep(pollIntervalMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new AssertionError(
                "Column " + columnName + " still exists in " + dorisTable + " after timeout");
    }

    private static List<Map<String, String>> collectRows(ResultSet rs, String... columns)
            throws SQLException {
        List<Map<String, String>> rows = new ArrayList<>();
        while (rs.next()) {
            Map<String, String> row = new LinkedHashMap<>();
            for (String col : columns) {
                row.put(col, rs.getString(col));
            }
            rows.add(row);
        }
        return rows;
    }

    private static List<Map<String, String>> tryCollectRows(ResultSet rs, String... columns) {
        try {
            return collectRows(rs, columns);
        } catch (SQLException e) {
            return Collections.emptyList();
        }
    }
}
