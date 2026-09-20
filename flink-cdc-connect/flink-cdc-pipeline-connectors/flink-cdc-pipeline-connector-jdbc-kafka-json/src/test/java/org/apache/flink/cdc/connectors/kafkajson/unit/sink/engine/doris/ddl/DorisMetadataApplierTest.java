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

package org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.ddl;

import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterColumnCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.sink.KafkaJsonDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.ddl.DorisMetadataApplier;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer.RecordedRequest;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer.Response;
import org.apache.flink.configuration.Configuration;

import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link DorisMetadataApplier}. */
public class DorisMetadataApplierTest {

    private static final TableId ORDERS = TableId.tableId("shop", "orders");

    @Test
    public void testApplyCreateTable() throws Exception {
        try (MockDorisServer server =
                new MockDorisServer(req -> Response.ok("{\"code\":0,\"msg\":\"OK\"}"))) {
            DorisMetadataApplier applier = applier(server);

            applier.applySchemaChange(
                    new CreateTableEvent(
                            ORDERS,
                            Schema.newBuilder()
                                    .physicalColumn("id", DataTypes.INT())
                                    .primaryKey("id")
                                    .build()));

            assertThat(server.recorded).hasSize(1);
            RecordedRequest request = server.recorded.get(0);
            assertThat(request.path).isEqualTo("/api/query/default_cluster/shop");
            assertThat(request.body)
                    .isEqualTo(
                            "{\"stmt\":\"CREATE TABLE IF NOT EXISTS `shop`.`orders` "
                                    + "(`id` INT) UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS AUTO\"}");
        }
    }

    @Test
    public void testApplyStandardAndCustomEvents() throws Exception {
        Schema schema = Schema.newBuilder().physicalColumn("id", DataTypes.INT()).build();
        try (MockDorisServer server =
                new MockDorisServer(req -> Response.ok("{\"code\":0,\"msg\":\"OK\"}"))) {
            DorisMetadataApplier applier = applier(server);

            applier.applySchemaChange(new CreateTableEvent(ORDERS, schema));
            applier.applySchemaChange(
                    new AlterColumnCommentEvent(
                            ORDERS, Collections.singletonMap("name", "display name")));

            assertThat(server.recorded).hasSize(2);
            assertThat(server.recorded.get(0).body).contains("CREATE TABLE IF NOT EXISTS");
            assertThat(server.recorded.get(1).path).isEqualTo("/api/query/default_cluster/shop");
            assertThat(server.recorded.get(1).body)
                    .isEqualTo(
                            "{\"stmt\":\"ALTER TABLE `shop`.`orders` MODIFY COLUMN `name` COMMENT 'display name'\"}");
        }
    }

    @Test
    public void testApplyFailureWrappedInSchemaEvolveException() throws Exception {
        try (MockDorisServer server =
                new MockDorisServer(req -> Response.ok("{\"code\":1105,\"msg\":\"err\"}"))) {
            DorisMetadataApplier applier = applier(server);

            assertThatThrownBy(
                            () ->
                                    applier.applySchemaChange(
                                            new CreateTableEvent(
                                                    ORDERS,
                                                    Schema.newBuilder()
                                                            .physicalColumn("id", DataTypes.INT())
                                                            .build())))
                    .isInstanceOf(SchemaEvolveException.class)
                    // SchemaEvolveException keeps the message in getExceptionMessage() and wires
                    // the (null) cause into getMessage().
                    .satisfies(
                            e ->
                                    assertThat(((SchemaEvolveException) e).getExceptionMessage())
                                            .contains("code 1105"));
            // No retry on an application-level DDL error.
            assertThat(server.recorded).hasSize(1);
        }
    }

    @Test
    public void testTableNameMappingAppliedByApplier() throws Exception {
        Configuration config = new Configuration();
        config.set(KafkaJsonDataSinkOptions.DATABASE_PREFIX, "dws_");
        config.set(KafkaJsonDataSinkOptions.TABLE_PREFIX, "ods_");
        try (MockDorisServer server =
                new MockDorisServer(req -> Response.ok("{\"code\":0,\"msg\":\"OK\"}"))) {
            config.set(DorisDataSinkOptions.FENODES, server.endpoint());
            config.set(DorisDataSinkOptions.USERNAME, "root");
            config.set(DorisDataSinkOptions.PASSWORD, "123456");
            DorisMetadataApplier applier =
                    new DorisMetadataApplier(new DorisDataSinkOptions(config));
            applier.applySchemaChange(
                    new CreateTableEvent(
                            ORDERS,
                            Schema.newBuilder()
                                    .physicalColumn("id", DataTypes.INT())
                                    .primaryKey("id")
                                    .build()));

            assertThat(server.recorded).hasSize(1);
            assertThat(server.recorded.get(0).path)
                    .isEqualTo("/api/query/default_cluster/dws_shop");
            assertThat(server.recorded.get(0).body)
                    .contains("CREATE TABLE IF NOT EXISTS `dws_shop`.`ods_orders`");
        }
    }

    @Test
    public void testApplyAlterColumnTypeGrowthEmitsModify() throws Exception {
        Schema oldSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(100))
                        .primaryKey("id")
                        .build();
        // The mock returns FINISHED for the SHOW ALTER TABLE poll, so the applier returns
        // immediately after the DDL.
        try (MockDorisServer server = schemaChangeServer("FINISHED")) {
            DorisMetadataApplier applier = applier(server);

            applier.applyAlterColumnType(
                    new AlterColumnTypeEvent(
                            ORDERS, Collections.singletonMap("name", DataTypes.VARCHAR(300))),
                    Optional.of(oldSchema));

            // 2 requests: 1 DDL (MODIFY COLUMN) + 1 SHOW (poll for completion)
            assertThat(server.recorded).hasSize(2);
            assertThat(server.recorded.get(0).body)
                    .isEqualTo(
                            "{\"stmt\":\"ALTER TABLE `shop`.`orders` MODIFY COLUMN `name` VARCHAR(900)\"}");
            assertThat(server.recorded.get(1).body).contains("SHOW ALTER TABLE COLUMN");
        }
    }

    @Test
    public void testApplyAlterColumnTypeShrinkSendsNoRequest() throws Exception {
        Schema oldSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(300))
                        .primaryKey("id")
                        .build();
        try (MockDorisServer server =
                new MockDorisServer(req -> Response.ok("{\"code\":0,\"msg\":\"OK\"}"))) {
            DorisMetadataApplier applier = applier(server);

            applier.applyAlterColumnType(
                    new AlterColumnTypeEvent(
                            ORDERS, Collections.singletonMap("name", DataTypes.VARCHAR(100))),
                    Optional.of(oldSchema));

            // The shrink is skipped with a warning; no DDL reaches Doris, and no SHOW poll.
            assertThat(server.recorded).isEmpty();
        }
    }

    // ===== Schema change polling tests =====

    @Test
    public void testAlterColumnTypePollsRunningThenFinishes() throws Exception {
        Schema oldSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(100))
                        .primaryKey("id")
                        .build();
        AtomicInteger showCount = new AtomicInteger();
        try (MockDorisServer server =
                new MockDorisServer(
                        req -> {
                            if (req.body.contains("SHOW ALTER TABLE")) {
                                int n = showCount.getAndIncrement();
                                // First poll: RUNNING with progress 3/10
                                // Second poll: FINISHED (progress is null/empty when finished)
                                return Response.ok(
                                        n == 0
                                                ? showAlterResponse("RUNNING", "3/10")
                                                : showAlterResponse("FINISHED", ""));
                            }
                            return Response.ok("{\"code\":0,\"msg\":\"OK\"}");
                        })) {
            DorisMetadataApplier applier = applierWithFastPoll(server);

            applier.applyAlterColumnType(
                    new AlterColumnTypeEvent(
                            ORDERS, Collections.singletonMap("name", DataTypes.VARCHAR(300))),
                    Optional.of(oldSchema));

            // 3 requests: 1 DDL + 2 SHOW (RUNNING 3/10, then FINISHED)
            assertThat(server.recorded).hasSize(3);
            assertThat(server.recorded.get(1).body).contains("SHOW ALTER TABLE");
            assertThat(server.recorded.get(2).body).contains("SHOW ALTER TABLE");
        }
    }

    @Test
    public void testAlterColumnTypeThrowsOnCancelled() throws Exception {
        Schema oldSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(100))
                        .primaryKey("id")
                        .build();
        try (MockDorisServer server = schemaChangeServer("CANCELLED")) {
            DorisMetadataApplier applier = applierWithFastPoll(server);

            assertThatThrownBy(
                            () ->
                                    applier.applyAlterColumnType(
                                            new AlterColumnTypeEvent(
                                                    ORDERS,
                                                    Collections.singletonMap(
                                                            "name", DataTypes.VARCHAR(300))),
                                            Optional.of(oldSchema)))
                    .isInstanceOf(SchemaEvolveException.class)
                    .satisfies(
                            e ->
                                    assertThat(((SchemaEvolveException) e).getExceptionMessage())
                                            .contains("cancelled"));

            // 2 requests: 1 DDL + 1 SHOW (CANCELLED)
            assertThat(server.recorded).hasSize(2);
        }
    }

    @Test
    public void testAlterContentTypeNoRowsTreatedAsComplete() throws Exception {
        Schema oldSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(100))
                        .primaryKey("id")
                        .build();
        // Mock returns empty rows for SHOW — job may have been cleaned up already.
        try (MockDorisServer server =
                new MockDorisServer(
                        req -> {
                            if (req.body.contains("SHOW ALTER TABLE")) {
                                return Response.ok(
                                        "{\"code\":0,\"msg\":\"OK\",\"data\":{\"rows\":[]}}");
                            }
                            return Response.ok("{\"code\":0,\"msg\":\"OK\"}");
                        })) {
            DorisMetadataApplier applier = applierWithFastPoll(server);

            applier.applyAlterColumnType(
                    new AlterColumnTypeEvent(
                            ORDERS, Collections.singletonMap("name", DataTypes.VARCHAR(300))),
                    Optional.of(oldSchema));

            // 2 requests: 1 DDL + 1 SHOW (empty) — returns immediately, no further polling.
            assertThat(server.recorded).hasSize(2);
        }
    }

    @Test
    public void testAlterColumnTypeTimesOut() throws Exception {
        Schema oldSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(100))
                        .primaryKey("id")
                        .build();
        // Always RUNNING with stalled progress — never finishes.
        try (MockDorisServer server = schemaChangeServer("RUNNING", "0/0")) {
            Configuration config = new Configuration();
            config.set(DorisDataSinkOptions.FENODES, server.endpoint());
            config.set(DorisDataSinkOptions.USERNAME, "root");
            config.set(DorisDataSinkOptions.PASSWORD, "123456");
            config.set(DorisDataSinkOptions.SCHEMA_CHANGE_POLL_INTERVAL, Duration.ofMillis(10));
            config.set(DorisDataSinkOptions.SCHEMA_CHANGE_POLL_MAX_INTERVAL, Duration.ofMillis(20));
            config.set(DorisDataSinkOptions.SCHEMA_CHANGE_MAX_WAIT, Duration.ofMillis(100));
            DorisMetadataApplier applier =
                    new DorisMetadataApplier(new DorisDataSinkOptions(config));

            assertThatThrownBy(
                            () ->
                                    applier.applyAlterColumnType(
                                            new AlterColumnTypeEvent(
                                                    ORDERS,
                                                    Collections.singletonMap(
                                                            "name", DataTypes.VARCHAR(300))),
                                            Optional.of(oldSchema)))
                    .isInstanceOf(SchemaEvolveException.class)
                    .satisfies(
                            e ->
                                    assertThat(((SchemaEvolveException) e).getExceptionMessage())
                                            .contains("timed out"));
        }
    }

    @Test
    public void testProgressAdvancingAcrossMultiplePolls() throws Exception {
        // Progress advances: 2/10 → 5/10 → 8/10 → FINISHED.
        // Each advancing poll should reset the backoff to the initial interval.
        Schema oldSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(100))
                        .primaryKey("id")
                        .build();
        AtomicInteger showCount = new AtomicInteger();
        try (MockDorisServer server =
                new MockDorisServer(
                        req -> {
                            if (req.body.contains("SHOW ALTER TABLE")) {
                                int n = showCount.getAndIncrement();
                                switch (n) {
                                    case 0:
                                        return Response.ok(showAlterResponse("RUNNING", "2/10"));
                                    case 1:
                                        return Response.ok(showAlterResponse("RUNNING", "5/10"));
                                    case 2:
                                        return Response.ok(showAlterResponse("RUNNING", "8/10"));
                                    default:
                                        return Response.ok(showAlterResponse("FINISHED", ""));
                                }
                            }
                            return Response.ok("{\"code\":0,\"msg\":\"OK\"}");
                        })) {
            DorisMetadataApplier applier = applierWithFastPoll(server);

            applier.applyAlterColumnType(
                    new AlterColumnTypeEvent(
                            ORDERS, Collections.singletonMap("name", DataTypes.VARCHAR(300))),
                    Optional.of(oldSchema));

            // 5 requests: 1 DDL + 4 SHOW (2/10, 5/10, 8/10, FINISHED)
            assertThat(server.recorded).hasSize(5);
            // Verify the SHOW queries were sent
            for (int i = 1; i <= 4; i++) {
                assertThat(server.recorded.get(i).body).contains("SHOW ALTER TABLE");
            }
        }
    }

    @Test
    public void testStalledProgressDoesNotResetBackoff() throws Exception {
        // Progress stalls: 5/10 → 5/10 → 5/10 → FINISHED.
        // Backoff should keep increasing since progress doesn't advance.
        // With poll-interval=10ms and max-interval=20ms, and max-wait=200ms,
        // we can verify the applier doesn't poll too frequently.
        Schema oldSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.VARCHAR(100))
                        .primaryKey("id")
                        .build();
        AtomicInteger showCount = new AtomicInteger();
        try (MockDorisServer server =
                new MockDorisServer(
                        req -> {
                            if (req.body.contains("SHOW ALTER TABLE")) {
                                int n = showCount.getAndIncrement();
                                if (n < 3) {
                                    return Response.ok(showAlterResponse("RUNNING", "5/10"));
                                }
                                return Response.ok(showAlterResponse("FINISHED", ""));
                            }
                            return Response.ok("{\"code\":0,\"msg\":\"OK\"}");
                        })) {
            Configuration config = new Configuration();
            config.set(DorisDataSinkOptions.FENODES, server.endpoint());
            config.set(DorisDataSinkOptions.USERNAME, "root");
            config.set(DorisDataSinkOptions.PASSWORD, "123456");
            config.set(DorisDataSinkOptions.SCHEMA_CHANGE_POLL_INTERVAL, Duration.ofMillis(10));
            config.set(DorisDataSinkOptions.SCHEMA_CHANGE_POLL_MAX_INTERVAL, Duration.ofMillis(20));
            config.set(DorisDataSinkOptions.SCHEMA_CHANGE_MAX_WAIT, Duration.ofSeconds(30));
            DorisMetadataApplier applier =
                    new DorisMetadataApplier(new DorisDataSinkOptions(config));

            applier.applyAlterColumnType(
                    new AlterColumnTypeEvent(
                            ORDERS, Collections.singletonMap("name", DataTypes.VARCHAR(300))),
                    Optional.of(oldSchema));

            // 5 requests: 1 DDL + 4 SHOW (stalled 5/10 × 3, then FINISHED)
            assertThat(server.recorded).hasSize(5);
        }
    }

    /**
     * Returns a mock server that answers SHOW ALTER TABLE with the given state and empty progress.
     */
    private MockDorisServer schemaChangeServer(String state) throws IOException {
        return schemaChangeServer(state, "");
    }

    /** Returns a mock server that answers SHOW ALTER TABLE with the given state and progress. */
    private MockDorisServer schemaChangeServer(String state, String progress) throws IOException {
        return new MockDorisServer(
                req -> {
                    if (req.body.contains("SHOW ALTER TABLE")) {
                        return Response.ok(showAlterResponse(state, progress));
                    }
                    return Response.ok("{\"code\":0,\"msg\":\"OK\"}");
                });
    }

    /**
     * Builds a SHOW ALTER TABLE COLUMN response with the given state at index 9 and progress at
     * index 11. The 13 columns match Doris {@code SchemaChangeProcDir.TITLE_NAMES}: JobId,
     * TableName, CreateTime, FinishTime, IndexName, IndexId, OriginIndexId, SchemaVersion,
     * TransactionId, State, Msg, Progress, Timeout.
     *
     * <p>When state is FINISHED or CANCELLED, progress is typically empty/null in Doris.
     */
    private static String showAlterResponse(String state, String progress) {
        return "{\"code\":0,\"msg\":\"OK\",\"data\":{\"rows\":[[\"123\",\"orders\","
                + "\"2024-01-01\",\"2024-01-01\",\"\",\"\",\"\",\"\",\"\","
                + "\""
                + state
                + "\",\"\",\""
                + progress
                + "\",\"\"]]}}";
    }

    private DorisMetadataApplier applierWithFastPoll(MockDorisServer server) {
        Configuration config = new Configuration();
        config.set(DorisDataSinkOptions.FENODES, server.endpoint());
        config.set(DorisDataSinkOptions.USERNAME, "root");
        config.set(DorisDataSinkOptions.PASSWORD, "123456");
        config.set(DorisDataSinkOptions.SCHEMA_CHANGE_POLL_INTERVAL, Duration.ofMillis(10));
        config.set(DorisDataSinkOptions.SCHEMA_CHANGE_POLL_MAX_INTERVAL, Duration.ofMillis(20));
        return new DorisMetadataApplier(new DorisDataSinkOptions(config));
    }

    private DorisMetadataApplier applier(MockDorisServer server) {
        Configuration config = new Configuration();
        config.set(DorisDataSinkOptions.FENODES, server.endpoint());
        config.set(DorisDataSinkOptions.USERNAME, "root");
        config.set(DorisDataSinkOptions.PASSWORD, "123456");
        return new DorisMetadataApplier(new DorisDataSinkOptions(config));
    }
}
