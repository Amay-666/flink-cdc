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

package org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterColumnCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.sink.KafkaJsonDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisMetadataApplier;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer.RecordedRequest;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer.Response;
import org.apache.flink.configuration.Configuration;

import org.junit.Test;

import java.util.Collections;

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
            assertThat(request.path).isEqualTo("/api/query/shop");
            assertThat(request.body)
                    .isEqualTo(
                            "{\"sql\":\"CREATE TABLE IF NOT EXISTS `shop`.`orders` "
                                    + "(`id` INT) UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS AUTO "
                                    + "PROPERTIES (\\\"enable_batch_delete_by_default\\\" = \\\"true\\\")\"}");
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
            assertThat(server.recorded.get(1).path).isEqualTo("/api/query/shop");
            assertThat(server.recorded.get(1).body)
                    .isEqualTo(
                            "{\"sql\":\"ALTER TABLE `shop`.`orders` MODIFY COLUMN `name` COMMENT 'display name'\"}");
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
                                    assertThat(
                                                    ((SchemaEvolveException) e)
                                                            .getExceptionMessage())
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
            assertThat(server.recorded.get(0).path).isEqualTo("/api/query/dws_shop");
            assertThat(server.recorded.get(0).body)
                    .contains("CREATE TABLE IF NOT EXISTS `dws_shop`.`ods_orders`");
        }
    }

    private DorisMetadataApplier applier(MockDorisServer server) {
        Configuration config = new Configuration();
        config.set(DorisDataSinkOptions.FENODES, server.endpoint());
        config.set(DorisDataSinkOptions.USERNAME, "root");
        config.set(DorisDataSinkOptions.PASSWORD, "123456");
        return new DorisMetadataApplier(new DorisDataSinkOptions(config));
    }
}
