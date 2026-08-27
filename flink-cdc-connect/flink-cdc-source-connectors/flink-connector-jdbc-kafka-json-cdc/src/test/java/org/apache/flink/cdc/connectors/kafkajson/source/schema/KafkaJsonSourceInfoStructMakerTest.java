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

package org.apache.flink.cdc.connectors.kafkajson.source.schema;

import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test for {@link KafkaJsonSourceInfoStructMaker}. */
class KafkaJsonSourceInfoStructMakerTest {

    private KafkaJsonSourceConfig buildConfig() {
        return new KafkaJsonSourceConfigFactory()
                .hostname("localhost")
                .username("root")
                .password("x")
                .databaseList("test")
                .tableList("test.users")
                .kafkaBootstrapServers("b")
                .kafkaTopics("t")
                .create(0);
    }

    @Test
    void testStreamSourceStruct() {
        MySqlConnectorConfig dbzConfig = buildConfig().getDbzConnectorConfig();
        KafkaJsonSourceInfoStructMaker maker =
                new KafkaJsonSourceInfoStructMaker(
                        "kafka.json", KafkaJsonSourceInfoStructMaker.DEBEZIUM_VERSION, dbzConfig);

        KafkaJsonSourceInfo sourceInfo =
                new KafkaJsonSourceInfo(
                        dbzConfig,
                        "test",
                        "users",
                        1598752886000L,
                        1598752886000L,
                        1598752887000L,
                        SnapshotRecord.FALSE);
        Struct struct = maker.struct(sourceInfo);

        assertEquals("1.9.8.Final", struct.getString("version"));
        assertEquals("kafka.json", struct.getString("connector"));
        assertEquals("kafka_json_cdc_source", struct.getString("name"));
        assertEquals(1598752886000L, struct.getInt64("ts_ms"));
        // SnapshotRecord.toSource is a no-op for FALSE (matches Debezium stream records)
        assertNull(struct.getString("snapshot"));
        assertEquals("test", struct.getString("db"));
        assertEquals("users", struct.getString("table"));
        assertEquals(1598752886000L, struct.getInt64("es"));
        assertEquals(1598752887000L, struct.getInt64("ts"));
    }

    @Test
    void testSnapshotSourceStruct() {
        MySqlConnectorConfig dbzConfig = buildConfig().getDbzConnectorConfig();
        KafkaJsonSourceInfoStructMaker maker =
                new KafkaJsonSourceInfoStructMaker(
                        "kafka.json", KafkaJsonSourceInfoStructMaker.DEBEZIUM_VERSION, dbzConfig);

        KafkaJsonSourceInfo sourceInfo =
                new KafkaJsonSourceInfo(
                        dbzConfig, "test", "users", 1598752886000L, 0L, 0L, SnapshotRecord.TRUE);
        Struct struct = maker.struct(sourceInfo);

        assertEquals("true", struct.getString("snapshot"));
        // no es/ts for snapshot records (both are optional schema fields, left unset)
        assertTrue(struct.schema().field("es").schema().isOptional());
        assertEquals(null, struct.get("es"));
        assertEquals(null, struct.get("ts"));
    }

    @Test
    void testSchemaShape() {
        MySqlConnectorConfig dbzConfig = buildConfig().getDbzConnectorConfig();
        KafkaJsonSourceInfoStructMaker maker =
                new KafkaJsonSourceInfoStructMaker(
                        "kafka.json", KafkaJsonSourceInfoStructMaker.DEBEZIUM_VERSION, dbzConfig);
        Schema schema = maker.schema();

        assertEquals("io.debezium.connector.kafka.json.Source", schema.name());
        assertTrue(schema.field("version") != null);
        assertTrue(schema.field("connector") != null);
        assertTrue(schema.field("name") != null);
        assertTrue(schema.field("ts_ms") != null);
        assertTrue(schema.field("snapshot") != null);
        assertTrue(schema.field("db") != null);
        assertTrue(schema.field("table") != null);
        assertTrue(schema.field("es") != null);
        assertTrue(schema.field("ts") != null);
    }
}
