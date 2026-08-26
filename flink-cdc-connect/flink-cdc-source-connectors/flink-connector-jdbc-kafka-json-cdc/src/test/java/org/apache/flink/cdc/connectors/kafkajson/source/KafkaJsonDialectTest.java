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

import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.DatabaseType;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.EventTime;
import org.apache.flink.cdc.connectors.kafkajson.source.connection.KafkaJsonJdbcConnection;
import org.apache.flink.cdc.connectors.kafkajson.source.dialect.KafkaJsonDialect;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit test for {@link KafkaJsonDialect} (the MySQL dialect). */
class KafkaJsonDialectTest {

    private static KafkaJsonSourceConfig config(DatabaseType databaseType, EventTime eventTime) {
        return new KafkaJsonSourceConfigFactory()
                .hostname("localhost")
                .username("root")
                .password("x")
                .databaseList("test")
                .tableList("test.users")
                .kafkaBootstrapServers("b")
                .kafkaTopics("t")
                .databaseType(databaseType)
                .eventTime(eventTime)
                .create(0);
    }

    @Test
    void testOpenJdbcConnectionReturnsKafkaJsonJdbcConnection() {
        // The KafkaJsonJdbcConnection drops the column default that the MySQL driver reports as a
        // literal string (e.g. `0x` for a BINARY default), which would otherwise fail Debezium's
        // TableSchemaBuilder during the snapshot schema read.
        KafkaJsonSourceConfig config = config(DatabaseType.MYSQL, EventTime.ES);
        KafkaJsonDialect dialect = new KafkaJsonDialect(config);
        // openJdbcConnection only constructs the lazy connection; no socket is opened here.
        assertThat(dialect.openJdbcConnection(config)).isInstanceOf(KafkaJsonJdbcConnection.class);
    }

    @Test
    void testMySqlUsesKafkaSampledBoundary() {
        // MySQL has no TSO: the snapshot boundary always comes from the Kafka-sampled position.
        KafkaJsonSourceConfig config = config(DatabaseType.MYSQL, EventTime.ES);
        KafkaJsonDialect dialect = new KafkaJsonDialect(config);
        KafkaJsonOffset kafka = new KafkaJsonOffset(42, 0, 0);
        dialect.setCurrentOffsetSupplierForTesting(() -> kafka);
        assertEquals(kafka, dialect.displayCurrentOffset(config));
    }
}
