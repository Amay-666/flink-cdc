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

package org.apache.flink.cdc.connectors.kafkajson.source.fetch;

import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.DatabaseType;
import org.apache.flink.cdc.connectors.kafkajson.source.dialect.KafkaJsonDialect;
import org.apache.flink.cdc.connectors.kafkajson.source.dialect.KafkaJsonTiDBDialect;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for the per-reader probing view of the shared {@link KafkaJsonDialect}. */
class KafkaJsonOffsetProbingDialectTest {

    @Test
    void testMysqlWatermarkProbesGoThroughTheReusableSupplier() {
        KafkaJsonSourceConfig config = config(DatabaseType.MYSQL);
        KafkaJsonDialect delegate = new KafkaJsonDialect(config);
        // The probe stands in for the reader's reusable KafkaJsonOffsetSupplier: every
        // displayCurrentOffset call must land on it. With the throw-away path the call would
        // instead try to open a real Kafka consumer against "bootstrap" and fail, so the
        // assertions below also prove the delegate's Kafka sampling was bypassed.
        AtomicInteger probes = new AtomicInteger();
        KafkaJsonOffsetProbingDialect view =
                new KafkaJsonOffsetProbingDialect(
                        delegate,
                        () -> new KafkaJsonOffset(1000 + probes.getAndIncrement(), -1, -1));

        assertEquals(new KafkaJsonOffset(1000, -1, -1), view.displayCurrentOffset(config));
        assertEquals(new KafkaJsonOffset(1001, -1, -1), view.displayCurrentOffset(config));
        assertEquals(2, probes.get());
    }

    @Test
    void testTidbWatermarkProbesGoThroughTheDelegate() {
        KafkaJsonSourceConfig config = config(DatabaseType.TIDB);
        KafkaJsonTiDBDialect delegate = new KafkaJsonTiDBDialect(config);
        // The TiDB TSO boundary comes from the database, not from Kafka, so those probes must
        // keep going through the delegate; its own test seam stands in for the TSO query here.
        delegate.setTidbOffsetSupplierForTesting(() -> new KafkaJsonOffset(2000, -1, -1));
        AtomicInteger probes = new AtomicInteger();
        KafkaJsonOffsetProbingDialect view =
                new KafkaJsonOffsetProbingDialect(
                        delegate,
                        () -> new KafkaJsonOffset(3000 + probes.getAndIncrement(), -1, -1));

        assertEquals(new KafkaJsonOffset(2000, -1, -1), view.displayCurrentOffset(config));
        assertEquals(0, probes.get());
    }

    @Test
    void testEverythingElseDelegatesToTheSharedDialect() {
        KafkaJsonSourceConfig config = config(DatabaseType.MYSQL);
        KafkaJsonDialect delegate = new KafkaJsonDialect(config);
        KafkaJsonOffsetProbingDialect view =
                new KafkaJsonOffsetProbingDialect(
                        delegate, () -> KafkaJsonOffset.INITIAL_OFFSET);

        assertEquals(delegate.getName(), view.getName());
        assertTrue(view.isIncludeDataCollection(config, TableId.parse("test.users")));
    }

    private static KafkaJsonSourceConfig config(DatabaseType databaseType) {
        return new KafkaJsonSourceConfigFactory()
                .hostname("localhost")
                .username("root")
                .password("x")
                .databaseList("test")
                .tableList("test.users")
                .kafkaBootstrapServers("bootstrap")
                .kafkaTopics("t")
                .serverTimeZone("UTC")
                .databaseType(databaseType)
                .create(0);
    }
}
