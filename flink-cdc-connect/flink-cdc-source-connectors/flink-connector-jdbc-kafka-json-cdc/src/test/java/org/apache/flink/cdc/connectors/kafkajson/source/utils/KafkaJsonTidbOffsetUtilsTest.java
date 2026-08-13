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

package org.apache.flink.cdc.connectors.kafkajson.source.utils;

import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.DatabaseType;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Unit test for {@link KafkaJsonTidbOffsetUtils}. */
class KafkaJsonTidbOffsetUtilsTest {

    @Test
    void testTsoToEventTimeTakesPhysicalPart() {
        long eventTimeMs = 1_693_310_000_000L;
        // A TSO is the physical time (millis since epoch) in the upper 46 bits plus an 18-bit
        // logical counter; only the physical part is the event time.
        long tso = (eventTimeMs << 18) | 0x2FFFFL;
        assertEquals(eventTimeMs, KafkaJsonTidbOffsetUtils.tsoToEventTime(tso));
    }

    @Test
    void testTsoToEventTimeZero() {
        assertEquals(0L, KafkaJsonTidbOffsetUtils.tsoToEventTime(0L));
    }

    @Test
    void testQueryCurrentOffsetReturnsNullOnConnectionFailure() {
        KafkaJsonSourceConfig config =
                new KafkaJsonSourceConfigFactory()
                        .hostname("127.0.0.1")
                        .port(1) // nothing listens on port 1: connection is refused immediately
                        .username("root")
                        .password("x")
                        .databaseList("test")
                        .tableList("test.users")
                        .kafkaBootstrapServers("b")
                        .kafkaTopics("t")
                        .databaseType(DatabaseType.TIDB)
                        .connectTimeout(Duration.ofSeconds(1))
                        .create(0);
        // A failed TSO query is a best-effort fallback, never a failure of the snapshot.
        assertNull(KafkaJsonTidbOffsetUtils.queryCurrentOffset(config));
    }
}
