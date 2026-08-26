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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test for {@link KafkaJsonDialectFactory}. */
class KafkaJsonDialectFactoryTest {

    private static KafkaJsonSourceConfig config(DatabaseType databaseType) {
        return new KafkaJsonSourceConfigFactory()
                .hostname("localhost")
                .username("root")
                .password("x")
                .databaseList("test")
                .tableList("test.users")
                .kafkaBootstrapServers("b")
                .kafkaTopics("t")
                .databaseType(databaseType)
                .eventTime(EventTime.ES)
                .create(0);
    }

    @Test
    void testMySqlCreatesMySqlDialect() {
        KafkaJsonDialect dialect =
                KafkaJsonDialectFactory.create(DatabaseType.MYSQL, config(DatabaseType.MYSQL));
        assertTrue(dialect instanceof KafkaJsonDialect);
        assertFalse(dialect instanceof KafkaJsonTiDBDialect);
    }

    @Test
    void testTidbCreatesTidbDialect() {
        KafkaJsonDialect dialect =
                KafkaJsonDialectFactory.create(DatabaseType.TIDB, config(DatabaseType.TIDB));
        assertTrue(dialect instanceof KafkaJsonTiDBDialect);
    }
}
