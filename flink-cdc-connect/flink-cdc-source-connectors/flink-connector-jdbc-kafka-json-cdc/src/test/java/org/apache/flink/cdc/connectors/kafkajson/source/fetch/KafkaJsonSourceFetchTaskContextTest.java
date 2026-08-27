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
import org.apache.flink.cdc.connectors.kafkajson.source.dialect.KafkaJsonDialect;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The streaming Kafka consumer is the only one that commits offsets to the consumer group (on
 * checkpoint completion), so it must be created with a user-provided {@code
 * properties.kafka.group.id} — never a random throw-away id.
 */
class KafkaJsonSourceFetchTaskContextTest {

    @Test
    void testStreamingConsumerRequiresUserGroupId() {
        KafkaJsonSourceConfig config = config(null);
        KafkaJsonSourceFetchTaskContext context =
                new KafkaJsonSourceFetchTaskContext(config, new KafkaJsonDialect(config));

        assertThrows(IllegalArgumentException.class, context::getKafkaConsumer);
    }

    @Test
    void testStreamingConsumerUsesConfiguredGroupId() {
        KafkaJsonSourceConfig config = config("test-group");
        KafkaJsonSourceFetchTaskContext context =
                new KafkaJsonSourceFetchTaskContext(config, new KafkaJsonDialect(config));

        try (KafkaConsumer<String, String> consumer = context.getKafkaConsumer()) {
            assertEquals("test-group", consumer.groupMetadata().groupId());
        }
    }

    private static KafkaJsonSourceConfig config(String groupId) {
        KafkaJsonSourceConfigFactory factory = new KafkaJsonSourceConfigFactory();
        factory.hostname("localhost")
                .username("root")
                .password("x")
                .kafkaBootstrapServers("localhost:9092")
                .kafkaTopics("t");
        if (groupId != null) {
            factory.kafkaGroupId(groupId);
        }
        return (KafkaJsonSourceConfig) factory.create(0);
    }
}
