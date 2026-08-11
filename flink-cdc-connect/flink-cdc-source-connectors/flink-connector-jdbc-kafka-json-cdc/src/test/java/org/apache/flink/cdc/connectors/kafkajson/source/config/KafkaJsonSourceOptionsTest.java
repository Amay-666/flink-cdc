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

package org.apache.flink.cdc.connectors.kafkajson.source.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit test for {@link KafkaJsonSourceOptions}. */
class KafkaJsonSourceOptionsTest {

    @Test
    void testOptionKeys() {
        assertEquals("scan.kafka.topics", KafkaJsonSourceOptions.SCAN_KAFKA_TOPICS.key());
        assertEquals(
                "properties.kafka.bootstrap.servers",
                KafkaJsonSourceOptions.KAFKA_BOOTSTRAP_SERVERS.key());
        assertEquals("properties.kafka.group.id", KafkaJsonSourceOptions.KAFKA_GROUP_ID.key());
        assertEquals("scan.message.format", KafkaJsonSourceOptions.MESSAGE_FORMAT.key());
        assertEquals("scan.message.event-time", KafkaJsonSourceOptions.EVENT_TIME.key());
        assertEquals("scan.boundary.mode", KafkaJsonSourceOptions.BOUNDARY_MODE.key());
        assertEquals("scan.kafka.startup.mode", KafkaJsonSourceOptions.KAFKA_STARTUP_MODE.key());
        assertEquals("scan.ddl.parser", KafkaJsonSourceOptions.CANAL_DDL_PARSER.key());
    }

    @Test
    void testDefaultValues() {
        assertEquals(3306, KafkaJsonSourceOptions.CANAL_MYSQL_PORT.defaultValue());
        assertEquals("canal", KafkaJsonSourceOptions.MESSAGE_FORMAT.defaultValue());
        assertEquals("es", KafkaJsonSourceOptions.EVENT_TIME.defaultValue());
        assertEquals("exactly-once", KafkaJsonSourceOptions.BOUNDARY_MODE.defaultValue());
        assertEquals("earliest", KafkaJsonSourceOptions.KAFKA_STARTUP_MODE.defaultValue());
        assertEquals("druid", KafkaJsonSourceOptions.CANAL_DDL_PARSER.defaultValue());
    }
}
