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

package org.apache.flink.cdc.connectors.canal.source.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit test for {@link CanalSourceOptions}. */
class CanalSourceOptionsTest {

    @Test
    void testOptionKeys() {
        assertEquals("scan.kafka.topics", CanalSourceOptions.SCAN_KAFKA_TOPICS.key());
        assertEquals(
                "properties.kafka.bootstrap.servers",
                CanalSourceOptions.KAFKA_BOOTSTRAP_SERVERS.key());
        assertEquals("properties.kafka.group.id", CanalSourceOptions.KAFKA_GROUP_ID.key());
        assertEquals("scan.message.format", CanalSourceOptions.MESSAGE_FORMAT.key());
        assertEquals("scan.canal.event-time", CanalSourceOptions.EVENT_TIME.key());
        assertEquals("scan.canal.boundary.mode", CanalSourceOptions.BOUNDARY_MODE.key());
        assertEquals("scan.kafka.startup.mode", CanalSourceOptions.KAFKA_STARTUP_MODE.key());
        assertEquals("scan.canalddl.parser", CanalSourceOptions.CANAL_DDL_PARSER.key());
    }

    @Test
    void testDefaultValues() {
        assertEquals(3306, CanalSourceOptions.CANAL_MYSQL_PORT.defaultValue());
        assertEquals("canal", CanalSourceOptions.MESSAGE_FORMAT.defaultValue());
        assertEquals("es", CanalSourceOptions.EVENT_TIME.defaultValue());
        assertEquals("exactly-once", CanalSourceOptions.BOUNDARY_MODE.defaultValue());
        assertEquals("earliest", CanalSourceOptions.KAFKA_STARTUP_MODE.defaultValue());
        assertEquals("druid", CanalSourceOptions.CANAL_DDL_PARSER.defaultValue());
    }
}
