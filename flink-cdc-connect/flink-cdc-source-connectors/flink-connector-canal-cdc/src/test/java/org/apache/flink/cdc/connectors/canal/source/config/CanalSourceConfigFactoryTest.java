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

import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit test for {@link CanalSourceConfigFactory} and {@link CanalSourceConfig}. */
class CanalSourceConfigFactoryTest {

    private CanalSourceConfigFactory buildFactory() {
        return new CanalSourceConfigFactory()
                .hostname("localhost")
                .username("root")
                .password("123456")
                .databaseList("test")
                .tableList("test.users")
                .kafkaBootstrapServers("localhost:9092")
                .kafkaGroupId("group-1")
                .kafkaTopics("canal-topic");
    }

    @Test
    void testCreateWithKafkaConfig() {
        CanalSourceConfig config = buildFactory().create(0);

        assertEquals(0, config.getSubtaskId());
        assertEquals("localhost", config.getHostname());
        assertEquals(3306, config.getPort());
        assertEquals("root", config.getUsername());
        assertEquals("123456", config.getPassword());
        assertEquals(Collections.singletonList("test"), config.getDatabaseList());
        assertEquals(Collections.singletonList("test.users"), config.getTableList());
        assertEquals("localhost:9092", config.getKafkaBootstrapServers());
        assertEquals("group-1", config.getKafkaGroupId());
        assertEquals(Collections.singletonList("canal-topic"), config.getKafkaTopics());
        assertEquals(CanalSourceOptions.MessageFormat.CANAL, config.getMessageFormat());
        assertEquals(CanalSourceOptions.EventTime.ES, config.getEventTime());
        assertEquals(CanalSourceOptions.BoundaryMode.EXACTLY_ONCE, config.getBoundaryMode());
        assertEquals(CanalSourceOptions.KafkaStartupMode.EARLIEST, config.getKafkaStartupMode());
        assertEquals(CanalSourceOptions.DdlParser.DRUID, config.getDdlParser());
        assertEquals("com.mysql.cj.jdbc.Driver", config.getDriverClassName());
    }

    @Test
    void testCustomKafkaOptions() {
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("max.poll.records", "500");

        CanalSourceConfig config =
                buildFactory()
                        .messageFormat(CanalSourceOptions.MessageFormat.DEBEZIUM)
                        .eventTime(CanalSourceOptions.EventTime.TS)
                        .boundaryMode(CanalSourceOptions.BoundaryMode.AT_LEAST_ONCE)
                        .kafkaStartupMode(CanalSourceOptions.KafkaStartupMode.LATEST)
                        .ddlParser(CanalSourceOptions.DdlParser.DEBEZIUM)
                        .kafkaProperties(kafkaProperties)
                        .create(0);

        assertEquals(CanalSourceOptions.MessageFormat.DEBEZIUM, config.getMessageFormat());
        assertEquals(CanalSourceOptions.EventTime.TS, config.getEventTime());
        assertEquals(CanalSourceOptions.BoundaryMode.AT_LEAST_ONCE, config.getBoundaryMode());
        assertEquals(CanalSourceOptions.KafkaStartupMode.LATEST, config.getKafkaStartupMode());
        assertEquals(CanalSourceOptions.DdlParser.DEBEZIUM, config.getDdlParser());
        assertEquals("500", config.getKafkaProperties().getProperty("max.poll.records"));
    }

    @Test
    void testSubtaskIdIsolation() {
        CanalSourceConfig config =
                new CanalSourceConfigFactory()
                        .hostname("localhost")
                        .username("root")
                        .password("x")
                        .kafkaBootstrapServers("b")
                        .kafkaTopics("t")
                        .create(3);
        assertEquals(3, config.getSubtaskId());
    }

    @Test
    void testMissingRequiredHostnameThrows() {
        assertThrows(
                NullPointerException.class,
                () ->
                        new CanalSourceConfigFactory()
                                .username("u")
                                .password("p")
                                .create(0));
    }
}
