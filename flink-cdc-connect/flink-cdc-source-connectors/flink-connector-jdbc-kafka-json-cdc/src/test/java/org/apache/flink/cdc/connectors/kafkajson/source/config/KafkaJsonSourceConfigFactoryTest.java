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

import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit test for {@link KafkaJsonSourceConfigFactory} and {@link KafkaJsonSourceConfig}. */
class KafkaJsonSourceConfigFactoryTest {

    private KafkaJsonSourceConfigFactory buildFactory() {
        return new KafkaJsonSourceConfigFactory()
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
        KafkaJsonSourceConfig config = buildFactory().create(0);

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
        assertEquals(KafkaJsonSourceOptions.MessageFormat.CANAL, config.getMessageFormat());
        assertEquals(KafkaJsonSourceOptions.DatabaseType.MYSQL, config.getDatabaseType());
        assertEquals(KafkaJsonSourceOptions.EventTime.ES, config.getEventTime());
        assertEquals(KafkaJsonSourceOptions.BoundaryMode.EXACTLY_ONCE, config.getBoundaryMode());
        assertEquals(
                KafkaJsonSourceOptions.KafkaStartupMode.EARLIEST, config.getKafkaStartupMode());
        assertEquals(KafkaJsonSourceOptions.DdlParser.DRUID, config.getDdlParser());
        assertEquals("com.mysql.cj.jdbc.Driver", config.getDriverClassName());
    }

    @Test
    void testCustomKafkaOptions() {
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("max.poll.records", "500");

        KafkaJsonSourceConfig config =
                buildFactory()
                        .messageFormat(KafkaJsonSourceOptions.MessageFormat.CANAL)
                        .eventTime(KafkaJsonSourceOptions.EventTime.TS)
                        .boundaryMode(KafkaJsonSourceOptions.BoundaryMode.AT_LEAST_ONCE)
                        .kafkaStartupMode(KafkaJsonSourceOptions.KafkaStartupMode.LATEST)
                        .ddlParser(KafkaJsonSourceOptions.DdlParser.DEBEZIUM)
                        .kafkaProperties(kafkaProperties)
                        .create(0);

        assertEquals(KafkaJsonSourceOptions.MessageFormat.CANAL, config.getMessageFormat());
        assertEquals(KafkaJsonSourceOptions.EventTime.TS, config.getEventTime());
        assertEquals(KafkaJsonSourceOptions.BoundaryMode.AT_LEAST_ONCE, config.getBoundaryMode());
        assertEquals(KafkaJsonSourceOptions.KafkaStartupMode.LATEST, config.getKafkaStartupMode());
        assertEquals(KafkaJsonSourceOptions.DdlParser.DEBEZIUM, config.getDdlParser());
        assertEquals("500", config.getKafkaProperties().getProperty("max.poll.records"));
    }

    /**
     * The extension seam fails fast at job setup: a declared-but-unimplemented message format
     * ({@code scan.message.format=debezium}) must not build a config that later fails deep inside
     * the read pipeline.
     */
    @Test
    void testUnimplementedMessageFormatFailsFast() {
        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                buildFactory()
                                        .messageFormat(
                                                KafkaJsonSourceOptions.MessageFormat.DEBEZIUM)
                                        .create(0));
        assertEquals(true, e.getMessage().contains("scan.message.format"));
        assertEquals(true, e.getMessage().contains("not implemented"));
    }

    /**
     * The extension seam fails fast at job setup: a declared-but-unimplemented database type
     * ({@code scan.database.type=postgres}) must not build a config.
     */
    @Test
    void testUnimplementedDatabaseTypeFailsFast() {
        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                buildFactory()
                                        .databaseType(KafkaJsonSourceOptions.DatabaseType.POSTGRES)
                                        .create(0));
        assertEquals(true, e.getMessage().contains("scan.database.type"));
        assertEquals(true, e.getMessage().contains("not implemented"));
    }

    /**
     * 'tidb' is accepted as an alias that reuses the MySQL-compatible JDBC/dialect path (TiDB
     * speaks the MySQL wire protocol), so it must not fail fast at job setup.
     */
    @Test
    void testTidbDatabaseTypeIsAccepted() {
        KafkaJsonSourceConfig config =
                buildFactory().databaseType(KafkaJsonSourceOptions.DatabaseType.TIDB).create(0);
        assertEquals(KafkaJsonSourceOptions.DatabaseType.TIDB, config.getDatabaseType());
        assertEquals("com.mysql.cj.jdbc.Driver", config.getDriverClassName());
    }

    @Test
    void testSubtaskIdIsolation() {
        KafkaJsonSourceConfig config =
                new KafkaJsonSourceConfigFactory()
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
                () -> new KafkaJsonSourceConfigFactory().username("u").password("p").create(0));
    }
}
