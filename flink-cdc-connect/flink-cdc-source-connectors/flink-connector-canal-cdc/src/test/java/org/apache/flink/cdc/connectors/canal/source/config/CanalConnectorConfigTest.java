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

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for the minimal Debezium MySQL connector config used by the Canal source, which is
 * provided through {@link CanalSourceConfig#getDbzConnectorConfig()}.
 */
class CanalConnectorConfigTest {

    private CanalSourceConfig buildConfig() {
        return new CanalSourceConfigFactory()
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
    void testDbzConnectorConfigLogicalName() {
        MySqlConnectorConfig dbzConfig = buildConfig().getDbzConnectorConfig();
        assertNotNull(dbzConfig);
        assertEquals("canal_cdc_source", dbzConfig.getLogicalName());
    }

    @Test
    void testTableFilters() {
        RelationalTableFilters filters = buildConfig().getTableFilters();
        assertNotNull(filters);
        assertTrue(filters.dataCollectionFilter().isIncluded(TableId.parse("test.users")));
        assertFalse(filters.dataCollectionFilter().isIncluded(TableId.parse("test.orders")));
        assertFalse(filters.dataCollectionFilter().isIncluded(TableId.parse("other.users")));
    }
}
