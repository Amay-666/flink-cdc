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

import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for {@link KafkaJsonSchema}'s single-table metadata filter: the filter that {@code
 * readTableSchema} passes to {@code JdbcConnection#readSchema} so a single-table request does not
 * fetch the metadata of every table matching the include-list.
 */
class KafkaJsonSchemaTest {

    private static KafkaJsonSourceConfig config(String... tables) {
        return new KafkaJsonSourceConfigFactory()
                .hostname("localhost")
                .username("root")
                .password("123456")
                .databaseList("test")
                .tableList(tables)
                .kafkaBootstrapServers("localhost:9092")
                .kafkaTopics("canal-topic")
                .create(0);
    }

    @Test
    void testAdmitsOnlyTheRequestedTable() {
        // the include-list captures two tables ...
        Tables.TableFilter includeList =
                config("test.users", "test.orders").getTableFilters().dataCollectionFilter();
        // ... sanity: both are admitted by the include-list itself
        assertTrue(includeList.isIncluded(TableId.parse("test.users")));
        assertTrue(includeList.isIncluded(TableId.parse("test.orders")));

        // the single-table filter admits only the requested one of them: this is the conjunct
        // that turns an N-table metadata walk into a one-table fetch
        Tables.TableFilter filter =
                KafkaJsonSchema.singleTableFilter(TableId.parse("test.users"), includeList);
        assertTrue(filter.isIncluded(TableId.parse("test.users")));
        assertFalse(filter.isIncluded(TableId.parse("test.orders")));
        assertFalse(filter.isIncluded(TableId.parse("test.shipments")));
    }

    @Test
    void testKeepsFailFastForTablesOutsideTheIncludeList() {
        // requesting a table the include-list does not capture keeps the previous readTableSchema
        // behaviour: the table is not read, tables.forTable stays null and the requireNonNull
        // fails fast
        Tables.TableFilter includeList =
                config("test.users").getTableFilters().dataCollectionFilter();
        Tables.TableFilter filter =
                KafkaJsonSchema.singleTableFilter(TableId.parse("test.orders"), includeList);
        assertFalse(filter.isIncluded(TableId.parse("test.orders")));
    }
}
