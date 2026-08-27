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

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderWithCommit;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.UserCodeClassLoader;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for the {@link KafkaJsonSource#createReader} wiring: the reader must be a {@link
 * IncrementalSourceReaderWithCommit} so that the checkpoint-complete callback reaches the Kafka
 * offset-commit path (the released base reader would swallow it with an empty default).
 */
class KafkaJsonSourceReaderTest {

    @Test
    void testCreateReaderBuildsCommitReader() throws Exception {
        KafkaJsonSource<String> source =
                KafkaJsonSourceBuilder.<String>builder()
                        .hostname("localhost")
                        .username("root")
                        .password("x")
                        .databaseList("test")
                        .tableList("test.users")
                        .kafkaBootstrapServers("bootstrap")
                        .kafkaTopics("t")
                        .serverTimeZone("UTC")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();

        assertTrue(
                source.createReader(new StubSourceReaderContext())
                        instanceof IncrementalSourceReaderWithCommit);
    }

    /** Minimal {@link SourceReaderContext} stub: only the methods {@code createReader} touches. */
    private static final class StubSourceReaderContext implements SourceReaderContext {

        @Override
        public SourceReaderMetricGroup metricGroup() {
            return UnregisteredMetricsGroup.createSourceReaderMetricGroup();
        }

        @Override
        public Configuration getConfiguration() {
            return new Configuration();
        }

        @Override
        public String getLocalHostName() {
            return "localhost";
        }

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public void sendSplitRequest() {
            // no-op for the unit test
        }

        @Override
        public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {
            // no-op for the unit test
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return null;
        }
    }
}
