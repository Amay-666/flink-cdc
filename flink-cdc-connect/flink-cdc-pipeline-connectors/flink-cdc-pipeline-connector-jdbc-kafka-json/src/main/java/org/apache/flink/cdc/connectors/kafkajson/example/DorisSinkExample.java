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

package org.apache.flink.cdc.connectors.kafkajson.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.KafkaJsonDataSinkBuilder;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkDialect;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.source.KafkaJsonDataSource;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.time.ZoneId;

/**
 * Runnable end-to-end assembly of the connector's own DataStream topology: a canal-sourced stream
 * (read from Kafka) through the {@link KafkaJsonDataSinkBuilder} chain into Doris.
 *
 * <p>No YAML pipeline, no registered {@code DataSinkFactory} SPI and no pipeline jar: the job is
 * assembled programmatically. The schema operator (released {@code SchemaOperator} + connector
 * coordinator) blocks data on a schema change until every sink subtask has flushed, executes the DDL
 * over Doris HTTP and resumes; the partitioning chain keys rows by {@code (table original name,
 * primary key)} so a large table is spread across the sink subtasks.
 *
 * <p>The static {@link #buildSource}/{@link #buildSink} methods are the reusable seams — tests and
 * the simulated-sink ITCase drive the same topology.
 */
public class DorisSinkExample {

    /** The connector source identifier of the underlying canal connector. */
    public static final String CANAL_SOURCE_IDENTIFIER = "jdbc-kafka-json-cdc";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000L);

        KafkaJsonSourceConfigFactory configFactory =
                new KafkaJsonSourceConfigFactory()
                        .hostname("localhost")
                        .port(3306)
                        .username("root")
                        .password("123456")
                        .databaseList("test")
                        .tableList("test.users")
                        .kafkaBootstrapServers("localhost:9092")
                        .kafkaGroupId("kafka-json-doris")
                        .kafkaTopics("canal-topic")
                        .startupOptions(StartupOptions.latest())
                        .serverTimeZone("Asia/Shanghai");

        DataStream<Event> source = buildSource(env, configFactory);

        Configuration sinkConfig = new Configuration();
        sinkConfig.set(DorisDataSinkOptions.FENODES, "localhost:8030");
        sinkConfig.set(DorisDataSinkOptions.USERNAME, "root");
        sinkConfig.set(DorisDataSinkOptions.PASSWORD, "123456");

        buildSink(
                source,
                new DorisDataSinkOptions(sinkConfig),
                4,
                Duration.ofSeconds(30),
                SchemaChangeBehavior.EVOLVE,
                "Asia/Shanghai");

        env.execute("kafka-json-cdc -> doris");
    }

    /**
     * Builds the canal-sourced {@code Event} stream. {@code KafkaJsonDataSource} wraps the Kafka
     * source whose deserializer produces the connector's own {@code KafkaJsonEventTypeInfo}, so the
     * whole downstream chain can serialize the five custom schema-change events.
     */
    public static DataStream<Event> buildSource(
            StreamExecutionEnvironment env, KafkaJsonSourceConfigFactory configFactory) {
        FlinkSourceProvider provider =
                (FlinkSourceProvider) new KafkaJsonDataSource(configFactory).getEventSourceProvider();
        return env.fromSource(
                provider.getSource(), WatermarkStrategy.noWatermarks(), "canal-source");
    }

    /**
     * Builds the sink topology for a Doris target: schema operator + partitioning chain + the
     * writer operator driving {@code DorisSink}. Returns the writer operator's (empty)
     * commit output stream; callers that only run the job can ignore it.
     */
    public static DataStream<CommittableMessage<Void>> buildSink(
            DataStream<Event> source,
            DorisDataSinkOptions sinkOptions,
            int sinkParallelism,
            Duration rpcTimeout,
            SchemaChangeBehavior schemaChangeBehavior,
            String timezone) {
        return new KafkaJsonDataSinkBuilder(
                        new DorisDataSinkDialect(sinkOptions, ZoneId.of(timezone)))
                .build(source, sinkParallelism, rpcTimeout, schemaChangeBehavior, timezone);
    }
}
