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
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

/**
 * Runnable end-to-end assembly of the connector's own DataStream topology: a canal-sourced stream
 * (read from Kafka) through the {@link KafkaJsonDataSinkBuilder} chain into Doris.
 *
 * <p>No YAML pipeline, no registered {@code DataSinkFactory} SPI and no pipeline jar: the job is
 * assembled programmatically. The schema operator (released {@code SchemaOperator} + connector
 * coordinator) blocks data on a schema change until every sink subtask has flushed, executes the
 * DDL over Doris HTTP and resumes; the partitioning chain keys rows by {@code (table original name,
 * primary key)} so a large table is spread across the sink subtasks.
 *
 * <p>The static {@link #buildSource}/{@link #buildSink} methods are the reusable seams — tests and
 * the simulated-sink ITCase drive the same topology. The example lives in the test sources because
 * it is a runnable reference, not part of the connector's published surface.
 *
 * <p>Source and sink run at one parallelism, like the released composer runs a job at one {@code
 * pipeline.parallelism}: the sink chain takes the parallelism of the stream it is given, so the
 * parallelism set on the source is the parallelism of the whole job. That single value matters --
 * if the sink ran at a different parallelism from the source, Flink would rebalance the events in
 * front of the schema operator and the rows of one primary key would no longer arrive in the order
 * they happened (see {@link KafkaJsonDataSinkBuilder}).
 *
 * <p>The endpoints and credentials are read from {@code e2e/env.yaml} (see {@link E2eConfig}); the
 * repository only carries the placeholder template, so no real connection details are committed.
 */
public class DorisSinkExample {

    /** The connector source identifier of the underlying canal connector. */
    public static final String SOURCE_IDENTIFIER = "jdbc-kafka-json-cdc";

    /** The parallelism of the whole job: the source and every sink operator run at this value. */
    private static final int PARALLELISM = 4;

    public static void main(String[] args) throws Exception {
        // The endpoints and credentials come from e2e/env.yaml (see E2eConfig); the repository only
        // carries e2e/env.yaml.template, so no real connection details are committed.
        E2eConfig e2e = E2eConfig.get();
        String database = e2e.getString("mysql.database");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(6_000L);
        // The source runs at this parallelism (buildSource inherits it) and the sink chain follows
        // the source, so one setting covers the whole job.
        env.setParallelism(PARALLELISM);

        KafkaJsonSourceConfigFactory configFactory =
                new KafkaJsonSourceConfigFactory()
                        .hostname(e2e.getString("mysql.host"))
                        .port(e2e.getInt("mysql.port"))
                        .username(e2e.getString("mysql.user"))
                        .password(e2e.getString("mysql.password"))
                        .databaseList(database)
                        .tableList(database + ".test_schema_change3|test_schema_change4")
                        .databaseType(KafkaJsonSourceOptions.DatabaseType.TIDB)
                        .eventTime(KafkaJsonSourceOptions.EventTime.TIDB_TSO)
                        .scanNewlyAddedTableEnabled(true)
                        .includeSchemaChanges(true)
                        .kafkaBootstrapServers(e2e.getString("kafka.servers"))
                        .kafkaGroupId(e2e.getString("kafka.group"))
                        .kafkaTopics(e2e.getString("kafka.topic"))
                        .startupOptions(StartupOptions.initial())
                        .serverTimeZone("Asia/Shanghai");
        // Consume what the source reads with:
        // kafka-console-consumer --bootstrap-server <kafka.servers> --topic <kafka.topic>

        DataStream<Event> source = buildSource(env, configFactory);

        Configuration sinkConfig = new Configuration();
        sinkConfig.set(DorisDataSinkOptions.FENODES, e2e.getString("doris.feHttp"));
        sinkConfig.set(DorisDataSinkOptions.USERNAME, e2e.getString("doris.user"));
        sinkConfig.set(DorisDataSinkOptions.PASSWORD, e2e.getString("doris.password"));
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("replication_allocation", "tag.location.group_a: 3");
        sinkConfig.set(DorisDataSinkOptions.TABLE_PROPERTIES, tableProperties);
        sinkConfig.set(DorisDataSinkOptions.TABLE_BUCKETS, 8);
        sinkConfig.set(
                DorisDataSinkOptions.GROUP_COMMIT_MODE,
                DorisDataSinkOptions.GroupCommitMode.ASYNC_MODE);
        sinkConfig.set(
                DorisDataSinkOptions.WRITER_MODE, DorisDataSinkOptions.WriterMode.STATEFUL.name());
        sinkConfig.set(DorisDataSinkOptions.IS_DROP_TABLE_BEFORE_CREATE, true);
        DorisDataSinkOptions dorisDataSinkOptions = new DorisDataSinkOptions(sinkConfig);
        dorisDataSinkOptions.withTableMapping(
                (tableId) -> tableId.getSchemaName() + "_" + tableId.getTableName());
        dorisDataSinkOptions.withDatabaseMapping((tableId) -> e2e.getString("doris.database"));

        buildSink(
                source,
                dorisDataSinkOptions,
                Duration.ofSeconds(30),
                SchemaChangeBehavior.EVOLVE,
                "Asia/Shanghai");

        env.execute("kafka-json-cdc -> doris");
    }

    /**
     * Builds the canal-sourced {@code Event} stream at the environment's parallelism, which the
     * sink chain then follows. {@code KafkaJsonDataSource} wraps the Kafka source whose
     * deserializer produces the connector's own {@code KafkaJsonEventTypeInfo}, so the whole
     * downstream chain can serialize the five custom schema-change events.
     */
    public static DataStream<Event> buildSource(
            StreamExecutionEnvironment env, KafkaJsonSourceConfigFactory configFactory) {
        FlinkSourceProvider provider =
                (FlinkSourceProvider)
                        new KafkaJsonDataSource(configFactory).getEventSourceProvider();
        return env.fromSource(
                provider.getSource(), WatermarkStrategy.noWatermarks(), SOURCE_IDENTIFIER);
    }

    /**
     * Builds the sink topology for a Doris target: schema operator + partitioning chain + the
     * writer operator driving {@code DorisSink}. The whole chain runs at the parallelism of {@code
     * source}. Returns the writer operator's (empty) commit output stream; callers that only run
     * the job can ignore it.
     */
    public static DataStream<CommittableMessage<Void>> buildSink(
            DataStream<Event> source,
            DorisDataSinkOptions sinkOptions,
            Duration rpcTimeout,
            SchemaChangeBehavior schemaChangeBehavior,
            String timezone) {
        return new KafkaJsonDataSinkBuilder(
                        new DorisDataSinkDialect(sinkOptions, ZoneId.of(timezone)))
                .build(source, rpcTimeout, schemaChangeBehavior, timezone);
    }
}
