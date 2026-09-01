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

package org.apache.flink.cdc.connectors.kafkajson.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AddColumnEvent.ColumnWithPosition;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.kafkajson.event.TruncateTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.example.DorisSinkExample;
import org.apache.flink.cdc.connectors.kafkajson.serializer.KafkaJsonEventSerializer;
import org.apache.flink.cdc.connectors.kafkajson.serializer.KafkaJsonEventTypeInfo;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer.RecordedRequest;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer.Response;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the DDL-blocking protocol of the sink topology, without Docker.
 *
 * <p>A deterministic {@link Event} stream is fed into the real {@link KafkaJsonDataSinkBuilder}
 * chain (schema operator + partitioning + writer) running on a MiniCluster with two sink subtasks,
 * and the Doris FE/BE endpoints are mocked by a JDK {@code HttpServer}. The protocol under test:
 * when a schema change arrives, upstream data is blocked until <em>every</em> sink subtask has
 * flushed its buffered rows (the {@code FlushEvent} broadcast), only then is the DDL executed over
 * the mock, and only then is data released under the new schema.
 *
 * <p>The mock records the order of the DDL requests ({@code POST
 * /api/query/default_cluster/{db}}) and the StreamLoad requests ({@code PUT
 * /api/{db}/{table}/_stream_load}), so the test can assert that all
 * pre-DDL rows hit Doris <em>before</em> the DDL and the post-DDL row <em>after</em> it. The source
 * is a single-parallelism bounded stream routed with {@code .global()} so the schema-change event
 * is fully applied before any data event is seen by the partitioning operator (which requires the
 * table's schema to derive its hash function).
 */
public class KafkaJsonDdlBlockingITCase {

    private static final TableId ORDERS = TableId.tableId("shop", "orders");

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    @Test
    public void testStandardAddColumnBlocksUntilAllSinkSubtasksFlush() throws Exception {
        try (MockDorisServer server = mockDorisServer()) {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(2);
            env.setRestartStrategy(RestartStrategies.noRestart());

            DataStream<Event> source =
                    env.addSource(
                                    new EventSequenceSource(blockingEvents()),
                                    "test-source",
                                    new KafkaJsonEventTypeInfo())
                            .global();
            buildSink(env, server, source);

            env.execute();

            List<RecordedRequest> all = server.recorded;
            List<RecordedRequest> ddls = ddlRequests(all);
            List<RecordedRequest> loads = streamLoads(all);

            assertThat(all.size()).isGreaterThanOrEqualTo(4);
            // The CREATE TABLE is the very first request: no data can flow before it is applied.
            assertThat(all.get(0).path).isEqualTo("/api/query/default_cluster/shop");
            assertThat(all.get(0).body).contains("CREATE TABLE IF NOT EXISTS");
            assertThat(ddls).hasSize(2);
            assertThat(ddls.get(1).body).contains("ALTER TABLE").contains("ADD COLUMN");
            assertThat(ddls.get(1).body).contains("`region`");

            int alterIndex = all.indexOf(ddls.get(1));
            List<RecordedRequest> preDdlLoads =
                    loads.stream()
                            .filter(sl -> all.indexOf(sl) < alterIndex)
                            .collect(Collectors.toList());
            List<RecordedRequest> postDdlLoads =
                    loads.stream()
                            .filter(sl -> all.indexOf(sl) > alterIndex)
                            .collect(Collectors.toList());

            // The pre-DDL rows (old schema, no `region`) were flushed by the FlushEvent broadcast
            // *before* the ALTER was applied.
            assertThat(preDdlLoads).isNotEmpty();
            assertThat(preDdlLoads).anySatisfy(sl -> assertThat(sl.body).contains("\"id\":1"));
            assertThat(preDdlLoads).allSatisfy(sl -> assertThat(sl.body).doesNotContain("region"));

            // The post-DDL row carries the new column and only reached Doris after the ALTER.
            assertThat(postDdlLoads).isNotEmpty();
            assertThat(postDdlLoads)
                    .anySatisfy(
                            sl -> {
                                assertThat(sl.body).contains("\"id\":3");
                                assertThat(sl.body).contains("\"region\":\"west\"");
                            });
        }
    }

    @Test
    public void testCustomTruncateTableEventFlowsThroughFullPipeline() throws Exception {
        try (MockDorisServer server = mockDorisServer()) {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(2);
            env.setRestartStrategy(RestartStrategies.noRestart());

            DataStream<Event> source =
                    env.addSource(
                                    new EventSequenceSource(truncateEvents()),
                                    "test-source",
                                    new KafkaJsonEventTypeInfo())
                            .global();
            buildSink(env, server, source);

            env.execute();

            List<RecordedRequest> all = server.recorded;
            List<RecordedRequest> ddls = ddlRequests(all);
            List<RecordedRequest> loads = streamLoads(all);

            assertThat(ddls).hasSize(2);
            assertThat(ddls.get(0).body).contains("CREATE TABLE IF NOT EXISTS");
            // The custom TruncateTableEvent survived the coordinator, the partition chain and the
            // writer, and was executed as a TRUNCATE over the mock Doris.
            assertThat(ddls.get(1).body).contains("TRUNCATE TABLE");

            int truncateIndex = all.indexOf(ddls.get(1));
            List<RecordedRequest> preTruncateLoads =
                    loads.stream()
                            .filter(sl -> all.indexOf(sl) < truncateIndex)
                            .collect(Collectors.toList());
            List<RecordedRequest> postTruncateLoads =
                    loads.stream()
                            .filter(sl -> all.indexOf(sl) > truncateIndex)
                            .collect(Collectors.toList());

            assertThat(preTruncateLoads).isNotEmpty();
            assertThat(preTruncateLoads).anySatisfy(sl -> assertThat(sl.body).contains("\"id\":1"));
            assertThat(postTruncateLoads).isNotEmpty();
            assertThat(postTruncateLoads).anySatisfy(sl -> assertThat(sl.body).contains("\"id\":3"));
        }
    }

    private static MockDorisServer mockDorisServer() throws IOException {
        return new MockDorisServer(
                req ->
                        "PUT".equals(req.method)
                                ? Response.ok("{\"Status\":\"Success\",\"NumberLoadedRows\":2}")
                                : Response.ok("{\"code\":0,\"msg\":\"OK\"}"));
    }

    private static void buildSink(
            StreamExecutionEnvironment env, MockDorisServer server, DataStream<Event> source) {
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(DorisDataSinkOptions.FENODES, server.endpoint());
        sinkConfig.set(DorisDataSinkOptions.USERNAME, "root");
        sinkConfig.set(DorisDataSinkOptions.PASSWORD, "123456");
        // No accidental buffer-full or periodic flush: pre-DDL rows may only reach Doris via the
        // FlushEvent of the DDL-blocking protocol.
        sinkConfig.set(DorisDataSinkOptions.BUFFER_SIZE, 1000);
        sinkConfig.set(DorisDataSinkOptions.FLUSH_INTERVAL, Duration.ZERO);
        DorisSinkExample.buildSink(
                source,
                new DorisDataSinkOptions(sinkConfig),
                2,
                Duration.ofSeconds(30),
                SchemaChangeBehavior.EVOLVE,
                "Asia/Shanghai");
    }

    private static List<RecordedRequest> ddlRequests(List<RecordedRequest> all) {
        return all.stream()
                .filter(r -> r.path.equals("/api/query/default_cluster/shop"))
                .collect(Collectors.toList());
    }

    private static List<RecordedRequest> streamLoads(List<RecordedRequest> all) {
        return all.stream()
                .filter(r -> r.method.equals("PUT") && r.path.endsWith("/_stream_load"))
                .collect(Collectors.toList());
    }

    /**
     * A bounded single-parallelism source emitting a fixed {@link Event} sequence in order. Used
     * instead of {@code fromElements}, which requires every element to be an instance of the first
     * element's class and cannot carry a mix of schema-change and data events.
     *
     * <p>The events are pre-serialized to {@code byte[]} at construction time with the pipeline's
     * own {@link KafkaJsonEventSerializer}, because the CDC events are not Java-serializable: a
     * raw {@code List<Event>} field makes Flink's {@code ClosureCleaner} (and the JobGraph
     * shipping) crash on the JDK module system. Holding only serialized bytes keeps the source
     * fully Java-serializable.
     */
    private static class EventSequenceSource extends RichSourceFunction<Event> {
        private static final long serialVersionUID = 1L;

        private final List<byte[]> serializedEvents;
        private final KafkaJsonEventSerializer serializer = KafkaJsonEventSerializer.INSTANCE;
        private volatile boolean running = true;

        EventSequenceSource(List<Event> events) {
            this.serializedEvents = new ArrayList<>(events.size());
            for (Event event : events) {
                DataOutputSerializer out = new DataOutputSerializer(64);
                try {
                    serializer.serialize(event, out);
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to pre-serialize source event", e);
                }
                serializedEvents.add(out.getCopyOfBuffer());
            }
        }

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            for (byte[] bytes : serializedEvents) {
                if (!running) {
                    return;
                }
                ctx.collect(serializer.deserialize(new DataInputDeserializer(bytes)));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /**
     * CreateTable(v1) → two rows over v1 → {@code ADD COLUMN region} → one row over v2. The two
     * pre-DDL rows are spread across the two sink subtasks by the primary-key hash; the DDL blocks
     * until both have flushed.
     */
    private static List<Event> blockingEvents() {
        Schema v1 = schemaV1();
        BinaryRecordDataGenerator rowV1 = rowV1Generator();
        return Arrays.asList(
                new CreateTableEvent(ORDERS, v1),
                DataChangeEvent.insertEvent(
                        ORDERS, rowV1.generate(new Object[] {1, BinaryStringData.fromString("a")})),
                DataChangeEvent.insertEvent(
                        ORDERS, rowV1.generate(new Object[] {2, BinaryStringData.fromString("b")})),
                new AddColumnEvent(
                        ORDERS,
                        Collections.singletonList(
                                new ColumnWithPosition(
                                        Column.physicalColumn("region", DataTypes.VARCHAR(32))))),
                DataChangeEvent.insertEvent(
                        ORDERS,
                        new BinaryRecordDataGenerator(
                                        RowType.of(
                                                DataTypes.INT(),
                                                DataTypes.VARCHAR(64),
                                                DataTypes.VARCHAR(32)))
                                .generate(
                                        new Object[] {
                                            3,
                                            BinaryStringData.fromString("c"),
                                            BinaryStringData.fromString("west")
                                        })));
    }

    /** Same shape as {@link #blockingEvents()} but with the custom {@link TruncateTableEvent}. */
    private static List<Event> truncateEvents() {
        Schema v1 = schemaV1();
        BinaryRecordDataGenerator rowV1 = rowV1Generator();
        return Arrays.asList(
                new CreateTableEvent(ORDERS, v1),
                DataChangeEvent.insertEvent(
                        ORDERS, rowV1.generate(new Object[] {1, BinaryStringData.fromString("a")})),
                DataChangeEvent.insertEvent(
                        ORDERS, rowV1.generate(new Object[] {2, BinaryStringData.fromString("b")})),
                new TruncateTableEvent(ORDERS, v1),
                DataChangeEvent.insertEvent(
                        ORDERS, rowV1.generate(new Object[] {3, BinaryStringData.fromString("c")})));
    }

    private static Schema schemaV1() {
        return Schema.newBuilder()
                .column(Column.physicalColumn("id", DataTypes.INT()))
                .column(Column.physicalColumn("name", DataTypes.VARCHAR(64)))
                .primaryKey("id")
                .build();
    }

    private static BinaryRecordDataGenerator rowV1Generator() {
        return new BinaryRecordDataGenerator(RowType.of(DataTypes.INT(), DataTypes.VARCHAR(64)));
    }
}
