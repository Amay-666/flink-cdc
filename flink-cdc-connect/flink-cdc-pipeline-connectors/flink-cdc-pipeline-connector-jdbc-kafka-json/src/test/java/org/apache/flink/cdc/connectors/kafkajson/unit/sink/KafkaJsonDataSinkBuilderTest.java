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

package org.apache.flink.cdc.connectors.kafkajson.unit.sink;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.cdc.connectors.kafkajson.serializer.KafkaJsonEventTypeInfo;
import org.apache.flink.cdc.connectors.kafkajson.sink.KafkaJsonDataSinkBuilder;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkDialect;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import org.junit.Test;

import java.time.Duration;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the sink chain runs at the parallelism of the stream it is built from.
 *
 * <p>The chain must never change the parallelism: two connected operators at different parallelisms
 * are joined by a rebalance, and a rebalance round-robins the events one subtask produced across
 * the downstream subtasks. The partitioning chain keys data by {@code (table original name, primary
 * key)}, so the rows of one key would then be re-hashed into its sink subtask from several upstream
 * subtasks, in whatever order those produced them -- a delete could be loaded before the insert it
 * followed. "Every stage runs at the source's parallelism" is therefore the property under test.
 */
public class KafkaJsonDataSinkBuilderTest {

    private static final TableId ORDERS = TableId.tableId("shop", "orders");
    private static final TableId ORDERS_ARCHIVE = TableId.tableId("shop", "orders_archive");
    private static final Schema SCHEMA =
            Schema.newBuilder().physicalColumn("id", DataTypes.INT()).primaryKey("id").build();

    private static final ZoneId ZONE = ZoneId.of("Asia/Shanghai");
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    public void testEveryStageTakesTheSourceParallelism() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> source = sourceStream(env, 3);
        DorisDataSinkDialect dialect = dialect();
        KafkaJsonDataSinkBuilder builder = new KafkaJsonDataSinkBuilder(dialect);
        OperatorID schemaOperatorID =
                KafkaJsonDataSinkBuilder.generateOperatorID(
                        KafkaJsonDataSinkBuilder.SCHEMA_OPERATOR_UID);

        DataStream<Event> schemaStream =
                builder.buildSchemaOperator(
                        source,
                        dialect.createMetadataApplier(),
                        TIMEOUT,
                        SchemaChangeBehavior.EVOLVE,
                        ZONE.getId());
        DataStream<Event> partitionedStream =
                builder.buildPartitionedStream(schemaStream, schemaOperatorID);
        DataStream<CommittableMessage<Void>> writerStream =
                builder.buildSinkWriter(partitionedStream, schemaOperatorID);

        assertThat(schemaStream.getParallelism()).isEqualTo(3);
        assertThat(partitionedStream.getParallelism()).isEqualTo(3);
        assertThat(writerStream.getParallelism()).isEqualTo(3);
    }

    @Test
    public void testBuildTakesTheSourceParallelismWithoutResizingTheSource() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> source = sourceStream(env, 2);

        DataStream<CommittableMessage<Void>> writerStream =
                new KafkaJsonDataSinkBuilder(dialect())
                        .build(source, TIMEOUT, SchemaChangeBehavior.EVOLVE, ZONE.getId());

        assertThat(writerStream.getParallelism()).isEqualTo(2);
        assertThat(source.getParallelism()).isEqualTo(2);
    }

    /**
     * A single-event stream at {@code parallelism}: this is the parallelism the job gives the sink
     * chain, since every sink operator takes the parallelism of the stream it is handed. The
     * environment's own parallelism is left at its default, so what the chain has to follow is the
     * stream's.
     */
    private static DataStream<Event> sourceStream(StreamExecutionEnvironment env, int parallelism) {
        Event rename = new RenameTableEvent(ORDERS, ORDERS_ARCHIVE, SCHEMA);
        return env.addSource(
                        new SingleEventSource(rename), "test-source", new KafkaJsonEventTypeInfo())
                .setParallelism(parallelism);
    }

    /** A Doris dialect; the endpoints are never contacted, the graph is only built here. */
    private static DorisDataSinkDialect dialect() {
        Configuration config = new Configuration();
        config.set(DorisDataSinkOptions.FENODES, "localhost:8030");
        config.set(DorisDataSinkOptions.USERNAME, "root");
        config.set(DorisDataSinkOptions.PASSWORD, "123456");
        return new DorisDataSinkDialect(new DorisDataSinkOptions(config), ZONE);
    }

    /**
     * Emits one event when the job runs; the graph is only built here, so it never does. It is a
     * {@link ParallelSourceFunction} rather than a plain {@code SourceFunction} because only the
     * former makes Flink mark the source operator parallel, and a non-parallel operator rejects any
     * parallelism other than 1.
     */
    private static class SingleEventSource implements ParallelSourceFunction<Event> {

        private static final long serialVersionUID = 1L;

        private final Event event;

        private volatile boolean running = true;

        SingleEventSource(Event event) {
            this.event = event;
        }

        @Override
        public void run(SourceContext<Event> ctx) {
            if (running) {
                ctx.collect(event);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
