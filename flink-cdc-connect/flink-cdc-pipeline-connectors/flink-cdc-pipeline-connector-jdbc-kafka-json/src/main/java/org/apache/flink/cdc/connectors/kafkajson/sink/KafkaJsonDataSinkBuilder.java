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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.kafkajson.serializer.KafkaJsonEventTypeInfo;
import org.apache.flink.cdc.connectors.kafkajson.sink.dialect.KafkaJsonDataSinkDialect;
import org.apache.flink.cdc.connectors.kafkajson.sink.partitioning.KafkaJsonPartitioningEventTypeInfo;
import org.apache.flink.cdc.connectors.kafkajson.sink.partitioning.KafkaJsonPrePartitionOperator;
import org.apache.flink.cdc.connectors.kafkajson.sink.schema.KafkaJsonSchemaOperatorFactory;
import org.apache.flink.cdc.runtime.operators.sink.DataSinkWriterOperatorFactory;
import org.apache.flink.cdc.runtime.partitioning.EventPartitioner;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEventKeySelector;
import org.apache.flink.cdc.runtime.partitioning.PostPartitionProcessor;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.shaded.guava31.com.google.common.hash.Hashing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Assembles the sink side of a kafka-json pipeline job as a plain DataStream chain.
 *
 * <p>This is the hand-written counterpart of the released {@code FlinkPipelineComposer} sink
 * topology, so the job never needs a YAML pipeline, a registered {@code DataSinkFactory} SPI or a
 * separate pipeline jar. The chain mirrors the composer's, but uses the connector's own serializers
 * and coordinators wherever the released ones cannot handle the five custom schema-change events:
 *
 * <pre>{@code
 * source
 *   └─ transform("kafka-json-schema-operator", KafkaJsonEventTypeInfo,
 *                KafkaJsonSchemaOperatorFactory)            // released SchemaOperator + connector coordinator
 *        .uid(SCHEMA_OPERATOR_UID).setParallelism(p)
 *   └─ transform("PrePartition", KafkaJsonPartitioningEventTypeInfo,
 *                KafkaJsonPrePartitionOperator)             // broadcast DDL/flush, hash data by table+pk
 *        .setParallelism(p)
 *        .partitionCustom(EventPartitioner, PartitioningEventKeySelector)
 *        .map(PostPartitionProcessor, KafkaJsonEventTypeInfo)
 *        .setParallelism(p).name("PostPartition")   // a map inherits the env parallelism by itself
 *   └─ transform("kafka-json-sink-writer", CommittableMessageTypeInfo.noOutput(),
 *                DataSinkWriterOperatorFactory)             // released writer operator drives the dialect Sink
 *        .setParallelism(p)
 * }</pre>
 *
 * <p>Every stage runs at the parallelism of the source stream it is given, so the chain stays
 * forward-connected and the events are never rebalanced on their way in. A rebalance round-robins
 * the events one upstream subtask produced across the downstream subtasks, which breaks the
 * per-primary-key order the partitioning chain relies on: the rows of one key would be re-hashed to
 * the same sink subtask from several upstream subtasks, in whatever order those produced them, and
 * a delete could be loaded before the insert it followed. The source stream's parallelism is
 * therefore the job's parallelism -- set it with {@code env.setParallelism(p)} or {@code
 * source.setParallelism(p)}, the way the released composer runs a whole job at one {@code
 * pipeline.parallelism}.
 *
 * <p>The schema operator's {@code OperatorID} is derived from its uid with the same murmur3_128(0)
 * hash Flink's {@code StreamGraphHasherV2} applies (see
 * org.apache.flink.cdc.composer.flink.coordination.OperatorIDGenerator): the partition operator and
 * the writer operator address the schema coordinator through this id, so it must match the id Flink
 * assigns at graph build time. The hash is reproduced here because the connector does not depend on
 * the composer module.
 */
public class KafkaJsonDataSinkBuilder {

    public static final String SCHEMA_OPERATOR_UID = "kafka-json-schema-operator";
    public static final String PRE_PARTITION_NAME = "PrePartition";
    public static final String POST_PARTITION_NAME = "PostPartition";
    public static final String SINK_WRITER_NAME = "kafka-json-sink-writer";

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonDataSinkBuilder.class);

    private final KafkaJsonDataSinkDialect dialect;

    public KafkaJsonDataSinkBuilder(KafkaJsonDataSinkDialect dialect) {
        this.dialect = dialect;
    }

    /**
     * Builds the full sink topology: schema operator, partitioning chain and the writer. Every
     * stage runs at the parallelism of {@code source}, so the whole chain is forward-connected. The
     * returned stream carries the writer operator's (empty) {@link CommittableMessage} output and
     * is normally ignored; it is returned so tests can attach collectors to it.
     *
     * @param source the event stream to consume. Its parallelism is the job's parallelism: give it
     *     {@code env.setParallelism(p)} (or {@code source.setParallelism(p)}) to run the sink with
     *     {@code p} subtasks.
     */
    public DataStream<CommittableMessage<Void>> build(
            DataStream<Event> source,
            Duration rpcTimeout,
            SchemaChangeBehavior schemaChangeBehavior,
            String timezone) {
        LOG.info(
                "Building the sink chain at parallelism {}: every sink operator takes the source"
                        + " stream's parallelism, so the events are not rebalanced on the way in.",
                source.getParallelism());
        MetadataApplier metadataApplier = dialect.createMetadataApplier();
        DataStream<Event> schemaStream =
                buildSchemaOperator(
                        source, metadataApplier, rpcTimeout, schemaChangeBehavior, timezone);
        OperatorID schemaOperatorID = generateOperatorID(SCHEMA_OPERATOR_UID);
        DataStream<Event> partitionedStream =
                buildPartitionedStream(schemaStream, schemaOperatorID);
        return buildSinkWriter(partitionedStream, schemaOperatorID);
    }

    /**
     * Adds the schema operator (released {@code SchemaOperator} + connector coordinator) at the
     * input's parallelism, like the released composer.
     */
    public DataStream<Event> buildSchemaOperator(
            DataStream<Event> input,
            MetadataApplier metadataApplier,
            Duration rpcTimeout,
            SchemaChangeBehavior schemaChangeBehavior,
            String timezone) {
        return input.transform(
                        SCHEMA_OPERATOR_UID,
                        new KafkaJsonEventTypeInfo(),
                        new KafkaJsonSchemaOperatorFactory(
                                metadataApplier, rpcTimeout, schemaChangeBehavior, timezone))
                .uid(SCHEMA_OPERATOR_UID)
                .setParallelism(input.getParallelism());
    }

    /**
     * Adds the partitioning chain that keys data by {@code (table original name, primary key)}:
     * schema-change/flush events are broadcast to every partition, data-change events are hashed to
     * a single partition. Mirrors the released {@code PartitioningTranslator} with the connector's
     * serializers, except that the post-partition map also gets its parallelism set explicitly: a
     * {@code map} otherwise takes the environment's default parallelism, which rebalances the
     * events between the partitioner and the very operator that is supposed to see them in order.
     * The released translator can leave it implicit because the composer runs the whole job at one
     * {@code pipeline.parallelism}, so the inherited value is that value.
     */
    public DataStream<Event> buildPartitionedStream(
            DataStream<Event> input, OperatorID schemaOperatorID) {
        int parallelism = input.getParallelism();
        return input.transform(
                        PRE_PARTITION_NAME,
                        new KafkaJsonPartitioningEventTypeInfo(),
                        new KafkaJsonPrePartitionOperator(
                                schemaOperatorID,
                                parallelism,
                                dialect.createHashFunctionProvider()))
                .setParallelism(parallelism)
                .partitionCustom(new EventPartitioner(), new PartitioningEventKeySelector())
                .map(new PostPartitionProcessor(), new KafkaJsonEventTypeInfo())
                .setParallelism(parallelism)
                .name(POST_PARTITION_NAME);
    }

    /**
     * Adds the writer operator of the released {@code DataSinkWriterOperatorFactory} driving the
     * dialect's {@link Sink}. The output is a non-2PC commit stream, so the chain terminates here
     * (the {@code CommittableMessage} output is {@code noOutput()}).
     */
    public DataStream<CommittableMessage<Void>> buildSinkWriter(
            DataStream<Event> input, OperatorID schemaOperatorID) {
        return input.transform(
                        SINK_WRITER_NAME,
                        CommittableMessageTypeInfo.noOutput(),
                        new DataSinkWriterOperatorFactory<>(dialect.createSink(), schemaOperatorID))
                .setParallelism(input.getParallelism());
    }

    /**
     * Reproduces Flink's operator-id hashing for a transformation uid ({@code
     * StreamGraphHasherV2#traverseStreamGraphAndGenerateHashes}): {@code murmur3_128(0)} over the
     * uid bytes.
     */
    public static OperatorID generateOperatorID(String transformationUid) {
        byte[] hash =
                Hashing.murmur3_128(0)
                        .newHasher()
                        .putString(transformationUid, UTF_8)
                        .hash()
                        .asBytes();
        return new OperatorID(hash);
    }
}
