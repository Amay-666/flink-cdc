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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
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
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;

import org.apache.flink.shaded.guava31.com.google.common.hash.Hashing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
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
 *   └─ transform("kafka-json-sink-committer",               // only for a two-phase sink, and a
 *                CommitterOperatorFactory)                  // dangling branch: nothing consumes it
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
    public static final String SINK_COMMITTER_NAME = "kafka-json-sink-committer";

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonDataSinkBuilder.class);

    private final KafkaJsonDataSinkDialect dialect;

    public KafkaJsonDataSinkBuilder(KafkaJsonDataSinkDialect dialect) {
        this.dialect = dialect;
    }

    /**
     * Builds the full sink topology: schema operator, partitioning chain, the writer and — for a
     * two-phase sink — the committer. Every stage runs at the parallelism of {@code source}, so the
     * whole chain is forward-connected. The returned stream carries the writer operator's {@link
     * CommittableMessage} output and is normally ignored; it is returned so tests can attach
     * collectors to it.
     *
     * <p>The element type of the returned stream depends on the dialect's {@code sink.writer} mode:
     * {@code Void} for the single-phase modes, whose writer emits no committable, and the sink's
     * committable for {@code stateful-2pc}. Callers that ignore the stream — all of them outside
     * tests — do not have to care, which is why one method serves both.
     *
     * @param source the event stream to consume. Its parallelism is the job's parallelism: give it
     *     {@code env.setParallelism(p)} (or {@code source.setParallelism(p)}) to run the sink with
     *     {@code p} subtasks.
     */
    public <CommT> DataStream<CommittableMessage<CommT>> build(
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
     * dialect's {@link Sink}, and — when that sink commits in two phases — the committer operator
     * that makes its pre-committed transactions visible once their checkpoint completes.
     *
     * <p>The committer branch is built and then dropped, because that is what a sink looks like in
     * the DataStream API: an operator nothing consumes. {@code DataStream#transform} registers it
     * with the environment, so dropping the reference loses nothing (the released composer leaves
     * its committer dangling the same way), and the returned stream stays the writer's, which is
     * what a test wants to collect from.
     *
     * <p>Both operators are given the input's parallelism on purpose. With equal parallelism Flink
     * connects a pair of operators forward, subtask for subtask, which is what the committer's
     * collector requires: it accounts for the committables of a writer subtask under a summary that
     * that same writer subtask emits, and rejects a committable whose summary it never saw. A
     * parallelism mismatch would replace the forward connection with a rebalance, spreading one
     * writer subtask's summary and committables over different committer subtasks.
     */
    public <CommT> DataStream<CommittableMessage<CommT>> buildSinkWriter(
            DataStream<Event> input, OperatorID schemaOperatorID) {
        Sink<Event> sink = dialect.createSink();
        int parallelism = input.getParallelism();
        if (!(sink instanceof TwoPhaseCommittingSink)) {
            // A single-phase writer emits no committable, so noOutput() is the honest type for its
            // output and there is no committer to attach. Nothing is ever emitted on this stream,
            // which is why the cast — the price of one method for both modes — cannot be wrong.
            return castToCommittable(
                    input.transform(
                                    SINK_WRITER_NAME,
                                    CommittableMessageTypeInfo.noOutput(),
                                    new DataSinkWriterOperatorFactory<>(sink, schemaOperatorID))
                            .setParallelism(parallelism));
        }

        TwoPhaseCommittingSink<Event, CommT> committingSink =
                (TwoPhaseCommittingSink<Event, CommT>) sink;
        TypeInformation<CommittableMessage<CommT>> committables =
                CommittableMessageTypeInfo.of(committingSink::getCommittableSerializer);
        DataStream<CommittableMessage<CommT>> written =
                input.transform(
                                SINK_WRITER_NAME,
                                committables,
                                new DataSinkWriterOperatorFactory<>(sink, schemaOperatorID))
                        .setParallelism(parallelism);
        written.transform(
                        SINK_COMMITTER_NAME,
                        committables,
                        createCommitterOperatorFactory(sink, false, true))
                .setParallelism(parallelism);
        return written;
    }

    @SuppressWarnings("unchecked")
    private static <CommT> DataStream<CommittableMessage<CommT>> castToCommittable(
            DataStream<CommittableMessage<Void>> stream) {
        return (DataStream<CommittableMessage<CommT>>) (DataStream<?>) stream;
    }

    /**
     * Creates the committer operator factory of the Flink version the job runs on.
     *
     * <p>Reflected rather than constructed for the reason the released composer reflects it: {@code
     * CommitterOperatorFactory} is an internal class whose constructor takes {@code
     * TwoPhaseCommittingSink} on Flink 1.18 and the {@code CommittingSink} that interface was split
     * into on 1.19, so a compiled call would pin this connector to the version it was built
     * against. The constructor is picked by signature, not by declaration order, so an added
     * overload cannot silently redirect it.
     */
    @SuppressWarnings("unchecked")
    private static <CommT>
            OneInputStreamOperatorFactory<CommittableMessage<CommT>, CommittableMessage<CommT>>
                    createCommitterOperatorFactory(
                            Sink<Event> sink, boolean isBatchMode, boolean isCheckpointingEnabled) {
        try {
            Class<?> factoryClass =
                    Class.forName(
                            "org.apache.flink.streaming.runtime.operators.sink"
                                    + ".CommitterOperatorFactory");
            for (Constructor<?> constructor : factoryClass.getDeclaredConstructors()) {
                Class<?>[] parameters = constructor.getParameterTypes();
                if (parameters.length == 3
                        && parameters[1] == boolean.class
                        && parameters[2] == boolean.class) {
                    return (OneInputStreamOperatorFactory<
                                    CommittableMessage<CommT>, CommittableMessage<CommT>>)
                            constructor.newInstance(sink, isBatchMode, isCheckpointingEnabled);
                }
            }
            throw new IllegalStateException(
                    "No (sink, boolean, boolean) constructor on " + factoryClass.getName());
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(
                    "Failed to create the committer operator for a " + sink.getClass().getName(),
                    e);
        }
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
