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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.kafkajson.infra.KafkaJsonSourceTestBase;
import org.apache.flink.cdc.connectors.kafkajson.infra.KafkaUtil;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.lifecycle.Startables;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Verifies the checkpoint-completion offset-commit semantics of the streaming reader against a real
 * Kafka broker.
 *
 * <p>The streaming consumer commits the consumed Kafka offsets to the consumer group only when a
 * checkpoint completes (the {@code IncrementalSourceReaderWithCommit} wiring forwards the
 * checkpoint-complete callback to the dialect, which fills the pending commit snapshot that the
 * fetcher thread later {@code commitSync}s). The committed group offset must therefore:
 *
 * <ol>
 *   <li>advance to the consumed position once the first checkpoint completes;
 *   <li>stay put while messages are consumed but the next checkpoint has not completed yet;
 *   <li>jump to the new consumed position only after that checkpoint completes.
 * </ol>
 *
 * <p>A long checkpoint interval (30s) makes the "between checkpoints" window wide enough to observe
 * deterministically. The source runs in stream-only mode against the canal message format, so no
 * database snapshot is involved; the canal resolver builds the table schema from the message
 * itself. The stream-only run is expressed as {@code startup-options=earliest} (the connector's
 * config factory explicitly whitelists {@code EARLIEST_OFFSET}, which the base factory rejects)
 * plus {@code scan.kafka.startup.mode=earliest}; with no snapshot the stream split carries the
 * initial offset, so the latter drives the consumer's seek to the beginning of the log.
 *
 * <p>Consumption is observed through a static counter incremented by the deserializer, which runs
 * inside the source operator and therefore reports each record as the reader consumes it, with no
 * client-side indirection. {@code executeAndCollect()} is deliberately avoided here: the collect
 * sink is checkpoint-gated (the client only sees results that were part of a completed checkpoint),
 * so with a 30s checkpoint interval a message consumed in the "between checkpoints" window would
 * only become visible to the client when the <em>next</em> checkpoint completes — exactly the
 * moment the test must still observe the group offset <em>unchanged</em>. A plain local {@code
 * AtomicInteger} captured by a user function would not work either: it is serialized into the task
 * and deserialized into a separate instance on the task thread. The static counter is shared with
 * the in-process MiniCluster's task threads, and the {@code AdminClient} query reads the committed
 * group offset independently.
 */
public class KafkaJsonOffsetCommitITCase extends KafkaJsonSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonOffsetCommitITCase.class);

    /** Long enough to hold the "between checkpoints" assertion window without races. */
    private static final int CHECKPOINT_INTERVAL_MS = 30_000;

    private static final String TOPIC = "offset-commit-" + UUID.randomUUID();
    private static final String GROUP_ID = "kafka-json-offset-commit-itcase";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

    /**
     * Number of records the source operator has emitted, incremented by {@link
     * CountingDeserializationSchema}. Reset at the start of the test. Shared with the task threads
     * of the in-process MiniCluster, so it reflects consumption immediately — independent of the
     * checkpoint-gated collect sink.
     */
    private static final AtomicInteger CONSUMED = new AtomicInteger();

    protected static final KafkaContainer KAFKA = KafkaUtil.createKafkaContainer(LOG, NETWORK);

    @BeforeClass
    public static void startContainer() throws Exception {
        checkDockerAvailable();
        LOG.info("Starting Kafka...");
        Startables.deepStart(Stream.of(KAFKA)).join();
        // testcontainers 1.21 KafkaContainer no longer has createTopic(); create the single
        // partition topic through the admin client so the produced order is deterministic.
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(props)) {
            admin.createTopics(Collections.singletonList(new NewTopic(TOPIC, 1, (short) 1)))
                    .all()
                    .get(10, TimeUnit.SECONDS);
        }
        LOG.info("Kafka is started.");
    }

    @AfterClass
    public static void stopContainer() {
        LOG.info("Stopping Kafka...");
        KAFKA.stop();
    }

    @Test(timeout = 300_000)
    public void testGroupOffsetAdvancesOnlyOnCheckpoint() throws Exception {
        // 1) first batch, consumed from earliest after the job starts
        KafkaUtil.produce(KAFKA.getBootstrapServers(), TOPIC, Arrays.asList(insert(1), insert(2)));
        CONSUMED.set(0);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);
        env.setRestartStrategy(RestartStrategies.noRestart());

        KafkaJsonSource<Integer> source =
                KafkaJsonSourceBuilder.<Integer>builder()
                        .hostname("localhost")
                        .username("root")
                        .password("x")
                        .kafkaBootstrapServers(KAFKA.getBootstrapServers())
                        .kafkaGroupId(GROUP_ID)
                        .kafkaTopics(TOPIC)
                        .startupOptions(StartupOptions.earliest())
                        .kafkaStartupMode(KafkaJsonSourceOptions.KafkaStartupMode.EARLIEST)
                        .serverTimeZone("UTC")
                        .deserializer(new CountingDeserializationSchema())
                        .build();

        // the deserializer increments CONSUMED inside the source operator, so the sink is only
        // needed to make the job executable
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-json")
                .addSink(new DiscardingSink<>());
        JobClient jobClient = env.executeAsync("kafka-json-offset-commit-itcase");

        try {
            // first batch consumed
            await(() -> CONSUMED.get() >= 2, 120_000, "first batch to be consumed");
            LOG.info("First batch consumed; waiting for the first checkpoint commit...");

            // the first checkpoint completion surfaces the committed offset
            await(
                    () -> committedOffset() >= 0,
                    120_000,
                    "first checkpoint to commit the consumed offset");
            long firstCommit = committedOffset();
            assertEquals(
                    "first checkpoint must commit exactly the consumed first batch",
                    2,
                    firstCommit);
            LOG.info("First checkpoint committed offset={}", firstCommit);

            // 2) second batch: consumed, but the next checkpoint has not completed yet
            KafkaUtil.produce(KAFKA.getBootstrapServers(), TOPIC, Arrays.asList(insert(3)));
            await(
                    () -> CONSUMED.get() >= 3,
                    60_000,
                    "second batch to be consumed (before the next checkpoint)");
            Thread.sleep(1_000); // small buffer so consumption is fully reflected
            assertEquals(
                    "consumed-but-uncommitted messages must not advance the group offset",
                    firstCommit,
                    committedOffset());
            LOG.info(
                    "Second batch consumed, group offset still {} (no checkpoint yet) ✓",
                    firstCommit);

            // 3) the next checkpoint completes → the offset catches up
            await(
                    () -> committedOffset() > firstCommit,
                    120_000,
                    "next checkpoint to commit the second batch");
            assertEquals(firstCommit + 1, committedOffset());
            LOG.info("Next checkpoint committed offset={} ✓", committedOffset());
        } finally {
            jobClient.cancel().get(10, TimeUnit.SECONDS);
        }
    }

    /** Polls the committed offset of {@link #PARTITION} for {@link #GROUP_ID}; -1 when none yet. */
    private static long committedOffset() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(props)) {
            Map<TopicPartition, OffsetAndMetadata> offsets =
                    admin.listConsumerGroupOffsets(GROUP_ID)
                            .partitionsToOffsetAndMetadata()
                            .get(10, TimeUnit.SECONDS);
            OffsetAndMetadata metadata = offsets.get(PARTITION);
            return metadata == null ? -1L : metadata.offset();
        } catch (Exception e) {
            throw new RuntimeException("Failed to query committed offsets of group " + GROUP_ID, e);
        }
    }

    private static void await(BooleanSupplier condition, long timeoutMs, String message)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(200);
        }
        fail("Timed out after " + timeoutMs + " ms waiting for: " + message);
    }

    /** A canal flatMessage INSERT for the standard {@code customers} table. */
    private static String insert(int id) {
        return String.format(
                "{\"data\":[{\"id\":\"%d\",\"name\":\"user_%d\",\"address\":\"addr_%d\"}],"
                        + "\"database\":\"test\",\"es\":1700000000000,\"id\":1,\"isDdl\":false,"
                        + "\"mysqlType\":{\"id\":\"int\",\"name\":\"varchar(255)\",\"address\":\"varchar(255)\"},"
                        + "\"old\":null,\"pkNames\":[\"id\"],\"sql\":\"\","
                        + "\"sqlType\":{\"id\":4,\"name\":12,\"address\":12},\"table\":\"customers\","
                        + "\"ts\":1700000000000,\"type\":\"INSERT\"}",
                id, id, id);
    }

    /**
     * Counts each delivered record without depending on the Debezium envelope serialization — this
     * test asserts the offset-commit semantics, not the deserialized payload. The counter is {@link
     * #CONSUMED}, a static field of the test class shared with the in-process MiniCluster's task
     * threads; a per-instance field would be a separate copy inside the deserialized schema.
     */
    private static final class CountingDeserializationSchema
            implements DebeziumDeserializationSchema<Integer> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(SourceRecord record, Collector<Integer> out) {
            CONSUMED.incrementAndGet();
            out.collect(1);
        }

        @Override
        public TypeInformation<Integer> getProducedType() {
            return Types.INT;
        }
    }
}
