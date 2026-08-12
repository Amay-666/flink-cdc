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

package org.apache.flink.cdc.connectors.kafkajson.testutils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Collection of methods to interact with a Kafka cluster for the integration tests.
 *
 * <p>The Kafka image is the same one {@code org.apache.flink.util.DockerImageVersions.KAFKA}
 * resolves to ({@code confluentinc/cp-kafka:7.2.2}); it is spelled out here so the test module does
 * not need {@code flink-test-utils-junit} on its classpath just to read that constant.
 */
public class KafkaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaUtil.class);
    private static final Duration CONSUMER_POLL_DURATION = Duration.ofSeconds(1);

    /** @see org.apache.flink.util.DockerImageVersions#KAFKA */
    private static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.2.2";

    private KafkaUtil() {}

    /** Creates a Kafka container with commonly used configurations, joined to the test network. */
    public static KafkaContainer createKafkaContainer(Logger logger, org.testcontainers.containers.Network network) {
        String logLevel;
        if (logger.isTraceEnabled()) {
            logLevel = "TRACE";
        } else if (logger.isDebugEnabled()) {
            logLevel = "DEBUG";
        } else if (logger.isInfoEnabled()) {
            logLevel = "INFO";
        } else if (logger.isWarnEnabled()) {
            logLevel = "WARN";
        } else if (logger.isErrorEnabled()) {
            logLevel = "ERROR";
        } else {
            logLevel = "OFF";
        }
        return new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE))
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE", "false")
                .withEnv("KAFKA_LOG4J_ROOT_LOGLEVEL", logLevel)
                .withEnv("KAFKA_LOG4J_LOGGERS", "state.change.logger=" + logLevel)
                .withEnv(
                        "KAFKA_TRANSACTION_MAX_TIMEOUT_MS",
                        String.valueOf(Duration.ofHours(2).toMillis()))
                .withEnv("KAFKA_LOG4J_TOOLS_ROOT_LOGLEVEL", logLevel)
                .withLogConsumer(new Slf4jLogConsumer(logger));
    }

    /**
     * Produces the given JSON messages to the topic, in order. A fixed key is used so all messages
     * land on a single Kafka partition, keeping the inter-partition order deterministic for the
     * source.
     */
    public static void produce(String bootstrapServers, String topic, List<String> jsonMessages) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (String message : jsonMessages) {
                producer.send(new ProducerRecord<>(topic, "test-key", message));
            }
            producer.flush();
        }
        LOG.info("Produced {} messages to topic {}.", jsonMessages.size(), topic);
    }

    /**
     * Drain all records available from the given topic from the beginning until the current highest
     * offset.
     *
     * <p>This method will fetch the latest offsets for the partitions once and only return records
     * until that point.
     *
     * @param topic to fetch from
     * @param properties used to configure the created {@link KafkaConsumer}
     * @return all {@link ConsumerRecord} in the topic
     * @throws KafkaException
     */
    public static List<ConsumerRecord<byte[], byte[]>> drainAllRecordsFromTopic(
            String topic, Properties properties) throws KafkaException {
        final Properties consumerConfig = new Properties();
        consumerConfig.putAll(properties);
        consumerConfig.put("key.deserializer", ByteArrayDeserializer.class.getName());
        consumerConfig.put("value.deserializer", ByteArrayDeserializer.class.getName());
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfig)) {
            Set<TopicPartition> topicPartitions = getAllPartitions(consumer, topic);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
            consumer.assign(topicPartitions);
            consumer.seekToBeginning(topicPartitions);

            final List<ConsumerRecord<byte[], byte[]>> consumerRecords = new ArrayList<>();
            while (!topicPartitions.isEmpty()) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(CONSUMER_POLL_DURATION);
                LOG.debug("Fetched {} records from topic {}.", records.count(), topic);

                // Remove partitions from polling which have reached its end.
                final List<TopicPartition> finishedPartitions = new ArrayList<>();
                for (final TopicPartition topicPartition : topicPartitions) {
                    final long position = consumer.position(topicPartition);
                    final long endOffset = endOffsets.get(topicPartition);
                    LOG.debug(
                            "Endoffset {} and current position {} for partition {}",
                            endOffset,
                            position,
                            topicPartition.partition());
                    if (endOffset - position > 0) {
                        continue;
                    }
                    finishedPartitions.add(topicPartition);
                }
                if (topicPartitions.removeAll(finishedPartitions)) {
                    consumer.assign(topicPartitions);
                }
                for (ConsumerRecord<byte[], byte[]> r : records) {
                    consumerRecords.add(r);
                }
            }
            return consumerRecords;
        }
    }

    private static Set<TopicPartition> getAllPartitions(
            KafkaConsumer<byte[], byte[]> consumer, String topic) {
        return consumer.partitionsFor(topic).stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .collect(Collectors.toSet());
    }
}
