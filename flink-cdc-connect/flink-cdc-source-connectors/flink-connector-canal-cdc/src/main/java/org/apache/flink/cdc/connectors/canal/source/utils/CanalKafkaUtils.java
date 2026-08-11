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

package org.apache.flink.cdc.connectors.canal.source.utils;

import org.apache.flink.cdc.connectors.canal.source.config.CanalSourceConfig;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.annotation.Nullable;

import java.util.Properties;
import java.util.UUID;

/** Shared Kafka consumer configuration helpers for the Canal source. */
public class CanalKafkaUtils {

    private CanalKafkaUtils() {}

    /**
     * Builds the {@link ConsumerConfig} properties for a Kafka consumer: the bootstrap servers, the
     * (optional) group id — a random one is used when {@code null} so that throw-away consumers
     * never participate in the configured group —, the {@link StringDeserializer}s and any
     * user-supplied {@code scan.kafka.properties.*}.
     */
    public static Properties buildConsumerProps(
            CanalSourceConfig sourceConfig, @Nullable String groupId) {
        Properties props = new Properties();
        props.setProperty(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceConfig.getKafkaBootstrapServers());
        props.setProperty(
                ConsumerConfig.GROUP_ID_CONFIG,
                groupId == null || groupId.isEmpty()
                        ? "canal-cdc-" + UUID.randomUUID()
                        : groupId);
        props.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.putAll(sourceConfig.getKafkaProperties());
        return props;
    }
}
