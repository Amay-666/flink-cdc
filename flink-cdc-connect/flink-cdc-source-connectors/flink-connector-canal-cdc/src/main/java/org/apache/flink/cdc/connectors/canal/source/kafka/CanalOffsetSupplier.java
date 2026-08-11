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

package org.apache.flink.cdc.connectors.canal.source.kafka;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.canal.source.config.CanalSourceConfig;
import org.apache.flink.cdc.connectors.canal.source.offset.CanalOffset;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Supplies the current position of the change-log stream, the {@link CanalOffset} that the snapshot
 * phase uses as the low/high watermark.
 *
 * <p>The position is the minimum canal event time ({@code es}/{@code ts}) of the newest message in
 * each Kafka partition: any change whose event time is before that position is guaranteed to be
 * present in Kafka, so starting the stream phase from it loses no change (at the cost of possibly
 * replaying a few already-snapshot rows, which the high watermark deduplicates).
 *
 * <p>A single {@link KafkaConsumer} is opened lazily and reused across {@link #current()} calls for
 * the whole reader lifetime; call {@link #close()} when the reader shuts down.
 */
public class CanalOffsetSupplier implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CanalOffsetSupplier.class);

    private final CanalSourceConfig sourceConfig;
    private final KafkaConsumer<String, String> consumer;

    public CanalOffsetSupplier(CanalSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
        this.consumer = new KafkaConsumer<>(CanalKafkaOffsetUtils.buildConsumerProps(sourceConfig));
    }

    /** Constructor with an externally provided consumer (used in unit tests). */
    @VisibleForTesting
    CanalOffsetSupplier(CanalSourceConfig sourceConfig, KafkaConsumer<String, String> consumer) {
        this.sourceConfig = sourceConfig;
        this.consumer = consumer;
    }

    /**
     * Returns the current position of the change-log stream; {@link CanalOffset#INITIAL_OFFSET} if
     * no message is available yet.
     */
    public CanalOffset current() {
        return CanalKafkaOffsetUtils.queryCurrentOffset(
                consumer, sourceConfig.getKafkaTopics(), sourceConfig.getEventTime());
    }

    @Override
    public void close() {
        try {
            consumer.close();
        } catch (Exception e) {
            LOG.warn("Failed to close the CanalOffsetSupplier consumer", e);
        }
    }
}
