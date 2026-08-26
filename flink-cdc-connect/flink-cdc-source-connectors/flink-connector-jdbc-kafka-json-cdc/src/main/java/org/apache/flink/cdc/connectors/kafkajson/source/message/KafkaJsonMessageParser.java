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

package org.apache.flink.cdc.connectors.kafkajson.source.message;

import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.MessageFormat;

import javax.annotation.Nullable;

/**
 * Parses one Kafka change-log message (raw JSON string) into a {@link KafkaJsonMessage}.
 *
 * <p>Instances are obtained from {@link KafkaJsonParserFactory} so that the {@code
 * scan.message.format} configuration selects the parser; the stream pipeline never constructs a
 * parser directly.
 */
public interface KafkaJsonMessageParser {

    /**
     * Parses the raw Kafka message into its message object.
     *
     * @param json the raw JSON payload of the Kafka record
     * @return the parsed message, or {@code null} when the payload is blank
     * @throws IllegalArgumentException when the JSON does not match the expected format
     */
    @Nullable
    KafkaJsonMessage parse(String json);

    /** The message format this parser handles. */
    MessageFormat getFormat();
}
