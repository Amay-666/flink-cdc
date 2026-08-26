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
import org.apache.flink.cdc.connectors.kafkajson.source.message.canal.CanalMessageParser;
import org.apache.flink.cdc.connectors.kafkajson.source.message.debezium.DebeziumMessageParser;

/**
 * Creates the {@link KafkaJsonMessageParser} for a configured {@link MessageFormat}.
 *
 * <p>This is the pluggable seam of the message parsing layer: the stream pipeline selects the parser
 * once per source (from {@code scan.message.format}) instead of hard-coding the canal format
 * everywhere (see docs/DEBEZIUM_PLAN.md §S1).
 */
public class KafkaJsonParserFactory {

    private KafkaJsonParserFactory() {}

    /** Returns the parser for the given message format. */
    public static KafkaJsonMessageParser create(MessageFormat format) {
        switch (format) {
            case CANAL:
                return new CanalMessageParser();
            case DEBEZIUM:
                return new DebeziumMessageParser();
            default:
                throw new UnsupportedOperationException("Unsupported message format: " + format);
        }
    }
}
