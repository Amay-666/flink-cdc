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

package org.apache.flink.cdc.connectors.kafkajson.source.message.debezium;

import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.MessageFormat;
import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonMessageParser;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;

/**
 * Parses Debezium-format messages into a {@link DebeziumMessage}.
 *
 * <p>Handles three shapes transparently:
 *
 * <ul>
 *   <li>the standard Debezium envelope {@code {schema, payload}};
 *   <li>schema-include=false ({@code payload} without {@code schema}, or the payload fields at the
 *       top level);
 *   <li>a bare Debezium schema-change record ({@code {databaseName, ddl, ...}}).
 * </ul>
 *
 * <p>No {@code standard}/{@code ticdc} distinction is needed: the TiCDC-specific fields
 * ({@code commit_ts}/{@code cluster_id}) are tolerated by {@code ignoreUnknown=false}-free binding,
 * and a {@code source.commit_ts} presence marks a TiCDC message.
 */
public class DebeziumMessageParser implements KafkaJsonMessageParser {

    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper()
                    // be tolerant to fields introduced by future Debezium / TiCDC versions
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    @Nullable
    public DebeziumMessage parse(String json) {
        if (json == null || json.trim().isEmpty()) {
            return null;
        }
        try {
            JsonNode root = OBJECT_MAPPER.readTree(json);
            // schema-include=false: the payload fields sit at the top level (or a bare
            // schema-change record); otherwise unwrap the envelope
            JsonNode payloadNode = root.has("payload") ? root.get("payload") : root;
            DebeziumMessage.Payload payload = null;
            if (payloadNode != null && payloadNode.isObject()) {
                payload = OBJECT_MAPPER.treeToValue(payloadNode, DebeziumMessage.Payload.class);
            }
            return new DebeziumMessage(payload);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid Debezium message: " + json, e);
        }
    }

    @Override
    public MessageFormat getFormat() {
        return MessageFormat.DEBEZIUM;
    }
}
