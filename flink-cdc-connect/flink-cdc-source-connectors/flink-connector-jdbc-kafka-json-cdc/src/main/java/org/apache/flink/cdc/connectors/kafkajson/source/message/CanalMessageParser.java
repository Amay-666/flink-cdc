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
import org.apache.flink.util.FlinkRuntimeException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;

/**
 * Parses the canal flatMessage JSON into a {@link CanalMessage}.
 *
 * <p>This is the {@code scan.message.format=canal} strategy of the pluggable parser layer; the
 * stream pipeline obtains it from {@link KafkaJsonParserFactory}. Its shape mirrors {@link
 * DebeziumMessageParser}.
 */
public class CanalMessageParser implements KafkaJsonMessageParser {

    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper()
                    // be tolerant to fields introduced by future canal versions
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    @Nullable
    public CanalMessage parse(String json) {
        if (json == null || json.trim().isEmpty()) {
            return null;
        }
        try {
            CanalMessage message =
                    OBJECT_MAPPER.readValue(json, CanalMessage.class);
            if (message == null) {
                return null;
            }
            if (message.getData() == null) {
                message.setData(Collections.emptyList());
            }
            if (message.getOld() == null) {
                message.setOld(Collections.emptyList());
            }
            if (message.getSqlType() == null) {
                message.setSqlType(Collections.emptyMap());
            }
            if (message.getMysqlType() == null) {
                message.setMysqlType(Collections.emptyMap());
            }
            return message;
        } catch (IOException e) {
            throw new FlinkRuntimeException("Failed to parse canal flatMessage: " + json, e);
        }
    }

    @Override
    public MessageFormat getFormat() {
        return MessageFormat.CANAL;
    }
}
