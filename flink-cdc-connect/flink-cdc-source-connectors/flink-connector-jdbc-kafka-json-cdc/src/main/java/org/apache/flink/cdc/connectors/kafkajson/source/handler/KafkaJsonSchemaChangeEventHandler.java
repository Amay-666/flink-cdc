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

package org.apache.flink.cdc.connectors.kafkajson.source.handler;

import org.apache.flink.cdc.connectors.base.relational.handler.SchemaChangeEventHandler;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.schema.SchemaChangeEvent;

import java.util.HashMap;
import java.util.Map;

/**
 * The {@link SchemaChangeEventHandler} of the Canal source: parses the {@code source} struct of a
 * {@link SchemaChangeEvent} into the source-map embedded in the schema-change history record.
 *
 * <p>The upstream canal flatMessage carries no per-event source struct (canal has no binlog
 * position/filename in the DDL message), so the map carries only the fields the downstream
 * deserializer expects: the database name, the server name and the event time.
 */
public class KafkaJsonSchemaChangeEventHandler implements SchemaChangeEventHandler {

    @Override
    public Map<String, Object> parseSource(SchemaChangeEvent event) {
        Map<String, Object> source = new HashMap<>();
        source.put(AbstractSourceInfo.DATABASE_NAME_KEY, event.getDatabase());
        return source;
    }
}
