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

package org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordinator;

import org.apache.flink.cdc.common.event.SchemaChangeEvent;

import java.util.Collections;
import java.util.List;

/**
 * Schema-change derivation of the kafka-json connector.
 *
 * <p>The connector does not support route rules (table merging / renaming onto a sink schema): the
 * released {@code SchemaDerivation} rewrites routed events through {@code
 * ChangeEventUtils.recreateSchemaChangeEvent}, which calls {@code getType()} and cannot represent
 * the connector's five custom events. Rather than failing deep inside the pipeline, route rules are
 * rejected up front by the coordinator, so this derivation is a pure pass-through — every event is
 * forwarded unchanged to the metadata applier.
 */
public class KafkaJsonSchemaDerivation {

    /** Forwards the event unchanged (no routing support in this connector). */
    public List<SchemaChangeEvent> applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
        return Collections.singletonList(schemaChangeEvent);
    }
}
