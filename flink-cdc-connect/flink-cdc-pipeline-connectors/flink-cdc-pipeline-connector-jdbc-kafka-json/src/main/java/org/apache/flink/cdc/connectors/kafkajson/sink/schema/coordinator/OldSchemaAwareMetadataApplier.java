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

import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Schema;

import java.util.Optional;

/**
 * An optional capability of a {@link org.apache.flink.cdc.common.sink.MetadataApplier}: applying an
 * {@link AlterColumnTypeEvent} with the schema the column had before the change.
 *
 * <p>An {@code AlterColumnTypeEvent} only carries the new type of each column, but some sink
 * engines need the old type to decide how to react — Doris, for instance, cannot shrink a {@code
 * CHAR}/ {@code VARCHAR} column the way MySQL/TiDB can, so it must recognise a length reduction and
 * skip the statement. The {@link KafkaJsonSchemaRegistryRequestHandler} resolves that old
 * (pre-change) schema from its {@link KafkaJsonSchemaManager} — it is fetched <em>before</em> the
 * change is applied, so it still reflects the state prior to this event — and hands it to appliers
 * that implement this interface; appliers that do not keep receiving plain {@code
 * applySchemaChange}.
 */
public interface OldSchemaAwareMetadataApplier {

    /**
     * Applies an {@link AlterColumnTypeEvent} knowing the {@code oldSchema} the target table had
     * before the change. The schema may be absent (e.g. no evolved schema registered yet), in which
     * case the applier must degrade to {@link
     * org.apache.flink.cdc.common.sink.MetadataApplier#applySchemaChange} semantics.
     */
    void applyAlterColumnType(AlterColumnTypeEvent event, Optional<Schema> oldSchema)
            throws SchemaEvolveException;
}
