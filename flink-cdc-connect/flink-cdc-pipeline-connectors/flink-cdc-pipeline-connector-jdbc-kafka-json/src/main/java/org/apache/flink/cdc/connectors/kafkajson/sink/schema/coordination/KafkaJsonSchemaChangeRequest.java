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

package org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordination;

import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.operators.schema.SchemaOperator;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaRegistry;
import org.apache.flink.cdc.runtime.operators.schema.event.SchemaChangeRequest;

/**
 * The kafkajson request from {@link SchemaOperator} to {@link SchemaRegistry}: the base {@link
 * SchemaChangeRequest} plus the {@code nonce} of FLINK-36690 that distinguishes flush events
 * corresponding to each schema change event from different subTasks, so the coordinator can track
 * them concurrently instead of serializing by subtask ID.
 */
public class KafkaJsonSchemaChangeRequest extends SchemaChangeRequest {

    private static final long serialVersionUID = 1L;

    /**
     * Nonce code to distinguish flush events corresponding to each schema change event from
     * different subTasks.
     */
    private final long nonce;

    public KafkaJsonSchemaChangeRequest(
            TableId tableId, SchemaChangeEvent schemaChangeEvent, int subTaskId, long nonce) {
        super(tableId, schemaChangeEvent, subTaskId);
        this.nonce = nonce;
    }

    public long getNonce() {
        return nonce;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaJsonSchemaChangeRequest)) {
            return false;
        }
        KafkaJsonSchemaChangeRequest that = (KafkaJsonSchemaChangeRequest) o;
        return super.equals(o) && nonce == that.nonce;
    }

    @Override
    public int hashCode() {
        return super.hashCode() * 31 + Long.hashCode(nonce);
    }

    @Override
    public String toString() {
        return super.toString() + ", nonce=" + nonce + '}';
    }
}
