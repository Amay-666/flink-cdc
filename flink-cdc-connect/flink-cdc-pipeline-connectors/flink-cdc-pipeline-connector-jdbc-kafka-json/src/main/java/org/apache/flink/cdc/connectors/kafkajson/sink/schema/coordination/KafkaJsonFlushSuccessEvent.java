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

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaRegistry;
import org.apache.flink.cdc.runtime.operators.schema.event.FlushSuccessEvent;
import org.apache.flink.cdc.runtime.operators.sink.DataSinkWriterOperator;

/**
 * The kafkajson flush-success event from {@link DataSinkWriterOperator} to {@link SchemaRegistry}:
 * the base {@link FlushSuccessEvent} plus the {@code nonce} of FLINK-36690, so the coordinator can
 * match the acknowledgment to the specific schema change request it belongs to, not just the
 * subtask.
 */
public class KafkaJsonFlushSuccessEvent extends FlushSuccessEvent {

    private static final long serialVersionUID = 1L;

    /**
     * Nonce code to distinguish flush events corresponding to each schema change event from
     * different subTasks.
     */
    private final long nonce;

    public KafkaJsonFlushSuccessEvent(int subtask, TableId tableId, long nonce) {
        super(subtask, tableId);
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
        if (!(o instanceof KafkaJsonFlushSuccessEvent)) {
            return false;
        }
        KafkaJsonFlushSuccessEvent that = (KafkaJsonFlushSuccessEvent) o;
        return super.equals(o) && nonce == that.nonce;
    }

    @Override
    public int hashCode() {
        return super.hashCode() * 31 + Long.hashCode(nonce);
    }

    @Override
    public String toString() {
        return super.toString().replaceAll("}$", "") + ", nonce=" + nonce + "}";
    }
}
