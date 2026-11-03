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

package org.apache.flink.cdc.connectors.kafkajson.event;

import org.apache.flink.cdc.common.event.TableId;

/**
 * The kafkaJson flush event: the base {@link org.apache.flink.cdc.common.event.FlushEvent} plus the
 * {@code nonce} of FLINK-36690 that distinguishes flush events corresponding to each schema change
 * event from different subTasks, so the sink writer's acknowledgment can be matched to the specific
 * schema change request it belongs to.
 *
 * <p>Extends the base: {@code tableId} and {@code getTableId()} come from the base; the {@code
 * nonce} is the only addition.
 */
public class FlushEvent extends org.apache.flink.cdc.common.event.FlushEvent {

    /**
     * Nonce code to distinguish flush events corresponding to each schema change event from
     * different subTasks.
     */
    private final long nonce;

    public FlushEvent(TableId tableId, long nonce) {
        super(tableId);
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
        if (!(o instanceof FlushEvent)) {
            return false;
        }
        FlushEvent that = (FlushEvent) o;
        return super.equals(o) && nonce == that.nonce;
    }

    @Override
    public int hashCode() {
        return super.hashCode() * 31 + Long.hashCode(nonce);
    }

    @Override
    public String toString() {
        return "FlushEvent{" + "tableId=" + getTableId() + ", nonce=" + nonce + '}';
    }
}
