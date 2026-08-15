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

package org.apache.flink.cdc.connectors.kafkajson.reconcile;

import java.util.Objects;

/**
 * One ground-truth mutation recorded by the {@link WorkloadDriver} after it has successfully
 * executed the corresponding SQL on the source database.
 *
 * <p>This is the authoritative "what the source actually did" record: it never travels through the
 * connector, so the reconciliation in {@link LedgerVerifier} can compare it one-to-one against what
 * the connector emitted.
 *
 * <p>{@code row} carries the full after-row for INSERT / UPDATE and the deleted row for DELETE; the
 * verifier compares full rows for INSERT / UPDATE and only the primary key for DELETE.
 */
public class LedgerEntry {

    /** The source-side operation, matching the DML that was executed. */
    public enum Op {
        INSERT,
        UPDATE,
        DELETE
    }

    /**
     * Whether the operation was part of the pre-snapshot baseline (absorbed into the JDBC snapshot)
     * or executed after the snapshot phase (must be emitted as a dedicated stream event, exactly
     * once).
     */
    public enum Phase {
        INITIAL,
        POST_SNAPSHOT
    }

    private final long seq;
    private final Op op;
    private final Phase phase;
    private final CustomerRow row;

    public LedgerEntry(long seq, Op op, Phase phase, CustomerRow row) {
        this.seq = seq;
        this.op = op;
        this.phase = phase;
        this.row = row;
    }

    public long seq() {
        return seq;
    }

    public Op op() {
        return op;
    }

    public Phase phase() {
        return phase;
    }

    public CustomerRow row() {
        return row;
    }

    public int pk() {
        return row.id();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LedgerEntry)) {
            return false;
        }
        LedgerEntry that = (LedgerEntry) o;
        return seq == that.seq
                && op == that.op
                && phase == that.phase
                && Objects.equals(row, that.row);
    }

    @Override
    public int hashCode() {
        return Objects.hash(seq, op, phase, row);
    }

    @Override
    public String toString() {
        return "LedgerEntry{seq=" + seq + ", op=" + op + ", phase=" + phase + ", row=" + row + "}";
    }
}
