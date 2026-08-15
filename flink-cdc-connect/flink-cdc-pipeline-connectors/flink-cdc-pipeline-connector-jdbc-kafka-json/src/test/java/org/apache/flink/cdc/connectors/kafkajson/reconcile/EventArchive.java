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

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.OperationType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The "what actually arrived" record of the connector's output: every {@link DataChangeEvent}
 * emitted by the source, archived per primary key in arrival order.
 *
 * <p>Primary keys and values are extracted straight from the {@link RecordData} of {@link
 * DataChangeEvent#after()} (INSERT/UPDATE) or {@link DataChangeEvent#before()} (DELETE), using the
 * fixed column layout of {@code customers(id INT, name VARCHAR, address VARCHAR)}. {@link
 * org.apache.flink.cdc.common.event.CreateTableEvent}s are ignored.
 *
 * <p>Together with the {@link Ledger} this is the input to {@link LedgerVerifier}: the ledger knows
 * what the source did, the archive knows what the connector emitted, and both use the same {@link
 * CustomerRow} value type so they can be compared field by field.
 */
public class EventArchive {

    /** One archived data event, reduced to the fields the reconciliation needs. */
    public static class EventEntry {
        /** The operation of the archived event, as emitted by the connector. */
        public enum Op {
            INSERT,
            UPDATE,
            DELETE
        }

        private final int pk;
        private final Op op;
        /** The full after-row for INSERT/UPDATE; {@code null} for DELETE. */
        private final CustomerRow after;

        public EventEntry(int pk, Op op, CustomerRow after) {
            this.pk = pk;
            this.op = op;
            this.after = after;
        }

        public int pk() {
            return pk;
        }

        public Op op() {
            return op;
        }

        public CustomerRow after() {
            return after;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof EventEntry)) {
                return false;
            }
            EventEntry that = (EventEntry) o;
            return pk == that.pk && op == that.op && Objects.equals(after, that.after);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pk, op, after);
        }

        @Override
        public String toString() {
            return "EventEntry{pk=" + pk + ", op=" + op + ", after=" + after + "}";
        }
    }

    private final Map<Integer, List<EventEntry>> byPk = new LinkedHashMap<>();

    /** Archives a data change event under its primary key, preserving arrival order. */
    public void add(DataChangeEvent event) {
        int pk;
        EventEntry.Op op = toOp(event.op());
        CustomerRow row;
        if (event.op() == OperationType.INSERT
                || event.op() == OperationType.REPLACE
                || event.op() == OperationType.UPDATE) {
            RecordData after = event.after();
            if (after == null) {
                throw new IllegalStateException(
                        "Expected after image on " + event.op() + " event: " + event);
            }
            pk = after.getInt(0);
            row =
                    new CustomerRow(
                            pk,
                            after.isNullAt(1) ? null : after.getString(1).toString(),
                            after.isNullAt(2) ? null : after.getString(2).toString());
        } else {
            RecordData before = event.before();
            if (before == null) {
                throw new IllegalStateException(
                        "Expected before image on DELETE event (canal sends the removed row in old): "
                                + event);
            }
            pk = before.getInt(0);
            row = null;
        }
        byPk.computeIfAbsent(pk, k -> new ArrayList<>()).add(new EventEntry(pk, op, row));
    }

    /** The archived events, grouped by primary key; each list is in arrival order. */
    public Map<Integer, List<EventEntry>> byPk() {
        return byPk;
    }

    public int size() {
        return byPk.values().stream().mapToInt(List::size).sum();
    }

    private static EventEntry.Op toOp(OperationType op) {
        switch (op) {
            case INSERT:
            case REPLACE:
                return EventEntry.Op.INSERT;
            case UPDATE:
                return EventEntry.Op.UPDATE;
            case DELETE:
                return EventEntry.Op.DELETE;
            default:
                throw new IllegalStateException("Unsupported operation type: " + op);
        }
    }
}
