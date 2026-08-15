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

import org.apache.flink.cdc.connectors.kafkajson.reconcile.EventArchive.EventEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.kafkajson.reconcile.EventArchive.EventEntry.Op.DELETE;
import static org.apache.flink.cdc.connectors.kafkajson.reconcile.EventArchive.EventEntry.Op.INSERT;
import static org.apache.flink.cdc.connectors.kafkajson.reconcile.EventArchive.EventEntry.Op.UPDATE;

/**
 * Reconciles what the connector emitted ({@link EventArchive}) against what the source actually did
 * ({@link Ledger}), proving the "no duplicate, no loss, final state converges" property.
 *
 * <p>For each primary key it compares the expected event sequence with the actual one:
 *
 * <ul>
 *   <li>a key present in the pre-snapshot baseline is expected to be delivered once by the JDBC
 *       snapshot ({@code INSERT}), followed by exactly the post-snapshot operations recorded in the
 *       ledger, one event each, in order;
 *   <li>a key created after the snapshot is expected to be delivered exactly once per recorded
 *       operation.
 * </ul>
 *
 * <p>Any surplus event is a duplicate, any missing event is a loss, and any value / operation
 * mismatch is reported with the exact index. Independently, the events are replayed into an
 * in-memory model and the resulting final state must equal both the driver's expected model and the
 * actual final state of the source table (queried out-of-band).
 */
public class LedgerVerifier {

    /**
     * A single reconciliation failure; {@link #toString()} is human-readable and keyed by category.
     */
    public static class Violation {

        /** The category of a reconciliation violation. */
        public enum Type {
            /**
             * An event exists that the ledger does not account for, or an expected event is
             * missing.
             */
            EVENT_SEQUENCE_MISMATCH,
            /** A row was inserted twice, or updated/deleted while it did not exist. */
            ILLEGAL_SEQUENCE,
            /** The final state reconstructed from events differs from the expected final state. */
            STATE_MISMATCH,
            /**
             * The driver's expected model and the real source table disagree (driver bug, not
             * connector).
             */
            SOURCE_MODEL_MISMATCH
        }

        private final Type type;
        private final String detail;

        public Violation(Type type, String detail) {
            this.type = type;
            this.detail = detail;
        }

        public Type type() {
            return type;
        }

        @Override
        public String toString() {
            return type + ": " + detail;
        }
    }

    private LedgerVerifier() {}

    /**
     * Runs the full reconciliation. {@code expectedModel} is the driver's in-memory final state,
     * {@code sourceFinalState} is the source table queried after the scenario has drained.
     */
    public static List<Violation> verify(
            Ledger ledger,
            EventArchive archive,
            Map<Integer, CustomerRow> expectedModel,
            List<CustomerRow> sourceFinalState) {
        List<Violation> violations = new ArrayList<>();

        Map<Integer, List<LedgerEntry>> ledgerByPk = new LinkedHashMap<>();
        for (LedgerEntry entry : ledger.all()) {
            ledgerByPk.computeIfAbsent(entry.pk(), k -> new ArrayList<>()).add(entry);
        }

        Set<Integer> keys = new TreeSet<>(ledgerByPk.keySet());
        keys.addAll(archive.byPk().keySet());

        Map<Integer, CustomerRow> reconstructed = new HashMap<>();

        for (Integer key : keys) {
            List<LedgerEntry> expectedLedger = ledgerByPk.getOrDefault(key, new ArrayList<>());
            List<EventEntry> actualEvents = archive.byPk().getOrDefault(key, new ArrayList<>());

            List<EventEntry> expectedEvents = expectedEventsFor(expectedLedger);
            List<String> diffs = diff(expectedEvents, actualEvents);
            if (!diffs.isEmpty()) {
                violations.add(
                        new Violation(
                                Violation.Type.EVENT_SEQUENCE_MISMATCH,
                                "key=" + key + " " + String.join("; ", diffs)));
            }

            // Structural legality + state reconstruction: replay events into an in-memory table.
            for (EventEntry event : actualEvents) {
                switch (event.op()) {
                    case INSERT:
                        if (reconstructed.containsKey(key)) {
                            violations.add(
                                    new Violation(
                                            Violation.Type.ILLEGAL_SEQUENCE,
                                            "key="
                                                    + key
                                                    + " INSERT while the row already exists (duplicate): "
                                                    + event));
                        }
                        reconstructed.put(key, event.after());
                        break;
                    case UPDATE:
                        if (!reconstructed.containsKey(key)) {
                            violations.add(
                                    new Violation(
                                            Violation.Type.ILLEGAL_SEQUENCE,
                                            "key="
                                                    + key
                                                    + " UPDATE of a row that does not exist: "
                                                    + event));
                        } else {
                            reconstructed.put(key, event.after());
                        }
                        break;
                    case DELETE:
                        if (!reconstructed.containsKey(key)) {
                            violations.add(
                                    new Violation(
                                            Violation.Type.ILLEGAL_SEQUENCE,
                                            "key="
                                                    + key
                                                    + " DELETE of a row that does not exist (duplicate delete): "
                                                    + event));
                        } else {
                            reconstructed.remove(key);
                        }
                        break;
                    default:
                        break;
                }
            }
        }

        List<CustomerRow> expectedFinal =
                expectedModel.values().stream()
                        .sorted((a, b) -> Integer.compare(a.id(), b.id()))
                        .collect(Collectors.toList());
        List<CustomerRow> reconstructedFinal =
                reconstructed.values().stream()
                        .sorted((a, b) -> Integer.compare(a.id(), b.id()))
                        .collect(Collectors.toList());

        if (!reconstructedFinal.equals(expectedFinal)) {
            violations.add(
                    new Violation(
                            Violation.Type.STATE_MISMATCH,
                            "reconstructed-from-events="
                                    + reconstructedFinal
                                    + " expected(driver model)="
                                    + expectedFinal));
        }
        if (!sourceFinalState.equals(expectedFinal)) {
            violations.add(
                    new Violation(
                            Violation.Type.SOURCE_MODEL_MISMATCH,
                            "source-db="
                                    + sourceFinalState
                                    + " driver-model="
                                    + expectedFinal
                                    + " (the workload itself did not behave as the driver expects)"));
        }
        return violations;
    }

    /**
     * The expected per-key event sequence: one snapshot {@code INSERT} if the key was part of the
     * pre-snapshot baseline, then exactly one {@link EventEntry} per post-snapshot ledger
     * operation, in ledger (binlog) order.
     */
    private static List<EventEntry> expectedEventsFor(List<LedgerEntry> ledgerForKey) {
        List<EventEntry> expected = new ArrayList<>();
        for (LedgerEntry entry : ledgerForKey) {
            if (entry.phase() == LedgerEntry.Phase.INITIAL) {
                expected.add(new EventEntry(entry.pk(), INSERT, entry.row()));
            } else {
                switch (entry.op()) {
                    case INSERT:
                        expected.add(new EventEntry(entry.pk(), INSERT, entry.row()));
                        break;
                    case UPDATE:
                        expected.add(new EventEntry(entry.pk(), UPDATE, entry.row()));
                        break;
                    case DELETE:
                        expected.add(new EventEntry(entry.pk(), DELETE, null));
                        break;
                    default:
                        break;
                }
            }
        }
        return expected;
    }

    /** Index-by-index diff of the expected vs actual event sequences; empty means they match. */
    private static List<String> diff(List<EventEntry> expected, List<EventEntry> actual) {
        List<String> diffs = new ArrayList<>();
        int n = Math.max(expected.size(), actual.size());
        for (int i = 0; i < n; i++) {
            boolean hasExpected = i < expected.size();
            boolean hasActual = i < actual.size();
            if (!hasExpected) {
                diffs.add("index " + i + ": extra (duplicate) event " + actual.get(i));
            } else if (!hasActual) {
                diffs.add("index " + i + ": missing (lost) event " + expected.get(i));
            } else if (!expected.get(i).equals(actual.get(i))) {
                diffs.add(
                        "index "
                                + i
                                + ": expected "
                                + expected.get(i)
                                + " but got "
                                + actual.get(i));
            }
        }
        return diffs;
    }
}
