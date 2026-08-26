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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * The ground-truth ledger: an append-only, thread-safe list of every mutation the {@link
 * WorkloadDriver} applied to the source table.
 *
 * <p>Written from the {@link WorkloadDriver} thread, read by the verification thread after the
 * scenario finishes. Entries are in driver execution order, which equals MySQL binlog order and
 * therefore canal/Kafka order for a single-threaded driver.
 */
public class Ledger {

    private final ConcurrentLinkedQueue<LedgerEntry> entries = new ConcurrentLinkedQueue<>();
    private final AtomicLong seq = new AtomicLong(0);

    /** Records one row of the pre-snapshot baseline (phase {@link LedgerEntry.Phase#INITIAL}). */
    public void recordInitialRow(CustomerRow row) {
        entries.add(
                new LedgerEntry(
                        seq.getAndIncrement(),
                        LedgerEntry.Op.INSERT,
                        LedgerEntry.Phase.INITIAL,
                        row));
    }

    /** Records an INSERT executed after the snapshot phase. */
    public void recordInsert(CustomerRow row) {
        entries.add(
                new LedgerEntry(
                        seq.getAndIncrement(),
                        LedgerEntry.Op.INSERT,
                        LedgerEntry.Phase.POST_SNAPSHOT,
                        row));
    }

    /** Records an UPDATE executed after the snapshot phase ({@code row} is the full after-row). */
    public void recordUpdate(CustomerRow row) {
        entries.add(
                new LedgerEntry(
                        seq.getAndIncrement(),
                        LedgerEntry.Op.UPDATE,
                        LedgerEntry.Phase.POST_SNAPSHOT,
                        row));
    }

    /**
     * Records a DELETE executed after the snapshot phase ({@code deletedRow} is the removed row).
     */
    public void recordDelete(CustomerRow deletedRow) {
        entries.add(
                new LedgerEntry(
                        seq.getAndIncrement(),
                        LedgerEntry.Op.DELETE,
                        LedgerEntry.Phase.POST_SNAPSHOT,
                        deletedRow));
    }

    /** All entries in execution order. */
    public List<LedgerEntry> all() {
        return new ArrayList<>(entries);
    }

    /** Entries of the post-snapshot phase (the ones that must be emitted exactly once each). */
    public List<LedgerEntry> postSnapshot() {
        return entries.stream()
                .filter(e -> e.phase() == LedgerEntry.Phase.POST_SNAPSHOT)
                .collect(Collectors.toList());
    }

    /** Number of recorded entries. */
    public int size() {
        return entries.size();
    }
}
