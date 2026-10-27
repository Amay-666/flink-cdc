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

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.runtime.serializer.event.CreateTableEventSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A group of {@link CreateTableEvent}s — the schemas of the tables to snapshot, read once at job
 * start — emitted as one record.
 *
 * <p>It exists so the snapshot's tables are created in a few schema-change rounds instead of one
 * per table. The sink applies every create of the batch under a single request, after the single
 * flush the batch triggers, and the schema operator waits for that one result before it forwards
 * the first row of any of the tables. A create needs no flush of its own (a table that is being
 * created cannot have buffered rows) and creates of different tables never conflict, so applying
 * them back to back is safe; the per-table {@link CreateTableEvent}s are unfolded by the
 * coordinator and travel downstream unchanged. A batch interrupted part-way (apply timeout, job
 * restart) is safe to replay: the tables it already created are skipped as redundant when the
 * coordinator's checkpointed schema state knows them, and table creation itself is idempotent
 * ({@code CREATE TABLE IF NOT EXISTS}) when it does not.
 *
 * <p>Deliberately not a {@link CreateTableEvent} subclass: one event carries many tables, and the
 * serializer dispatches on {@code instanceof CreateTableEvent} to the single-table encoder, so a
 * subclass would be silently encoded as one table.
 */
public class BatchCreateTableEvents implements SchemaChangeEvent {

    private static final long serialVersionUID = 1L;

    /**
     * Representative id used for the request logs and the flush nonce. It is not a table — every
     * consumer either unfolds the batch or only logs it.
     */
    public static final TableId BATCH_TABLE_ID =
            TableId.tableId("<kafkajson>", "<create-table-batch>");

    private final List<CreateTableEvent> createTableEvents;

    public BatchCreateTableEvents(List<CreateTableEvent> createTableEvents) {
        Preconditions.checkArgument(
                createTableEvents != null && !createTableEvents.isEmpty(),
                "A create-table batch must not be empty.");
        this.createTableEvents = Collections.unmodifiableList(new ArrayList<>(createTableEvents));
    }

    /** Returns the create events of this batch, in emission order. */
    public List<CreateTableEvent> getCreateTableEvents() {
        return createTableEvents;
    }

    /**
     * Splits the given creates into batches of at most {@code maxTables} tables and at most {@code
     * maxBytes} encoded bytes each, keeping the input order. A table whose own encoded size exceeds
     * {@code maxBytes} still travels as a single-table batch, so the split always makes progress.
     *
     * <p>Two limits rather than one because they bound different things: the table count bounds how
     * many DDLs one schema-change round applies (one connection to the external system, one
     * blocking window for the data flow), while the byte count bounds the encoded record — its
     * memory and the size of the coordination request carrying it. A table is measured as its
     * {@link CreateTableEvent}, the exact payload the batch serializer writes for it, so the sum of
     * a chunk's sizes is the size of the record it becomes.
     */
    public static List<BatchCreateTableEvents> partition(
            List<CreateTableEvent> createTableEvents, int maxTables, int maxBytes) {
        Preconditions.checkArgument(
                maxTables > 0 && maxBytes > 0,
                "A create-table batch needs a positive table and byte limit.");

        List<BatchCreateTableEvents> batches = new ArrayList<>();
        List<CreateTableEvent> currentBatch = new ArrayList<>();
        ByteArrayOutputStream sizeCounter = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper sizeView = new DataOutputViewStreamWrapper(sizeCounter);
        int currentBatchBytes = 0;
        for (CreateTableEvent createTableEvent : createTableEvents) {
            int eventBytes = encodedSize(createTableEvent, sizeCounter, sizeView);
            if (currentBatch.size() >= maxTables
                    || (!currentBatch.isEmpty() && currentBatchBytes + eventBytes > maxBytes)) {
                batches.add(new BatchCreateTableEvents(currentBatch));
                currentBatch = new ArrayList<>();
                currentBatchBytes = 0;
            }
            currentBatch.add(createTableEvent);
            currentBatchBytes += eventBytes;
        }
        if (!currentBatch.isEmpty()) {
            batches.add(new BatchCreateTableEvents(currentBatch));
        }
        return batches;
    }

    private static int encodedSize(
            CreateTableEvent createTableEvent,
            ByteArrayOutputStream sizeCounter,
            DataOutputViewStreamWrapper sizeView) {
        sizeCounter.reset();
        try {
            CreateTableEventSerializer.INSTANCE.serialize(createTableEvent, sizeView);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to measure the schema of table " + createTableEvent.tableId(), e);
        }
        return sizeCounter.size();
    }

    @Override
    public TableId tableId() {
        return BATCH_TABLE_ID;
    }

    @Override
    public SchemaChangeEventType getType() {
        return SchemaChangeEventType.CREATE_TABLE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BatchCreateTableEvents)) {
            return false;
        }
        BatchCreateTableEvents that = (BatchCreateTableEvents) o;
        return createTableEvents.equals(that.createTableEvents);
    }

    @Override
    public int hashCode() {
        return createTableEvents.hashCode();
    }

    @Override
    public String toString() {
        return "BatchCreateTableEvents{" + createTableEvents.size() + " tables}";
    }
}
