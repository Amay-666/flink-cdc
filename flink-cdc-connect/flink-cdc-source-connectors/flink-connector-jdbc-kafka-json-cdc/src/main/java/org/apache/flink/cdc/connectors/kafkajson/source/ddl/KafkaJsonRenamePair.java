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

package org.apache.flink.cdc.connectors.kafkajson.source.ddl;

import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * One {@code old TO new} pair of a table rename, with the schema of the table before and after the
 * rename when known.
 *
 * <p>A statement may rename several tables at once ({@code RENAME TABLE a TO b, c TO d}, or the
 * {@code a TO b, b TO a} swap idiom), so the pairs of one statement travel together and in
 * statement order: the database applies them atomically, and only the full set tells a downstream
 * whether the names form a cycle that a single {@code ALTER TABLE ... RENAME} cannot express.
 *
 * <p>Both schemas are {@code null} until resolved against the shared table registry — the DDL
 * parser only knows the SQL, not the pre-rename schemas ({@link
 * KafkaJsonDdlParsedResult#resolveSchemas}).
 */
public class KafkaJsonRenamePair {

    private final TableId oldTableId;
    private final TableId newTableId;
    @Nullable private final Table oldTable;
    @Nullable private final Table newTable;

    public KafkaJsonRenamePair(
            TableId oldTableId,
            TableId newTableId,
            @Nullable Table oldTable,
            @Nullable Table newTable) {
        this.oldTableId = oldTableId;
        this.newTableId = newTableId;
        this.oldTable = oldTable;
        this.newTable = newTable;
    }

    /** Returns a pair carrying its two table ids but no schemas. */
    public static KafkaJsonRenamePair of(TableId oldTableId, TableId newTableId) {
        return new KafkaJsonRenamePair(oldTableId, newTableId, null, null);
    }

    /** Returns a copy of this pair with the given schemas attached. */
    public KafkaJsonRenamePair withSchemas(@Nullable Table oldTable, @Nullable Table newTable) {
        return new KafkaJsonRenamePair(oldTableId, newTableId, oldTable, newTable);
    }

    public TableId getOldTableId() {
        return oldTableId;
    }

    public TableId getNewTableId() {
        return newTableId;
    }

    /** Returns the schema before the rename, or {@code null} when it was never observed. */
    @Nullable
    public Table getOldTable() {
        return oldTable;
    }

    /**
     * Returns the schema after the rename (the pre-rename schema under the new table id), or {@code
     * null} when the pre-rename schema was never observed.
     */
    @Nullable
    public Table getNewTable() {
        return newTable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaJsonRenamePair)) {
            return false;
        }
        KafkaJsonRenamePair that = (KafkaJsonRenamePair) o;
        return Objects.equals(oldTableId, that.oldTableId)
                && Objects.equals(newTableId, that.newTableId)
                && Objects.equals(oldTable, that.oldTable)
                && Objects.equals(newTable, that.newTable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oldTableId, newTableId, oldTable, newTable);
    }

    @Override
    public String toString() {
        return oldTableId + " -> " + newTableId;
    }
}
