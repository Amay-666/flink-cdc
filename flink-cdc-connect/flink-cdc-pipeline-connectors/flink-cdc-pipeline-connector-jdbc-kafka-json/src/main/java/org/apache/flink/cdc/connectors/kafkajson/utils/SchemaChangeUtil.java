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

package org.apache.flink.cdc.connectors.kafkajson.utils;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.kafkajson.event.AlterColumnCommentEvent;
import org.apache.flink.cdc.connectors.kafkajson.source.utils.KafkaJsonColumnMeta;

import io.debezium.relational.Column;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Computes the minimal set of column-level schema change events that transforms one column list
 * into another using rename / add / drop / alter-type / alter-comment operations. A column that
 * only toggles its nullability is treated as unchanged.
 */
public class SchemaChangeUtil {

    /**
     * Finds the minimum schema change events to transform beforeCols into afterCols using recursion
     * with memoization. Available operations: rename column, add column at last, drop column, alter
     * column type, alter column comment. Recursion depth bounded by total column count.
     */
    public static List<SchemaChangeEvent> inferMinimalSchemaChanges(
            TableId cdcTableId, List<Column> beforeCols, List<Column> afterCols) {

        int n = beforeCols.size();
        int m = afterCols.size();

        // memo[i][j] = min cost from state (i, j), -1 means unvisited
        int[][] memo = new int[n + 1][m + 1];
        for (int[] row : memo) {
            Arrays.fill(row, -1);
        }

        // Fill memoization table via recursion
        minCost(0, 0, n, m, beforeCols, afterCols, memo);

        // Traceback to build schema change events
        return tracebackEvents(cdcTableId, n, m, beforeCols, afterCols, memo);
    }

    /**
     * Recursively computes the minimum number of individual column operations to align
     * before[i..n-1] with after[j..m-1], where {@code i} is the current index into {@code
     * beforeCols} (0..n) and {@code j} is the current index into {@code afterCols} (0..m). Boundary
     * cases ({@code i == n} or {@code j == m}) return immediately without list access. At each
     * non-boundary state, either drop before[i] (cost 1) or match before[i] to after[j] (cost =
     * rename(0/1) + alterType(0/1) + alterComment(0/1)). Unmatched after columns are added at the
     * end.
     */
    private static int minCost(
            int i,
            int j,
            int n,
            int m,
            List<Column> beforeCols,
            List<Column> afterCols,
            int[][] memo) {

        if (i == n) {
            return m - j; // add remaining after columns
        }
        if (j == m) {
            return n - i; // drop remaining before columns
        }
        if (memo[i][j] != -1) {
            return memo[i][j];
        }

        // Option 1: drop beforeCols[i]
        int dropCost = 1 + minCost(i + 1, j, n, m, beforeCols, afterCols, memo);

        // Option 2: match beforeCols[i] to afterCols[j]
        int matchCost =
                columnMatchCost(beforeCols.get(i), afterCols.get(j))
                        + minCost(i + 1, j + 1, n, m, beforeCols, afterCols, memo);

        memo[i][j] = Math.min(dropCost, matchCost);
        return memo[i][j];
    }

    /**
     * Computes the min cost of matching a before column to an after column (0 to 3): +1 when the
     * name differs (rename), +1 when the type differs ignoring nullability (alter type), +1 when
     * the comment differs (alter comment). A nullability-only change costs 0.
     */
    private static int columnMatchCost(Column before, Column after) {
        int cost = 0;
        if (!before.name().equals(after.name())) {
            cost++;
        }
        if (!dataTypeIgnoringNullability(before).equals(dataTypeIgnoringNullability(after))) {
            cost++;
        }
        if (!Objects.equals(before.comment(), after.comment())) {
            cost++;
        }
        return cost;
    }

    /**
     * Returns the column's CDC {@link DataType} with nullability pinned to nullable, for change
     * detection. {@code convertToCdcType()} never embeds nullability, so comparing two columns
     * through this helper makes the nullability component cancel out and only real type
     * differences (name, length, precision, ...) affect the comparison.
     */
    private static DataType dataTypeIgnoringNullability(Column column) {
        return KafkaJsonColumnMeta.fromColumn(column).toCdcDataType(true);
    }

    /**
     * Traces back through the memoization table to reconstruct the optimal sequence of schema
     * change events. Events are emitted per-column in the natural left-to-right scan order.
     */
    private static List<SchemaChangeEvent> tracebackEvents(
            TableId cdcTableId,
            int n,
            int m,
            List<Column> beforeCols,
            List<Column> afterCols,
            int[][] memo) {

        List<SchemaChangeEvent> events = new ArrayList<>();

        int i = 0;
        int j = 0;
        while (i < n && j < m) {
            int dropCost = 1 + memoOrBase(i + 1, j, n, m, memo);
            int matchCost =
                    columnMatchCost(beforeCols.get(i), afterCols.get(j))
                            + memoOrBase(i + 1, j + 1, n, m, memo);

            if (dropCost < matchCost) {
                events.add(
                        new DropColumnEvent(
                                cdcTableId, Collections.singletonList(beforeCols.get(i).name())));
                i++;
            } else {
                Column bc = beforeCols.get(i);
                Column ac = afterCols.get(j);

                if (!bc.name().equals(ac.name())) {
                    events.add(
                            new RenameColumnEvent(
                                    cdcTableId, Collections.singletonMap(bc.name(), ac.name())));
                }

                if (!dataTypeIgnoringNullability(bc).equals(dataTypeIgnoringNullability(ac))) {
                    DataType afterType =
                            KafkaJsonColumnMeta.fromColumn(ac).toCdcDataType(ac.isOptional());
                    events.add(
                            new AlterColumnTypeEvent(
                                    cdcTableId, Collections.singletonMap(ac.name(), afterType)));
                }

                if (!Objects.equals(bc.comment(), ac.comment())) {
                    events.add(
                            new AlterColumnCommentEvent(
                                    cdcTableId, Collections.singletonMap(ac.name(), ac.comment())));
                }

                i++;
                j++;
            }
        }

        // Drop remaining before columns
        while (i < n) {
            events.add(
                    new DropColumnEvent(
                            cdcTableId, Collections.singletonList(beforeCols.get(i).name())));
            i++;
        }

        // Add remaining after columns at last. Adds are always trailing because the only available
        // operation is "add column at last". Surviving before-columns (after drops/renames/alter-
        // types) map to a prefix of the after-columns; the unmatched suffix can only be fulfilled
        // by appending.
        while (j < m) {
            events.add(
                    new AddColumnEvent(
                            cdcTableId,
                            Collections.singletonList(
                                    AddColumnEvent.last(toColumn(afterCols.get(j))))));
            j++;
        }

        return events;
    }

    /** Returns memoized cost or computes base case for boundary conditions. */
    private static int memoOrBase(int i, int j, int n, int m, int[][] memo) {
        if (i == n) {
            return m - j;
        }
        if (j == m) {
            return n - i;
        }
        return memo[i][j];
    }

    public static org.apache.flink.cdc.common.schema.Column toColumn(
            io.debezium.relational.Column column) {
        if (column.defaultValueExpression().isPresent()) {
            return org.apache.flink.cdc.common.schema.Column.physicalColumn(
                    column.name(),
                    KafkaJsonColumnMeta.fromColumn(column).toCdcDataType(column.isOptional()),
                    column.comment(),
                    column.defaultValueExpression().get());
        } else {
            return org.apache.flink.cdc.common.schema.Column.physicalColumn(
                    column.name(),
                    KafkaJsonColumnMeta.fromColumn(column).toCdcDataType(column.isOptional()),
                    column.comment());
        }
    }
}
