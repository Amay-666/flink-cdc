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

package org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.ddl;

import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.http.DorisHttpClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Helps monitor the completion of heavy (asynchronous) Doris schema changes by polling {@code SHOW
 * ALTER TABLE COLUMN}.
 *
 * <p>Doris executes a {@code MODIFY COLUMN} DDL in two phases: the SQL statement returns
 * immediately, but the actual data rewriting runs in the background as a schema change job. If data
 * writes (especially Group Commit) resume before the job finishes, Doris rejects them. This helper
 * blocks the calling thread until the job reaches a final state ({@code FINISHED} or {@code
 * CANCELLED}).
 *
 * <p>Polling uses <b>progress-aware exponential backoff</b>:
 *
 * <ul>
 *   <li>When the job is in {@code RUNNING} state and the {@code Progress} column shows more tasks
 *       finished since the last poll, the interval <b>resets</b> to the initial value — the job is
 *       alive and making progress, so we poll more eagerly to detect completion.
 *   <li>If progress stalls (same task count), the interval doubles, capped at {@code
 *       poll-max-interval}.
 *   <li>{@code PENDING} and {@code WAITING_TXN} states have no progress info and always use
 *       exponential backoff.
 * </ul>
 *
 * <p>The column order in the {@code SHOW ALTER TABLE COLUMN} result matches Doris's {@code
 * SchemaChangeProcDir.TITLE_NAMES}: JobId(0), TableName(1), CreateTime(2), FinishTime(3),
 * IndexName(4), IndexId(5), OriginIndexId(6), SchemaVersion(7), TransactionId(8), State(9),
 * Msg(10), Progress(11), Timeout(12). The {@code Progress} column has the format {@code
 * "finishedTaskNum/totalTaskNum"} (e.g. {@code "5/10"}) when the job is in {@code RUNNING} state
 * with tasks; it is null or empty when the job is finished or cancelled.
 */
public class DorisSchemaChangeMonitor implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DorisSchemaChangeMonitor.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final DorisDataSinkOptions options;

    public DorisSchemaChangeMonitor(DorisDataSinkOptions options) {
        this.options = options;
    }

    /**
     * Polls {@code SHOW ALTER TABLE COLUMN} with progress-aware exponential backoff until the most
     * recent schema change job for the table reaches a final state.
     *
     * @param httpClient the HTTP client for executing the SHOW query
     * @param event the AlterColumnTypeEvent that triggered the schema change (for error context)
     */
    @SuppressWarnings("BusyWait")
    public void waitForSchemaChangeCompletion(
            DorisHttpClient httpClient, AlterColumnTypeEvent event) throws SchemaEvolveException {
        String database = options.mapDatabase(event.tableId());
        String table = options.mapTable(event.tableId());
        String sql =
                String.format(
                        "SHOW ALTER TABLE COLUMN FROM `%s` WHERE TableName = \"%s\" "
                                + "ORDER BY CreateTime DESC LIMIT 1",
                        database, table);

        long initialIntervalMs = options.getSchemaChangePollInterval().toMillis();
        long maxIntervalMs = options.getSchemaChangePollMaxInterval().toMillis();
        long pollIntervalMs = initialIntervalMs;
        long deadline = System.currentTimeMillis() + options.getSchemaChangeMaxWait().toMillis();
        int attempt = 0;
        int lastFinishedTasks = -1;

        while (System.currentTimeMillis() < deadline) {
            try {
                String responseBody = httpClient.executeQuery(database, sql);
                SchemaChangeStatus status = extractStatus(responseBody);
                if (status.state == null) {
                    LOG.info(
                            "No schema change job found for {}.{} (already cleaned up), "
                                    + "assuming complete.",
                            database,
                            table);
                    return;
                }
                switch (status.state.toUpperCase()) {
                    case "FINISHED":
                        LOG.info(
                                "Schema change completed for {}.{} after {} poll attempt(s).",
                                database,
                                table,
                                attempt + 1);
                        return;
                    case "CANCELLED":
                        throw new SchemaEvolveException(
                                event,
                                "Schema change was cancelled for " + database + "." + table,
                                null);
                    default:
                        if (status.totalTasks > 0) {
                            int pct = status.finishedTasks * 100 / status.totalTasks;
                            if (status.finishedTasks > lastFinishedTasks) {
                                pollIntervalMs = initialIntervalMs;
                            }
                            LOG.info(
                                    "Waiting for schema change on {}.{}: state={}, "
                                            + "progress={}/{} ({}%)",
                                    database,
                                    table,
                                    status.state,
                                    status.finishedTasks,
                                    status.totalTasks,
                                    pct);
                            lastFinishedTasks = status.finishedTasks;
                        } else if (attempt == 0) {
                            LOG.info(
                                    "Waiting for heavy schema change on {}.{} (state: {}). "
                                            + "Group Commit writes will resume after completion.",
                                    database,
                                    table,
                                    status.state);
                        }
                        break;
                }
            } catch (SchemaEvolveException e) {
                throw e;
            } catch (Exception e) {
                LOG.warn(
                        "Failed to poll schema change status for {}.{} (attempt {}): {}",
                        database,
                        table,
                        attempt + 1,
                        e.getMessage());
            }

            attempt++;
            long sleepMs = Math.min(pollIntervalMs, maxIntervalMs);
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SchemaEvolveException(
                        event, "Interrupted while waiting for schema change", null);
            }
            pollIntervalMs = Math.min(pollIntervalMs * 2, maxIntervalMs);
        }

        throw new SchemaEvolveException(
                event,
                "Schema change timed out after "
                        + options.getSchemaChangeMaxWait()
                        + " for "
                        + database
                        + "."
                        + table,
                null);
    }

    private static final class SchemaChangeStatus {
        final String state;
        final int finishedTasks;
        final int totalTasks;

        SchemaChangeStatus(String state, int finishedTasks, int totalTasks) {
            this.state = state;
            this.finishedTasks = finishedTasks;
            this.totalTasks = totalTasks;
        }
    }

    private static SchemaChangeStatus extractStatus(String responseBody) throws Exception {
        JsonNode root = OBJECT_MAPPER.readTree(responseBody);
        JsonNode data = root.path("data");
        JsonNode rows = data.path("rows");
        if (!rows.isArray() || rows.isEmpty()) {
            return new SchemaChangeStatus(null, -1, -1);
        }
        JsonNode firstRow = rows.get(0);
        JsonNode stateNode = firstRow.path(9);
        String state = stateNode.isMissingNode() ? null : stateNode.asText();

        int finished = -1;
        int total = -1;
        JsonNode progressNode = firstRow.path(11);
        if (!progressNode.isMissingNode()) {
            String progress = progressNode.asText("");
            int slash = progress.indexOf('/');
            if (slash > 0) {
                try {
                    finished = Integer.parseInt(progress.substring(0, slash).trim());
                    total = Integer.parseInt(progress.substring(slash + 1).trim());
                } catch (NumberFormatException ignored) {
                    // Progress not in "X/Y" format
                }
            }
        }
        return new SchemaChangeStatus(state, finished, total);
    }
}
