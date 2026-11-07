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

package org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris;

import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.writer.DorisSinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import javax.annotation.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Write metrics of the {@link DorisSinkWriter}, registered under the {@code doris} scope group of
 * the sink operator: {@code doris.writeRows}, {@code doris.writeBytes}, {@code
 * doris.streamLoadCount}, {@code doris.streamLoadFailed}, {@code doris.flushCount}, {@code
 * doris.loadedRows}, {@code doris.filteredRows} and the live {@code doris.bufferedRows} gauge.
 * They let a pipeline operator profile StreamLoad throughput and buffering behaviour without
 * parsing the task logs.
 *
 * <p>{@code loadedRows}/{@code filteredRows} carry Doris's own per-load accounting ({@code
 * NumberLoadedRows}/{@code NumberFilteredRows} from the load responses, reported by the HTTP
 * client). With the default {@code max_filter_ratio=0} a filtered row fails the whole load, but a
 * ratio configured above zero lets Doris drop rows under a {@code Status: Success} — these
 * counters are what makes that visible: a sink whose {@code writeRows} grows while {@code
 * loadedRows} does not follow is losing rows on the Doris side.
 *
 * <p>When Group Commit is enabled ({@code groupCommitMode != off}) two more counters appear:
 * {@code doris.groupCommitLoads} and {@code doris.groupCommitFallbackLoads}, fed from the {@code
 * GroupCommit} field of each load response — Doris's own answer to "was this batch really group
 * committed". A {@code groupCommitFallbackLoads} that grows means Doris silently degraded loads
 * to normal mode (missing sequence column, heavy schema change, WAL space insufficient for
 * async_mode, ...), the same condition the HTTP client logs as a WARN. With Group Commit off the
 * pair is not registered at all, so the metric tree says nothing it cannot back up.
 *
 * <p>A {@code null} {@link MetricGroup} (e.g. the hand-stubbed {@code InitContext} of unit tests)
 * disables the metrics: every {@code record*}/{@code set*} call becomes a no-op.
 */
public class DorisWriteMetrics {

    private final Counter writeRows;
    private final Counter writeBytes;
    private final Counter streamLoadCount;
    private final Counter streamLoadFailed;
    private final Counter flushCount;
    private final Counter loadedRows;
    private final Counter filteredRows;
    @Nullable private final Counter groupCommitLoads;
    @Nullable private final Counter groupCommitFallbackLoads;
    private final AtomicInteger bufferedRows = new AtomicInteger();

    public DorisWriteMetrics(MetricGroup metricGroup, boolean groupCommitEnabled) {
        if (metricGroup != null) {
            MetricGroup doris = metricGroup.addGroup("doris");
            this.writeRows = doris.counter("writeRows");
            this.writeBytes = doris.counter("writeBytes");
            this.streamLoadCount = doris.counter("streamLoadCount");
            this.streamLoadFailed = doris.counter("streamLoadFailed");
            this.flushCount = doris.counter("flushCount");
            this.loadedRows = doris.counter("loadedRows");
            this.filteredRows = doris.counter("filteredRows");
            doris.gauge("bufferedRows", bufferedRows::get);
            if (groupCommitEnabled) {
                this.groupCommitLoads = doris.counter("groupCommitLoads");
                this.groupCommitFallbackLoads = doris.counter("groupCommitFallbackLoads");
            } else {
                this.groupCommitLoads = null;
                this.groupCommitFallbackLoads = null;
            }
        } else {
            this.writeRows = null;
            this.writeBytes = null;
            this.streamLoadCount = null;
            this.streamLoadFailed = null;
            this.flushCount = null;
            this.loadedRows = null;
            this.filteredRows = null;
            this.groupCommitLoads = null;
            this.groupCommitFallbackLoads = null;
        }
    }

    /** Records one row buffered for StreamLoad. */
    public void recordWriteRow() {
        if (writeRows != null) {
            writeRows.inc();
        }
    }

    /** Records a successful StreamLoad that sent {@code bytes} in its JSON body. */
    public void recordStreamLoad(int bytes) {
        if (streamLoadCount != null) {
            streamLoadCount.inc();
            writeBytes.inc(bytes);
        }
    }

    /** Records a StreamLoad that failed (the writer surfaces the {@code IOException}). */
    public void recordStreamLoadFailure() {
        if (streamLoadFailed != null) {
            streamLoadFailed.inc();
        }
    }

    /** Records a forced flush (per {@code FlushEvent} / checkpoint / periodic timer). */
    public void recordFlush() {
        if (flushCount != null) {
            flushCount.inc();
        }
    }

    /** Publishes the current number of rows held in the writer buffer. */
    public void setBufferedRows(int rows) {
        bufferedRows.set(rows);
    }

    /**
     * Records the rows Doris reported as loaded by a committed load ({@code NumberLoadedRows}).
     * Growing roughly in step with {@code writeRows} is the healthy shape; a gap means rows are
     * being lost between the writer and the storage.
     */
    public void addLoadedRows(long rows) {
        if (loadedRows != null && rows > 0) {
            loadedRows.inc(rows);
        }
    }

    /**
     * Records the rows Doris reported as filtered by a committed load ({@code
     * NumberFilteredRows}). Only possible with a {@code max_filter_ratio} above zero, but
     * whatever the cause, a non-zero counter is a silent data loss in progress.
     */
    public void addFilteredRows(long rows) {
        if (filteredRows != null && rows > 0) {
            filteredRows.inc(rows);
        }
    }

    /**
     * Records the {@code GroupCommit} flag of a committed Group-Commit load response: whether
     * Doris really applied group commit to the batch. A {@code false} is the silent fallback the
     * HTTP client also logs as a WARN; a growing {@code groupCommitFallbackLoads} next to a flat
     * {@code groupCommitLoads} shows the degradation rate on a dashboard. No-op unless Group
     * Commit was enabled when the metrics were built.
     */
    public void recordGroupCommitLoad(boolean groupCommitUsed) {
        if (groupCommitLoads != null && groupCommitFallbackLoads != null) {
            (groupCommitUsed ? groupCommitLoads : groupCommitFallbackLoads).inc();
        }
    }
}
