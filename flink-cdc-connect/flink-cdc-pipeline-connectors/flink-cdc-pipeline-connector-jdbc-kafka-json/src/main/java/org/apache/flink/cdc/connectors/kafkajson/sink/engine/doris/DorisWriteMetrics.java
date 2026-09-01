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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Write metrics of the {@link DorisSinkWriter}, registered under the {@code doris} scope group of
 * the sink operator: {@code doris.writeRows}, {@code doris.writeBytes}, {@code
 * doris.streamLoadCount}, {@code doris.streamLoadFailed}, {@code doris.flushCount} and the live
 * {@code doris.bufferedRows} gauge. They let a pipeline operator profile StreamLoad throughput and
 * buffering behaviour without parsing the task logs.
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
    private final AtomicInteger bufferedRows = new AtomicInteger();

    public DorisWriteMetrics(MetricGroup metricGroup) {
        if (metricGroup != null) {
            MetricGroup doris = metricGroup.addGroup("doris");
            this.writeRows = doris.counter("writeRows");
            this.writeBytes = doris.counter("writeBytes");
            this.streamLoadCount = doris.counter("streamLoadCount");
            this.streamLoadFailed = doris.counter("streamLoadFailed");
            this.flushCount = doris.counter("flushCount");
            doris.gauge("bufferedRows", bufferedRows::get);
        } else {
            this.writeRows = null;
            this.writeBytes = null;
            this.streamLoadCount = null;
            this.streamLoadFailed = null;
            this.flushCount = null;
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
}
