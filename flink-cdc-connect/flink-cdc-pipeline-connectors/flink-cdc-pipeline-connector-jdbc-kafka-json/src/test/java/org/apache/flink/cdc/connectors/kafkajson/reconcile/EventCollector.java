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

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.util.CloseableIterator;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Consumes the connector's {@link Event} iterator on a background thread and exposes a blocking
 * poll with timeout, so a scenario can drain "until quiet" instead of waiting for a fixed count.
 *
 * <p>This is the observable "what arrived" channel: the test thread polls events from here, feeds
 * them into the {@link EventArchive}, and decides when the stream has gone quiet.
 */
public class EventCollector implements Runnable {

    private final CloseableIterator<Event> iterator;
    private final LinkedBlockingQueue<Event> queue = new LinkedBlockingQueue<>();
    private volatile boolean done = false;
    private volatile Throwable failure = null;

    public EventCollector(CloseableIterator<Event> iterator) {
        this.iterator = iterator;
    }

    @Override
    public void run() {
        try {
            while (iterator.hasNext()) {
                queue.put(iterator.next());
            }
        } catch (Throwable t) {
            failure = t;
        } finally {
            done = true;
        }
    }

    /** Returns the next event, or {@code null} if none arrives within the timeout. */
    public Event poll(long timeoutMs) throws InterruptedException {
        return queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /** True once the underlying iterator has been exhausted (or failed). */
    public boolean isDone() {
        return done;
    }

    /** Non-null if the iterator threw while being consumed. */
    public Throwable failure() {
        return failure;
    }
}
