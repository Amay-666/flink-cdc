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

package org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.commit;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.http.DorisHttpClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

/**
 * Commits the pre-committed StreamLoad transactions of a completed checkpoint, making their rows
 * visible in Doris.
 *
 * <p>Every failure is thrown rather than retried or signalled, and that is the one decision worth
 * explaining here. {@code CommitRequest} offers three ways out of a failed commit, and two of them
 * lose rows:
 *
 * <ul>
 *   <li>{@code signalFailedWithKnownReason} marks the request failed — and in Flink 1.18.1 that is
 *       all it does. The failure strategy is still a {@code TODO} there (FLINK-25857): {@code
 *       SubtaskCommittableManager.drainCommitted} removes a failed request without committing it
 *       and only bumps a metric. The rows would be gone while the job kept running.
 *   <li>{@code retryLater} would retry forever, which turns a permanent rejection such as Doris's
 *       {@code transaction [N] is already aborted} into a job that spins and never admits it has
 *       lost a batch.
 *   <li>A thrown {@link IOException} fails the task, so the job restarts from its last completed
 *       checkpoint and the source replays the rows into a fresh transaction. That is the only
 *       outcome here that cannot lose data.
 * </ul>
 *
 * <p>The retries that <em>are</em> worth doing happen inside {@link DorisHttpClient}, which retries
 * transport failures and server errors a few times with a backoff before giving up. What reaches
 * this class is either a success (including "already visible", which a restarted job does see,
 * since the commit may have reached Doris before the job died) or something Doris has decided
 * about.
 *
 * <p>One consequence is worth stating plainly, because it is an operational hazard rather than a
 * bug: a batch's transaction is only recoverable while Doris keeps it. If a job is down for longer
 * than {@code stream_load_default_precommit_timeout_second} (3600 s by default), Doris reclaims the
 * pre-committed transaction, and the commit that follows fails on every restart until the job is
 * restarted from a checkpoint older than that downtime.
 */
public class DorisCommitter implements Committer<DorisCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(DorisCommitter.class);

    private final DorisHttpClient httpClient;

    public DorisCommitter(DorisDataSinkOptions options) {
        this(
                new DorisHttpClient(
                        options.getFenodes(),
                        options.getUsername(),
                        options.getPassword(),
                        options.getMaxRetries(),
                        options.getStreamLoadProperties()));
    }

    DorisCommitter(DorisHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public void commit(Collection<CommitRequest<DorisCommittable>> requests)
            throws IOException, InterruptedException {
        for (CommitRequest<DorisCommittable> request : requests) {
            DorisCommittable committable = request.getCommittable();
            httpClient.commitTransaction(committable.getDatabase(), committable.getTransactionId());
            LOG.info(
                    "Committed Doris transaction {} of {}.{} (label {}).",
                    committable.getTransactionId(),
                    committable.getDatabase(),
                    committable.getTable(),
                    committable.getLabel());
        }
    }

    @Override
    public void close() throws Exception {
        httpClient.close();
    }
}
