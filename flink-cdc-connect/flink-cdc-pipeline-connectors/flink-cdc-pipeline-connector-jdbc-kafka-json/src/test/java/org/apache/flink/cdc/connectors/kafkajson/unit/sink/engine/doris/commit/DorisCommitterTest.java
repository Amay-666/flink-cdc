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

package org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.commit;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.commit.DorisCommittable;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.commit.DorisCommitter;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.DorisSinkFixtures;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer.Response;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit test for {@link DorisCommitter}: the control call it makes per transaction, and what it does
 * when Doris refuses one.
 *
 * <p>{@code CommitRequest} is implemented by hand rather than mocked — the module's tests do not
 * use mocking frameworks, and Flink's own {@code CommitRequestImpl} lives in an internal package.
 */
public class DorisCommitterTest {

    @Test
    public void testCommitMakesEveryTransactionOfTheCheckpointVisible() throws Exception {
        try (MockDorisServer server = successServer()) {
            DorisCommitter committer = committer(server);

            committer.commit(
                    Arrays.asList(
                            request(new DorisCommittable("shop", "orders", "cdc_l0", 3001L)),
                            request(new DorisCommittable("shop", "orders", "cdc_l1", 3002L))));

            assertThat(server.recorded).hasSize(2);
            assertThat(server.recorded.get(0).path).isEqualTo("/api/shop/_stream_load_2pc");
            assertThat(server.recorded.get(0).method).isEqualTo("PUT");
            assertThat(server.recorded.get(0).headers)
                    .containsEntry("txn_operation", "commit")
                    .containsEntry("txn_id", "3001");
            assertThat(server.recorded.get(1).headers).containsEntry("txn_id", "3002");
            committer.close();
        }
    }

    @Test
    public void testAnEmptyCollectionCommitsNothing() throws Exception {
        try (MockDorisServer server = successServer()) {
            DorisCommitter committer = committer(server);

            committer.commit(Collections.emptyList());

            // A checkpoint with no rows in this subtask still calls the committer.
            assertThat(server.recorded).isEmpty();
            committer.close();
        }
    }

    @Test
    public void testAnAlreadyVisibleTransactionCountsAsCommitted() throws Exception {
        try (MockDorisServer server =
                new MockDorisServer(
                        request ->
                                Response.ok(
                                        "{\"status\":\"Fail\",\"msg\":\"transaction [3001] is"
                                                + " already visible\"}"))) {
            DorisCommitter committer = committer(server);

            // The commit reached Doris, the job died before recording it, and the retry after the
            // restart sees this: the rows are published, which is what the checkpoint wanted.
            committer.commit(
                    Collections.singletonList(
                            request(new DorisCommittable("shop", "orders", "cdc_l0", 3001L))));

            assertThat(server.recorded).hasSize(1);
            committer.close();
        }
    }

    @Test
    public void testCommitOfAnAbortedTransactionFailsTheTask() throws Exception {
        try (MockDorisServer server =
                new MockDorisServer(
                        request ->
                                Response.ok(
                                        "{\"status\":\"Fail\",\"msg\":\"transaction [3001] is"
                                                + " already aborted\"}"))) {
            DorisCommitter committer = committer(server);

            // The rows are gone and nothing will bring them back under this transaction, so the job
            // has to restart from its last checkpoint and replay rather than run on quietly.
            assertThatThrownBy(
                            () ->
                                    committer.commit(
                                            Collections.singletonList(
                                                    request(
                                                            new DorisCommittable(
                                                                    "shop", "orders", "cdc_l0",
                                                                    3001L)))))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("already aborted");
            committer.close();
        }
    }

    @Test
    public void testAFailedCommitIsThrownAndNotSignalled() throws Exception {
        try (MockDorisServer server =
                new MockDorisServer(
                        request ->
                                Response.ok(
                                        "{\"status\":\"Fail\",\"msg\":\"table [orders] is not"
                                                + " found\"}"))) {
            DorisCommitter committer = committer(server);
            FakeCommitRequest failed =
                    request(new DorisCommittable("shop", "orders", "cdc_l0", 3001L));

            assertThatThrownBy(() -> committer.commit(Collections.singletonList(failed)))
                    .isInstanceOf(IOException.class);

            // Why this matters: signalFailedWithKnownReason only marks the request failed in Flink
            // 1.18.1 — SubtaskCommittableManager.drainCommitted then drops it and bumps a metric
            // (FLINK-25857), so the batch would be lost with the job still running. Throwing fails
            // the task instead, and the source replays the rows.
            assertThat(failed.signals).isEmpty();
            committer.close();
        }
    }

    @Test
    public void testCommitStopsAtTheFirstTransactionDorisRefuses() throws Exception {
        try (MockDorisServer server =
                new MockDorisServer(
                        request -> Response.ok("{\"status\":\"Fail\",\"msg\":\"disk full\"}"))) {
            DorisCommitter committer = committer(server);

            assertThatThrownBy(
                            () ->
                                    committer.commit(
                                            Arrays.asList(
                                                    request(
                                                            new DorisCommittable(
                                                                    "shop", "orders", "cdc_l0",
                                                                    3001L)),
                                                    request(
                                                            new DorisCommittable(
                                                                    "shop", "orders", "cdc_l1",
                                                                    3002L)))))
                    .isInstanceOf(IOException.class);

            // The task is failing anyway; continuing would only produce a second, redundant error.
            assertThat(server.recorded).hasSize(1);
            committer.close();
        }
    }

    private static DorisCommitter committer(MockDorisServer server) {
        return new DorisCommitter(DorisSinkFixtures.options(server));
    }

    private static MockDorisServer successServer() throws IOException {
        return new MockDorisServer(request -> Response.ok("{\"status\":\"Success\"}"));
    }

    private static FakeCommitRequest request(DorisCommittable committable) {
        return new FakeCommitRequest(committable);
    }

    /** A {@link CommitRequest} that records everything the committer could have told Flink. */
    private static class FakeCommitRequest implements Committer.CommitRequest<DorisCommittable> {

        private final DorisCommittable committable;
        private final List<String> signals = new ArrayList<>();

        private FakeCommitRequest(DorisCommittable committable) {
            this.committable = committable;
        }

        @Override
        public DorisCommittable getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {
            signals.add("failed-with-known-reason");
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable t) {
            signals.add("failed-with-unknown-reason");
        }

        @Override
        public void retryLater() {
            signals.add("retry-later");
        }

        @Override
        public void updateAndRetryLater(DorisCommittable committable) {
            signals.add("update-and-retry-later");
        }

        @Override
        public void signalAlreadyCommitted() {
            signals.add("already-committed");
        }
    }
}
