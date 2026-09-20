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

package org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http;

import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.http.DorisHttpClient;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.http.DorisHttpClient.LabelAlreadyExistsException;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer.Response;

import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit test for the two-phase commit calls of {@link DorisHttpClient}: the pre-commit that opens a
 * transaction, and the commit/abort control calls that decide its fate.
 *
 * <p>The protocol itself was measured against doris-2.1.8 (see {@code
 * src/test/resources/doris/two-phase-commit-experiment.sql}); what is pinned here is that this
 * client speaks it — most importantly that {@code two_phase_commit: true} reaches the backend leg
 * of the redirect, since a header set only on the FE leg would let the load commit immediately.
 *
 * <p>Rejections are asserted as {@link IOException}: the client's own exception type is
 * package-private, so a test outside the connector can only name its supertype. The message is
 * asserted alongside it, which is what actually separates the cases.
 */
public class DorisHttpClientTwoPhaseCommitTest {

    @Test
    public void testPrecommitCarriesTwoPhaseCommitOnBothLegs() throws IOException {
        AtomicReference<String> backend = new AtomicReference<>();
        AtomicInteger requests = new AtomicInteger();
        try (MockDorisServer server =
                new MockDorisServer(
                        req ->
                                requests.getAndIncrement() == 0
                                        ? new Response(
                                                307,
                                                "",
                                                Collections.singletonMap(
                                                        "Location",
                                                        backend.get()
                                                                + "/api/shop/orders/_stream_load"))
                                        : Response.ok(
                                                "{\"Status\":\"Success\",\"TxnId\":4001,"
                                                        + "\"TwoPhaseCommit\":\"true\"}"))) {
            backend.set("http://" + server.endpoint());
            DorisHttpClient client = client(server);

            DorisHttpClient.PrecommitResult precommit =
                    client.precommitStreamLoad(
                            "shop",
                            "orders",
                            "cdc_shop_orders_0_8_0",
                            Collections.singletonList(Collections.singletonMap("id", 1)));

            assertThat(precommit.getTransactionId()).isEqualTo(4001L);
            assertThat(precommit.getBodyBytes()).isEqualTo(10);
            assertThat(server.recorded).hasSize(2);
            // The FE leg.
            assertThat(server.recorded.get(0).path).isEqualTo("/api/shop/orders/_stream_load");
            assertThat(server.recorded.get(0).headers)
                    .containsEntry("label", "cdc_shop_orders_0_8_0")
                    .containsEntry("two_phase_commit", "true")
                    .containsEntry("Expect", "100-continue");
            // The BE leg, which is the one that actually writes: a plain 307 is not a proxy, so the
            // header has to be sent again.
            assertThat(server.recorded.get(1).body).isEqualTo("[{\"id\":1}]");
            assertThat(server.recorded.get(1).headers)
                    .containsEntry("label", "cdc_shop_orders_0_8_0")
                    .containsEntry("two_phase_commit", "true");
        }
    }

    @Test
    public void testPrecommitWithoutATransactionIdFails() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(req -> Response.ok("{\"Status\":\"Success\"}"))) {
            DorisHttpClient client = client(server);

            // Without the id the transaction can never be committed; failing here says which load
            // did it, while letting it pass would leave the rows invisible until Doris timed out.
            assertThatThrownBy(
                            () ->
                                    client.precommitStreamLoad(
                                            "shop",
                                            "orders",
                                            "cdc_shop_orders_0_8_0",
                                            Collections.singletonList(
                                                    Collections.singletonMap("id", 1))))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("without reporting a transaction id");
        }
    }

    @Test
    public void testPrecommitLabelCollisionReportsTheOpenTransaction() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(
                        req ->
                                Response.ok(
                                        "{\"Status\":\"Label Already Exists\","
                                                + "\"ExistingJobStatus\":\"PRECOMMITTED\","
                                                + "\"Message\":\"Label [cdc_shop_orders_0_8_0] has"
                                                + " already been used, relate to txn [4001], status"
                                                + " [PRECOMMITTED].\"}"))) {
            DorisHttpClient client = client(server);

            assertThatThrownBy(
                            () ->
                                    client.precommitStreamLoad(
                                            "shop",
                                            "orders",
                                            "cdc_shop_orders_0_8_0",
                                            Collections.singletonList(
                                                    Collections.singletonMap("id", 1))))
                    .isInstanceOf(LabelAlreadyExistsException.class)
                    .satisfies(
                            thrown -> {
                                LabelAlreadyExistsException conflict =
                                        (LabelAlreadyExistsException) thrown;
                                assertThat(conflict.getLabel()).isEqualTo("cdc_shop_orders_0_8_0");
                                assertThat(conflict.getExistingJobStatus())
                                        .isEqualTo("PRECOMMITTED");
                                // Read out of the message: Doris reports -1 in the TxnId field
                                // here.
                                assertThat(conflict.getExistingTransactionId()).isEqualTo(4001L);
                                assertThat(conflict.isPrecommitted()).isTrue();
                                assertThat(conflict.isFinished()).isFalse();
                            });
        }
    }

    @Test
    public void testFinishedLabelCollisionIsNotAbortable() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(
                        req ->
                                Response.ok(
                                        "{\"Status\":\"Label Already Exists\","
                                                + "\"ExistingJobStatus\":\"FINISHED\","
                                                + "\"Message\":\"Label [cdc_shop_orders_0_8_0] has"
                                                + " already been used, relate to txn [4001], status"
                                                + " [FINISHED].\"}"))) {
            DorisHttpClient client = client(server);

            assertThatThrownBy(
                            () ->
                                    client.precommitStreamLoad(
                                            "shop",
                                            "orders",
                                            "cdc_shop_orders_0_8_0",
                                            Collections.singletonList(
                                                    Collections.singletonMap("id", 1))))
                    .isInstanceOf(LabelAlreadyExistsException.class)
                    .satisfies(
                            thrown -> {
                                LabelAlreadyExistsException conflict =
                                        (LabelAlreadyExistsException) thrown;
                                assertThat(conflict.isFinished()).isTrue();
                                assertThat(conflict.isPrecommitted()).isFalse();
                            });
        }
    }

    @Test
    public void testCommitTransactionSendsTheControlRequest() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(req -> Response.ok("{\"status\":\"Success\"}"))) {
            DorisHttpClient client = client(server);

            client.commitTransaction("shop", 4001L);

            assertThat(server.recorded).hasSize(1);
            assertThat(server.recorded.get(0).method).isEqualTo("PUT");
            assertThat(server.recorded.get(0).path).isEqualTo("/api/shop/_stream_load_2pc");
            assertThat(server.recorded.get(0).headers)
                    .containsEntry("txn_operation", "commit")
                    .containsEntry("txn_id", "4001");
        }
    }

    @Test
    public void testCommitTransactionFollowsFeRedirect() throws IOException {
        AtomicReference<String> backend = new AtomicReference<>();
        AtomicInteger requests = new AtomicInteger();
        try (MockDorisServer server =
                new MockDorisServer(
                        req ->
                                requests.getAndIncrement() == 0
                                        ? new Response(
                                                307,
                                                "",
                                                Collections.singletonMap(
                                                        "Location",
                                                        backend.get()
                                                                + "/api/shop/_stream_load_2pc"))
                                        : Response.ok("{\"status\":\"Success\"}"))) {
            backend.set("http://" + server.endpoint());
            DorisHttpClient client = client(server);

            client.commitTransaction("shop", 4001L);

            // The endpoint redirects like a load does, so the control call is re-issued against the
            // backend it points at.
            assertThat(server.recorded).hasSize(2);
            assertThat(server.recorded.get(1).path).isEqualTo("/api/shop/_stream_load_2pc");
            assertThat(server.recorded.get(1).headers)
                    .containsEntry("txn_operation", "commit")
                    .containsEntry("txn_id", "4001");
        }
    }

    @Test
    public void testCommitOfAnAlreadyVisibleTransactionSucceeds() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(
                        req ->
                                Response.ok(
                                        "{\"status\":\"Fail\",\"msg\":\"transaction [4001] is"
                                                + " already visible\"}"))) {
            DorisHttpClient client = client(server);

            // A restart commits a transaction the previous attempt already committed: the rows are
            // published, which is what the checkpoint wanted, so there is nothing left to do.
            client.commitTransaction("shop", 4001L);
        }
    }

    @Test
    public void testCommitOfAnAbortedTransactionFails() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(
                        req ->
                                Response.ok(
                                        "{\"status\":\"Fail\",\"msg\":\"transaction [4001] is"
                                                + " already aborted\"}"))) {
            DorisHttpClient client = client(server);

            // The batch is gone; committing it is impossible and pretending otherwise would let the
            // job run on with rows missing.
            assertThatThrownBy(() -> client.commitTransaction("shop", 4001L))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("already aborted");
        }
    }

    @Test
    public void testAbortOfAMissingTransactionSucceeds() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(
                        req ->
                                Response.ok(
                                        "{\"status\":\"Fail\",\"msg\":\"transaction [4001] not"
                                                + " found\"}"))) {
            DorisHttpClient client = client(server);

            // Doris has already reclaimed it, so there is nothing left to abort. This is what a
            // restart sees when the paused job outlived the transaction timeout.
            client.abortTransaction("shop", 4001L);
        }
    }

    @Test
    public void testAbortOfAnAlreadyAbortedTransactionFails() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(
                        req ->
                                Response.ok(
                                        "{\"status\":\"Fail\",\"msg\":\"transaction [4001] is"
                                                + " already aborted\"}"))) {
            DorisHttpClient client = client(server);

            // Measured on doris-2.1.8: this comes back when the transaction was aborted by someone
            // else — another subtask, or an operator clearing a label by hand. Only "not found"
            // means Doris has forgotten the transaction, so this one stays a failure.
            assertThatThrownBy(() -> client.abortTransaction("shop", 4001L))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("already aborted");
        }
    }

    @Test
    public void testAbortTransactionSendsTheControlRequest() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(req -> Response.ok("{\"status\":\"Success\"}"))) {
            DorisHttpClient client = client(server);

            client.abortTransaction("shop", 4001L);

            assertThat(server.recorded).hasSize(1);
            assertThat(server.recorded.get(0).path).isEqualTo("/api/shop/_stream_load_2pc");
            assertThat(server.recorded.get(0).headers)
                    .containsEntry("txn_operation", "abort")
                    .containsEntry("txn_id", "4001");
        }
    }

    @Test
    public void testARejectedControlCallReportsTheServerMessage() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(
                        req -> Response.ok("{\"status\":\"Fail\",\"msg\":\"unknown table\"}"))) {
            DorisHttpClient client = client(server);

            assertThatThrownBy(() -> client.commitTransaction("shop", 4001L))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("unknown table");
        }
    }

    private static DorisHttpClient client(MockDorisServer server) {
        return new DorisHttpClient(server.endpoint(), "root", "123456", 1);
    }
}
