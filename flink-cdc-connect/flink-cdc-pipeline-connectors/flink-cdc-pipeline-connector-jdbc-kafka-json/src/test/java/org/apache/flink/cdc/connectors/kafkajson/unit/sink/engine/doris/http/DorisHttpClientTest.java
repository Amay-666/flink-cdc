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
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer.RecordedRequest;
import org.apache.flink.cdc.connectors.kafkajson.unit.sink.engine.doris.http.MockDorisServer.Response;

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit test for {@link DorisHttpClient} against a JDK {@code HttpServer} standing in for the Doris
 * FE/BE HTTP endpoints. No external dependency.
 */
public class DorisHttpClientTest {

    @Test
    public void testStreamLoadSuccess() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(req -> Response.ok("{\"Status\":\"Success\"}"))) {
            DorisHttpClient client = client(server);

            client.streamLoad(
                    "shop",
                    "orders",
                    "cdc_label_1",
                    Collections.singletonList(Collections.singletonMap("id", 1)));

            assertThat(server.recorded).hasSize(1);
            RecordedRequest request = server.recorded.get(0);
            assertThat(request.method).isEqualTo("PUT");
            assertThat(request.path).isEqualTo("/api/shop/orders/_stream_load");
            assertThat(request.headers.get("label")).isEqualTo("cdc_label_1");
            assertThat(request.headers.get("format")).isEqualTo("json");
            assertThat(request.headers.get("strip_outer_array")).isEqualTo("true");
            assertThat(request.headers.get("Authorization")).isEqualTo(basicAuth("root", "123456"));
            assertThat(request.headers).containsEntry("hidden_columns", "__DORIS_DELETE_SIGN__");
            assertThat(request.body).isEqualTo("[{\"id\":1}]");
        }
    }

    @Test
    public void testStreamLoadLabelAlreadyExistsTreatedAsSuccess() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(req -> Response.ok("{\"Status\":\"Label Already Exists\"}"))) {
            DorisHttpClient client = client(server);

            client.streamLoad(
                    "shop", "orders", "cdc_label_1", Collections.singletonList(Collections.emptyMap()));

            assertThat(server.recorded).hasSize(1);
        }
    }

    @Test
    public void testStreamLoadRetriesWithSameLabelThenSucceeds() throws IOException {
        // maxRetries = 1 -> attempt 0 fails, attempt 1 succeeds.
        AtomicInteger attempts = new AtomicInteger();
        try (MockDorisServer server =
                new MockDorisServer(
                        req ->
                                attempts.getAndIncrement() == 0
                                        ? Response.ok("{\"Status\":\"Fail\",\"Message\":\"tired\"}")
                                        : Response.ok("{\"Status\":\"Success\"}"))) {
            DorisHttpClient client = new DorisHttpClient(server.endpoint(), "root", "123456", 1);

            client.streamLoad(
                    "shop", "orders", "cdc_label_1", Collections.singletonList(Collections.emptyMap()));

            assertThat(server.recorded).hasSize(2);
            assertThat(server.recorded.get(0).headers.get("label")).isEqualTo("cdc_label_1");
            assertThat(server.recorded.get(1).headers.get("label")).isEqualTo("cdc_label_1");
        }
    }

    @Test
    public void testStreamLoadGivesUpAfterMaxRetries() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(req -> Response.ok("{\"Status\":\"Fail\",\"Message\":\"x\"}"))) {
            DorisHttpClient client = new DorisHttpClient(server.endpoint(), "root", "123456", 1);

            assertThatThrownBy(
                            () ->
                                    client.streamLoad(
                                            "shop",
                                            "orders",
                                            "cdc_label_1",
                                            Collections.singletonList(Collections.emptyMap())))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("StreamLoad failed");
            assertThat(server.recorded).hasSize(2);
        }
    }

    @Test
    public void testStreamLoadSkipsEmptyBatch() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(req -> Response.ok("{\"Status\":\"Success\"}"))) {
            DorisHttpClient client = client(server);

            client.streamLoad("shop", "orders", "cdc_label_1", Collections.emptyList());

            assertThat(server.recorded).isEmpty();
        }
    }

    @Test
    public void testStreamLoadFollowsFeRedirectToBackend() throws IOException {
        // The FE answers the first PUT with a 307 pointing at the BE; the load is then re-issued
        // against the Location, as the real Doris FE redirects StreamLoad to the backend. The
        // redirect target can only be known after the mock is bound, so it is captured lazily.
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
                                        : Response.ok("{\"Status\":\"Success\"}"))) {
            backend.set("http://" + server.endpoint());
            DorisHttpClient client = client(server);

            int bytes =
                    client.streamLoad(
                            "shop",
                            "orders",
                            "cdc_label_1",
                            Collections.singletonList(Collections.singletonMap("id", 1)));

            assertThat(bytes).isEqualTo(10);
            assertThat(server.recorded).hasSize(2);
            // The FE request carries the Expect header Doris requires...
            assertThat(server.recorded.get(0).headers).containsEntry("Expect", "100-continue");
            // ...and the backend request carries the body and the load headers.
            assertThat(server.recorded.get(1).path).isEqualTo("/api/shop/orders/_stream_load");
            assertThat(server.recorded.get(1).body).isEqualTo("[{\"id\":1}]");
            assertThat(server.recorded.get(1).headers).containsEntry("label", "cdc_label_1");
        }
    }

    @Test
    public void testEveryStreamLoadCarriesHiddenColumnsHeader() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(req -> Response.ok("{\"Status\":\"Success\"}"))) {
            DorisHttpClient client = client(server);

            client.streamLoad(
                    "shop",
                    "orders",
                    "cdc_label_1",
                    Collections.singletonList(Collections.emptyMap()));

            assertThat(server.recorded).hasSize(1);
            // The delete-sign column is declared via hidden_columns so Doris applies its per-row
            // upsert/delete semantics on every batch, without a merge_type.
            assertThat(server.recorded.get(0).headers)
                    .containsEntry("hidden_columns", "__DORIS_DELETE_SIGN__");
        }
    }

    @Test
    public void testExecuteSqlSuccess() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(req -> Response.ok("{\"code\":0,\"msg\":\"OK\"}"))) {
            DorisHttpClient client = client(server);

            client.executeSql("shop", "CREATE TABLE `orders` (`id` INT)");

            assertThat(server.recorded).hasSize(1);
            RecordedRequest request = server.recorded.get(0);
            assertThat(request.method).isEqualTo("POST");
            assertThat(request.path).isEqualTo("/api/query/default_cluster/shop");
            assertThat(request.body).isEqualTo("{\"stmt\":\"CREATE TABLE `orders` (`id` INT)\"}");
        }
    }

    @Test
    public void testExecuteSqlApplicationErrorIsNotRetried() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(req -> Response.ok("{\"code\":1105,\"msg\":\"err\"}"))) {
            DorisHttpClient client = new DorisHttpClient(server.endpoint(), "root", "123456", 3);

            assertThatThrownBy(() -> client.executeSql("shop", "CREATE TABLE x"))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("code 1105");
            // DDL is not idempotent: the application error surfaces immediately, no retry.
            assertThat(server.recorded).hasSize(1);
        }
    }

    @Test
    public void testExecuteSqlRetriesNetworkError() throws IOException {
        AtomicInteger attempts = new AtomicInteger();
        try (MockDorisServer server =
                new MockDorisServer(
                        req ->
                                attempts.getAndIncrement() == 0
                                        ? new Response(500, "boom")
                                        : Response.ok("{\"code\":0,\"msg\":\"OK\"}"))) {
            DorisHttpClient client = new DorisHttpClient(server.endpoint(), "root", "123456", 1);

            client.executeSql("shop", "CREATE TABLE x");

            assertThat(server.recorded).hasSize(2);
        }
    }

    @Test
    public void testFenodesAreRoundRobined() throws IOException {
        try (MockDorisServer server1 =
                        new MockDorisServer(req -> Response.ok("{\"Status\":\"Success\"}"));
                MockDorisServer server2 =
                        new MockDorisServer(req -> Response.ok("{\"Status\":\"Success\"}"))) {
            DorisHttpClient client =
                    new DorisHttpClient(
                            server1.endpoint() + "," + server2.endpoint(), "root", "123456", 1);

            client.streamLoad("shop", "orders", "l1", Collections.singletonList(Collections.emptyMap()));
            client.streamLoad("shop", "orders", "l2", Collections.singletonList(Collections.emptyMap()));

            assertThat(server1.recorded).hasSize(1);
            assertThat(server2.recorded).hasSize(1);
        }
    }

    private DorisHttpClient client(MockDorisServer server) {
        return new DorisHttpClient(server.endpoint(), "root", "123456", 1);
    }

    private static String basicAuth(String username, String password) {
        return "Basic "
                + Base64.getEncoder()
                        .encodeToString(
                                (username + ":" + password)
                                        .getBytes(StandardCharsets.UTF_8));
    }
}
