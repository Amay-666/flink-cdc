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
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
    public void testStreamLoadLabelAlreadyExistsWithFinishedJobIsIdempotentSuccess()
            throws IOException {
        // Doris answers a re-issued label with "Label Already Exists"; the earlier job with that
        // label FINISHED, so this batch is already in Doris and must not be loaded a second time.
        try (MockDorisServer server =
                new MockDorisServer(
                        req ->
                                Response.ok(
                                        "{\"Status\":\"Label Already Exists\","
                                                + "\"ExistingJobStatus\":\"FINISHED\"}"))) {
            DorisHttpClient client = client(server);

            client.streamLoad(
                    "shop",
                    "orders",
                    "cdc_label_1",
                    Collections.singletonList(Collections.emptyMap()));

            assertThat(server.recorded).hasSize(1);
        }
    }

    @Test
    public void testStreamLoadLabelAlreadyExistsWithUnfinishedJobFails() throws IOException {
        // "Label Already Exists" alone says nothing about the rows: when the earlier job was
        // CANCELLED the batch was never written, so it must not be reported as a success.
        try (MockDorisServer server =
                new MockDorisServer(
                        req ->
                                Response.ok(
                                        "{\"Status\":\"Label Already Exists\","
                                                + "\"ExistingJobStatus\":\"CANCELLED\"}"))) {
            DorisHttpClient client = client(server);

            assertThatThrownBy(
                            () ->
                                    client.streamLoad(
                                            "shop",
                                            "orders",
                                            "cdc_label_1",
                                            Collections.singletonList(Collections.emptyMap())))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Label Already Exists");
            // Retried once with the same label (maxRetries = 1), then surfaced.
            assertThat(server.recorded).hasSize(2);
        }
    }

    @Test
    public void testStreamLoadLabelAlreadyExistsWithoutJobStatusFails() throws IOException {
        // Defensive: a response that reports neither a job status nor a success is not a success.
        try (MockDorisServer server =
                new MockDorisServer(req -> Response.ok("{\"Status\":\"Label Already Exists\"}"))) {
            DorisHttpClient client = client(server);

            assertThatThrownBy(
                            () ->
                                    client.streamLoad(
                                            "shop",
                                            "orders",
                                            "cdc_label_1",
                                            Collections.singletonList(Collections.emptyMap())))
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    public void testStreamLoadPublishTimeoutIsSuccess() throws IOException {
        // Doris reports this once the rows are written and the transaction is still publishing;
        // the released Doris connector counts it as a success as well.
        try (MockDorisServer server =
                new MockDorisServer(req -> Response.ok("{\"Status\":\"Publish Timeout\"}"))) {
            DorisHttpClient client = client(server);

            int bytes =
                    client.streamLoad(
                            "shop",
                            "orders",
                            "cdc_label_1",
                            Collections.singletonList(Collections.singletonMap("id", 1)));

            assertThat(bytes).isGreaterThan(0);
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
                    "shop",
                    "orders",
                    "cdc_label_1",
                    Collections.singletonList(Collections.emptyMap()));

            assertThat(server.recorded).hasSize(2);
            assertThat(server.recorded.get(0).headers.get("label")).isEqualTo("cdc_label_1");
            assertThat(server.recorded.get(1).headers.get("label")).isEqualTo("cdc_label_1");
        }
    }

    @Test
    public void testStreamLoadGivesUpAfterMaxRetries() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(
                        req -> Response.ok("{\"Status\":\"Fail\",\"Message\":\"x\"}"))) {
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

            client.streamLoad(
                    "shop", "orders", "l1", Collections.singletonList(Collections.emptyMap()));
            client.streamLoad(
                    "shop", "orders", "l2", Collections.singletonList(Collections.emptyMap()));

            assertThat(server1.recorded).hasSize(1);
            assertThat(server2.recorded).hasSize(1);
        }
    }

    @Test
    public void testRetryHitsTheNextFeAfterAFailedAttempt() throws IOException {
        try (MockDorisServer fe1 = new MockDorisServer(req -> new Response(500, "boom"));
                MockDorisServer fe2 =
                        new MockDorisServer(req -> Response.ok("{\"Status\":\"Success\"}"))) {
            DorisHttpClient client =
                    new DorisHttpClient(fe1.endpoint() + "," + fe2.endpoint(), "root", "123456", 1);

            client.streamLoad(
                    "shop",
                    "orders",
                    "l1",
                    Collections.singletonList(Collections.singletonMap("id", 1)));

            // The retry has to be rebuilt against the next FE. A request built once for the whole
            // operation keeps pointing at the endpoint that just failed, so the second FE is never
            // reached and an unreachable first FE fails the job forever — the multi-FE list would
            // be dead weight.
            assertThat(fe1.recorded).hasSize(1);
            assertThat(fe2.recorded).hasSize(1);
        }
    }

    @Test
    public void testStreamLoadPassesThroughConfiguredProperties() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(req -> Response.ok("{\"Status\":\"Success\"}"))) {
            DorisHttpClient client =
                    new DorisHttpClient(
                            server.endpoint(),
                            "root",
                            "123456",
                            1,
                            mapOf(
                                    "max_filter_ratio", "0.1",
                                    "columns", "a,b,c",
                                    "timezone", "Asia/Shanghai"));
            client.streamLoad(
                    "shop",
                    "orders",
                    "cdc_label_1",
                    Collections.singletonList(Collections.emptyMap()));

            assertThat(server.recorded).hasSize(1);
            RecordedRequest request = server.recorded.get(0);
            assertThat(request.headers).containsEntry("max_filter_ratio", "0.1");
            assertThat(request.headers).containsEntry("columns", "a,b,c");
            assertThat(request.headers).containsEntry("timezone", "Asia/Shanghai");
            // The properties the write protocol depends on keep the connector's values.
            assertThat(request.headers).containsEntry("format", "json");
            assertThat(request.headers).containsEntry("strip_outer_array", "true");
        }
    }

    @Test
    public void testReservedStreamLoadPropertiesAreRejected() {
        // Each of these either carries the write protocol, decides the idempotency contract, or
        // would leave the load uncommitted: overriding them would silently change the semantics of
        // every write, so the client refuses to be constructed with one.
        for (String reserved : Arrays.asList("label", "format", "strip_outer_array")) {
            assertThatThrownBy(
                            () ->
                                    new DorisHttpClient(
                                            "http://localhost:8030",
                                            "root",
                                            "123456",
                                            1,
                                            Collections.singletonMap(reserved, "x")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(reserved);
        }
    }

    @Test
    public void testReservedStreamLoadPropertyNamesAreCaseInsensitive() {
        // HTTP header names are case-insensitive, and Doris accepts either spelling.
        assertThatThrownBy(
                        () ->
                                new DorisHttpClient(
                                        "http://localhost:8030",
                                        "root",
                                        "123456",
                                        1,
                                        Collections.singletonMap("Hidden_Columns", "x")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Hidden_Columns");
    }

    @Test
    public void testReservedTwoPhaseCommitPropertyIsRejected() {
        // This client loads in one shot: with two_phase_commit=true Doris would hold the rows
        // uncommitted until a commit call that never comes, and the data would never be visible.
        assertThatThrownBy(
                        () ->
                                new DorisHttpClient(
                                        "http://localhost:8030",
                                        "root",
                                        "123456",
                                        1,
                                        Collections.singletonMap("two_phase_commit", "true")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("two_phase_commit");
    }

    // ===== Group Commit tests =====

    @Test
    public void testGroupCommitSendsGroupCommitHeaderAndNoLabel() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(
                        req ->
                                Response.ok(
                                        "{\"Status\":\"Success\",\"GroupCommit\":true,"
                                                + "\"Label\":\"group_commit_xxx\"}"))) {
            DorisHttpClient client =
                    new DorisHttpClient(
                            server.endpoint(),
                            "root",
                            "123456",
                            1,
                            Collections.emptyMap(),
                            "sync_mode");

            client.streamLoad(
                    "shop",
                    "orders",
                    null, // null label = Group Commit mode
                    Collections.singletonList(Collections.singletonMap("id", 1)));

            assertThat(server.recorded).hasSize(1);
            RecordedRequest request = server.recorded.get(0);
            // group_commit header is sent
            assertThat(request.headers).containsEntry("group_commit", "sync_mode");
            // No label header
            assertThat(request.headers).doesNotContainKey("label");
            // hidden_columns still present
            assertThat(request.headers).containsEntry("hidden_columns", "__DORIS_DELETE_SIGN__");
        }
    }

    @Test
    public void testGroupCommitAsyncMode() throws IOException {
        try (MockDorisServer server =
                new MockDorisServer(
                        req -> Response.ok("{\"Status\":\"Success\",\"GroupCommit\":true}"))) {
            DorisHttpClient client =
                    new DorisHttpClient(
                            server.endpoint(),
                            "root",
                            "123456",
                            1,
                            Collections.emptyMap(),
                            "async_mode");

            client.streamLoad(
                    "shop", "orders", null, Collections.singletonList(Collections.emptyMap()));

            assertThat(server.recorded.get(0).headers).containsEntry("group_commit", "async_mode");
        }
    }

    @Test
    public void testGroupCommitResponseWithoutGroupCommitFieldStillSucceeds() throws IOException {
        // If Doris falls back to non-GC, the response may lack GroupCommit=true.
        // The load still succeeds (the warn is logged, not thrown).
        try (MockDorisServer server =
                new MockDorisServer(req -> Response.ok("{\"Status\":\"Success\"}"))) {
            DorisHttpClient client =
                    new DorisHttpClient(
                            server.endpoint(),
                            "root",
                            "123456",
                            1,
                            Collections.emptyMap(),
                            "sync_mode");

            int bytes =
                    client.streamLoad(
                            "shop",
                            "orders",
                            null,
                            Collections.singletonList(Collections.singletonMap("id", 1)));

            assertThat(bytes).isGreaterThan(0);
        }
    }

    @Test
    public void testGroupCommitRetryIsIdempotent() throws IOException {
        // Retries with null label (GC mode) are safe: same data, same sequence values.
        AtomicInteger attempts = new AtomicInteger();
        try (MockDorisServer server =
                new MockDorisServer(
                        req ->
                                attempts.getAndIncrement() == 0
                                        ? Response.ok("{\"Status\":\"Fail\",\"Message\":\"tired\"}")
                                        : Response.ok(
                                                "{\"Status\":\"Success\",\"GroupCommit\":true}"))) {
            DorisHttpClient client =
                    new DorisHttpClient(
                            server.endpoint(),
                            "root",
                            "123456",
                            1,
                            Collections.emptyMap(),
                            "sync_mode");

            client.streamLoad(
                    "shop", "orders", null, Collections.singletonList(Collections.emptyMap()));

            assertThat(server.recorded).hasSize(2);
            // Both attempts carry the group_commit header (no label)
            assertThat(server.recorded.get(0).headers).containsEntry("group_commit", "sync_mode");
            assertThat(server.recorded.get(0).headers).doesNotContainKey("label");
            assertThat(server.recorded.get(1).headers).containsEntry("group_commit", "sync_mode");
        }
    }

    @Test
    public void testGroupCommitFollowsFeRedirectToBackend() throws IOException {
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
                                                "{\"Status\":\"Success\",\"GroupCommit\":true}"))) {
            backend.set("http://" + server.endpoint());
            DorisHttpClient client =
                    new DorisHttpClient(
                            server.endpoint(),
                            "root",
                            "123456",
                            1,
                            Collections.emptyMap(),
                            "sync_mode");

            client.streamLoad(
                    "shop",
                    "orders",
                    null,
                    Collections.singletonList(Collections.singletonMap("id", 1)));

            assertThat(server.recorded).hasSize(2);
            // FE request: group_commit header, no label
            assertThat(server.recorded.get(0).headers).containsEntry("group_commit", "sync_mode");
            assertThat(server.recorded.get(0).headers).doesNotContainKey("label");
            // BE request: same headers
            assertThat(server.recorded.get(1).headers).containsEntry("group_commit", "sync_mode");
            assertThat(server.recorded.get(1).headers).doesNotContainKey("label");
        }
    }

    private DorisHttpClient client(MockDorisServer server) {
        return new DorisHttpClient(server.endpoint(), "root", "123456", 1);
    }

    private static Map<String, String> mapOf(String... keyValues) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    private static String basicAuth(String username, String password) {
        return "Basic "
                + Base64.getEncoder()
                        .encodeToString(
                                (username + ":" + password).getBytes(StandardCharsets.UTF_8));
    }
}
