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

package org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HTTP client for the Doris StreamLoad API and the FE query (DDL) API.
 *
 * <p>This is the self-contained replacement for the released doris-flink connector: the connector
 * talks to Doris purely over HTTP, with no Doris jar on the classpath. The client uses OkHttp 3.14.9
 * (shaded into the connector jar) for its connection pool and efficient request/response handling.
 *
 * <p>Two endpoints are used:
 *
 * <ul>
 *   <li>{@code PUT /api/{database}/{table}/_stream_load} — batch JSON write. The body is a JSON
 *       array ({@code strip_outer_array=true}). A failed load is retried with the <em>same</em>
 *       {@code label}, which Doris deduplicates, making retries idempotent.
 *   <li>{@code POST /api/query/{database}} with body {@code {"sql": ...}} — DDL execution (requires
 *       {@code is_execute_sql_in_http=true} on the FE). Application-level failures are surfaced
 *       immediately because DDL is not idempotent; only network-level errors are retried.
 * </ul>
 */
public class DorisHttpClient implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DorisHttpClient.class);

    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String[] fenodes;
    private final int maxRetries;
    private final OkHttpClient client;
    private final String authorizationHeader;
    private final AtomicInteger nextFe = new AtomicInteger();

    public DorisHttpClient(String fenodes, String username, String password, int maxRetries) {
        this.fenodes = fenodes.split(",");
        this.maxRetries = Math.max(1, maxRetries);
        this.authorizationHeader =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (username + ":" + password)
                                                .getBytes(StandardCharsets.UTF_8));
        this.client =
                new OkHttpClient.Builder()
                        .connectTimeout(30, TimeUnit.SECONDS)
                        .readTimeout(60, TimeUnit.SECONDS)
                        .writeTimeout(60, TimeUnit.SECONDS)
                        .build();
    }

    /**
     * Stream-loads a batch of rows into the Doris table.
     *
     * @param database target database (already mapped via the dialect options)
     * @param table target table (already mapped via the dialect options)
     * @param label the load label, deduplicated by Doris across retries
     * @param rows the rows to write, each a column-name to JSON-ready value map
     */
    public void streamLoad(
            String database, String table, String label, List<Map<String, Object>> rows)
            throws IOException {
        if (rows.isEmpty()) {
            return;
        }
        Request request =
                new Request.Builder()
                        .url(feEndpoint() + "/api/" + database + "/" + table + "/_stream_load")
                        .put(RequestBody.create(JSON, OBJECT_MAPPER.writeValueAsBytes(rows)))
                        .addHeader("label", label)
                        .addHeader("format", "json")
                        .addHeader("strip_outer_array", "true")
                        .addHeader("Authorization", authorizationHeader)
                        .build();
        for (int attempt = 0; ; attempt++) {
            try {
                try (Response response = client.newCall(request).execute()) {
                    if (!response.isSuccessful()) {
                        throw new IOException(
                                "HTTP "
                                        + response.code()
                                        + " for StreamLoad of "
                                        + database
                                        + "."
                                        + table);
                    }
                    String responseBody =
                            response.body() != null ? response.body().string() : "";
                    String status = OBJECT_MAPPER.readTree(responseBody).path("Status").asText();
                    if ("Success".equals(status) || "Label Already Exists".equals(status)) {
                        return;
                    }
                    throw new IOException(
                            "StreamLoad failed for "
                                    + database
                                    + "."
                                    + table
                                    + " (label "
                                    + label
                                    + "): "
                                    + responseBody);
                }
            } catch (IOException e) {
                // A StreamLoad failure is retried with the same label (idempotent in Doris).
                if (attempt >= maxRetries) {
                    throw e;
                }
                LOG.warn(
                        "StreamLoad attempt {} for {}.{} failed: {}. Retrying with label {}.",
                        attempt + 1,
                        database,
                        table,
                        e.getMessage(),
                        label);
                sleepQuietly(attempt);
            }
        }
    }

    /** Executes a DDL statement in the given database. */
    public void executeSql(String database, String sql) throws IOException {
        Request request =
                new Request.Builder()
                        .url(feEndpoint() + "/api/query/" + database)
                        .post(
                                RequestBody.create(
                                        JSON,
                                        OBJECT_MAPPER.writeValueAsBytes(
                                                Collections.singletonMap("sql", sql))))
                        .addHeader("Authorization", authorizationHeader)
                        .build();
        for (int attempt = 0; ; attempt++) {
            try {
                try (Response response = client.newCall(request).execute()) {
                    if (!response.isSuccessful()) {
                        throw new IOException(
                                "HTTP "
                                        + response.code()
                                        + " for DDL on database "
                                        + database
                                        + ": "
                                        + sql);
                    }
                    String responseBody =
                            response.body() != null ? response.body().string() : "";
                    JsonNode root = OBJECT_MAPPER.readTree(responseBody);
                    JsonNode codeNode = root.get("code");
                    if (codeNode == null || codeNode.asInt(-1) == 0) {
                        return;
                    }
                    // Application-level DDL failure: surface immediately, do not retry — DDL is
                    // not idempotent (e.g. CREATE TABLE is only valid once).
                    throw new DorisHttpException(
                            "DDL failed on database "
                                    + database
                                    + " (code "
                                    + codeNode.asInt(-1)
                                    + "): "
                                    + responseBody
                                    + "; sql: "
                                    + sql);
                }
            } catch (DorisHttpException e) {
                throw e;
            } catch (IOException e) {
                if (attempt >= maxRetries) {
                    throw e;
                }
                LOG.warn(
                        "DDL attempt {} on database {} failed: {}. sql: {}",
                        attempt + 1,
                        database,
                        e.getMessage(),
                        sql);
                sleepQuietly(attempt);
            }
        }
    }

    @Override
    public void close() {
        client.dispatcher().executorService().shutdown();
        client.connectionPool().evictAll();
    }

    private String feEndpoint() {
        String fe = fenodes[Math.floorMod(nextFe.getAndIncrement(), fenodes.length)];
        return fe.startsWith("http://") || fe.startsWith("https://") ? fe : "http://" + fe;
    }

    private static void sleepQuietly(int attempt) {
        try {
            Thread.sleep(500L * (attempt + 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Raised when Doris rejects a request at the application level (non-retryable). */
    static class DorisHttpException extends IOException {
        private static final long serialVersionUID = 1L;

        DorisHttpException(String message) {
            super(message);
        }
    }
}
