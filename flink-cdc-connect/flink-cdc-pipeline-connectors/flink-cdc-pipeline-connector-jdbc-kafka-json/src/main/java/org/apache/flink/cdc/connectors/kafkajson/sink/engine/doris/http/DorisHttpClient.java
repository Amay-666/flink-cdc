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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HTTP client for the Doris StreamLoad API and the FE query (DDL) API.
 *
 * <p>This is the self-contained replacement for the released doris-flink connector: the connector
 * talks to Doris purely over HTTP, with no Doris jar on the classpath. The client uses OkHttp
 * 3.14.9 (shaded into the connector jar) for its connection pool and efficient request/response
 * handling.
 *
 * <p>Two endpoints are used:
 *
 * <ul>
 *   <li>{@code PUT /api/{database}/{table}/_stream_load} — batch JSON write. The body is a JSON
 *       array ({@code strip_outer_array=true}). Doris 2.x answers the PUT on the FE with a {@code
 *       307} whose {@code Location} points at the BE owning the tablets; OkHttp does not follow
 *       redirects for {@code PUT}, so the client performs the two-step dance itself — it asks the
 *       FE first (which requires an {@code Expect: 100-continue} header) and re-issues the load
 *       against the backend it redirects to. A failed load is retried with the <em>same</em> {@code
 *       label}, which Doris deduplicates, making retries idempotent.
 *   <li>{@code POST /api/query/default_cluster/{database}} with body {@code {"stmt": ...}} — DDL
 *       execution (requires {@code is_execute_sql_in_http=true} on the FE). Application-level
 *       failures are surfaced immediately because DDL is not idempotent; only network-level errors
 *       are retried.
 * </ul>
 */
public class DorisHttpClient implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DorisHttpClient.class);

    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /** The FE answers a StreamLoad PUT with this and a {@code Location} pointing at the BE. */
    private static final int HTTP_TEMP_REDIRECT = 307;

    /**
     * Column name of the per-row delete marker carried in the imported data. A row with this column
     * set to {@code true} is deleted (matched by primary key); {@code false} upserts it. Doris
     * applies the semantics directly from the {@code hidden_columns} header — no {@code merge_type}
     * is needed.
     */
    public static final String DELETE_SIGN_COLUMN = "__DORIS_DELETE_SIGN__";

    private final String[] fenodes;
    private final int maxRetries;
    private final OkHttpClient client;
    private final String authorizationHeader;
    private final AtomicInteger nextFe = new AtomicInteger();

    /** Extra StreamLoad request headers/properties passed through from {@code sink.properties}. */
    private final Map<String, String> streamLoadProperties;

    public DorisHttpClient(String fenodes, String username, String password, int maxRetries) {
        this(fenodes, username, password, maxRetries, Collections.emptyMap());
    }

    /**
     * @param streamLoadProperties arbitrary Doris StreamLoad properties (e.g. {@code columns},
     *     {@code max_filter_ratio}, {@code timezone}) sent as request headers. A property may
     *     override the protocol defaults ({@code format}/{@code strip_outer_array}/{@code
     *     hidden_columns}/{@code label}); {@code Authorization} and {@code Expect} stay managed by
     *     this client.
     */
    public DorisHttpClient(
            String fenodes,
            String username,
            String password,
            int maxRetries,
            Map<String, String> streamLoadProperties) {
        this.fenodes = fenodes.split(",");
        this.maxRetries = Math.max(1, maxRetries);
        this.authorizationHeader =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (username + ":" + password)
                                                .getBytes(StandardCharsets.UTF_8));
        this.streamLoadProperties =
                streamLoadProperties == null ? Collections.emptyMap() : streamLoadProperties;
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
     * <p>Every row must carry {@link #DELETE_SIGN_COLUMN} — {@code true} deletes the row (matched
     * by primary key), {@code false} upserts it — so one batch can mix upserts and deletes in
     * arrival order. The {@code hidden_columns} header declares the marker column so Doris applies
     * its semantics without an extra {@code merge_type}.
     *
     * @param database target database (already mapped via the dialect options)
     * @param table target table (already mapped via the dialect options)
     * @param label the load label, deduplicated by Doris across retries
     * @param rows the rows to write, each a column-name to JSON-ready value map including the
     *     {@link #DELETE_SIGN_COLUMN} marker
     * @return the number of bytes sent in the request body (for write-throughput metrics)
     */
    public int streamLoad(
            String database, String table, String label, List<Map<String, Object>> rows)
            throws IOException {
        if (rows.isEmpty()) {
            return 0;
        }
        byte[] body = OBJECT_MAPPER.writeValueAsBytes(rows);
        // Doris 2.x answers the StreamLoad PUT on the FE with a 307 whose Location points at the BE
        // owning the tablets. OkHttp does not follow redirects for PUT, so the two-step dance is
        // performed here: the FE is asked first (it requires the Expect: 100-continue header) and,
        // when it redirects, the load is re-issued against the backend. A non-redirecting FE (or a
        // test stub) evaluates the response directly.
        Request feRequest =
                loadRequest(
                        feEndpoint() + "/api/" + database + "/" + table + "/_stream_load",
                        body,
                        label,
                        true);
        for (int attempt = 0; ; attempt++) {
            try {
                try (Response response = client.newCall(feRequest).execute()) {
                    if (response.code() == HTTP_TEMP_REDIRECT) {
                        String location = response.header("Location");
                        if (location == null) {
                            throw new IOException(
                                    "FE redirected StreamLoad of "
                                            + database
                                            + "."
                                            + table
                                            + " without a Location header.");
                        }
                        // The FE bakes the credentials into the redirect target; OkHttp strips the
                        // Authorization header on a cross-host redirect, so strip the userinfo and
                        // let the load request's Authorization header carry the credentials.
                        return performStreamLoad(
                                stripUserInfo(location), body, database, table, label);
                    }
                    return evaluateStreamLoad(response, database, table, label, body.length);
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

    /**
     * Re-issues a load against the backend URL from the FE redirect, retrying with the same label.
     */
    private int performStreamLoad(
            String beUrl, byte[] body, String database, String table, String label)
            throws IOException {
        Request request = loadRequest(beUrl, body, label, false);
        for (int attempt = 0; ; attempt++) {
            try {
                try (Response response = client.newCall(request).execute()) {
                    return evaluateStreamLoad(response, database, table, label, body.length);
                }
            } catch (IOException e) {
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

    private Request loadRequest(String url, byte[] body, String label, boolean expectContinue) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("label", label);
        headers.put("format", "json");
        headers.put("strip_outer_array", "true");
        // hidden_columns declares the per-row delete marker so Doris applies its
        // semantics (true = delete by key, false = upsert) without any merge_type.
        headers.put("hidden_columns", DELETE_SIGN_COLUMN);
        // sink.properties pass-through: user-supplied StreamLoad properties override the defaults
        // above (e.g. format=csv, columns=..., max_filter_ratio=..., timezone=...).
        headers.putAll(streamLoadProperties);
        Request.Builder builder =
                new Request.Builder().url(url).put(RequestBody.create(JSON, body));
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            builder.header(entry.getKey(), entry.getValue());
        }
        // Framework-managed headers, never overridable via sink.properties.
        builder.header("Authorization", authorizationHeader);
        if (expectContinue) {
            // The FE's StreamLoad handler rejects a request without this header.
            builder.header("Expect", "100-continue");
        }
        return builder.build();
    }

    private int evaluateStreamLoad(
            Response response, String database, String table, String label, int bytes)
            throws IOException {
        if (!response.isSuccessful()) {
            throw new IOException(
                    "HTTP " + response.code() + " for StreamLoad of " + database + "." + table);
        }
        String responseBody = response.body() != null ? response.body().string() : "";
        String status = OBJECT_MAPPER.readTree(responseBody).path("Status").asText();
        if ("Success".equals(status) || "Label Already Exists".equals(status)) {
            return bytes;
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

    /** Removes the {@code user:password@} prefix the FE bakes into its redirect target. */
    private static String stripUserInfo(String location) {
        return location.replaceFirst("^([a-zA-Z][a-zA-Z0-9+.-]*://)[^@/]*@", "$1");
    }

    /** Executes a DDL statement in the given database. */
    public void executeSql(String database, String sql) throws IOException {
        Request request =
                new Request.Builder()
                        .url(feEndpoint() + "/api/query/default_cluster/" + database)
                        .post(
                                RequestBody.create(
                                        JSON,
                                        OBJECT_MAPPER.writeValueAsBytes(
                                                Collections.singletonMap("stmt", sql))))
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
                    String responseBody = response.body() != null ? response.body().string() : "";
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
