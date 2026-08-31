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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * A JDK {@link HttpServer} standing in for the Doris FE/BE HTTP endpoints. Records every request and
 * answers through the {@code responder} supplied at construction. No external dependency — this is
 * the test double the whole sink suite runs against.
 */
public class MockDorisServer implements Closeable {

    /** A request as observed by the mock server. */
    public static class RecordedRequest {
        public final String method;
        public final String path;
        public final String body;
        public final Map<String, String> headers;

        RecordedRequest(String method, String path, String body, Map<String, String> headers) {
            this.method = method;
            this.path = path;
            this.body = body;
            this.headers = headers;
        }

        static RecordedRequest from(HttpExchange exchange) throws IOException {
            // The JDK HttpServer normalizes incoming header names to Title Case; look them up
            // case-insensitively so tests can assert the lower-case names OkHttp sends.
            Map<String, String> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            exchange.getRequestHeaders()
                    .forEach((name, values) -> headers.put(name, values.get(0)));
            String body =
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            return new RecordedRequest(
                    exchange.getRequestMethod(), exchange.getRequestURI().getPath(), body, headers);
        }
    }

    /** A response the mock server returns. */
    public static class Response {
        public final int status;
        public final byte[] body;

        public Response(int status, String body) {
            this.status = status;
            this.body = body.getBytes(StandardCharsets.UTF_8);
        }

        public static Response ok(String json) {
            return new Response(200, json);
        }
    }

    private final HttpServer server;
    public final List<RecordedRequest> recorded = new ArrayList<>();

    public MockDorisServer(Function<RecordedRequest, Response> responder) throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext(
                "/",
                exchange -> {
                    try {
                        RecordedRequest request = RecordedRequest.from(exchange);
                        recorded.add(request);
                        Response response = responder.apply(request);
                        byte[] body = response.body == null ? new byte[0] : response.body;
                        exchange.sendResponseHeaders(response.status, body.length);
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(body);
                        }
                    } finally {
                        exchange.close();
                    }
                });
        server.start();
    }

    /** {@code localhost:port} of the mock server. */
    public String endpoint() {
        return "localhost:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
