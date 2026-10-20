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

package org.apache.flink.cdc.connectors.kafkajson.example;

import org.apache.flink.cdc.connectors.kafkajson.utils.YamlUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * The E2E environment configuration — MySQL/TiDB source, Doris sink, Kafka topic, and verification
 * timings — loaded from {@code e2e/env.yaml} on the test classpath. That file holds real connection
 * details and is therefore git-ignored: copy the committed {@code e2e/env.yaml.template} to it and
 * fill in the endpoints. A different environment is selected with the {@code e2e.config} system
 * property:
 *
 * <pre>mvn test -Dtest=DorisE2ETest -De2e.config=file:///D:/envs/custom-env.yaml</pre>
 *
 * <p>Tests that need an environment call {@link #find()} and skip themselves when it is absent; the
 * runnable examples call {@link #get()}, which fails with instructions instead.
 */
final class E2eConfig {

    /** The provisioned configuration; git-ignored because it carries real endpoints. */
    static final String DEFAULT_RESOURCE = "e2e/env.yaml";

    /** The committed skeleton, carrying every key with placeholder values. */
    static final String TEMPLATE_RESOURCE = "e2e/env.yaml.template";

    private static E2eConfig instance;

    private final Map<String, Object> values;

    private E2eConfig(String location) {
        try {
            values = load(location);
        } catch (IOException | RuntimeException e) {
            throw new IllegalStateException(
                    "Failed to load the E2E environment config from '" + location + "'", e);
        }
    }

    /**
     * Returns the configured environment, or empty when neither the {@code e2e.config} property nor
     * the default {@code e2e/env.yaml} resource is present. A configured location that fails to
     * load is an error rather than an absent environment.
     */
    static synchronized Optional<E2eConfig> find() {
        String location = System.getProperty("e2e.config");
        if (location == null || location.isEmpty()) {
            if (E2eConfig.class.getClassLoader().getResource(DEFAULT_RESOURCE) == null) {
                return Optional.empty();
            }
            location = DEFAULT_RESOURCE;
        }
        if (instance == null) {
            instance = new E2eConfig(location);
        }
        return Optional.of(instance);
    }

    /**
     * Returns the configured environment, failing with a message that says how to create one. For
     * the runnable examples, which have nothing to run without an environment.
     */
    static E2eConfig get() {
        return find().orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "E2E environment not configured: copy "
                                                + "src/test/resources/"
                                                + TEMPLATE_RESOURCE
                                                + " to "
                                                + DEFAULT_RESOURCE
                                                + " (git-ignored) and fill in the endpoints, "
                                                + "or pass -De2e.config=<file-or-resource>"));
    }

    /** Loads a specific file or classpath resource; used to validate the template's keys. */
    static E2eConfig loadFrom(String location) {
        return new E2eConfig(location);
    }

    /** Returns the string value at the dot-separated key, e.g. {@code mysql.host}. */
    String getString(String key) {
        Object value = lookup(key);
        return value == null ? null : String.valueOf(value);
    }

    /** Returns the integer value at the dot-separated key, e.g. {@code mysql.port}. */
    int getInt(String key) {
        Object value = lookup(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        throw new IllegalStateException(
                "The E2E config key '" + key + "' is not a number: " + value);
    }

    private Object lookup(String key) {
        Object current = values;
        for (String part : key.split("\\.")) {
            if (!(current instanceof Map)) {
                throw new IllegalStateException("Missing key '" + key + "' in the E2E config");
            }
            current = ((Map<?, ?>) current).get(part);
            if (current == null) {
                throw new IllegalStateException("Missing key '" + key + "' in the E2E config");
            }
        }
        return current;
    }

    private static Map<String, Object> load(String location) throws IOException {
        if (location.startsWith("file:")) {
            return YamlUtils.loadYamlFromFile(stripFilePrefix(location));
        }
        if (new File(location).isFile()) {
            return YamlUtils.loadYamlFromFile(location);
        }
        return YamlUtils.loadYamlFromResource(location);
    }

    private static String stripFilePrefix(String location) {
        if (location.startsWith("file:///")) {
            return location.substring("file:///".length());
        }
        if (location.startsWith("file://")) {
            return location.substring("file://".length());
        }
        return location.substring("file:".length());
    }
}
