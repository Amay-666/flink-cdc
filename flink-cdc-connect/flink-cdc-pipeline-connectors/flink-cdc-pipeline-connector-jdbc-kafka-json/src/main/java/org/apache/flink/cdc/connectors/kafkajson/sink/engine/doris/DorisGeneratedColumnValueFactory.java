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

package org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Creates the value generator for the sink's generated column ({@code sink.generated-column}).
 *
 * <p>The function option ({@code sink.function}) accepts:
 *
 * <ul>
 *   <li><b>Built-in</b> — {@code NOW} or {@code COUNTER}:
 *       <ul>
 *         <li>{@code NOW} (default): for a {@code DATETIME} column the value is the string form
 *             Doris accepts ({@code "yyyy-MM-dd HH:mm:ss"}); for a numeric column {@code
 *             System.currentTimeMillis()}.
 *         <li>{@code COUNTER}: a wall-clock-seeded BIGINT counter, one per subtask.
 *       </ul>
 *   <li><b>Custom</b> — a fully-qualified class name implementing {@code
 *       java.util.function.Function<Long, Object>}. The class must have a public no-arg
 *       constructor. The input is {@code null}; the return value is put into the row map.
 *       This lets users plug in any value source (a business timestamp, an ID sequence, a
 *       lookup, …) without touching the connector.
 * </ul>
 */
public class DorisGeneratedColumnValueFactory {

    private static final Logger LOG =
            LoggerFactory.getLogger(DorisGeneratedColumnValueFactory.class);

    private static final DateTimeFormatter DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private DorisGeneratedColumnValueFactory() {}

    /**
     * Fills the generated column on every row of a drained batch. Called at flush time — before
     * the rows leave the writer's buffer and reach Doris — so every send path (timer, buffer
     * full, FlushEvent, close, TwoPhase's landBatch) is covered. All rows of a batch share one
     * generator call (one timestamp for the batch); rows whose map already carries the column
     * (the source provided it, or a per-row injection already ran) keep their existing value.
     *
     * @param rows the drained batch, mutated in place
     * @param columnName the generated column name, or {@code null} to no-op
     * @param generator the value generator, or {@code null} to no-op
     */
    public static void fillBatch(
            java.util.List<java.util.Map<String, Object>> rows,
            String columnName,
            Function<Long, Object> generator) {
        if (columnName == null || generator == null || rows == null || rows.isEmpty()) {
            return;
        }
        Object value = generator.apply(null);
        for (java.util.Map<String, Object> row : rows) {
            row.putIfAbsent(columnName, value);
        }
    }

    /**
     * Creates the value generator, or {@code null} when the generated column is not configured
     * (no column name), so the writers can skip the put entirely.
     */
    public static Function<Long, Object> create(DorisDataSinkOptions options) {
        String columnName = options.getGeneratedColumnName();
        if (columnName == null) {
            return null;
        }
        String function = options.getGeneratedColumnFunction();
        String type = options.getGeneratedColumnType();
        return create(function, type);
    }

    /**
     * Creates the value generator from the function name and column type. Visible for tests.
     */
    static Function<Long, Object> create(String function, String columnType) {
        String fn = function == null ? "" : function.trim();

        // Built-in: NOW
        if ("NOW".equalsIgnoreCase(fn) || fn.isEmpty()) {
            boolean isDatetime =
                    columnType != null && columnType.toUpperCase().startsWith("DATETIME");
            if (isDatetime) {
                return unused -> LocalDateTime.now().format(DATETIME_FORMATTER);
            }
            return unused -> System.currentTimeMillis();
        }

        // Built-in: COUNTER
        if ("COUNTER".equalsIgnoreCase(fn)) {
            AtomicLong counter = new AtomicLong(System.currentTimeMillis() * 1_000_000L);
            return unused -> counter.getAndIncrement();
        }

        // Custom: a fully-qualified class name implementing Function<Long, Object>
        return createCustom(fn);
    }

    @SuppressWarnings("unchecked")
    private static Function<Long, Object> createCustom(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            Object instance = clazz.getDeclaredConstructor().newInstance();
            if (instance instanceof Function) {
                LOG.info("Created custom generated-column function from {}", className);
                return (Function<Long, Object>) instance;
            }
            throw new IllegalArgumentException(
                    String.format(
                            "Class %s does not implement java.util.function.Function<Long, Object>",
                            className));
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot instantiate custom generated-column function %s: %s",
                            className, e.getMessage()),
                    e);
        }
    }
}
