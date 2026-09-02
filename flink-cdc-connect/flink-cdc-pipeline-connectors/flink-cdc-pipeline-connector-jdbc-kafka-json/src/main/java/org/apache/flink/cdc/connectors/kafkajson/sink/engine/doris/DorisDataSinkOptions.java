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

import org.apache.flink.cdc.connectors.kafkajson.sink.KafkaJsonDataSinkOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import java.time.Duration;
import java.util.Map;

/**
 * Doris-specific sink options.
 *
 * <p>Adds the Doris endpoints, credentials, StreamLoad batching and retry knobs to the shared
 * {@link KafkaJsonDataSinkOptions} (which carries the table/database name mapping). Each target
 * engine gets its own options class under {@code sink/engine/}, so adding e.g. an Iceberg sink
 * means a sibling {@code IcebergDataSinkOptions} instead of growing this one.
 */
public class DorisDataSinkOptions extends KafkaJsonDataSinkOptions {

    private static final long serialVersionUID = 1L;

    /** Comma-separated list of Doris FE/coordinator endpoints ({@code host:port,...}). */
    public static final ConfigOption<String> FENODES =
            ConfigOptions.key("fenodes").stringType().noDefaultValue();

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username").stringType().noDefaultValue();

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password").stringType().noDefaultValue();

    /** Whether DELETE data change events map to a delete marker on the target. */
    public static final ConfigOption<Boolean> ENABLE_BATCH_DELETE =
            ConfigOptions.key("sink.enable.batch-delete").booleanType().defaultValue(true);

    /** Number of buffered rows before a forced flush of that table. */
    public static final ConfigOption<Integer> BUFFER_SIZE =
            ConfigOptions.key("sink.buffer.size").intType().defaultValue(1024);

    /** Upper bound on the total rows buffered across all tables before a forced flush. */
    public static final ConfigOption<Integer> MAX_BUFFERED_ROWS =
            ConfigOptions.key("sink.buffer.max-buffered-rows").intType().defaultValue(50_000);

    /** Max time rows may sit in the buffer before a flush is triggered. */
    public static final ConfigOption<Duration> FLUSH_INTERVAL =
            ConfigOptions.key("sink.flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(5));

    /** Extra properties passed through to the StreamLoad request. */
    public static final ConfigOption<Map<String, String>> STREAM_LOAD_PROPERTIES =
            ConfigOptions.key("sink.properties").mapType().noDefaultValue();

    /** Max retries for a failed StreamLoad / DDL request. */
    public static final ConfigOption<Integer> MAX_RETRIES =
            ConfigOptions.key("sink.max-retries").intType().defaultValue(3);

    /** Extra properties passed through to the Doris table DDL. */
    public static final ConfigOption<Map<String, String>> TABLE_PROPERTIES =
            ConfigOptions.key("sink.table.properties").mapType().noDefaultValue();

    /** Number of buckets for the target table. default value is 0, which means auto bucket. */
    public static final ConfigOption<Integer> TABLE_BUCKETS =
            ConfigOptions.key("sink.table.buckets").intType().defaultValue(0);

    public DorisDataSinkOptions(Configuration config) {
        super(config);
    }

    @Override
    public DorisDataSinkOptions withDatabaseMapping(TableIdMapping mapping) {
        super.withDatabaseMapping(mapping);
        return this;
    }

    @Override
    public DorisDataSinkOptions withTableMapping(TableIdMapping mapping) {
        super.withTableMapping(mapping);
        return this;
    }

    public String getFenodes() {
        return getConfig().get(FENODES);
    }

    public String getUsername() {
        return getConfig().get(USERNAME);
    }

    public String getPassword() {
        return getConfig().get(PASSWORD);
    }

    public boolean isEnableBatchDelete() {
        return getConfig().get(ENABLE_BATCH_DELETE);
    }

    public int getBufferSize() {
        return getConfig().get(BUFFER_SIZE);
    }

    public int getMaxBufferedRows() {
        return getConfig().get(MAX_BUFFERED_ROWS);
    }

    public Duration getFlushInterval() {
        return getConfig().get(FLUSH_INTERVAL);
    }

    public Map<String, String> getStreamLoadProperties() {
        return getConfig().get(STREAM_LOAD_PROPERTIES);
    }

    public int getMaxRetries() {
        return getConfig().get(MAX_RETRIES);
    }

    public Map<String, String> getTableProperties() {
        return getConfig().get(TABLE_PROPERTIES);
    }

    public int getTableBuckets() {
        return getConfig().get(TABLE_BUCKETS);
    }
}
