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
import org.apache.flink.configuration.MemorySize;

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

    /**
     * Group Commit modes supported by Doris Stream Load.
     *
     * <p>The enum constant name (lowercased) is used directly as the {@code group_commit} header
     * value sent to Doris, e.g. {@code SYNC_MODE} → {@code group_commit:sync_mode}.
     */
    public enum GroupCommitMode {
        /** Group Commit disabled — traditional Stream Load with a user-supplied label. */
        OFF,
        /**
         * Doris batches multiple loads into one transaction; returns after commit; data immediately
         * visible.
         */
        SYNC_MODE,
        /** Doris writes to WAL first and returns immediately; data visible after async commit. */
        ASYNC_MODE;

        /**
         * Returns the header value to send to Doris (e.g. {@code "sync_mode"}), or {@code null}
         * when disabled.
         */
        public String headerValue() {
            return this == OFF ? null : name().toLowerCase();
        }
    }

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

    /**
     * Doris Group Commit mode.
     *
     * <p>When enabled, the sink stops sending Stream Load labels (specifying a label degrades to
     * non-Group-Commit), adds a {@code group_commit} header, and injects a monotonically increasing
     * sequence value into every row so Doris can resolve ordering across reordered batch commits.
     * Requires a UNIQUE-model table (i.e. the source schema must declare a primary key); the DDL
     * builder automatically adds the sequence physical column and the {@code
     * function_column.sequence_col} property.
     *
     * <ul>
     *   <li>{@code OFF} (default) — traditional Stream Load.
     *   <li>{@code SYNC_MODE} — Doris batches loads into one transaction; returns after commit;
     *       data immediately visible (~10 s latency).
     *   <li>{@code ASYNC_MODE} — Doris writes to WAL first and returns immediately; data visible
     *       after async commit. WAL is single-replica — a disk failure can lose uncommitted data.
     * </ul>
     */
    public static final ConfigOption<GroupCommitMode> GROUP_COMMIT_MODE =
            ConfigOptions.key("sink.group-commit")
                    .enumType(GroupCommitMode.class)
                    .defaultValue(GroupCommitMode.OFF);

    /**
     * Name of the physical {@code BIGINT} column injected into the Doris DDL as the sequence
     * column. The DDL builder also sets {@code "function_column.sequence_col" = <this value>} in
     * the table PROPERTIES so Doris uses this column for version ordering. Only takes effect when
     * {@link #GROUP_COMMIT_MODE} is not {@code OFF}.
     */
    public static final ConfigOption<String> SEQUENCE_COLUMN_NAME =
            ConfigOptions.key("sink.group-commit.sequence-column")
                    .stringType()
                    .defaultValue("cdc_sequence");

    /**
     * Group Commit auto-commit interval in milliseconds. Doris commits a batch when either this
     * interval or {@link #GROUP_COMMIT_DATA_BYTES} is reached. Default is {@code 10000} (10 s),
     * matching Doris's built-in default. Injected into DDL PROPERTIES as {@code
     * group_commit_interval_ms}. Only takes effect when {@link #GROUP_COMMIT_MODE} is not {@code
     * OFF}.
     */
    public static final ConfigOption<Long> GROUP_COMMIT_INTERVAL_MS =
            ConfigOptions.key("sink.group-commit.interval-ms").longType().defaultValue(10_000L);

    /**
     * Group Commit auto-commit data volume in bytes. Doris commits a batch when either this data
     * volume or {@link #GROUP_COMMIT_INTERVAL_MS} is reached. Default is {@code 67108864} (64 MB),
     * matching Doris's built-in default. Injected into DDL PROPERTIES as {@code
     * group_commit_data_bytes}. Only takes effect when {@link #GROUP_COMMIT_MODE} is not {@code
     * OFF}.
     */
    public static final ConfigOption<Long> GROUP_COMMIT_DATA_BYTES =
            ConfigOptions.key("sink.group-commit.data-bytes")
                    .longType()
                    .defaultValue(MemorySize.parse("64mb").getBytes());

    /**
     * Initial polling interval for checking heavy schema change completion. After executing a
     * {@code MODIFY COLUMN} DDL, the metadata applier polls {@code SHOW ALTER TABLE COLUMN} with
     * exponential backoff: the interval doubles after each poll, capped by {@link
     * #SCHEMA_CHANGE_POLL_MAX_INTERVAL}. This way fast schema changes (e.g. empty tables) are
     * detected within the first second, while slow ones don't spam Doris with queries.
     */
    public static final ConfigOption<Duration> SCHEMA_CHANGE_POLL_INTERVAL =
            ConfigOptions.key("sink.schema-change.poll-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1));

    /**
     * Maximum polling interval (cap for the exponential backoff). The interval doubles after each
     * poll attempt but never exceeds this value.
     */
    public static final ConfigOption<Duration> SCHEMA_CHANGE_POLL_MAX_INTERVAL =
            ConfigOptions.key("sink.schema-change.poll-max-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30));

    /**
     * Maximum time to wait for a heavy schema change to complete. If the schema change job is still
     * not finished after this duration, a {@link
     * org.apache.flink.cdc.common.exceptions.SchemaEvolveException} is thrown. Default is 30
     * minutes — a pragmatic choice that covers most table sizes while not blocking the pipeline
     * indefinitely. Doris's own schema change timeout is much longer (default 30 days, {@code
     * alter_table_timeout_second = 86400 * 30}); this option should be set lower than Doris's
     * timeout. Increase it for very large tables where schema changes take hours.
     */
    public static final ConfigOption<Duration> SCHEMA_CHANGE_MAX_WAIT =
            ConfigOptions.key("sink.schema-change.max-wait")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(30));

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

    public boolean isGroupCommitEnabled() {
        return getConfig().get(GROUP_COMMIT_MODE) != GroupCommitMode.OFF;
    }

    public GroupCommitMode getGroupCommitMode() {
        return getConfig().get(GROUP_COMMIT_MODE);
    }

    public String getSequenceColumnName() {
        return getConfig().get(SEQUENCE_COLUMN_NAME);
    }

    public long getGroupCommitIntervalMs() {
        return getConfig().get(GROUP_COMMIT_INTERVAL_MS);
    }

    public long getGroupCommitDataBytes() {
        return getConfig().get(GROUP_COMMIT_DATA_BYTES);
    }

    public Duration getSchemaChangePollInterval() {
        return getConfig().get(SCHEMA_CHANGE_POLL_INTERVAL);
    }

    public Duration getSchemaChangePollMaxInterval() {
        return getConfig().get(SCHEMA_CHANGE_POLL_MAX_INTERVAL);
    }

    public Duration getSchemaChangeMaxWait() {
        return getConfig().get(SCHEMA_CHANGE_MAX_WAIT);
    }
}
