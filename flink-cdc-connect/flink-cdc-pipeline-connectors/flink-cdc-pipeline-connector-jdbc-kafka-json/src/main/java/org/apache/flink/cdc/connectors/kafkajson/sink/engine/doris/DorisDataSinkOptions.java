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

    /**
     * Which writer implementation the Doris sink builds.
     *
     * <p>The three modes coexist so a job can move to the new behaviour on its own schedule:
     *
     * <ul>
     *   <li>{@code LEGACY} (default) — the original stateless {@code DorisSinkWriter}. Every flush
     *       is an independent Stream Load under a fresh UUID label, the buffer lives only in
     *       memory, and a restart replays from the source. Behaviourally identical to previous
     *       releases.
     *   <li>{@code STATEFUL} — the same single-phase Stream Load protocol, but the writer keeps its
     *       sequence counter in Flink state, so it stays strictly monotonic across restarts instead
     *       of leaning on the wall clock. Rows are still visible as soon as a flush returns.
     *   <li>{@code STATEFUL_2PC} — additionally runs each checkpoint's loads through Doris's
     *       two-phase Stream Load, so a checkpoint's rows become visible only once that checkpoint
     *       is complete, and a failed checkpoint's rows are aborted. Requires Doris 2.1+ and cannot
     *       be combined with Group Commit ({@link #GROUP_COMMIT_MODE}).
     * </ul>
     */
    public enum WriterMode {
        LEGACY,
        STATEFUL,
        STATEFUL_2PC
    }

    /**
     * Writer implementation to build: {@code legacy}, {@code stateful} or {@code stateful-2pc}
     * ({@code stateful_2pc} is accepted as well). Defaults to {@code legacy}, so an existing job
     * keeps the behaviour it has today unless it opts in.
     *
     * <p>Deliberately a string rather than an {@code enumType} option: Flink converts an enum by
     * upper-casing the configured value and looking the constant up by name, so the natural
     * spelling {@code stateful-2pc} would be looked up as {@code STATEFUL-2PC} — not a legal Java
     * identifier, and never a hit. Parsing it here keeps the readable spelling and lets an unknown
     * value fail with the list of valid ones.
     */
    public static final ConfigOption<String> WRITER_MODE =
            ConfigOptions.key("sink.writer").stringType().defaultValue("legacy");

    /**
     * Prefix of the Stream Load labels the writer generates, so two jobs writing into one Doris
     * cluster (or a job restarted from a different savepoint) can be told apart.
     *
     * <p>Read where a label is actually built from it (see {@code usesLabelPrefix}): the stateful
     * writer labels a load {@code {prefix}_{database}_{table}_{subtask}_{uuid}}, and {@link
     * WriterMode#STATEFUL_2PC} labels it {@code {prefix}_{database}_{table}_{subtask}_{epoch}_{n}},
     * where the epoch is the checkpoint the load belongs to. The legacy writer builds its labels
     * from its own built-in prefix instead, and a Group Commit load carries no label at all, so in
     * those configurations this value is inert. Database and table names are sanitized before they
     * are spliced in.
     *
     * <p>Restoring state written under a different prefix is reported but not refused: the labels
     * of a checkpoint that is still open are found through the label this writer is about to reuse,
     * so the restore itself is unaffected. The report is for the one case where writes can collide
     * — a prefix shared with another job.
     */
    public static final ConfigOption<String> LABEL_PREFIX =
            ConfigOptions.key("sink.label-prefix").stringType().defaultValue("cdc");

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
        WriterMode mode = getWriterMode();
        GroupCommitMode groupCommit = config.get(GROUP_COMMIT_MODE);
        rejectTwoPhaseCommitWithGroupCommit(mode, groupCommit);
        if (usesLabelPrefix(mode, groupCommit)) {
            rejectBlankLabelPrefix(config.get(LABEL_PREFIX));
        }
    }

    /**
     * Whether this configuration ever builds a Stream Load label out of {@link #LABEL_PREFIX}.
     *
     * <p>Only two combinations do: the two-phase writer, and the stateful writer with Group Commit
     * off. The legacy writer keeps its own built-in prefix, and a Group Commit load carries no
     * label at all (a label degrades it to a non-Group-Commit load) — so in those configurations
     * the option is inert and a blank value breaks nothing.
     */
    private static boolean usesLabelPrefix(WriterMode mode, GroupCommitMode groupCommit) {
        return mode == WriterMode.STATEFUL_2PC
                || (mode == WriterMode.STATEFUL && groupCommit == GroupCommitMode.OFF);
    }

    /**
     * Two-phase commit and Group Commit are two answers to the same question — when a batch becomes
     * visible — and they cannot be layered: Group Commit decides the commit moment itself, and
     * specifying a label (which the 2PC epoch scheme is built on) is exactly what degrades a load
     * to non-Group-Commit. Failing at construction names both options instead of letting the job
     * discover it as a Doris-side rejection.
     */
    private static void rejectTwoPhaseCommitWithGroupCommit(
            WriterMode mode, GroupCommitMode groupCommit) {
        if (mode == WriterMode.STATEFUL_2PC && groupCommit != GroupCommitMode.OFF) {
            throw new IllegalArgumentException(
                    String.format(
                            "Options %s=stateful-2pc and %s=%s cannot be combined: two-phase commit"
                                    + " decides when a batch becomes visible by committing the"
                                    + " checkpoint's transactions, while Group Commit decides that"
                                    + " on its own and treats a user-supplied label as opting out"
                                    + " of it.",
                            WRITER_MODE.key(), GROUP_COMMIT_MODE.key(), groupCommit));
        }
    }

    /**
     * Called only for the configurations {@link #usesLabelPrefix} selects. A blank prefix costs the
     * label the only part of it that identifies the job, which is what the option is for.
     */
    private static void rejectBlankLabelPrefix(String prefix) {
        if (prefix == null || prefix.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Option %s must not be blank", LABEL_PREFIX.key()));
        }
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

    /**
     * Parses {@link #WRITER_MODE}. Both spellings people reach for are accepted — {@code
     * stateful-2pc} and {@code stateful_2pc} — and an unknown value names itself and the valid ones
     * instead of falling back to the default.
     */
    public WriterMode getWriterMode() {
        String configured = getConfig().get(WRITER_MODE);
        String value = configured == null ? "" : configured.trim().toLowerCase().replace('_', '-');
        switch (value) {
            case "legacy":
                return WriterMode.LEGACY;
            case "stateful":
                return WriterMode.STATEFUL;
            case "stateful-2pc":
                return WriterMode.STATEFUL_2PC;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unsupported %s: '%s'. Supported values are: legacy, stateful,"
                                        + " stateful-2pc",
                                WRITER_MODE.key(), configured));
        }
    }

    /** Whether the writer keeps its state, i.e. any mode but {@link WriterMode#LEGACY}. */
    public boolean isStatefulWriter() {
        return getWriterMode() != WriterMode.LEGACY;
    }

    /** Whether the writer commits each checkpoint's Stream Loads through Doris 2PC. */
    public boolean isTwoPhaseCommit() {
        return getWriterMode() == WriterMode.STATEFUL_2PC;
    }

    public String getLabelPrefix() {
        return getConfig().get(LABEL_PREFIX);
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
