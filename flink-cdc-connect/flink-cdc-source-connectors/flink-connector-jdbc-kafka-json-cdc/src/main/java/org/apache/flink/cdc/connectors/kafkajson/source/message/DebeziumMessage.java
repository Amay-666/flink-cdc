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

package org.apache.flink.cdc.connectors.kafkajson.source.message;

import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.EventTime;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

/**
 * A message in the Debezium envelope format (standard Debezium or TiCDC's Debezium-compatible
 * output): an optional {@code schema} field plus a {@code payload} that carries {@code before} /
 * {@code after} structs, the {@code source} block ({@code db}/{@code table}/{@code ts_ms}, and the
 * TiCDC-only {@code commit_ts}/{@code cluster_id}), the {@code op} and the connector processing time
 * {@code ts_ms}.
 *
 * <p>Implements the {@link KafkaJsonMessage} abstraction for the Debezium format. Unlike the canal
 * flatMessage, the row values are typed (JSON numbers / strings / arrays), so {@code before}/{@code
 * after} are kept as raw {@link JsonNode}s; the DML→record conversion is the Debezium-specific part
 * of the pipeline (see docs/DEBEZIUM_PLAN.md §S3).
 *
 * <p>Also covers the Debezium schema-change record shape ({@code databaseName}/{@code ddl} without a
 * {@code source} block), which a bare (schema-include=false) message or the schema-history topic
 * carries.
 */
public class DebeziumMessage extends KafkaJsonMessage {

    private static final long serialVersionUID = 1L;

    @Nullable private final Payload payload;

    public DebeziumMessage(@Nullable Payload payload) {
        this.payload = payload;
    }

    /** Returns the payload, or {@code null} for a tombstone message ({@code payload: null}). */
    @Nullable
    public Payload getPayload() {
        return payload;
    }

    @Override
    public MessageType getMessageType() {
        if (payload == null) {
            return MessageType.UNKNOWN;
        }
        if (payload.ddl != null && !payload.ddl.isEmpty()) {
            return MessageType.DDL;
        }
        String op = payload.op == null ? "" : payload.op;
        switch (op) {
            case "c":
            case "u":
            case "d":
            case "r":
                return MessageType.DML;
            case "m":
                // TiDB watermark marker emitted by the TiCDC Debezium-compatible format when
                // enable-tidb-extension is on; carries no rows, only progress tracking
                return MessageType.TIDB_WATERMARK;
            default:
                return MessageType.UNKNOWN;
        }
    }

    @Override
    @Nullable
    public String getDatabase() {
        if (payload == null) {
            return null;
        }
        String db = null;
        if (payload.source != null) {
            db = payload.source.db;
        }
        if (db == null || db.isEmpty()) {
            // the schema-change record shape carries the database in the payload, not in source
            db = payload.databaseName;
        }
        return blankToNull(db);
    }

    @Override
    @Nullable
    public String getTable() {
        if (payload == null || payload.source == null) {
            return null;
        }
        return blankToNull(payload.source.table);
    }

    @Override
    @Nullable
    public String getSql() {
        if (payload == null) {
            return null;
        }
        return payload.ddl;
    }

    @Override
    @Nullable
    public Long getEventTimeValue(EventTime mode) {
        if (payload == null) {
            return null;
        }
        switch (mode) {
            case ES:
                // source.ts_ms is the time the change was made in the source database (the
                // canal es equivalent); payload.ts_ms is the connector processing time (the canal
                // ts equivalent). A message may carry only one of the two (a bare schema-change
                // record has neither), so fall back across them to keep the ordering key usable.
                if (payload.source != null && payload.source.tsMs != null) {
                    return payload.source.tsMs;
                }
                return payload.tsMs;
            case TS:
                if (payload.tsMs != null) {
                    return payload.tsMs;
                }
                return payload.source == null ? null : payload.source.tsMs;
            case TIDB_TSO:
                // the TiDB commit TSO occupies the upper 46 bits as physical millis; the lower 18
                // bits are a logical counter (see KafkaJsonTidbOffsetUtils#tsoToEventTime)
                if (payload.source == null || payload.source.commitTs == null) {
                    return null;
                }
                return payload.source.commitTs >> 18;
            default:
                return null;
        }
    }

    /**
     * The source change time ({@code source.ts_ms}), the {@code es} equivalent of the canal
     * flatMessage; {@code 0} when absent (e.g. a bare schema-change record).
     */
    public long getEs() {
        return payload != null && payload.source != null && payload.source.tsMs != null
                ? payload.source.tsMs
                : 0L;
    }

    /**
     * The connector processing time ({@code payload.ts_ms}), the {@code ts} equivalent of the canal
     * flatMessage; {@code 0} when absent.
     */
    public long getTs() {
        return payload != null && payload.tsMs != null ? payload.tsMs : 0L;
    }

    @Nullable
    private static String blankToNull(String value) {
        return value == null || value.isEmpty() ? null : value;
    }

    /**
     * The {@code payload} of a Debezium message. Unknown fields (e.g. {@code transaction}) are
     * ignored.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Payload {

        /** The {@code before} image, keyed by column name; {@code null} for INSERT / READ. */
        @JsonProperty("before")
        @Nullable
        private JsonNode before;

        /** The {@code after} image, keyed by column name; {@code null} for DELETE. */
        @JsonProperty("after")
        @Nullable
        private JsonNode after;

        @JsonProperty("source")
        @Nullable
        private Source source;

        /** One of {@code c}/{@code u}/{@code d}/{@code r}, or {@code m} for a TiDB watermark. */
        @JsonProperty("op")
        @Nullable
        private String op;

        /** The connector processing time (Unix millis). */
        @JsonProperty("ts_ms")
        @Nullable
        private Long tsMs;

        /** The DDL statement of a schema-change record. */
        @JsonProperty("ddl")
        @Nullable
        private String ddl;

        /** The database of a bare schema-change record (which has no {@code source} block). */
        @JsonProperty("databaseName")
        @Nullable
        private String databaseName;

        @Nullable
        public JsonNode getBefore() {
            // an explicit JSON null (NullNode) and an absent field both mean "no image"
            return before == null || before.isNull() ? null : before;
        }

        @Nullable
        public JsonNode getAfter() {
            return after == null || after.isNull() ? null : after;
        }

        @Nullable
        public Source getSource() {
            return source;
        }

        @Nullable
        public String getOp() {
            return op;
        }

        @Nullable
        public Long getTsMs() {
            return tsMs;
        }

        @Nullable
        public String getDdl() {
            return ddl;
        }

        @Nullable
        public String getDatabaseName() {
            return databaseName;
        }
    }

    /** The {@code source} block of a Debezium payload. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Source {

        @JsonProperty("db")
        @Nullable
        private String db;

        @JsonProperty("table")
        @Nullable
        private String table;

        /** The time the change was made in the source database (Unix millis). */
        @JsonProperty("ts_ms")
        @Nullable
        private Long tsMs;

        /** TiCDC-only: the TiDB commit TSO of the transaction. */
        @JsonProperty("commit_ts")
        @Nullable
        private Long commitTs;

        /** TiCDC-only: the TiDB cluster id. */
        @JsonProperty("cluster_id")
        @Nullable
        private String clusterId;

        @Nullable
        public String getDb() {
            return db;
        }

        @Nullable
        public String getTable() {
            return table;
        }

        @Nullable
        public Long getTsMs() {
            return tsMs;
        }

        @Nullable
        public Long getCommitTs() {
            return commitTs;
        }

        @Nullable
        public String getClusterId() {
            return clusterId;
        }
    }
}
