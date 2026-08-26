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

package org.apache.flink.cdc.connectors.kafkajson.source.message.canal;

import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.EventTime;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.MessageFormat;
import org.apache.flink.cdc.connectors.kafkajson.source.message.KafkaJsonMessage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * The flat message emitted by canal to Kafka when {@code canal.instance.memory.batch.mode} is
 * {@code MEMSIZE} / the {@code flatMessage} serializer is enabled.
 *
 * <p>All column values are {@code String}s — the DML payload is carried by {@link #getData()} (the
 * {@code after} image, or the {@code before} image for {@code DELETE}) and {@link #getOld()} (the
 * {@code before} image for {@code UPDATE}).
 *
 * <p>Implements the {@link KafkaJsonMessage} abstraction for the canal format. Note that {@link
 * #getDatabase()} / {@link #getTable()} return the raw field (which canal always fills, possibly
 * {@code ""}); the empty-string → {@code null} normalization is a {@code DebeziumMessage} concern,
 * whose {@code source} object may be absent entirely.
 *
 * <p>See https://github.com/alibaba/canal/wiki/ClientExample for the canonical JSON layout.
 */
public class CanalMessage extends KafkaJsonMessage {

    private static final long serialVersionUID = 1L;

    /** Monotonic sequence number assigned by canal. */
    @JsonProperty private long id;

    @JsonProperty private String database;

    @JsonProperty private String table;

    @JsonProperty private List<String> pkNames;

    @JsonProperty private boolean isDdl;

    /**
     * One of INSERT / UPDATE / DELETE / CREATE / ALTER / ERASE / QUERY / TRUNCATE / GTID / ... plus
     * {@code TIDB_WATERMARK} for TiCDC's marker events (not DML, see {@code
     * KafkaJsonRecordConverter}).
     */
    @JsonProperty private String type;

    /** The binlog execution time of the event (millis). */
    @JsonProperty private long es;

    /** The time canal send the message (millis). */
    @JsonProperty private long ts;

    /** The SQL statement, present for DDL events. */
    @JsonProperty private String sql;

    @JsonProperty private Map<String, Integer> sqlType;

    @JsonProperty private Map<String, String> mysqlType;

    /** The changed rows: {@code after} image for INSERT/UPDATE, {@code before} image for DELETE. */
    @JsonProperty private List<Map<String, String>> data;

    /** The {@code before} image, present for UPDATE (and only for the updated columns). */
    @JsonProperty private List<Map<String, String>> old;

    @JsonProperty("_tidb")
    private TidbInfo tidbInfo;

    /** The {@code _tidb} extension object carried by TiCDC canal-json messages. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TidbInfo implements Serializable {
        private Long commitTs;
        private Long watermarkTs;

        public Long getCommitTs() {
            return commitTs;
        }

        public void setCommitTs(Long commitTs) {
            this.commitTs = commitTs;
        }

        public Long getWatermarkTs() {
            return watermarkTs;
        }

        public void setWatermarkTs(Long watermarkTs) {
            this.watermarkTs = watermarkTs;
        }

        /**
         * The commit time of the TiDB transaction. A watermark event carries {@code watermarkTs}
         * (the TSO at which every transaction with a smaller commit TSO has already been published)
         * instead of {@code commitTs}; both are shifted to physical millis.
         */
        @JsonProperty("commit-ts")
        public Long getCommitTimeStamp(String type) {
            if (Objects.equals(type, "TIDB_WATERMARK")) {
                return watermarkTs != null ? watermarkTs >> 18 : null;
            }
            return commitTs != null ? commitTs >> 18 : null;
        }

        @Override
        public String toString() {
            return "{" +
                    "commitTs=" + commitTs +
                    ", watermarkTs=" + watermarkTs +
                    '}';
        }
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    @Override
    @Nullable
    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    @Override
    @Nullable
    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    @Nullable
    public List<String> getPkNames() {
        return pkNames;
    }

    public void setPkNames(List<String> pkNames) {
        this.pkNames = pkNames;
    }

    public boolean isDdl() {
        return isDdl;
    }

    public void setDdl(boolean ddl) {
        isDdl = ddl;
    }

    @Nullable
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getEs() {
        return es;
    }

    public void setEs(long es) {
        this.es = es;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    @Override
    @Nullable
    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    /**
     * Classifies the message by its canal {@code isDdl}/{@code type} fields: DDL messages, the DML
     * ops (INSERT / UPDATE / DELETE / QUERY), TiDB watermark markers and everything else.
     */
    @Override
    public MessageType getMessageType() {
        if (isDdl) {
            return MessageType.DDL;
        }
        if (type == null) {
            return MessageType.UNKNOWN;
        }
        switch (type.toUpperCase(Locale.ROOT)) {
            case "INSERT":
            case "UPDATE":
            case "DELETE":
            case "QUERY":
                return MessageType.DML;
            case "TIDB_WATERMARK":
                return MessageType.TIDB_WATERMARK;
            default:
                return MessageType.UNKNOWN;
        }
    }

    @Override
    public MessageFormat getFormat() {
        return MessageFormat.CANAL;
    }

    @Override
    @Nullable
    public Long getEventTimeValue(EventTime mode) {
        switch (mode) {
            case ES:
                return es;
            case TS:
                return ts;
            case TIDB_TSO:
                // The commit TSO (physical millis) carried by TiCDC's `_tidb` extension: `commitTs`
                // for a transaction, `watermarkTs` for a watermark event. Both are handled by
                // TidbInfo#getCommitTimeStamp; a plain canal flatMessage carries no `_tidb` and
                // yields null.
                TidbInfo tidb = tidbInfo;
                return tidb != null ? tidb.getCommitTimeStamp(type) : null;
            default:
                return null;
        }
    }

    @Nullable
    public Map<String, Integer> getSqlType() {
        return sqlType;
    }

    public void setSqlType(Map<String, Integer> sqlType) {
        this.sqlType = sqlType;
    }

    @Nullable
    public Map<String, String> getMysqlType() {
        return mysqlType;
    }

    public void setMysqlType(Map<String, String> mysqlType) {
        this.mysqlType = mysqlType;
    }

    @Nullable
    public List<Map<String, String>> getData() {
        return data;
    }

    public void setData(List<Map<String, String>> data) {
        this.data = data;
    }

    @Nullable
    public List<Map<String, String>> getOld() {
        return old;
    }

    public void setOld(List<Map<String, String>> old) {
        this.old = old;
    }

    @JsonProperty("_tidb")
    public TidbInfo getTidbInfo() {
        return this.tidbInfo;
    }

    @JsonProperty("_tidb")
    public void setTidbInfo(TidbInfo tidbInfo) {
        this.tidbInfo = tidbInfo;
    }

    @Override
    public String toString() {
        return "CanalMessage{"
                + "id="
                + id
                + ", database='"
                + database
                + '\''
                + ", table='"
                + table
                + '\''
                + ", isDdl="
                + isDdl
                + ", type='"
                + type
                + '\''
                + ", es="
                + es
                + ", ts="
                + ts
                + ", sql='"
                + sql
                + '\''
                + ", dataSize="
                + (data == null ? 0 : data.size())
                + ", oldSize="
                + (old == null ? 0 : old.size())
                + ", tidbInfo="
                + tidbInfo
                + '}';
    }
}
