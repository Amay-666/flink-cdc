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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * The flat message emitted by canal to Kafka when {@code canal.instance.memory.batch.mode} is
 * {@code MEMSIZE} / the {@code flatMessage} serializer is enabled.
 *
 * <p>All column values are {@code String}s — the DML payload is carried by {@link #getData()} (the
 * {@code after} image, or the {@code before} image for {@code DELETE}) and {@link #getOld()} (the
 * {@code before} image for {@code UPDATE}).
 *
 * <p>See https://github.com/alibaba/canal/wiki/ClientExample for the canonical JSON layout.
 */
public class KafkaJsonFlatMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Monotonic sequence number assigned by canal. */
    @JsonProperty private long id;

    @JsonProperty private String database;

    @JsonProperty private String table;

    @JsonProperty private List<String> pkNames;

    @JsonProperty private boolean isDdl;

    /**
     * One of INSERT / UPDATE / DELETE / CREATE / ALTER / ERASE / QUERY / TRUNCATE / GTID / ...
     * plus {@code TIDB_WATERMARK} for TiCDC's marker events (not DML, see {@code
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

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TidbInfo implements Serializable {
        private Long commitTs;

        public Long getCommitTs() {
            return commitTs;
        }

        public void setCommitTs(Long commitTs) {
            this.commitTs = commitTs;
        }

        // The commit time of the TiDB transaction.
        @JsonProperty("commit-ts")
        public Long getCommitTimeStamp() {
            return commitTs != null ? commitTs >> 18 : null;
        }

        @Override
        public String toString() {
            return "{" +
                    "commitTs=" + commitTs +
                    '}';
        }
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    @Nullable
    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

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

    @Nullable
    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
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
        return "KafkaJsonFlatMessage{"
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
