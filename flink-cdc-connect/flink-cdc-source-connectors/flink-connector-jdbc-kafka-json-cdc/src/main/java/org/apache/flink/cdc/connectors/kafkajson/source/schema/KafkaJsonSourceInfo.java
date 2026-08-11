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

package org.apache.flink.cdc.connectors.kafkajson.source.schema;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;

import java.time.Instant;

/**
 * The connector-specific source info of the Canal source: the {@code source} struct of the emitted
 * records.
 *
 * <p>Besides the common fields ({@code version}/{@code connector}/{@code name}/{@code
 * ts_ms}/{@code snapshot}/{@code db}) it carries the canal execution time ({@code es}) and send
 * time ({@code ts}).
 *
 * <p>Note: this class intentionally does <em>not</em> override {@link #struct()} — the struct is
 * always rendered through {@link KafkaJsonSourceInfoStructMaker} so that the data records and the
 * offset context use the very same schema.
 */
public class KafkaJsonSourceInfo extends AbstractSourceInfo {

    private final String database;
    private final String table;
    /** The event time of the record, i.e. {@code es} or {@code ts} (the configured field). */
    private final long eventTime;
    /** The binlog execution time reported by canal (millis). */
    private final long executeTime;
    /** The time canal sent the message (millis). */
    private final long sendTime;
    private final SnapshotRecord snapshot;

    public KafkaJsonSourceInfo(
            CommonConnectorConfig config,
            String database,
            String table,
            long eventTime,
            long executeTime,
            long sendTime,
            SnapshotRecord snapshot) {
        super(config);
        this.database = database;
        this.table = table;
        this.eventTime = eventTime;
        this.executeTime = executeTime;
        this.sendTime = sendTime;
        this.snapshot = snapshot;
    }

    @Override
    protected Instant timestamp() {
        return Instant.ofEpochMilli(eventTime);
    }

    @Override
    protected SnapshotRecord snapshot() {
        return snapshot;
    }

    @Override
    protected String database() {
        return database;
    }

    public String table() {
        return table;
    }

    public long getEventTime() {
        return eventTime;
    }

    public long getExecuteTime() {
        return executeTime;
    }

    public long getSendTime() {
        return sendTime;
    }

    public boolean isSnapshot() {
        return snapshot == SnapshotRecord.TRUE || snapshot == SnapshotRecord.LAST;
    }
}
