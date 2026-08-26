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

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * The common traits of a change-log message consumed from Kafka, regardless of the external tool
 * that produced it (canal flatMessage JSON, standard Debezium envelope, TiCDC's Debezium-compatible
 * format).
 *
 * <p>The abstraction carries only what the stream pipeline needs uniformly: the message kind ({@link
 * MessageType}), the affected table and the event-time value. Everything that genuinely differs
 * between the formats — most notably how the DML rows are represented (canal carries string rows,
 * Debezium carries typed structs) — stays on the concrete subclasses, which the pipeline dispatches
 * on via {@code instanceof} rather than forcing one row model onto both (see
 * docs/DEBEZIUM_PLAN.md §2.3-1).
 */
public abstract class KafkaJsonMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The kind of change a message carries. */
    public enum MessageType {
        /** A schema change (DDL); produces no data records but updates the shared schema. */
        DDL,
        /** A data change (INSERT / UPDATE / DELETE / READ). */
        DML,
        /** A TiDB watermark marker event (no rows, only progress tracking). */
        TIDB_WATERMARK,
        /** Anything else (GTID / SAVEPOINT / XACOMPLETE / unknown op ...). */
        UNKNOWN
    }

    /** Returns the kind of change this message carries. */
    public abstract MessageType getMessageType();

    /** Returns the database name, or {@code null} when the message does not carry one. */
    @Nullable
    public abstract String getDatabase();

    /** Returns the table name, or {@code null} when the message does not carry one. */
    @Nullable
    public abstract String getTable();

    /** Returns the raw SQL of a DDL message, or {@code null} for non-DDL / unknown messages. */
    @Nullable
    public abstract String getSql();

    /**
     * Returns the event-time (Unix millis) of this message for the configured {@link EventTime}
     * mode, or {@code null} when the message does not carry the corresponding timestamp.
     *
     * @param mode the configured event-time mode; each subclass maps it to its own timestamp fields
     *     (canal {@code es}/{@code ts}, Debezium {@code source.ts_ms}/{@code ts_ms}, TiDB {@code
     *     commit_ts}).
     */
    @Nullable
    public abstract Long getEventTimeValue(EventTime mode);
}
