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

package org.apache.flink.cdc.connectors.canal.source.message;

import org.apache.flink.cdc.connectors.canal.source.config.CanalSourceConfig;
import org.apache.flink.cdc.connectors.canal.source.config.CanalSourceOptions.EventTime;
import org.apache.flink.cdc.connectors.canal.source.schema.CanalSourceInfo;
import org.apache.flink.cdc.connectors.canal.source.utils.CanalTableUtils;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.data.Envelope;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Converts a canal flatMessage into the list of Debezium-shaped {@link SourceRecord}s.
 *
 * <p>canal batches the rows of a single DML statement into {@code data[]}/{@code old[]}; each row
 * becomes one {@link SourceRecord}:
 *
 * <ul>
 *   <li>{@code INSERT} → {@code op=c} with {@code after}=data row;
 *   <li>{@code UPDATE} → {@code op=u} with {@code before}=old row and {@code after}=data row;
 *   <li>{@code DELETE} → {@code op=d} with {@code before}=data row;
 *   <li>other types ({@code GTID}/{@code SAVEPOINT}/…) and DDL messages produce no data records.
 * </ul>
 *
 * <p>DDL messages are forwarded to the schema-change pipeline (Phase 8) and therefore skipped here.
 */
public class CanalRecordConverter {

    private final CanalRecordFactory factory;
    private final EventTime eventTimeMode;
    private final MySqlConnectorConfig dbzConfig;

    public CanalRecordConverter(CanalRecordFactory factory, CanalSourceConfig sourceConfig) {
        this.factory = factory;
        this.eventTimeMode = sourceConfig.getEventTime();
        this.dbzConfig = sourceConfig.getDbzConnectorConfig();
    }

    /**
     * Converts one flatMessage into the data records it carries.
     *
     * @param message the parsed flatMessage (not a DDL message)
     * @param topic the Kafka topic the message was consumed from
     * @param partition the Kafka partition
     * @param kafkaOffset the Kafka partition-local offset
     * @return the emitted records; possibly empty (e.g. for DDL or non-DML messages)
     */
    /**
     * Returns the ordering event time (millis) of a flatMessage for the configured {@link
     * EventTime} mode: the binlog execution time {@code es} or the canal send time {@code ts}.
     */
    public static long eventTime(CanalFlatMessage message, EventTime eventTimeMode) {
        return eventTimeMode == EventTime.ES ? message.getEs() : message.getTs();
    }

    public List<SourceRecord> convert(
            CanalFlatMessage message, String topic, int partition, long kafkaOffset) {
        if (message.isDdl()) {
            return Collections.emptyList();
        }
        String op = message.getType() == null ? "" : message.getType().toUpperCase(Locale.ROOT);
        switch (op) {
            case "INSERT":
            case "QUERY":
                return convertRows(message, topic, partition, kafkaOffset, Envelope.Operation.CREATE);
            case "UPDATE":
                return convertRows(message, topic, partition, kafkaOffset, Envelope.Operation.UPDATE);
            case "DELETE":
                return convertRows(message, topic, partition, kafkaOffset, Envelope.Operation.DELETE);
            default:
                // GTID / XACOMPLETE / SAVEPOINT / ... : no data rows
                return Collections.emptyList();
        }
    }

    private List<SourceRecord> convertRows(
            CanalFlatMessage message,
            String topic,
            int partition,
            long kafkaOffset,
            Envelope.Operation op) {
        Table table = resolveTable(message);
        if (table == null) {
            return Collections.emptyList();
        }
        long eventTime = eventTime(message, eventTimeMode);
        CanalSourceInfo sourceInfo =
                new CanalSourceInfo(
                        dbzConfig,
                        message.getDatabase(),
                        message.getTable(),
                        eventTime,
                        message.getEs(),
                        message.getTs(),
                        SnapshotRecord.FALSE);

        List<Map<String, String>> data = message.getData();
        List<Map<String, String>> old = message.getOld();
        List<SourceRecord> records = new ArrayList<>(data.size());
        for (int i = 0; i < data.size(); i++) {
            Map<String, String> afterRow = data.get(i);
            Map<String, String> beforeRow = old.size() > i ? old.get(i) : null;
            if (op == Envelope.Operation.DELETE) {
                // canal puts the (before) row into data for DELETE
                records.add(
                        factory.createRecord(
                                table,
                                factory.canalRowData(table, afterRow),
                                null,
                                op,
                                sourceInfo,
                                topic,
                                partition,
                                kafkaOffset));
            } else if (op == Envelope.Operation.UPDATE) {
                records.add(
                        factory.createRecord(
                                table,
                                beforeRow == null ? null : factory.canalRowData(table, beforeRow),
                                factory.canalRowData(table, afterRow),
                                op,
                                sourceInfo,
                                topic,
                                partition,
                                kafkaOffset));
            } else {
                records.add(
                        factory.createRecord(
                                table,
                                null,
                                factory.canalRowData(table, afterRow),
                                op,
                                sourceInfo,
                                topic,
                                partition,
                                kafkaOffset));
            }
        }
        return records;
    }

    /** Returns the registered (JDBC) schema, or rebuilds one from the message {@code mysqlType}. */
    private Table resolveTable(CanalFlatMessage message) {
        TableId tableId =
                new TableId(message.getDatabase() == null ? "" : message.getDatabase(), null, message.getTable());
        Table table = factory.tableFor(tableId);
        if (table == null) {
            table = CanalTableUtils.buildTable(message);
            if (table == null) {
                return null;
            }
            factory.registerTable(table);
        }
        return table;
    }
}
