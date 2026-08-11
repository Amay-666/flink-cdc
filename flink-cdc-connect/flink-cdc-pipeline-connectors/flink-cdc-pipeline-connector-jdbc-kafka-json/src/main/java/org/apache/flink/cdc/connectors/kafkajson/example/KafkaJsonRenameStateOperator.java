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

package org.apache.flink.cdc.connectors.kafkajson.example;

import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * Reference downstream operator for a canal-sourced job that uses the connector's own serialization
 * stack (no YAML pipeline, no SchemaOperator): it shows how to {@code instanceof}-dispatch a {@link
 * RenameTableEvent} and migrate per-table state from the old table id to the new one.
 *
 * <p>Example wiring in a custom DataStream job:
 *
 * <pre>{@code
 * FlinkSourceProvider provider =
 *         (FlinkSourceProvider) new KafkaJsonDataSource(factory).getEventSourceProvider();
 * StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 * DataStreamSource<Event> source =
 *         env.fromSource(
 *                 provider.getSource(), WatermarkStrategy.noWatermarks(), "canal-source");
 * source.process(new KafkaJsonRenameStateOperator()).sinkTo(yourSink);
 * }</pre>
 *
 * <p>Any per-table state the operator keeps (offsets, buffered records, an in-flight schema) must be
 * moved to the new table id when the source table is renamed; otherwise the state accumulated under
 * the old name would be orphaned. Here the state is a plain map to keep the example self-contained —
 * in a real job it would be a {@code MapState}/{@code ValueState} managed by the runtime (use a
 * {@code KeyedProcessFunction} keyed on the table id so the migration is a copy+delete of the keyed
 * entries).
 */
public class KafkaJsonRenameStateOperator extends ProcessFunction<Event, Event> {

    private static final long serialVersionUID = 1L;

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(KafkaJsonRenameStateOperator.class);

    /**
     * Per-table state keyed by table id. In a real job this is Flink-managed state (see class
     * javadoc); a plain map keeps the example runnable without a keyed context.
     */
    private transient Map<TableId, byte[]> perTableState;

    @Override
    public void open(Configuration parameters) {
        perTableState = new HashMap<>();
    }

    @Override
    public void processElement(Event event, Context context, Collector<Event> out)
            throws Exception {
        if (event instanceof RenameTableEvent) {
            handleRename((RenameTableEvent) event);
        } else if (event instanceof DataChangeEvent) {
            // The normal data path: look up the per-table state by the event's table id.
            TableId tableId = ((DataChangeEvent) event).tableId();
            byte[] state = perTableState.get(tableId);
            if (state != null) {
                LOG.debug("Using state for {}", tableId);
            }
        }
        out.collect(event);
    }

    private void handleRename(RenameTableEvent rename) {
        TableId oldTableId = rename.getOldTableId();
        TableId newTableId = rename.getNewTableId();
        byte[] migrated = perTableState.remove(oldTableId);
        if (migrated != null) {
            perTableState.put(newTableId, migrated);
            LOG.info(
                    "Table renamed {} -> {} (sql: {}): migrated {} bytes of per-table state",
                    oldTableId,
                    newTableId,
                    rename.getSql(),
                    migrated.length);
        } else {
            LOG.info("Table renamed {} -> {}: no state to migrate", oldTableId, newTableId);
        }
    }
}
