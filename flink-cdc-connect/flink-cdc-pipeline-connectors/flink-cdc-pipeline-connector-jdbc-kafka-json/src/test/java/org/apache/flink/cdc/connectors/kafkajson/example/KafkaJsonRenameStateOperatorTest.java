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

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.kafkajson.event.RenameTableEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link KafkaJsonRenameStateOperator}: a {@link RenameTableEvent} is dispatched by
 * the {@code instanceof} check and forwarded downstream while the operator migrates its per-table
 * state from the old table id to the new one. The operator is driven directly ({@code open} needs
 * no runtime context and {@code processElement} ignores it) so the test needs no Flink test
 * harness.
 */
public class KafkaJsonRenameStateOperatorTest {

    @Test
    public void testRenameTableEventIsForwarded() throws Exception {
        KafkaJsonRenameStateOperator operator = new KafkaJsonRenameStateOperator();
        operator.open(new Configuration());

        List<Event> forwarded = new ArrayList<>();
        operator.processElement(
                new RenameTableEvent(
                        TableId.tableId("test", "users"),
                        TableId.tableId("test", "vip_users"),
                        Schema.newBuilder().build()),
                null,
                new Collector<Event>() {
                    @Override
                    public void collect(Event record) {
                        forwarded.add(record);
                    }

                    @Override
                    public void close() {}
                });

        assertThat(forwarded).hasSize(1);
        assertThat(forwarded.get(0)).isInstanceOf(RenameTableEvent.class);
        RenameTableEvent rename = (RenameTableEvent) forwarded.get(0);
        assertThat(rename.getOldTableId()).isEqualTo(TableId.tableId("test", "users"));
        assertThat(rename.getNewTableId()).isEqualTo(TableId.tableId("test", "vip_users"));
    }
}
