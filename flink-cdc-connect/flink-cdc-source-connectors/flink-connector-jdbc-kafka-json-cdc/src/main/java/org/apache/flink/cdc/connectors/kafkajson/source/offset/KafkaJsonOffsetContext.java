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

package org.apache.flink.cdc.connectors.kafkajson.source.offset;

import org.apache.flink.cdc.connectors.kafkajson.source.schema.KafkaJsonSourceInfo;
import org.apache.flink.cdc.connectors.kafkajson.source.schema.KafkaJsonSourceInfoStructMaker;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.DataCollectionId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;
import java.util.Map;

/**
 * The {@link OffsetContext} of the Canal source.
 *
 * <p>The offset is a {@link KafkaJsonOffset} — the Kafka position of the last consumed message, keyed
 * by its event time. The context is updated on every emitted record so that checkpointing and
 * restart always resume from the latest consumed message.
 */
public class KafkaJsonOffsetContext implements OffsetContext {

    private final KafkaJsonSourceInfoStructMaker sourceInfoStructMaker;
    private KafkaJsonOffset offset = KafkaJsonOffset.INITIAL_OFFSET;
    private KafkaJsonSourceInfo sourceInfo;

    public KafkaJsonOffsetContext(MySqlConnectorConfig config) {
        this.sourceInfoStructMaker =
                new KafkaJsonSourceInfoStructMaker("canal", KafkaJsonSourceInfoStructMaker.DEBEZIUM_VERSION, config);
    }

    /** Updates the current offset. */
    public void updateOffset(KafkaJsonOffset offset) {
        this.offset = offset;
    }

    public KafkaJsonOffset getKafkaJsonOffset() {
        return offset;
    }

    /** Sets the source info used for the {@code source} struct of the offset. */
    public void setSourceInfo(KafkaJsonSourceInfo sourceInfo) {
        this.sourceInfo = sourceInfo;
    }

    @Override
    public Map<String, ?> getOffset() {
        return offset.getOffset();
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoStructMaker.schema();
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo == null ? null : sourceInfoStructMaker.struct(sourceInfo);
    }

    @Override
    public boolean isSnapshotRunning() {
        return false;
    }

    @Override
    public void markLastSnapshotRecord() {
        // the canal source never runs a snapshot inside the offset context
    }

    @Override
    public void preSnapshotStart() {
        // not used
    }

    @Override
    public void preSnapshotCompletion() {
        // not used
    }

    @Override
    public void postSnapshotCompletion() {
        // not used
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        // not used
    }

    @Override
    public TransactionContext getTransactionContext() {
        return null;
    }
}
