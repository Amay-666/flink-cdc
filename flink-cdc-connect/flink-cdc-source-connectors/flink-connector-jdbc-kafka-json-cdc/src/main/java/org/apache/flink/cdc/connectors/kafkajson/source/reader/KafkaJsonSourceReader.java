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

package org.apache.flink.cdc.connectors.kafkajson.source.reader;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderContext;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderWithCommit;
import org.apache.flink.cdc.connectors.kafkajson.source.meta.events.KafkaJsonStreamSplitMetaAssembledEvent;
import org.apache.flink.cdc.connectors.kafkajson.source.meta.events.KafkaJsonStreamSplitMetaEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.List;
import java.util.function.Supplier;

/**
 * The Kafka-json source reader. Extends {@link IncrementalSourceReaderWithCommit} so that the
 * checkpoint-complete callback (invoked by Flink on the task thread) is forwarded to {@link
 * org.apache.flink.cdc.connectors.kafkajson.source.dialect.KafkaJsonDialect#notifyCheckpointComplete},
 * which in turn commits the consumed Kafka offsets to the consumer group (performed on the fetcher
 * thread by {@link
 * org.apache.flink.cdc.connectors.kafkajson.source.fetch.KafkaJsonStreamFetchTask#commitCurrentOffset}).
 *
 * <p>The released base {@code IncrementalSourceReader} does not override Flink's {@code
 * SourceReader.notifyCheckpointComplete} (an empty default), so without this reader the checkpoint
 * completion would never reach the offset-commit path. Only the postgres connector wires the commit
 * variant in upstream; the Kafka-json source needs it for the same reason.
 *
 * <p>Connector-side port of FLINK-40697: when a stream split arrives complete (restored from a
 * checkpoint, assembled from the meta-group exchange, or small enough to carry its metadata
 * inline), this reader reports it to the coordinator via {@link
 * KafkaJsonStreamSplitMetaAssembledEvent}, so the coordinator can release the snapshot-split
 * metadata from memory once the assignment is checkpoint-covered.
 */
public class KafkaJsonSourceReader extends IncrementalSourceReaderWithCommit {

    /**
     * The stream-split assignment generation served by the coordinator, echoed back in the
     * assembled report so the coordinator can reject a stale report from a failed-over assignment.
     * Defaults to {@code COMPLETE_WITHOUT_META_GENERATION}, which the coordinator always accepts.
     * That default still applies when a reader never requests meta groups, which covers a split
     * restored already assembled and one that carries its metadata inline.
     */
    private int streamSplitMetaAssignmentGeneration =
            KafkaJsonStreamSplitMetaAssembledEvent.COMPLETE_WITHOUT_META_GENERATION;

    public KafkaJsonSourceReader(
            FutureCompletingBlockingQueue elementQueue,
            Supplier supplier,
            RecordEmitter recordEmitter,
            Configuration config,
            IncrementalSourceReaderContext incrementalSourceReaderContext,
            SourceConfig sourceConfig,
            SourceSplitSerializer sourceSplitSerializer,
            DataSourceDialect dialect) {
        super(
                elementQueue,
                supplier,
                recordEmitter,
                config,
                incrementalSourceReaderContext,
                sourceConfig,
                sourceSplitSerializer,
                dialect);
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof KafkaJsonStreamSplitMetaEvent) {
            // remember the assignment generation to echo it back when the split is fully
            // assembled
            this.streamSplitMetaAssignmentGeneration =
                    ((KafkaJsonStreamSplitMetaEvent) sourceEvent).getAssignmentGeneration();
        }
        super.handleSourceEvents(sourceEvent);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void addSplits(List splits) {
        super.addSplits(splits);
        // FLINK-40697: a stream split that arrives through addSplits is always complete —
        // either restored from a checkpoint already assembled, handed back after the
        // newly-added-table process, or the base reader just finished assembling it from the
        // meta-group exchange (requestStreamSplitMetaIfNeeded calls this.addSplits when the
        // split becomes complete). Report it so the coordinator can arm the release.
        for (Object split : splits) {
            SourceSplitBase sourceSplit = (SourceSplitBase) split;
            if (sourceSplit.isStreamSplit()) {
                StreamSplit streamSplit = sourceSplit.asStreamSplit();
                if (streamSplit.isCompletedSplit()) {
                    context.sendSourceEventToCoordinator(
                            new KafkaJsonStreamSplitMetaAssembledEvent(
                                    streamSplit.splitId(),
                                    streamSplit.getTotalFinishedSplitSize(),
                                    streamSplitMetaAssignmentGeneration));
                }
            }
        }
    }
}
