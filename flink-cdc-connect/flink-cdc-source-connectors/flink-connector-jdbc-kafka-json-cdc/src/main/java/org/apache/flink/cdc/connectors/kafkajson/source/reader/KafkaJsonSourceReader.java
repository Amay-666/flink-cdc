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

import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderContext;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderWithCommit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

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
 */
public class KafkaJsonSourceReader extends IncrementalSourceReaderWithCommit {

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
}
