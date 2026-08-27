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

package org.apache.flink.cdc.connectors.kafkajson.source;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfigFactory;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderContext;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceSplitReader;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.reader.KafkaJsonSourceReader;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.function.Supplier;

/**
 * The Kafka-json source based on the incremental snapshot framework (FLIP-27 + watermark signal
 * algorithm). It reads the table snapshot through JDBC chunking and consumes the incremental data
 * changes (plus DDL) from Kafka messages produced by external tools (canal, in flatMessage JSON, or
 * a Debezium / TiCDC connector).
 *
 * <p>This class is the skeleton entry point. The {@code createEnumerator} / {@code
 * restoreEnumerator} wiring is inherited from the base framework; {@code createReader} is overridden
 * only to build the {@link KafkaJsonSourceReader}, whose {@code
 * IncrementalSourceReaderWithCommit} base forwards the checkpoint-complete callback to the dialect
 * so the consumed Kafka offsets can be committed to the consumer group (see the reader's javadoc).
 */
@Experimental
public class KafkaJsonSource<T> extends JdbcIncrementalSource<T> {

    public KafkaJsonSource(
            JdbcSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema,
            OffsetFactory offsetFactory,
            JdbcDataSourceDialect dataSourceDialect) {
        super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
    }

    @Override
    public KafkaJsonSourceReader createReader(SourceReaderContext readerContext) throws Exception {
        // create source config for the given subtask (e.g. unique server id)
        KafkaJsonSourceConfig sourceConfig =
                (KafkaJsonSourceConfig) configFactory.create(readerContext.getIndexOfSubtask());
        FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecords>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        final SourceReaderMetrics sourceReaderMetrics =
                new SourceReaderMetrics(readerContext.metricGroup());

        sourceReaderMetrics.registerMetrics();
        IncrementalSourceReaderContext incrementalSourceReaderContext =
                new IncrementalSourceReaderContext(readerContext);
        Supplier<IncrementalSourceSplitReader<JdbcSourceConfig>> splitReaderSupplier =
                () ->
                        new IncrementalSourceSplitReader<>(
                                readerContext.getIndexOfSubtask(),
                                dataSourceDialect,
                                sourceConfig,
                                incrementalSourceReaderContext,
                                snapshotHooks);
        return new KafkaJsonSourceReader(
                elementsQueue,
                splitReaderSupplier,
                createRecordEmitter(sourceConfig, sourceReaderMetrics),
                readerContext.getConfiguration(),
                incrementalSourceReaderContext,
                sourceConfig,
                sourceSplitSerializer,
                dataSourceDialect);
    }
}
