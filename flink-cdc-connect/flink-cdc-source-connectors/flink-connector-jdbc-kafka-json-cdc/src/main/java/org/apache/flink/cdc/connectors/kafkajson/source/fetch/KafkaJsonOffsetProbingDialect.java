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

package org.apache.flink.cdc.connectors.kafkajson.source.fetch;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions;
import org.apache.flink.cdc.connectors.kafkajson.source.dialect.KafkaJsonDialect;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A per-reader view of the shared {@link KafkaJsonDialect} that answers the snapshot phase's
 * low/high watermark probes ({@link #displayCurrentOffset}) through this reader's reusable
 * {@code KafkaJsonOffsetSupplier} instead of the throw-away {@code KafkaConsumer} that
 * {@code KafkaJsonKafkaOffsetUtils#queryCurrentOffset} opens per probe.
 *
 * <p>The base scan fetch task ({@code AbstractScanFetchTask#execute}) asks the {@code
 * FetchTask.Context} for its dialect and calls {@code displayCurrentOffset} once per split
 * (twice when the backfill is enabled). The context hands out this view so that every probe of a
 * reader lands on the reader's own, single-threaded consumer: the dialect itself is one shared
 * instance per source (it also serves the enumerator), so a per-reader supplier could not be
 * stored on it — readers of a parallelism &gt; 1 job would overwrite each other's field and funnel
 * their probes through one {@code KafkaConsumer} from several fetch threads, which is not
 * thread-safe.
 *
 * <p>Only the Kafka-sampled boundary benefits from the reuse: a {@code tidb} source queries the
 * current TSO on the database instead, so those calls (and any test seam installed on the
 * delegate) keep going through the delegate. Everything else delegates to the shared dialect
 * unchanged.
 */
public class KafkaJsonOffsetProbingDialect implements JdbcDataSourceDialect {

    private final KafkaJsonDialect delegate;

    /**
     * Supplies the current stream position on the reader's fetch thread; in production this is
     * the owning context's {@code getOffsetSupplier().current()}.
     */
    private final Supplier<KafkaJsonOffset> probe;

    public KafkaJsonOffsetProbingDialect(
            KafkaJsonDialect delegate, Supplier<KafkaJsonOffset> probe) {
        this.delegate = checkNotNull(delegate);
        this.probe = checkNotNull(probe);
    }

    @Override
    public Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {
        // Reuse the reader's probe consumer only for the Kafka-sampled boundary: the TiDB TSO
        // path queries the database (no consumer to reuse), and the delegate keeps its own test
        // seams working.
        if (((KafkaJsonSourceConfig) sourceConfig).getDatabaseType()
                == KafkaJsonSourceOptions.DatabaseType.MYSQL) {
            return probe.get();
        }
        return delegate.displayCurrentOffset(sourceConfig);
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        return delegate.discoverDataCollections(sourceConfig);
    }

    @Override
    public Map<TableId, TableChange> discoverDataCollectionSchemas(
            JdbcSourceConfig sourceConfig) {
        return delegate.discoverDataCollectionSchemas(sourceConfig);
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        return delegate.isDataCollectionIdCaseSensitive(sourceConfig);
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return delegate.createChunkSplitter(sourceConfig);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        return delegate.createFetchTask(sourceSplitBase);
    }

    @Override
    public FetchTask.Context createFetchTaskContext(JdbcSourceConfig sourceConfig) {
        return delegate.createFetchTaskContext(sourceConfig);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId, Offset offset) throws Exception {
        delegate.notifyCheckpointComplete(checkpointId, offset);
    }

    @Override
    public boolean isIncludeDataCollection(
            JdbcSourceConfig sourceConfig, TableId tableId) {
        return delegate.isIncludeDataCollection(sourceConfig, tableId);
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        return delegate.openJdbcConnection(sourceConfig);
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return delegate.getPooledDataSourceFactory();
    }

    @Override
    public TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        return delegate.queryTableSchema(jdbc, tableId);
    }
}
