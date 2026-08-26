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

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffsetFactory;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;

import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The source builder for {@link KafkaJsonSource}. */
@Experimental
public class KafkaJsonSourceBuilder<T> {

    private final KafkaJsonSourceConfigFactory configFactory = new KafkaJsonSourceConfigFactory();
    private DebeziumDeserializationSchema<T> deserializer;

    private KafkaJsonSourceBuilder() {}

    /** The hostname of the MySQL database to monitor for changes. */
    public KafkaJsonSourceBuilder<T> hostname(String hostname) {
        this.configFactory.hostname(hostname);
        return this;
    }

    /** Integer port number of the MySQL database server. */
    public KafkaJsonSourceBuilder<T> port(int port) {
        this.configFactory.port(port);
        return this;
    }

    /** The name of the MySQL database from which to read the snapshot. */
    public KafkaJsonSourceBuilder<T> username(String username) {
        this.configFactory.username(username);
        return this;
    }

    /** Password to use when connecting to the MySQL database server. */
    public KafkaJsonSourceBuilder<T> password(String password) {
        this.configFactory.password(password);
        return this;
    }

    /**
     * The session time zone in the MySQL database server, e.g. "UTC". It controls how the TIMESTAMP
     * type in MySQL is converted to a string/instant.
     */
    public KafkaJsonSourceBuilder<T> serverTimeZone(String timeZone) {
        this.configFactory.serverTimeZone(timeZone);
        return this;
    }

    /** An optional list of regular expressions that match database names to be monitored. */
    public KafkaJsonSourceBuilder<T> databaseList(String... databaseList) {
        this.configFactory.databaseList(databaseList);
        return this;
    }

    /** An optional list of regular expressions that match table identifiers to be monitored. */
    public KafkaJsonSourceBuilder<T> tableList(String... tableList) {
        this.configFactory.tableList(tableList);
        return this;
    }

    /** The split size (number of rows) of table snapshot. */
    public KafkaJsonSourceBuilder<T> splitSize(int splitSize) {
        this.configFactory.splitSize(splitSize);
        return this;
    }

    /** The group size of split meta. */
    public KafkaJsonSourceBuilder<T> splitMetaGroupSize(int splitMetaGroupSize) {
        this.configFactory.splitMetaGroupSize(splitMetaGroupSize);
        return this;
    }

    /** The maximum fetch size for per poll when reading table snapshot. */
    public KafkaJsonSourceBuilder<T> fetchSize(int fetchSize) {
        this.configFactory.fetchSize(fetchSize);
        return this;
    }

    /** The chunk key column of table snapshot. */
    public KafkaJsonSourceBuilder<T> chunkKeyColumn(String chunkKeyColumn) {
        this.configFactory.chunkKeyColumn(chunkKeyColumn);
        return this;
    }

    /** Specifies the startup options. */
    public KafkaJsonSourceBuilder<T> startupOptions(StartupOptions startupOptions) {
        this.configFactory.startupOptions(startupOptions);
        return this;
    }

    /** Whether to skip backfill in snapshot reading phase. */
    public KafkaJsonSourceBuilder<T> skipSnapshotBackfill(boolean skipSnapshotBackfill) {
        this.configFactory.skipSnapshotBackfill(skipSnapshotBackfill);
        return this;
    }

    /** Whether the source should scan the newly added tables or not. */
    public KafkaJsonSourceBuilder<T> scanNewlyAddedTableEnabled(
            boolean scanNewlyAddedTableEnabled) {
        this.configFactory.scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled);
        return this;
    }

    /** Whether to close idle readers at the end of the snapshot phase. */
    public KafkaJsonSourceBuilder<T> closeIdleReaders(boolean closeIdleReaders) {
        this.configFactory.closeIdleReaders(closeIdleReaders);
        return this;
    }

    /** The connection pool size. */
    public KafkaJsonSourceBuilder<T> connectionPoolSize(int connectionPoolSize) {
        this.configFactory.connectionPoolSize(connectionPoolSize);
        return this;
    }

    /** The maximum time that the connector should wait to connect to the MySQL server. */
    public KafkaJsonSourceBuilder<T> connectTimeout(Duration connectTimeout) {
        this.configFactory.connectTimeout(connectTimeout);
        return this;
    }

    /** The max retry times to get connection. */
    public KafkaJsonSourceBuilder<T> connectMaxRetries(int connectMaxRetries) {
        this.configFactory.connectMaxRetries(connectMaxRetries);
        return this;
    }

    /** The Kafka bootstrap servers that external tools (canal) write the messages to. */
    public KafkaJsonSourceBuilder<T> kafkaBootstrapServers(String bootstrapServers) {
        this.configFactory.kafkaBootstrapServers(bootstrapServers);
        return this;
    }

    /** The Kafka consumer group id. */
    public KafkaJsonSourceBuilder<T> kafkaGroupId(String groupId) {
        this.configFactory.kafkaGroupId(groupId);
        return this;
    }

    /** The Kafka topics that carry the incremental messages. */
    public KafkaJsonSourceBuilder<T> kafkaTopics(String... topics) {
        this.configFactory.kafkaTopics(topics);
        return this;
    }

    /** The message format of the Kafka messages. */
    public KafkaJsonSourceBuilder<T> messageFormat(
            KafkaJsonSourceOptions.MessageFormat messageFormat) {
        this.configFactory.messageFormat(messageFormat);
        return this;
    }

    /** The database type of the source. */
    public KafkaJsonSourceBuilder<T> databaseType(
            KafkaJsonSourceOptions.DatabaseType databaseType) {
        this.configFactory.databaseType(databaseType);
        return this;
    }

    /** The timestamp field used as the offset event time. */
    public KafkaJsonSourceBuilder<T> eventTime(KafkaJsonSourceOptions.EventTime eventTime) {
        this.configFactory.eventTime(eventTime);
        return this;
    }

    /** The boundary handling mode of the full->incremental switch. */
    public KafkaJsonSourceBuilder<T> boundaryMode(
            KafkaJsonSourceOptions.BoundaryMode boundaryMode) {
        this.configFactory.boundaryMode(boundaryMode);
        return this;
    }

    /** The Kafka consumer startup mode for stream-only scenarios. */
    public KafkaJsonSourceBuilder<T> kafkaStartupMode(
            KafkaJsonSourceOptions.KafkaStartupMode kafkaStartupMode) {
        this.configFactory.kafkaStartupMode(kafkaStartupMode);
        return this;
    }

    /** The DDL parser implementation. */
    public KafkaJsonSourceBuilder<T> ddlParser(KafkaJsonSourceOptions.DdlParser ddlParser) {
        this.configFactory.ddlParser(ddlParser);
        return this;
    }

    /** Arbitrary Kafka consumer properties. */
    public KafkaJsonSourceBuilder<T> kafkaProperties(Properties kafkaProperties) {
        this.configFactory.kafkaProperties(kafkaProperties);
        return this;
    }

    /** The Debezium connector properties (e.g. from the {@code debezium.} SQL option prefix). */
    public KafkaJsonSourceBuilder<T> debeziumProperties(Properties debeziumProperties) {
        this.configFactory.debeziumProperties(debeziumProperties);
        return this;
    }

    /** The deserializer used to convert from consumed {@code SourceRecord}. */
    public KafkaJsonSourceBuilder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    /**
     * Build the {@link KafkaJsonSource}.
     *
     * <p>Wires the {@link KafkaJsonOffsetFactory} and the {@link KafkaJsonDialect} into the
     * incremental snapshot framework of flink-cdc-base. The base framework drives the whole
     * pipeline: the enumerator discovers the tables and assigns the splits, and the reader executes
     * each split through the dialect-created fetch tasks (see {@code IncrementalSource}).
     *
     * @return a KafkaJsonSource with the settings made for this builder.
     */
    public KafkaJsonSource<T> build() {
        KafkaJsonOffsetFactory offsetFactory = new KafkaJsonOffsetFactory();
        KafkaJsonDialect dialect = new KafkaJsonDialect(configFactory.create(0));
        return new KafkaJsonSource<>(
                configFactory, checkNotNull(deserializer), offsetFactory, dialect);
    }

    public static <T> KafkaJsonSourceBuilder<T> builder() {
        return new KafkaJsonSourceBuilder<>();
    }
}
