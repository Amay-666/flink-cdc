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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfigFactory;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

/** A {@link DataSource} for canal cdc connector. */
@Internal
public class KafkaJsonDataSource implements DataSource {

    private final KafkaJsonSourceConfigFactory configFactory;
    private final KafkaJsonSourceConfig sourceConfig;

    public KafkaJsonDataSource(KafkaJsonSourceConfigFactory configFactory) {
        this.configFactory = configFactory;
        this.sourceConfig = configFactory.create(0);
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        KafkaJsonEventDeserializer deserializer =
                new KafkaJsonEventDeserializer(
                        DebeziumChangelogMode.ALL, sourceConfig.isIncludeSchemaChanges());

        KafkaJsonEventSource source = new KafkaJsonEventSource(configFactory, deserializer);

        return FlinkSourceProvider.of(source);
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new KafkaJsonMetadataAccessor(sourceConfig);
    }

    @VisibleForTesting
    public KafkaJsonSourceConfig getSourceConfig() {
        return sourceConfig;
    }
}
