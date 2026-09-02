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

package org.apache.flink.cdc.connectors.kafkajson.source.dialect;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceConfig;
import org.apache.flink.cdc.connectors.kafkajson.source.config.KafkaJsonSourceOptions.EventTime;
import org.apache.flink.cdc.connectors.kafkajson.source.offset.KafkaJsonOffset;
import org.apache.flink.cdc.connectors.kafkajson.source.utils.KafkaJsonTidbOffsetUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.function.Supplier;

/**
 * The dialect for the TiDB source.
 *
 * <p>TiDB speaks the MySQL wire protocol, so the snapshot read (the JDBC incremental snapshot of
 * flink-cdc-base) and the shared schema/discovery logic are inherited from the MySQL {@link
 * KafkaJsonDialect}. The single TiDB-specific behavior is {@link #displayCurrentOffset}: the
 * snapshot low/high watermark is the current TSO queried from the database (an authoritative
 * commit-clock upper bound) instead of the Kafka-sampled value, for the commit-clock event-time
 * modes ({@code es}, {@code tidb_tso}). A TiCDC watermark event is consumed like any other message:
 * it carries its TSO in {@code _tidb.watermarkTs} (see {@code
 * CanalMessage.TidbInfo#getCommitTimeStamp}), which keeps the offset advancing during quiet periods
 * and lets the bounded backfill cross its ending offset.
 *
 * <p>Selected by {@code scan.database.type=tidb} through {@link KafkaJsonDialectFactory}, as a peer
 * of the MySQL dialect (see docs/ARCHITECTURE.md).
 */
public class KafkaJsonTiDBDialect extends KafkaJsonDialect {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonTiDBDialect.class);

    @Nullable private transient Supplier<KafkaJsonOffset> tidbOffsetSupplier;

    public KafkaJsonTiDBDialect(KafkaJsonSourceConfig sourceConfig) {
        super(sourceConfig);
    }

    @Override
    public String getName() {
        return "TiDB";
    }

    @Override
    public Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {
        KafkaJsonSourceConfig canalSourceConfig = (KafkaJsonSourceConfig) sourceConfig;
        EventTime eventTime = canalSourceConfig.getEventTime();
        // TSO is an authoritative commit-clock position (an upper bound on the `es` of every change
        // already visible to the JDBC read), whereas the Kafka-sampled boundary trails the database
        // by the publish lag and is empty before the first change is published. It is a valid
        // boundary for every commit-clock mode (`es`, and `tidb_tso` which degrades to `es`); with
        // `ts` (producer send time) the boundary stays on the Kafka-sampled value so the two clock
        // domains are never mixed.
        if (eventTime == EventTime.ES || eventTime == EventTime.TIDB_TSO) {
            KafkaJsonOffset tidbOffset =
                    tidbOffsetSupplier != null
                            ? tidbOffsetSupplier.get()
                            : KafkaJsonTidbOffsetUtils.queryCurrentOffset(canalSourceConfig);
            if (tidbOffset != null) {
                return tidbOffset;
            }
            LOG.warn(
                    "TiDB current TSO boundary is unavailable; falling back to the Kafka-sampled"
                            + " boundary");
        }
        return super.displayCurrentOffset(sourceConfig);
    }

    /** Injects a supplier of the current TiDB TSO boundary (used in unit tests). */
    @VisibleForTesting
    public void setTidbOffsetSupplierForTesting(Supplier<KafkaJsonOffset> supplier) {
        this.tidbOffsetSupplier = supplier;
    }
}
