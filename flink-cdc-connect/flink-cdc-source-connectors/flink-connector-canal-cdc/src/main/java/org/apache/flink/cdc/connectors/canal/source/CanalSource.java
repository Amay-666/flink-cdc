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

package org.apache.flink.cdc.connectors.canal.source;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfigFactory;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;

/**
 * The Canal source based on the incremental snapshot framework (FLIP-27 + watermark signal
 * algorithm). It reads the table snapshot through JDBC chunking and consumes the incremental data
 * changes (plus DDL) from Kafka messages produced by external tools (canal, in flatMessage JSON).
 *
 * <p>This class is the skeleton entry point. The {@code createEnumerator} / {@code
 * restoreEnumerator} / {@code createReader} wiring will be added in later phases (see
 * CANAL_KAFKA_CDC_PLAN.md Phase 7).
 */
@Experimental
public class CanalSource<T> extends JdbcIncrementalSource<T> {

    public CanalSource(
            JdbcSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema,
            OffsetFactory offsetFactory,
            JdbcDataSourceDialect dataSourceDialect) {
        super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
    }
}
