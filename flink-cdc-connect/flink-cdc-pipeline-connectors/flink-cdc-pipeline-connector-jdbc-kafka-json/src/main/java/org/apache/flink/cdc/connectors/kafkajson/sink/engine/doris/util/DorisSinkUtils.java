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

package org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.util;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Start-up helpers shared by the Doris sink writers. The drop-before-create option is intentionally
 * a fresh-job convenience, so a restored job turns it off before its writers open any table.
 */
public final class DorisSinkUtils {
    private DorisSinkUtils() {}

    private static final Logger LOG = LoggerFactory.getLogger(DorisSinkUtils.class);

    /**
     * Disables "DROP TABLE BEFORE CREATE" when the job is being restored from checkpoint /
     * savepoint. Prevents restarting the job from wiping the target table's data when the option is
     * configured as {@code true}.
     */
    public static void maybeDisableDropOnRestore(
            DorisDataSinkOptions options, Sink.InitContext context) {
        if (context.getRestoredCheckpointId().isPresent() && options.isDropTableBeforeCreate()) {
            options.getConfig().set(DorisDataSinkOptions.IS_DROP_TABLE_BEFORE_CREATE, false);
            LOG.info(
                    "Doris sink restored from checkpoint {}, disabling drop-before-create "
                            + "to avoid data loss on restart. Target tables will be created "
                            + "with IF NOT EXISTS (idempotent).",
                    context.getRestoredCheckpointId().getAsLong());
        }
    }
}
