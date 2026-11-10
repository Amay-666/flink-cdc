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

package org.apache.flink.cdc.connectors.kafkajson.sink.schema.alert;

import org.apache.flink.cdc.common.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * User extension point for delivering {@link SchemaEvolutionAlert}s to an alerting system.
 *
 * <p>Configure an implementation with its fully-qualified class name through {@code
 * sink.schema-change.alert-handler} (see {@code DorisDataSinkOptions}). The class needs a public
 * no-argument constructor; the applier instantiates it on the JobManager, on the schema-evolution
 * coordinator thread. No alert is delivered unless a non-idempotent schema change was skipped, so
 * this handler does not see routine schema evolution.
 *
 * <p>The handler is called synchronously. An implementation should return quickly (or hand the
 * alert off to an internal queue/thread) and must not throw for a transient failure of the alerting
 * backend: the applier logs and swallows handler failures so that alert delivery can never fail the
 * data pipeline. Alerts are delivered at least once — a schema-change round that does not commit to
 * a checkpoint is replayed after a failover — so a handler that needs exactly-once delivery should
 * deduplicate on {@link SchemaEvolutionAlert#getAlertId()}.
 */
@PublicEvolving
@FunctionalInterface
public interface SchemaEvolutionAlertHandler extends Serializable {

    /**
     * Called when the applier deliberately skipped a schema change that the target engine would
     * have rejected. Implementations should not throw; the applier catches and logs failures.
     */
    void onAlert(SchemaEvolutionAlert alert) throws Exception;
}
