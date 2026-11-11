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

package org.apache.flink.cdc.connectors.kafkajson.sink.schema.coordinator;

/**
 * A {@link org.apache.flink.cdc.common.sink.MetadataApplier} that is told when the schema
 * coordinator restores from a checkpoint.
 *
 * <p>The coordinator calls {@link #onRestore()} before it replays any schema change, so an applier
 * can switch to a restore-safe behaviour (for example, not dropping and recreating a table whose
 * {@code CREATE TABLE} event is replayed).
 */
public interface RestoreAwareMetadataApplier {

    /** Called once when the schema coordinator restores from a checkpoint. */
    void onRestore();
}
