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

package org.apache.flink.cdc.connectors.kafkajson.source.assigner;

import org.apache.flink.cdc.connectors.base.source.assigner.SplitAssigner;

/**
 * The assigner contract of the jdbc-kafka-json source: the base {@link SplitAssigner} plus the
 * snapshot-metadata release of FLINK-40697 (ported connector-side, see {@code
 * KafkaJsonSnapshotSplitAssigner#releaseSnapshotMetadata()}).
 *
 * <p>This lives on a kafkajson interface because the base one cannot be extended without modifying
 * the pristine flink-cdc-base; only the hybrid and snapshot assigners of this connector implement
 * it. A stream-only {@code StreamSplitAssigner} holds no snapshot metadata, so the release paths
 * guard with an instanceof check instead.
 */
public interface KafkaJsonSplitAssigner extends SplitAssigner {

    /**
     * Releases the bulk snapshot-split metadata once the reader has assembled it and a checkpoint
     * has covered that assignment. Generalizes the MySQL mechanism (FLINK-39775) per FLINK-40697.
     */
    void releaseSnapshotMetadata();

    /** Whether the bulk snapshot split metadata has been released (FLINK-40697). */
    boolean isSnapshotMetaReleased();
}
