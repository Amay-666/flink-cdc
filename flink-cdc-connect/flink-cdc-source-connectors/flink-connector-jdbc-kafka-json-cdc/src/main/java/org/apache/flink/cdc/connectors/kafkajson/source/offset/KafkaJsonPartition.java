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

package org.apache.flink.cdc.connectors.kafkajson.source.offset;

import io.debezium.pipeline.spi.Partition;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The partition of the Canal source, mirroring {@code MySqlPartition}: the {@code source} partition
 * of the emitted records is {@code {"server": <logical name>}}.
 */
public class KafkaJsonPartition implements Partition, Serializable {

    private static final long serialVersionUID = 1L;

    public static final String SERVER_PARTITION_KEY = "server";

    private final String serverName;
    private final Map<String, String> sourcePartition;

    public KafkaJsonPartition(String serverName) {
        this.serverName = serverName;
        Map<String, String> partition = new HashMap<>();
        partition.put(SERVER_PARTITION_KEY, serverName);
        this.sourcePartition = Collections.unmodifiableMap(partition);
    }

    public String getServerName() {
        return serverName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return sourcePartition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaJsonPartition)) {
            return false;
        }
        return sourcePartition.equals(((KafkaJsonPartition) o).sourcePartition);
    }

    @Override
    public int hashCode() {
        return sourcePartition.hashCode();
    }

    @Override
    public String toString() {
        return "KafkaJsonPartition{server='" + serverName + "'}";
    }
}
