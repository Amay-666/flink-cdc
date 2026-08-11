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

package org.apache.flink.cdc.connectors.canal.source.offset;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Map;

/** The factory to create {@link CanalOffset}. */
public class CanalOffsetFactory extends OffsetFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public Offset newOffset(Map<String, String> offset) {
        return new CanalOffset(offset);
    }

    @Override
    public Offset newOffset(String filename, Long position) {
        throw new FlinkRuntimeException(
                "not supported create new CanalOffset by Filename and Long position.");
    }

    @Override
    public Offset newOffset(Long position) {
        throw new FlinkRuntimeException("not supported create new CanalOffset by Long position.");
    }

    @Override
    public Offset createTimestampOffset(long timestampMillis) {
        return new CanalOffset(timestampMillis, -1, -1L);
    }

    @Override
    public Offset createInitialOffset() {
        return CanalOffset.INITIAL_OFFSET;
    }

    @Override
    public Offset createNoStoppingOffset() {
        return CanalOffset.NO_STOPPING_OFFSET;
    }
}
