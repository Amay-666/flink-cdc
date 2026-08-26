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

package org.apache.flink.cdc.connectors.kafkajson.source.ddl;

import javax.annotation.Nullable;

/**
 * Information about a column-level change within an {@code ALTER TABLE} statement.
 *
 * <p>Captures which column changed, what kind of change occurred (type, comment, default, position),
 * and the old/new values where applicable. A single {@code MODIFY COLUMN} or {@code CHANGE COLUMN}
 * statement may produce multiple {@link ColumnChangeInfo} entries if it touches more than one aspect
 * of the column.
 */
public class ColumnChangeInfo {

    /** The name of the affected column. */
    private final String columnName;

    /** The specific type of change. */
    private final KafkaJsonTableChangeType changeType;

    /** The old value before the change, or {@code null} if not applicable. */
    @Nullable private final String oldValue;

    /** The new value after the change, or {@code null} if not applicable. */
    @Nullable private final String newValue;

    public ColumnChangeInfo(
            String columnName,
            KafkaJsonTableChangeType changeType,
            @Nullable String oldValue,
            @Nullable String newValue) {
        this.columnName = columnName;
        this.changeType = changeType;
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    /** Returns the name of the affected column. */
    public String getColumnName() {
        return columnName;
    }

    /** Returns the specific type of change. */
    public KafkaJsonTableChangeType getChangeType() {
        return changeType;
    }

    /** Returns the old value before the change, or {@code null} if not applicable. */
    @Nullable
    public String getOldValue() {
        return oldValue;
    }

    /** Returns the new value after the change, or {@code null} if not applicable. */
    @Nullable
    public String getNewValue() {
        return newValue;
    }

    @Override
    public String toString() {
        return "ColumnChangeInfo{"
                + "columnName='"
                + columnName
                + '\''
                + ", changeType="
                + changeType
                + ", oldValue="
                + oldValue
                + ", newValue="
                + newValue
                + '}';
    }
}
