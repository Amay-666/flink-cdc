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

package org.apache.flink.cdc.connectors.kafkajson.event;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;

import java.util.Map;
import java.util.Objects;

/**
 * A schema change event announcing that the comment of one or more columns changed.
 *
 * <p>The released flink-cdc runtime has no {@code ALTER_COLUMN_COMMENT} event type, so a column
 * comment change would otherwise be indistinguishable from a plain {@code ALTER_TABLE}. This event
 * carries the per-column new comments so a downstream can react to the comment change explicitly; a
 * {@code null} value means the column's comment was removed.
 */
@PublicEvolving
public class AlterColumnCommentEvent implements SchemaChangeEvent {
    private static final long serialVersionUID = 1L;

    private final TableId tableId;

    /**
     * key => column name, value => new column comment ({@code null} when the comment was removed).
     */
    private final Map<String, String> commentMapping;

    public AlterColumnCommentEvent(TableId tableId, Map<String, String> commentMapping) {
        this.tableId = tableId;
        this.commentMapping = commentMapping;
    }

    /** Returns the per-column new comment mapping. */
    public Map<String, String> getCommentMapping() {
        return commentMapping;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AlterColumnCommentEvent)) {
            return false;
        }
        AlterColumnCommentEvent that = (AlterColumnCommentEvent) o;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(commentMapping, that.commentMapping);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, commentMapping);
    }

    @Override
    public String toString() {
        return "AlterColumnCommentEvent{"
                + "tableId="
                + tableId
                + ", commentMapping="
                + commentMapping
                + '}';
    }

    @Override
    public TableId tableId() {
        return tableId;
    }

    @Override
    public SchemaChangeEventType getType() {
        throw new UnsupportedOperationException(
                "AlterColumnCommentEvent is not supported by released flink-cdc runtime.");
    }
}
