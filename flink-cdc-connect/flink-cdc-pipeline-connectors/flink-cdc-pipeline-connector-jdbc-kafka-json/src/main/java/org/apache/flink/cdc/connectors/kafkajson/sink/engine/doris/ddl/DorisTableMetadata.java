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

package org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.ddl;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * A snapshot of one Doris table's columns, as returned by {@code SHOW FULL COLUMNS}.
 *
 * <p>Used by {@link DorisMetadataApplier} to decide whether a schema-change DDL was already applied
 * before a checkpoint restore replayed it. Column lookup is case-insensitive: Doris folds
 * identifiers for lookup in most deployments, and a source column that differs only in case should
 * be recognised as the same column rather than trigger a duplicate {@code ADD COLUMN}.
 */
public final class DorisTableMetadata implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final DorisTableMetadata ABSENT =
            new DorisTableMetadata(false, Collections.emptyMap());

    private final boolean tableExists;
    private final Map<String, ColumnMetadata> columns;
    /** Lower-cased column name → column, for case-insensitive lookup. */
    private final Map<String, ColumnMetadata> columnsByName;

    private DorisTableMetadata(boolean tableExists, Map<String, ColumnMetadata> columns) {
        this.tableExists = tableExists;
        this.columns = Collections.unmodifiableMap(new LinkedHashMap<>(columns));
        Map<String, ColumnMetadata> byName = new LinkedHashMap<>(columns.size());
        for (ColumnMetadata column : columns.values()) {
            byName.put(column.getName().toLowerCase(Locale.ROOT), column);
        }
        this.columnsByName = Collections.unmodifiableMap(byName);
    }

    /** A table that does not exist (or the {@code information_schema} view has no row for it). */
    public static DorisTableMetadata absent() {
        return ABSENT;
    }

    public static DorisTableMetadata present(Map<String, ColumnMetadata> columns) {
        return new DorisTableMetadata(true, columns);
    }

    public boolean tableExists() {
        return tableExists;
    }

    public boolean hasColumn(String name) {
        return name != null && columnsByName.containsKey(name.toLowerCase(Locale.ROOT));
    }

    @Nullable
    public ColumnMetadata column(String name) {
        return name == null ? null : columnsByName.get(name.toLowerCase(Locale.ROOT));
    }

    public boolean isKeyColumn(String name) {
        ColumnMetadata column = column(name);
        return column != null && column.isKey();
    }

    /**
     * The current type of a column, normalised for comparison with the type strings the DDL builder
     * emits (see {@link DorisMetadataInspector#normalizeType(String)}).
     */
    public Optional<String> normalizedType(String name) {
        ColumnMetadata column = column(name);
        return column == null
                ? Optional.empty()
                : Optional.of(DorisMetadataInspector.normalizeType(column.getType()));
    }

    /** One column of the table. */
    public static final class ColumnMetadata implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String name;
        private final String type;
        private final boolean key;
        @Nullable private final String comment;

        public ColumnMetadata(String name, String type, boolean key, @Nullable String comment) {
            this.name = name;
            this.type = type;
            this.key = key;
            this.comment = comment;
        }

        public String getName() {
            return name;
        }

        /** The raw Doris type string, e.g. {@code VARCHAR(900)}. */
        public String getType() {
            return type;
        }

        public boolean isKey() {
            return key;
        }

        @Nullable
        public String getComment() {
            return comment;
        }

        @Override
        public String toString() {
            return name + " " + type + (key ? " KEY" : "");
        }
    }

    @Override
    public String toString() {
        return "DorisTableMetadata{"
                + "tableExists="
                + tableExists
                + ", columns="
                + columns.values()
                + '}';
    }
}
