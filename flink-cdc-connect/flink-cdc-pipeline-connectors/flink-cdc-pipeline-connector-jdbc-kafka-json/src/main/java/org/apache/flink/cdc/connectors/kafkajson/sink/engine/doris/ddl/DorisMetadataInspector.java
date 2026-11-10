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

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;
import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.http.DorisHttpClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Reads the current column layout of a Doris table over the FE HTTP query endpoint.
 *
 * <p>The schema-evolution applier runs on the JobManager and may replay a DDL after a checkpoint
 * restore: the schema registry state is restored to the last completed checkpoint, so every
 * schema-change round that ran after it is requested again. Applying the same {@code ADD COLUMN}
 * (or {@code DROP}/{@code RENAME}) a second time makes Doris reject the DDL and fails the job.
 * Before executing a schema change the applier therefore asks Doris what the table currently looks
 * like and skips the operations that are already reflected there.
 *
 * <p>The snapshot is derived from {@code SHOW FULL COLUMNS FROM db.table}. Its rows carry the
 * column name, the Doris type, the key marker and the comment; all four are needed by the applier
 * (duplicate detection, already-applied type comparison, primary-key detection and comment
 * preservation). A missing table is reported as {@link DorisTableMetadata#absent()} rather than an
 * error, because a {@code CREATE TABLE} replay legitimately finds the table before creating it. Any
 * other query failure is propagated: a network problem must not be mistaken for "the table does not
 * exist", which would cause the applier to re-run DDLs that already succeeded.
 */
public class DorisMetadataInspector implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DorisMetadataInspector.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /** Title of the position whose value marks a key column in {@code SHOW FULL COLUMNS}. */
    private static final String COLUMN_KEY = "Key";

    private final DorisDataSinkOptions options;

    public DorisMetadataInspector(DorisDataSinkOptions options) {
        this.options = options;
    }

    /**
     * Reads the current layout of the table the event refers to (after database/table mapping).
     *
     * @throws IOException if Doris answers with anything other than "the table does not exist";
     *     callers must not treat such a failure as an absent table
     */
    public DorisTableMetadata describe(DorisHttpClient client, TableId tableId) throws IOException {
        String database = options.mapDatabase(tableId);
        String table = options.mapTable(tableId);
        String sql =
                "SHOW FULL COLUMNS FROM "
                        + quoteIdentifier(database)
                        + "."
                        + quoteIdentifier(table);
        String responseBody;
        try {
            responseBody = client.executeQuery(database, sql);
        } catch (IOException e) {
            if (isMissingTable(e)) {
                LOG.debug(
                        "Doris table {}.{} does not exist yet ({}); schema-change pre-check treats"
                                + " it as absent.",
                        database,
                        table,
                        e.getMessage());
                return DorisTableMetadata.absent();
            }
            throw e;
        }
        return parse(database, table, responseBody);
    }

    private static DorisTableMetadata parse(String database, String table, String responseBody)
            throws IOException {
        JsonNode root = OBJECT_MAPPER.readTree(responseBody);
        JsonNode data = root.path("data");
        JsonNode rows = data.path("rows");
        if (!rows.isArray() || rows.isEmpty()) {
            return DorisTableMetadata.absent();
        }
        JsonNode title = data.path("title");
        int fieldIndex = indexOf(title, "Field", 0);
        int typeIndex = indexOf(title, "Type", 1);
        int keyIndex = indexOf(title, COLUMN_KEY, 4);
        int commentIndex = indexOf(title, "Comment", 8);

        Map<String, DorisTableMetadata.ColumnMetadata> columns = new LinkedHashMap<>();
        for (JsonNode row : rows) {
            String name = text(row, fieldIndex);
            if (name == null || name.isEmpty()) {
                continue;
            }
            String type = text(row, typeIndex);
            boolean key = isKeyMarker(text(row, keyIndex));
            String comment = text(row, commentIndex);
            columns.put(
                    name,
                    new DorisTableMetadata.ColumnMetadata(
                            name, type == null ? "" : type, key, comment));
        }
        LOG.debug(
                "Read Doris table {}.{} layout for schema-change pre-check: {}",
                database,
                table,
                columns.values());
        return DorisTableMetadata.present(columns);
    }

    /**
     * Returns the index of {@code title} in the response's title array, or {@code fallback}.
     *
     * <p>Doris's {@code SHOW FULL COLUMNS} title is {@code [Field, Type, Collation, Null, Key,
     * Default, Extra, Privileges, Comment]}; the fallback positions exist because tests and older
     * FE versions may omit the title array. Location by title is preferred so a column inserted
     * into that list above ours does not silently shift the parse.
     */
    private static int indexOf(JsonNode title, String name, int fallback) {
        if (title != null && title.isArray()) {
            for (int i = 0; i < title.size(); i++) {
                if (name.equalsIgnoreCase(title.get(i).asText())) {
                    return i;
                }
            }
        }
        return fallback;
    }

    private static boolean isKeyMarker(@Nullable String value) {
        if (value == null) {
            return false;
        }
        String normalised = value.trim().toLowerCase(Locale.ROOT);
        // Doris reports "true" in DESC/SHOW COLUMNS; MySQL-compatible clients may see PRI/UNI.
        return normalised.equals("true")
                || normalised.equals("yes")
                || normalised.equals("pri")
                || normalised.equals("uni");
    }

    @Nullable
    private static String text(JsonNode row, int index) {
        if (row == null || !row.isArray() || index < 0 || index >= row.size()) {
            return null;
        }
        JsonNode value = row.get(index);
        return value == null || value.isNull() ? null : value.asText();
    }

    /**
     * Normalises a Doris type (and a type string produced by {@link DorisDdlBuilder}) so the two
     * can be compared. Doris reports its own spelling through {@code SHOW FULL COLUMNS} — {@code
     * decimalv3(10, 2)}, lower case, sometimes with a display width — while the builder emits the
     * canonical DDL spelling ({@code DECIMAL(10, 2)}, {@code INT}, …).
     */
    public static String normalizeType(@Nullable String type) {
        if (type == null) {
            return "";
        }
        String normalised = type.trim().toLowerCase(Locale.ROOT).replaceAll("\\s+", "");
        // Doris 2.x exposes DECIMAL as decimalv3 in some metadata paths; the DDL builder emits
        // DECIMAL. Both name the same physical type.
        normalised = normalised.replace("decimalv3", "decimal");
        normalised = normalised.replace("integer", "int");
        // BOOLEAN is stored as TINYINT(1) and is reported that way by information_schema.
        if (normalised.equals("tinyint(1)")) {
            return "boolean";
        }
        // Integer display widths are cosmetic (INT(11)); a width on a string type is not.
        normalised =
                normalised.replaceAll("^(tinyint|smallint|mediumint|int|bigint)\\(\\d+\\)", "$1");
        return normalised;
    }

    /**
     * Whether the {@link IOException} means "no such table/database". Doris returns an
     * application-level error with {@code errCode = 2} and a message naming the missing object; the
     * message check is deliberately narrow so an authorization or network failure is never mistaken
     * for a missing table.
     */
    private static boolean isMissingTable(IOException e) {
        String message = e.getMessage();
        if (message == null) {
            return false;
        }
        String lower = message.toLowerCase(Locale.ROOT);
        return lower.contains("unknown table")
                || lower.contains("unknown database")
                || lower.contains("table not found")
                || lower.contains("database not found")
                || lower.contains("does not exist");
    }

    private static String quoteIdentifier(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }
}
