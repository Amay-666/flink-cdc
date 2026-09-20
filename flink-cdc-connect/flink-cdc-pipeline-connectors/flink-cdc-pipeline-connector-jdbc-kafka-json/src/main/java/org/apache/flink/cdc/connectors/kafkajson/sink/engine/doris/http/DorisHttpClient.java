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

package org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.http;

import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.DorisDataSinkOptions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.cdc.common.utils.Preconditions.checkArgument;

/**
 * HTTP client for the Doris StreamLoad API and the FE query (DDL) API.
 *
 * <p>This is the self-contained replacement for the released doris-flink connector: the connector
 * talks to Doris purely over HTTP, with no Doris jar on the classpath. The client uses OkHttp
 * 3.14.9 (shaded into the connector jar) for its connection pool and efficient request/response
 * handling.
 *
 * <p>Three endpoints are used:
 *
 * <ul>
 *   <li>{@code PUT /api/{database}/{table}/_stream_load} — batch JSON write. The body is a JSON
 *       array ({@code strip_outer_array=true}). Doris 2.x answers the PUT on the FE with a {@code
 *       307} whose {@code Location} points at the BE owning the tablets; OkHttp does not follow
 *       redirects for {@code PUT}, so the client performs the two-step dance itself — it asks the
 *       FE first (which requires an {@code Expect: 100-continue} header) and re-issues the load
 *       against the backend it redirects to. A failed load is retried with the <em>same</em> {@code
 *       label}, which Doris deduplicates, making retries idempotent. With a {@code
 *       two_phase_commit: true} header this same request becomes a <em>pre-commit</em>: the rows
 *       are written but stay invisible until the transaction is committed.
 *   <li>{@code PUT /api/{database}/_stream_load_2pc} with headers {@code txn_operation:
 *       commit|abort} and {@code txn_id: N} — ends a pre-committed transaction (note: no table in
 *       the path). It redirects from the FE to a BE just like the load does, hence the same
 *       two-step handling. Doris 2.1+.
 *   <li>{@code POST /api/query/default_cluster/{database}} with body {@code {"stmt": ...}} — DDL
 *       execution (requires {@code is_execute_sql_in_http=true} on the FE). Application-level
 *       failures are surfaced immediately because DDL is not idempotent; only network-level errors
 *       are retried.
 * </ul>
 *
 * <p>The two-phase protocol needs no SQL and no extra privilege: a pre-commit is an ordinary load
 * request plus one header, and both control operations are HTTP calls. An account that can
 * stream-load into a table can also commit and abort its transactions on that table.
 */
public class DorisHttpClient implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DorisHttpClient.class);

    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /** The FE answers a StreamLoad PUT with this and a {@code Location} pointing at the BE. */
    private static final int HTTP_TEMP_REDIRECT = 307;

    /**
     * Endpoint that commits or aborts a pre-committed StreamLoad transaction. The table is
     * deliberately absent from the path: a transaction id already names one load into one table.
     */
    private static final String TWO_PHASE_COMMIT_ENDPOINT = "_stream_load_2pc";

    /** {@code Status} Doris reports for a load whose label a previous transaction already used. */
    private static final String LABEL_ALREADY_EXISTS = "Label Already Exists";

    /**
     * Pulls the transaction id out of a label-collision message. Doris does not put it in a field
     * of its own — the {@code TxnId} of such a response is {@code -1} — so the only way to learn
     * which transaction owns the colliding label is to read it out of this sentence. Verified
     * verbatim against doris-2.1.8; a message that stops matching yields no transaction id, and the
     * caller then has nothing to abort and fails rather than guessing (see {@link
     * LabelAlreadyExistsException#getExistingTransactionId()}).
     */
    private static final Pattern LABEL_EXIST_PATTERN =
            Pattern.compile("Label \\[(.*)\\] has already been used, relate to txn \\[(\\d+)\\]");

    /**
     * Marks the two messages that mean "this transaction is already decided" — {@code "transaction
     * [N] is already visible, not pre-committed."} from a repeated commit and {@code "transaction
     * [N] is already VISIBLE, could not abort."} from aborting a committed one. Both spellings
     * occur in doris-2.1.8, differing only in case, and the same message can arrive from either
     * operation: a job that dies after the commit reached Doris but before Flink recorded it has to
     * treat the replay as done, and one that finds a transaction already visible has nothing left
     * to undo.
     */
    private static final Pattern TRANSACTION_ALREADY_DECIDED_PATTERN =
            Pattern.compile("already visible", Pattern.CASE_INSENSITIVE);

    /**
     * Marks {@code "transaction [N] is already aborted. abort reason: User Abort"} — committing
     * such a transaction would throw the rows away, so it must not be mistaken for a success.
     */
    private static final Pattern TRANSACTION_ALREADY_ABORTED_PATTERN =
            Pattern.compile("already aborted", Pattern.CASE_INSENSITIVE);

    /**
     * Marks {@code "transaction [N] not found"}. Only aborting treats this as an outcome (there is
     * nothing left to abort); a commit that cannot find its transaction means the rows will never
     * become visible, which is data loss and is reported as a failure.
     */
    private static final Pattern TRANSACTION_NOT_FOUND_PATTERN =
            Pattern.compile("transaction \\[\\d+\\] not found", Pattern.CASE_INSENSITIVE);

    /** Base of the linear retry backoff: attempt {@code n} waits {@code n * this} milliseconds. */
    private static final long RETRY_BASE_BACKOFF_MILLIS = 500L;

    /** Reported in errors so a rejected endpoint list can be traced back to its option. */
    private static final String FENODES_OPTION_KEY = DorisDataSinkOptions.FENODES.key();

    /**
     * Doris {@code Status} values that mean the batch was committed. Doris reports {@code "Publish
     * Timeout"} when the rows were written and the transaction is still being published — the data
     * becomes visible shortly after, and the released Doris Flink connector treats it as a success.
     */
    private static final Set<String> DORIS_SUCCESS_STATUS =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList("Success", "Publish Timeout")));

    /**
     * StreamLoad properties this client manages itself; a {@code sink.properties} entry must not
     * override them. Each one either carries the write protocol ({@code format}, {@code
     * strip_outer_array}, {@code hidden_columns}), decides the idempotency contract ({@code label},
     * {@code group_commit}), or switches the load to two-phase commit ({@code two_phase_commit} —
     * set by this client only when the sink runs in its two-phase mode, and only together with the
     * commit call that ends the transaction; a stray header would leave the rows written but never
     * visible). Overriding any of them would silently change the write semantics instead of
     * failing, so they are rejected when the client is constructed.
     */
    private static final Set<String> RESERVED_STREAM_LOAD_PROPERTIES =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(
                                    "label",
                                    "format",
                                    "strip_outer_array",
                                    "hidden_columns",
                                    "group_commit",
                                    "two_phase_commit")));

    /**
     * Column name of the per-row delete marker carried in the imported data. A row with this column
     * set to {@code true} is deleted (matched by primary key); {@code false} upserts it. Doris
     * applies the semantics directly from the {@code hidden_columns} header — no {@code merge_type}
     * is needed.
     */
    public static final String DELETE_SIGN_COLUMN = "__DORIS_DELETE_SIGN__";

    private final String[] fenodes;
    private final int maxRetries;
    private final OkHttpClient client;
    private final String authorizationHeader;
    private final AtomicInteger nextFe = new AtomicInteger();

    /** Extra StreamLoad request headers/properties passed through from {@code sink.properties}. */
    private final Map<String, String> streamLoadProperties;

    /**
     * Group Commit mode: {@code "off"}, {@code "sync_mode"} or {@code "async_mode"}. When not
     * {@code "off"}, the caller passes a {@code null} label to {@link #streamLoad} so the {@code
     * group_commit} header is sent instead of a label (specifying a label degrades to
     * non-Group-Commit in Doris).
     */
    private final String groupCommitMode;

    public DorisHttpClient(String fenodes, String username, String password, int maxRetries) {
        this(fenodes, username, password, maxRetries, Collections.emptyMap(), "off");
    }

    /**
     * @param streamLoadProperties extra Doris StreamLoad properties (e.g. {@code columns}, {@code
     *     max_filter_ratio}, {@code timezone}, {@code exec_mem_limit}) sent as request headers.
     *     Properties the write protocol depends on ({@code format}, {@code strip_outer_array},
     *     {@code hidden_columns}, {@code label}, {@code group_commit}, {@code two_phase_commit})
     *     are managed by this client and rejected here; {@code Authorization} and {@code Expect}
     *     stay managed by this client as well.
     */
    public DorisHttpClient(
            String fenodes,
            String username,
            String password,
            int maxRetries,
            Map<String, String> streamLoadProperties) {
        this(fenodes, username, password, maxRetries, streamLoadProperties, "off");
    }

    /**
     * @param streamLoadProperties extra Doris StreamLoad properties (e.g. {@code columns}, {@code
     *     max_filter_ratio}, {@code timezone}, {@code exec_mem_limit}) sent as request headers.
     *     Properties the write protocol depends on ({@code format}, {@code strip_outer_array},
     *     {@code hidden_columns}, {@code label}, {@code group_commit}, {@code two_phase_commit})
     *     are managed by this client and rejected here; {@code Authorization} and {@code Expect}
     *     stay managed by this client as well.
     * @param groupCommitMode Group Commit mode: {@code "off"}, {@code "sync_mode"} or {@code
     *     "async_mode"}. When not {@code "off"}, callers should pass a {@code null} label to {@link
     *     #streamLoad} so the {@code group_commit} header is sent instead of a label.
     */
    public DorisHttpClient(
            String fenodes,
            String username,
            String password,
            int maxRetries,
            Map<String, String> streamLoadProperties,
            String groupCommitMode) {
        this.fenodes = parseFenodes(fenodes);
        // Retries on top of the first attempt, so 0 means "fail without retrying" — clamping this
        // to 1 instead would quietly turn sink.max-retries=0 into one retry.
        this.maxRetries = Math.max(0, maxRetries);
        this.authorizationHeader =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (username + ":" + password)
                                                .getBytes(StandardCharsets.UTF_8));
        this.streamLoadProperties =
                validateStreamLoadProperties(
                        streamLoadProperties == null
                                ? Collections.emptyMap()
                                : streamLoadProperties);
        this.groupCommitMode = groupCommitMode == null ? "off" : groupCommitMode.toLowerCase();
        this.client =
                new OkHttpClient.Builder()
                        .connectTimeout(30, TimeUnit.SECONDS)
                        .readTimeout(60, TimeUnit.SECONDS)
                        .writeTimeout(60, TimeUnit.SECONDS)
                        .build();
    }

    /**
     * Stream-loads a batch of rows into the Doris table.
     *
     * <p>Every row must carry {@link #DELETE_SIGN_COLUMN} — {@code true} deletes the row (matched
     * by primary key), {@code false} upserts it — so one batch can mix upserts and deletes in
     * arrival order. The {@code hidden_columns} header declares the marker column so Doris applies
     * its semantics without an extra {@code merge_type}.
     *
     * <p>When {@code label} is {@code null} and Group Commit is enabled (the instance's {@code
     * groupCommitMode} is not {@code "off"}), the request carries a {@code group_commit} header
     * instead of a {@code label} header. Doris internally assigns a {@code group_commit_...} label
     * and batches multiple loads into one transaction. Retries with a {@code null} label are safe
     * because the same rows carry the same sequence values, so Doris's "larger sequence replaces
     * smaller" semantics make the retry a no-op for keys that already committed.
     *
     * @param database target database (already mapped via the dialect options)
     * @param table target table (already mapped via the dialect options)
     * @param label the load label, deduplicated by Doris across retries; {@code null} when Group
     *     Commit is enabled (no label is sent, {@code group_commit} header is sent instead)
     * @param rows the rows to write, each a column-name to JSON-ready value map including the
     *     {@link #DELETE_SIGN_COLUMN} marker
     * @return the number of bytes sent in the request body (for write-throughput metrics)
     */
    public int streamLoad(
            String database, String table, String label, List<Map<String, Object>> rows)
            throws IOException {
        if (rows.isEmpty()) {
            return 0;
        }
        byte[] body = OBJECT_MAPPER.writeValueAsBytes(rows);
        String operation = describeStreamLoad(database, table, label);
        // Doris 2.x answers the StreamLoad PUT on the FE with a 307 whose Location points at the BE
        // owning the tablets. OkHttp does not follow redirects for PUT, so the two-step dance is
        // performed here: the FE is asked first (it requires the Expect: 100-continue header) and,
        // when it redirects, the load is re-issued against the backend. A non-redirecting FE (or a
        // test stub) evaluates the response directly. A StreamLoad failure is retried with the same
        // label, which Doris deduplicates, making the retry idempotent.
        return executeWithRetry(
                () ->
                        loadRequest(
                                feEndpoint() + "/api/" + database + "/" + table + "/_stream_load",
                                body,
                                label,
                                true,
                                false),
                response -> {
                    if (response.code() != HTTP_TEMP_REDIRECT) {
                        return evaluateStreamLoad(response, database, table, label, body.length);
                    }
                    String location = response.header("Location");
                    if (location == null) {
                        throw new IOException(
                                "FE redirected " + operation + " without a Location header.");
                    }
                    // The FE bakes the credentials into the redirect target; OkHttp strips the
                    // Authorization header on a cross-host redirect, so strip the userinfo and let
                    // the load request's Authorization header carry the credentials.
                    return performStreamLoad(stripUserInfo(location), body, database, table, label);
                },
                operation,
                HTTP_TEMP_REDIRECT);
    }

    /**
     * Re-issues a load against the backend URL from the FE redirect, retrying with the same label.
     * The backend is the one Doris chose for those tablets, so unlike the FE list there is nothing
     * to fail over to: the URL is fixed and only the request is rebuilt per attempt.
     */
    private int performStreamLoad(
            String beUrl, byte[] body, String database, String table, String label)
            throws IOException {
        return executeWithRetry(
                () -> loadRequest(beUrl, body, label, false, false),
                response -> evaluateStreamLoad(response, database, table, label, body.length),
                describeStreamLoad(database, table, label));
    }

    /**
     * Names the operation for logs and error messages. A {@code null} label is the Group Commit
     * case, where Doris assigns the label itself — saying so is more useful than printing "null".
     */
    private static String describeStreamLoad(String database, String table, String label) {
        return "StreamLoad of "
                + database
                + "."
                + table
                + " ("
                + (label != null ? "label " + label : "group commit")
                + ")";
    }

    /**
     * Pre-commits a batch of rows: they are written and their transaction is opened, but they stay
     * invisible to readers until {@link #commitTransaction} is called with the returned transaction
     * id. The rows otherwise follow {@link #streamLoad} exactly, including the {@link
     * #DELETE_SIGN_COLUMN} marker.
     *
     * <p>The label goes through the same dance on the BE leg (see the class javadoc). A label Doris
     * has already seen is not an error to retry — it names a transaction, which the caller has to
     * deal with before reusing the label — so it is reported as {@link LabelAlreadyExistsException}
     * rather than retried.
     *
     * @return the transaction the rows now live in, needed to commit or abort them
     * @throws LabelAlreadyExistsException when the label belongs to a transaction Doris remembers
     */
    public PrecommitResult precommitStreamLoad(
            String database, String table, String label, List<Map<String, Object>> rows)
            throws IOException {
        checkArgument(
                rows != null && !rows.isEmpty(),
                "Cannot pre-commit an empty batch into %s.%s: an empty load is a request Doris "
                        + "would open a transaction for and nothing would ever commit it. Callers "
                        + "skip empty batches instead.",
                database,
                table);
        return precommit(database, table, label, OBJECT_MAPPER.writeValueAsBytes(rows));
    }

    /**
     * The transaction a pre-commit opened, with the size of the batch that went into it — the write
     * metrics count bytes, and only this call knows how many.
     */
    public static class PrecommitResult {

        private final long transactionId;
        private final int bodyBytes;

        PrecommitResult(long transactionId, int bodyBytes) {
            this.transactionId = transactionId;
            this.bodyBytes = bodyBytes;
        }

        public long getTransactionId() {
            return transactionId;
        }

        public int getBodyBytes() {
            return bodyBytes;
        }
    }

    /**
     * Commits a pre-committed transaction, making its rows visible.
     *
     * <p>Safe to repeat: a transaction that is already visible counts as success, because the only
     * way to get here twice is a job that died after Doris committed but before that was recorded,
     * and there is nothing left to do about it.
     *
     * <p>Not safe to lose: a transaction Doris can no longer find, or one that was aborted, means
     * the rows will never be visible, so both are raised as failures instead of being logged away.
     *
     * @param database the database the transaction's rows were loaded into
     * @param transactionId the id returned by {@link #precommitStreamLoad}
     */
    public void commitTransaction(String database, long transactionId) throws IOException {
        twoPhaseOperation(database, "commit", transactionId);
    }

    /**
     * Aborts a pre-committed transaction, discarding its rows.
     *
     * <p>Safe to repeat, and safe to call for a transaction Doris no longer knows: a transaction
     * that is already visible counts as success — the rows are published and cannot be taken back,
     * and the commit is what the checkpoint wanted anyway — and one that does not exist has nothing
     * left to abort. Only an unexpected rejection is reported.
     *
     * @param database the database the transaction's rows were loaded into
     * @param transactionId the id returned by {@link #precommitStreamLoad}
     */
    public void abortTransaction(String database, long transactionId) throws IOException {
        twoPhaseOperation(database, "abort", transactionId);
    }

    /**
     * Pre-commits {@code body} under {@code label}, asking the FE first and re-issuing against the
     * BE it redirects to. Same two-step flow as {@link #streamLoad}, for the same reason: OkHttp
     * does not follow a {@code 307} for a PUT.
     */
    private PrecommitResult precommit(String database, String table, String label, byte[] body)
            throws IOException {
        String operation = "2PC precommit of " + database + "." + table + " (label " + label + ")";
        return executeWithRetry(
                () ->
                        loadRequest(
                                feEndpoint() + "/api/" + database + "/" + table + "/_stream_load",
                                body,
                                label,
                                true,
                                true),
                response -> {
                    if (response.code() != HTTP_TEMP_REDIRECT) {
                        return evaluatePrecommit(response, database, table, label, body.length);
                    }
                    String location = response.header("Location");
                    if (location == null) {
                        throw new IOException(
                                "FE redirected " + operation + " without a Location header.");
                    }
                    return executeWithRetry(
                            () -> loadRequest(stripUserInfo(location), body, label, false, true),
                            retried ->
                                    evaluatePrecommit(retried, database, table, label, body.length),
                            operation);
                },
                operation,
                HTTP_TEMP_REDIRECT);
    }

    private static PrecommitResult evaluatePrecommit(
            Response response, String database, String table, String label, int bodyBytes)
            throws IOException {
        if (!response.isSuccessful()) {
            throw new IOException(
                    "HTTP " + response.code() + " for 2PC precommit of " + database + "." + table);
        }
        String responseBody = response.body() != null ? response.body().string() : "";
        JsonNode root = OBJECT_MAPPER.readTree(responseBody);
        String status = root.path("Status").asText();
        if (DORIS_SUCCESS_STATUS.contains(status)) {
            long transactionId = root.path("TxnId").asLong(-1L);
            if (transactionId < 0) {
                // Without the id the transaction can never be committed; it would sit invisible
                // until Doris timed it out, holding the batch's rows. Fail while it is still clear
                // which load did it.
                throw new DorisHttpException(
                        "Doris pre-committed "
                                + database
                                + "."
                                + table
                                + " (label "
                                + label
                                + ") without reporting a transaction id: "
                                + responseBody);
            }
            return new PrecommitResult(transactionId, bodyBytes);
        }
        if (LABEL_ALREADY_EXISTS.equals(status)) {
            String message = root.path("Message").asText();
            Matcher matcher = LABEL_EXIST_PATTERN.matcher(message);
            long existing = matcher.find() ? Long.parseLong(matcher.group(2)) : -1L;
            throw new LabelAlreadyExistsException(
                    label, root.path("ExistingJobStatus").asText(), existing, message);
        }
        // A rejection Doris understood and made up its mind about — an unknown table, a full disk,
        // a
        // rejected batch — is surfaced without retrying: the same request would be refused again.
        throw new DorisHttpException(
                "2PC precommit failed for "
                        + database
                        + "."
                        + table
                        + " (label "
                        + label
                        + "): "
                        + responseBody);
    }

    /**
     * Runs one commit/abort call, asking the FE first and re-issuing against the BE it redirects to
     * — the same two-step flow as a load, since this endpoint redirects the same way.
     */
    private void twoPhaseOperation(String database, String operation, long transactionId)
            throws IOException {
        String described =
                "2PC " + operation + " of transaction " + transactionId + " in " + database;
        executeWithRetry(
                () ->
                        twoPhaseCommitRequest(
                                feEndpoint() + "/api/" + database + "/" + TWO_PHASE_COMMIT_ENDPOINT,
                                operation,
                                transactionId,
                                true),
                response -> {
                    if (response.code() != HTTP_TEMP_REDIRECT) {
                        evaluateTwoPhaseOperation(response, database, operation, transactionId);
                        return null;
                    }
                    String location = response.header("Location");
                    if (location == null) {
                        throw new IOException(
                                "FE redirected " + described + " without a Location header.");
                    }
                    executeWithRetry(
                            () ->
                                    twoPhaseCommitRequest(
                                            stripUserInfo(location),
                                            operation,
                                            transactionId,
                                            false),
                            retried -> {
                                evaluateTwoPhaseOperation(
                                        retried, database, operation, transactionId);
                                return null;
                            },
                            described);
                    return null;
                },
                described,
                HTTP_TEMP_REDIRECT);
    }

    private static void evaluateTwoPhaseOperation(
            Response response, String database, String operation, long transactionId)
            throws IOException {
        if (!response.isSuccessful()) {
            throw new IOException(
                    "HTTP "
                            + response.code()
                            + " for 2PC "
                            + operation
                            + " of transaction "
                            + transactionId);
        }
        String responseBody = response.body() != null ? response.body().string() : "";
        JsonNode root = OBJECT_MAPPER.readTree(responseBody);
        // Unlike a load, this endpoint answers with lower-case "status"/"msg" fields.
        if ("Success".equalsIgnoreCase(root.path("status").asText())) {
            return;
        }
        String message = root.path("msg").asText();
        if (TRANSACTION_ALREADY_DECIDED_PATTERN.matcher(message).find()) {
            LOG.info(
                    "Transaction {} in {} was already visible; treating the 2PC {} as done.",
                    transactionId,
                    database,
                    operation);
            return;
        }
        if ("abort".equals(operation) && TRANSACTION_NOT_FOUND_PATTERN.matcher(message).find()) {
            LOG.info(
                    "Transaction {} in {} no longer exists; there is nothing to abort.",
                    transactionId,
                    database);
            return;
        }
        if (TRANSACTION_ALREADY_ABORTED_PATTERN.matcher(message).find()) {
            // Committing an aborted transaction would throw the batch away. Doris is telling us the
            // rows are gone, so this must stay a failure no matter which operation asked.
            throw new DorisHttpException(
                    "Cannot commit transaction "
                            + transactionId
                            + " in "
                            + database
                            + ": "
                            + responseBody);
        }
        throw new DorisHttpException(
                "2PC "
                        + operation
                        + " failed for transaction "
                        + transactionId
                        + " in "
                        + database
                        + ": "
                        + responseBody);
    }

    /** Builds a {@code PUT /api/{database}/_stream_load_2pc} control request. */
    private Request twoPhaseCommitRequest(
            String url, String txnOperation, long transactionId, boolean expectContinue) {
        Request.Builder builder =
                new Request.Builder()
                        .url(url)
                        .put(RequestBody.create(JSON, new byte[0]))
                        .header("txn_operation", txnOperation)
                        .header("txn_id", Long.toString(transactionId))
                        .header("Authorization", authorizationHeader);
        if (expectContinue) {
            builder.header("Expect", "100-continue");
        }
        return builder.build();
    }

    /**
     * @param expectContinue {@code true} for the leg that goes to the FE, which rejects a load
     *     without the header; the load re-issued against the BE must not carry it
     * @param twoPhaseCommit {@code true} to pre-commit instead of committing: the rows are written
     *     but stay invisible until {@link #commitTransaction} is called for the returned
     *     transaction. It has to be re-sent on the BE leg as well — the FE's redirect is a plain
     *     {@code 307}, not a proxy, so a header set only on the first leg is silently absent from
     *     the load that actually writes the rows, and they then commit immediately.
     */
    private Request loadRequest(
            String url, byte[] body, String label, boolean expectContinue, boolean twoPhaseCommit) {
        boolean groupCommit = label == null && !"off".equalsIgnoreCase(groupCommitMode);
        Map<String, String> headers = new LinkedHashMap<>();
        // label: only in non-Group-Commit mode (specifying a label degrades to non-GC in Doris)
        if (label != null) {
            headers.put("label", label);
        }
        headers.put("format", "json");
        headers.put("strip_outer_array", "true");
        if (twoPhaseCommit) {
            headers.put("two_phase_commit", "true");
        }
        // hidden_columns declares the per-row delete marker so Doris applies its
        // semantics (true = delete by key, false = upsert) without any merge_type.
        headers.put("hidden_columns", DELETE_SIGN_COLUMN);
        // group_commit: sent instead of a label when Group Commit is enabled
        if (groupCommit) {
            headers.put("group_commit", groupCommitMode);
        }
        // sink.properties pass-through: user-supplied StreamLoad properties (columns=...,
        // max_filter_ratio=..., timezone=..., ...). A key colliding with one of the protocol
        // headers set above has already been rejected by the constructor.
        headers.putAll(streamLoadProperties);
        Request.Builder builder =
                new Request.Builder().url(url).put(RequestBody.create(JSON, body));
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            builder.header(entry.getKey(), entry.getValue());
        }
        // Framework-managed headers, never overridable via sink.properties.
        builder.header("Authorization", authorizationHeader);
        if (expectContinue) {
            // The FE's StreamLoad handler rejects a request without this header.
            builder.header("Expect", "100-continue");
        }
        return builder.build();
    }

    private int evaluateStreamLoad(
            Response response, String database, String table, String label, int bytes)
            throws IOException {
        if (!response.isSuccessful()) {
            throw new IOException(
                    "HTTP " + response.code() + " for StreamLoad of " + database + "." + table);
        }
        String responseBody = response.body() != null ? response.body().string() : "";
        JsonNode root = OBJECT_MAPPER.readTree(responseBody);
        String status = root.path("Status").asText();
        boolean groupCommit = label == null;
        if (groupCommit) {
            // Group Commit mode: "Label Already Exists" never occurs (Doris assigns labels).
            if ("Success".equals(status)) {
                // Warn if Doris silently fell back to non-GC (table lacks sequence column, heavy
                // schema change, WAL space insufficient for async_mode, etc.)
                boolean groupCommitUsed = root.path("GroupCommit").asBoolean(false);
                if (!groupCommitUsed) {
                    LOG.warn(
                            "Group commit was requested but Doris fell back to normal mode for "
                                    + "{}.{}: possible causes include missing sequence column, "
                                    + "heavy schema change in progress, or WAL space "
                                    + "insufficient (async_mode auto-switches to sync_mode).",
                            database,
                            table);
                }
                return bytes;
            }
        } else {
            if (DORIS_SUCCESS_STATUS.contains(status)) {
                return bytes;
            }
            // "Label Already Exists" on its own is not a success: it only says that some load with
            // this label reached Doris before. The batch counts as committed only when that earlier
            // job actually FINISHED — a RUNNING/PRECOMMITTED job can still fail, and a CANCELLED
            // one means the rows were never written. Treating the status as an unconditional
            // success would silently drop the batch, so anything but FINISHED is left to the retry
            // loop below, which re-issues the same label and is therefore still idempotent.
            if ("Label Already Exists".equals(status)
                    && "FINISHED".equals(root.path("ExistingJobStatus").asText())) {
                LOG.info(
                        "StreamLoad label {} for {}.{} was already used by a FINISHED job; "
                                + "treating this batch as committed.",
                        label,
                        database,
                        table);
                return bytes;
            }
        }
        throw new IOException(
                "StreamLoad failed for "
                        + database
                        + "."
                        + table
                        + " (label "
                        + (label != null ? label : "group_commit")
                        + "): "
                        + responseBody);
    }

    /** Removes the {@code user:password@} prefix the FE bakes into its redirect target. */
    private static String stripUserInfo(String location) {
        return location.replaceFirst("^([a-zA-Z][a-zA-Z0-9+.-]*://)[^@/]*@", "$1");
    }

    /**
     * Rejects {@code sink.properties} entries that collide with a header this client manages itself
     * (see {@link #RESERVED_STREAM_LOAD_PROPERTIES}). Header names are case-insensitive in HTTP, so
     * the comparison is done on lower-cased keys.
     */
    private static Map<String, String> validateStreamLoadProperties(
            Map<String, String> properties) {
        for (String key : properties.keySet()) {
            if (key != null
                    && RESERVED_STREAM_LOAD_PROPERTIES.contains(key.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException(
                        "StreamLoad property '"
                                + key
                                + "' is managed by this connector and cannot be set through"
                                + " sink.properties. Reserved properties: "
                                + RESERVED_STREAM_LOAD_PROPERTIES);
            }
        }
        return properties;
    }

    /** Executes a DDL statement in the given database. */
    public void executeSql(String database, String sql) throws IOException {
        executeWithRetry(
                () -> queryRequest(feEndpoint(), database, sql),
                response -> {
                    // Only a 5xx/408/429 reaches the handler as an unsuccessful response (the
                    // non-retryable ones were already surfaced as DorisHttpException). Such a body
                    // is an error page, not the {"code":...} envelope the check below expects:
                    // reading it as if it were would find no "code" field and report a failed DDL
                    // as done.
                    requireSuccessful(response);
                    String responseBody = response.body() != null ? response.body().string() : "";
                    JsonNode root = OBJECT_MAPPER.readTree(responseBody);
                    JsonNode codeNode = root.get("code");
                    if (codeNode == null || codeNode.asInt(-1) == 0) {
                        return null;
                    }
                    // Application-level DDL failure: surface immediately, do not retry — DDL is
                    // not idempotent (e.g. CREATE TABLE is only valid once).
                    throw new DorisHttpException(
                            "DDL failed on database "
                                    + database
                                    + " (code "
                                    + codeNode.asInt(-1)
                                    + "): "
                                    + responseBody
                                    + "; sql: "
                                    + sql);
                },
                "DDL on database " + database + ": " + sql);
    }

    /**
     * Executes a SQL query (SHOW / SELECT) and returns the raw JSON response body. Unlike {@link
     * #executeSql} which discards the result, this method returns the full response so the caller
     * can parse the returned rows.
     *
     * <p>The response body has the structure {@code {"code":0,"msg":"OK","data":{...}}}. The caller
     * parses {@code data} to extract the rows.
     *
     * @param database target database (already mapped via the dialect options)
     * @param sql the SQL statement to execute
     * @return the raw response body string
     */
    public String executeQuery(String database, String sql) throws IOException {
        return executeWithRetry(
                () -> queryRequest(feEndpoint(), database, sql),
                response -> {
                    requireSuccessful(response);
                    String responseBody = response.body() != null ? response.body().string() : "";
                    JsonNode root = OBJECT_MAPPER.readTree(responseBody);
                    JsonNode codeNode = root.get("code");
                    if (codeNode == null || codeNode.asInt(-1) == 0) {
                        return responseBody;
                    }
                    // Application-level failure: surface immediately (not a transient network
                    // error).
                    throw new IOException(
                            "Query failed on database "
                                    + database
                                    + " (code "
                                    + codeNode.asInt(-1)
                                    + "): "
                                    + responseBody
                                    + "; sql: "
                                    + sql);
                },
                "query on database " + database + ": " + sql);
    }

    /** Builds a {@code POST /api/query/default_cluster/{database}} statement request. */
    private Request queryRequest(String feEndpoint, String database, String sql)
            throws IOException {
        return new Request.Builder()
                .url(feEndpoint + "/api/query/default_cluster/" + database)
                .post(
                        RequestBody.create(
                                JSON,
                                OBJECT_MAPPER.writeValueAsBytes(
                                        Collections.singletonMap("stmt", sql))))
                .addHeader("Authorization", authorizationHeader)
                .build();
    }

    /**
     * Rejects an unsuccessful response whose body is not the JSON envelope the caller parses. Such
     * a response only gets here when it is retryable (5xx/408/429 — the non-retryable statuses were
     * already surfaced as {@link DorisHttpException}), so the thrown {@link IOException} sends the
     * operation to the next attempt, or out of the retry loop once the attempts run out.
     */
    private static void requireSuccessful(Response response) throws IOException {
        if (!response.isSuccessful()) {
            throw new IOException("HTTP " + response.code());
        }
    }

    @Override
    public void close() {
        client.dispatcher().executorService().shutdown();
        client.connectionPool().evictAll();
    }

    /**
     * Splits the configured FE list and rejects one that cannot yield a usable endpoint. A blank
     * entry — {@code "fe1:8030, fe2:8030"} after a naive split, or a trailing comma — would
     * otherwise reach OkHttp as the URL {@code http://} and fail the job with a bare {@code
     * IllegalArgumentException} from OkHttp instead of naming the option to fix. An input with no
     * endpoint at all would make {@link #feEndpoint()} divide by zero.
     */
    private static String[] parseFenodes(String fenodes) {
        checkArgument(
                fenodes != null && !fenodes.trim().isEmpty(),
                "Option '%s' must be set to a non-empty, comma-separated list of Doris FE "
                        + "addresses, e.g. \"fe1:8030,fe2:8030\".",
                FENODES_OPTION_KEY);
        List<String> endpoints = new ArrayList<>();
        for (String candidate : fenodes.split(",")) {
            String endpoint = candidate.trim();
            if (!endpoint.isEmpty()) {
                endpoints.add(endpoint);
            }
        }
        checkArgument(
                !endpoints.isEmpty(),
                "Option '%s' is set to \"%s\", which contains no usable Doris FE address.",
                FENODES_OPTION_KEY,
                fenodes);
        return endpoints.toArray(new String[0]);
    }

    private String feEndpoint() {
        String fe = fenodes[Math.floorMod(nextFe.getAndIncrement(), fenodes.length)];
        return fe.startsWith("http://") || fe.startsWith("https://") ? fe : "http://" + fe;
    }

    /**
     * Builds the request for one attempt. It is deliberately a factory rather than a request: see
     * {@link #executeWithRetry}.
     */
    @FunctionalInterface
    private interface RequestFactory {
        Request create() throws IOException;
    }

    /** Turns one HTTP exchange into the operation's result, or throws to fail or retry it. */
    @FunctionalInterface
    private interface ResponseHandler<T> {
        T handle(Response response) throws IOException;
    }

    /**
     * Runs an operation against the FE list, retrying transient failures on the next endpoint.
     *
     * <p>The request is built once per attempt, not once per operation. A request built up front
     * keeps pointing at the endpoint that just failed, so every retry would hit the same dead FE —
     * that turns a multi-FE {@code sink.fenodes} list into dead weight and, because each new
     * attempt starts again at the first entry, leaves an unreachable first FE failing the job
     * forever.
     *
     * <p>Failures Doris understood and refused are surfaced right away rather than retried: an
     * application-level rejection ({@link DorisHttpException}) or a 4xx other than 408/429 means
     * the request itself was wrong — bad credentials, unknown table, malformed SQL — and repeating
     * it cannot succeed (for DDL it is not even idempotent). A 5xx or a transport failure is
     * transient, so it is worth another endpoint.
     *
     * @param handledStatuses non-2xx statuses that are an outcome rather than a failure for this
     *     operation, so the handler has to see them: the StreamLoad {@code 307} pointing at the
     *     backend that owns the tablets. Such a status is neither retried nor reported as an error;
     *     every other operation classifies it as a failure as usual.
     */
    private <T> T executeWithRetry(
            RequestFactory requestFactory,
            ResponseHandler<T> handler,
            String operation,
            int... handledStatuses)
            throws IOException {
        for (int attempt = 0; ; attempt++) {
            try {
                Request request = requestFactory.create();
                try (Response response = client.newCall(request).execute()) {
                    int status = response.code();
                    if (!response.isSuccessful()
                            && !isRetryableStatus(status)
                            && !isHandledStatus(status, handledStatuses)) {
                        throw new DorisHttpException("HTTP " + status + " for " + operation);
                    }
                    return handler.handle(response);
                }
            } catch (DorisHttpException e) {
                throw e;
            } catch (IOException e) {
                if (attempt >= maxRetries) {
                    throw e;
                }
                LOG.warn(
                        "{} attempt {} failed: {}. Retrying ({} of {}).",
                        operation,
                        attempt + 1,
                        e.getMessage(),
                        attempt + 1,
                        maxRetries);
                sleepBeforeRetry(attempt);
            }
        }
    }

    /**
     * Whether an HTTP status is worth another attempt. Only server-side failures and the two
     * transient client statuses qualify; the remaining 4xx codes describe a request that will be
     * rejected just as firmly next time.
     */
    private static boolean isRetryableStatus(int status) {
        return status >= 500 || status == 408 || status == 429;
    }

    /** Whether the caller declared this status as an outcome it handles itself. */
    private static boolean isHandledStatus(int status, int[] handledStatuses) {
        for (int handled : handledStatuses) {
            if (handled == status) {
                return true;
            }
        }
        return false;
    }

    /**
     * Backs off before the next attempt. A task thread is cancelled by interrupting it, so the wait
     * is abandoned on interrupt instead of being swallowed: keeping the interrupt flag and retrying
     * anyway would hold the cancelled task alive until the retries run out.
     */
    private static void sleepBeforeRetry(int attempt) throws IOException {
        try {
            Thread.sleep(RETRY_BASE_BACKOFF_MILLIS * (attempt + 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting to retry", e);
        }
    }

    /** Raised when Doris rejects a request at the application level (non-retryable). */
    static class DorisHttpException extends IOException {
        private static final long serialVersionUID = 1L;

        DorisHttpException(String message) {
            super(message);
        }
    }

    /**
     * Raised when a pre-commit uses a label a previous transaction already took. Not retryable: the
     * label is not free until that transaction is resolved, so the caller has to commit it, abort
     * it, or pick another label.
     *
     * <p>Doris reports three things about the other transaction, and which one it is decides what
     * the caller should do. {@code PRECOMMITTED} — an earlier incarnation of this job pre-committed
     * this batch and died before it was checkpointed; the transaction can be aborted (which frees
     * the label, as doris-2.1.8 was measured to do) and the batch re-pre-committed. {@code
     * FINISHED} — that batch is already visible, so this job's labels have been used before and
     * reusing them will keep colliding. Anything else leaves the label stuck and the caller with
     * nothing safe to do.
     */
    public static class LabelAlreadyExistsException extends DorisHttpException {

        private static final long serialVersionUID = 1L;

        private final String label;
        private final String existingJobStatus;
        private final long existingTransactionId;

        LabelAlreadyExistsException(
                String label,
                String existingJobStatus,
                long existingTransactionId,
                String message) {
            super("Label '" + label + "' already exists: " + message);
            this.label = label;
            this.existingJobStatus = existingJobStatus;
            this.existingTransactionId = existingTransactionId;
        }

        public String getLabel() {
            return label;
        }

        /** Doris's own status for the other transaction, e.g. {@code PRECOMMITTED}. */
        public String getExistingJobStatus() {
            return existingJobStatus;
        }

        /**
         * The transaction holding the label, or {@code -1} when it could not be read out of Doris's
         * message. A caller that gets {@code -1} has no transaction to abort and must not assume
         * the label is free.
         */
        public long getExistingTransactionId() {
            return existingTransactionId;
        }

        /** Whether the other transaction is still open, hence abortable. */
        public boolean isPrecommitted() {
            return "PRECOMMITTED".equalsIgnoreCase(existingJobStatus);
        }

        /** Whether the other transaction's rows are already visible. */
        public boolean isFinished() {
            return "FINISHED".equalsIgnoreCase(existingJobStatus);
        }
    }
}
