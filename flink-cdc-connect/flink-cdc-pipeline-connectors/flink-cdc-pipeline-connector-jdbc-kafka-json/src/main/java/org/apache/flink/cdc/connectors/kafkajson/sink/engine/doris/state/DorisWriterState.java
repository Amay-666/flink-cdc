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

package org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.state;

import org.apache.flink.cdc.connectors.kafkajson.sink.engine.doris.writer.StatefulDorisSinkWriter;

/**
 * The state one Doris sink subtask checkpoints, and restores from.
 *
 * <p>It holds two things, and deliberately not a third:
 *
 * <ul>
 *   <li>the {@link #getSequenceCounter() sequence counter} of the Group Commit sequence values.
 *       Persisting it keeps the values strictly increasing across restarts instead of relying on
 *       the wall clock having advanced — see {@link StatefulDorisSinkWriter}.
 *   <li>the {@link #getLabelPrefix() label prefix} the state was written under. It is not needed to
 *       do anything (the two-phase writer resolves a pending transaction through the label it is
 *       about to reuse, not through the state), but it makes a savepoint self-describing: a job
 *       restored under a different prefix says so in the log instead of leaving an operator to
 *       wonder which job wrote it.
 * </ul>
 *
 * <p>What is <em>not</em> here is the set of the subtask's open transactions, which earlier designs
 * of this feature called for. Recording them would be actively wrong: the transactions open at the
 * checkpoint being restored from are exactly the ones the {@code Committer} operator is about to
 * commit — it holds them in its own state, and the commit notification is asynchronous, so a job
 * can die after the checkpoint completed but before the committables were committed. A writer that
 * aborted them on restore would make the restarted committer fail with "transaction [N] is already
 * aborted" and the job could never start again. Transactions whose checkpoint never completed are
 * not orphans either — {@code getCheckpointCommittablesUpTo} commits them once a later checkpoint
 * completes — and the ones that genuinely belong to nobody (the table arrived in an attempt that
 * never checkpointed) are reclaimed by Doris itself after {@code
 * stream_load_default_precommit_timeout_second}.
 *
 * <p>Nor does it hold the buffered rows: the writer's buffer is always empty when the state is
 * taken, because every checkpoint barrier is preceded by a full flush.
 */
public class DorisWriterState {

    private final String labelPrefix;
    private final long sequenceCounter;

    public DorisWriterState(String labelPrefix, long sequenceCounter) {
        this.labelPrefix = labelPrefix;
        this.sequenceCounter = sequenceCounter;
    }

    public String getLabelPrefix() {
        return labelPrefix;
    }

    /**
     * The next Group Commit sequence value the writer would have used. Since the restored writer
     * resumes from a value above every one already written, a row that arrives late (a replay after
     * a restart) can never carry a sequence smaller than a row that was written before it.
     */
    public long getSequenceCounter() {
        return sequenceCounter;
    }

    @Override
    public String toString() {
        return "DorisWriterState{labelPrefix='"
                + labelPrefix
                + "', sequenceCounter="
                + sequenceCounter
                + '}';
    }
}
