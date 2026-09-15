# Message fixtures

Raw Kafka message bodies, one directory per producer/format combination. `KafkaJsonRealMessageFixtureTest`
drives the files at this level through the real parser, so a change to the wire-format
assumptions of the connector fails a test instead of a job; `KafkaJsonCapturedMessageTest` replays
`captured/` end to end.

There are two kinds of file here, and they are not equally trustworthy:

| | Meaning | Rule |
|---|---|---|
| **captured/** | Bytes read off a real topic by a real producer. | Evidence. Never edited — see `captured/README.md` for the recipes. |
| everything else | Written by hand from the producer's wire format, to have *some* message of that shape before a capture was possible. | An assumption. It says what we believed the producer does, which is not the same thing. |

A hand-written file must not be edited to make a test pass either. When a capture contradicts one,
the capture wins and the hand-written file stays as it is — as the record of what was assumed, and
as a guard that the code no longer depends on the part that was wrong. `mysql-canal/rename-table.json`
is exactly that case (trap 1).

```
captured/          real sessions, one directory per producer and run
  mysql-canal/            canal-server 1.1.8 → flat message
  mysql-canal-swap/       canal-server 1.1.8 → the two-table name swap
  mysql-debezium/         Debezium MySQL 1.9.7 → data topic + schema-change records
  tidb-canal/             TiCDC 8.5.1 → canal-json protocol
  tidb-debezium/          TiCDC 8.5.1 → debezium protocol (no DDL at all in 8.5.1)
mysql-canal/       hand-written: a canal flat message (MySQL)
mysql-debezium/    hand-written: the Debezium 1.9 MySQL envelope and a schema-history record
tidb-canal/        hand-written: TiCDC's canal-json, including two messages from a real job
tidb-debezium/     hand-written: TiCDC's debezium protocol
```

## Producer matrix (as captured, not as assumed)

| | `table` of a rename | multi-pair `RENAME TABLE` | rename written as `ALTER … RENAME TO` | DDL records at all |
|---|---|---|---|---|
| canal-server 1.1.8 (flat) | the **new** name — of the *first* pair for a multi-pair statement | **one** message, whole statement in `sql` | reported as `type: "RENAME"` | yes |
| TiCDC 8.5.1 (`canal-json`) | the **new** name | **split**: one message per pair, each with a rewritten single-pair `sql` | reported as `type: "RENAME"` | yes |
| Debezium MySQL 1.9.7 | `source.table` is a comma-joined list of *every* name of the statement (`"t2_new,t1_new"`) — no table name at all | **split**: one record per pair, each with a rewritten `ddl` | not captured | yes, on the schema-history topic |
| TiCDC 8.5.1 (`debezium`) | — | — | — | **none**: DML only |

The consequence for the connector: a rename's two table names always come from the **DDL SQL**, never
from the message. `table` contributes nothing on any of the four, and the `debezium` protocol's
`table` is `null` besides.

## The traps these files exist for

1. **`table` of a canal rename is the *post*-rename name.** `captured/tidb-canal/06-rename-table.json`
   and `captured/mysql-canal/08-rename-table.json` both announce the name the table is renamed
   *to* — the old name appears only in the `sql`. A reader that took the old name from this field
   finds no schema and registers the renamed table empty, which is the bug the captures were taken
   to pin down. For a multi-pair statement the field holds the new name of the **first** pair
   (`captured/mysql-canal/21-rename-multi-pair.json` announces `t1_new` for a statement that also
   renames `t2`).
   The hand-written `mysql-canal/rename-table.json` carries the **old** name (`orders`) in that
   field, which is what canal-server was believed to do when it was written. The capture disproved
   it. The file is kept: it still passes, because the connector reads both names from the SQL, and
   it keeps the code from quietly starting to depend on the field again.
2. **`table` of a Debezium schema-change record is `null`.** The record names its database
   (`databaseName`, or `source.db`) and its DDL, but no table; the tables it touched are described by
   `tableChanges` — which may be **empty**, even for a tracked table
   (`captured/mysql-debezium/16-schema-change-rename-swap-1-a-to-a_tmp.json`). A Debezium DDL record
   is therefore not a description of the schema it changed: the connector rebuilds the table from the
   SQL and its own registry.
3. **A DDL record and a DML record of the same table disagree about the name** while the rename is in
   flight: the DML message that follows a rename announces the new name only, so the connector's
   table registry has to have followed the rename, or the record cannot be decoded at all
   (`captured/mysql-canal/10-insert-orders_archive.json` after `08-rename-table.json`).
4. **`mysql-debezium` writes DDL to the schema-history topic, not the data topic.** Feeding a rename
   into a Debezium MySQL pipeline therefore means consuming that topic — a deployment concern, not a
   parsing one. `captured/mysql-debezium/connector-config.json` is the connector that produced the
   records next to it.
5. **Producers split multi-pair statements, and only some of them.** canal-server delivers one
   message (so the connector can see the whole statement and apply it atomically), TiCDC and
   Debezium deliver one per pair (so it cannot). The connector applies whatever it receives in
   statement order, and folds a table moved through a temporary name into the rename that started it
   — see `KafkaJsonSchemaChangeHandler.applyRename`.
6. **MySQL will not write a two-table swap as a direct exchange.** `RENAME TABLE a TO b, b TO a` and
   the shorter `RENAME TABLE a TO b, b TO c` both fail with `ERROR 1050 Table 'b' already exists`
   (verified on MySQL 8.0), so the only swap a producer can emit is the temporary-name chain
   `RENAME TABLE a TO a_tmp, b TO a, a_tmp TO b` — captured in `captured/mysql-canal-swap/` and in
   `captured/mysql-debezium/16`–`18`.

## Postgres (not implemented)

`postgres-debezium` is the natural next format, and the message layer would be nearly free: the
envelope and `source` block (`db`/`schema`/`table`/`lsn`) are the ones already parsed, and a
PostgreSQL `ALTER TABLE … RENAME TO` renames exactly one table, so none of the multi-pair or swap
handling above is needed. The cost sits elsewhere:

- the dialect: `KafkaJsonDialectFactory` builds only the TiDB and MySQL dialects and fails fast for
  any other `scan.database.type`, and the type mapping (`KafkaJsonTableUtils`,
  `KafkaJsonColumnMeta`) has no PostgreSQL branch;
- the snapshot: PostgreSQL has no `SHOW MASTER STATUS`; a snapshot needs a replication slot and a
  consistent snapshot exported from it, and the offset model is an LSN, not a GTID;
- DDL arrives from a logical replication slot only with a decoding plugin that decodes it
  (`pgoutput` does not), so the schema-history route differs from MySQL's.

None of that is a reason to reshape the message layer, which is why this directory only reserves the
name.
