# Captured messages

Every file here is a **verbatim** Kafka message body, read off a topic by a console consumer while a
real producer wrote it. Nothing was reformatted, reordered, or hand-edited — one message per file,
one line per file. They are the evidence behind the connector's wire-format assumptions, and
`KafkaJsonCapturedMessageTest` replays them through the real parser, schema-change handler and record
converter.

**A captured file is never edited to make a test pass.** If a capture disproves what the code
assumes, the code is wrong or the assumption has to go — a second capture is added instead. The one
thing worse than a missing fixture is a fixture that agrees with the bug.

## How these were captured

The stack ran in one Docker network (`cap-net`) so the producers could reach both databases and the
broker by container name:

| Container | Image | Role |
|---|---|---|
| `cap-mysql` | `mysql:8.0` (`root`/`root`, user `canal`/`canal` with `REPLICATION SLAVE`, `REPLICATION CLIENT`, `SELECT`) | the MySQL source; `binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_rows_query_log_events=ON` |
| `cap-kafka` | `confluentinc/cp-kafka:7.2.2`, KRaft (single node, formatted with `kafka-storage format`) | the broker the producers write to |
| `cap-connect` | `debezium/connect:1.9` with `debezium-connector-mysql` 1.9.7 | runs the MySQL connector whose records are captured in `mysql-debezium/` |
| `cap-canal` | `canal/canal-server:v1.1.8` | writes the flat messages captured in `mysql-canal/` |
| TiDB (external host, 8.5.1) | TiCDC 8.5.1 changefeeds, `canal-json` and `debezium` protocols | `tidb-canal/` and `tidb-debezium/` |

Each session was one topic, one partition. The sessions were captured by starting a console consumer
on the topic **first**, then applying a SQL script through the `mysql` client (or running the DML on
TiDB), then letting the consumer run a few seconds longer and killing it. `canal-server` keeps no
committed cursor for a topic it has not seen, so it replays from an early binlog position whenever it
is restarted: a freshly created topic can therefore open with messages from **earlier** sessions. Each
captured directory is sliced from that session's own last `DROP DATABASE IF EXISTS …` — the first
message of the file set is that reset, so the replay in the test starts from an empty database.

Timestamps in the files are the producers' own (`es`/`ts` for canal, `source.ts_ms` for Debezium) and
are not touched.

### The sessions

| Directory | Producer and protocol | Topic | Session |
|---|---|---|---|
| `mysql-canal/` | canal-server 1.1.8, flat message | `cap-mysql-canal` | reset, `orders` created and inserted, updated, renamed to `orders_archive` with DML after the rename, a two-pair `RENAME TABLE`, a rename written as `ALTER TABLE … RENAME TO`, `TRUNCATE`, `DROP`, `ADD COLUMN` |
| `mysql-canal-swap/` | canal-server 1.1.8, flat message | `cap-mysql-canal2` | reset, `a` and `b` created and inserted, then the swap idiom `RENAME TABLE a TO a_tmp, b TO a, a_tmp TO b` as one message |
| `mysql-debezium/` | Debezium MySQL 1.9.7 via Kafka Connect 1.9 | `dbz-inventory.inventory`, `dbz-inventory2.inventory` (config in `connector-config.json`) | `orders` created, renamed, altered, with DML around each DDL; then `t1`/`t2` created and renamed by one statement; then `a`/`b` created and swapped by one statement |
| `tidb-canal/` | TiCDC 8.5.1, `canal-json` | TiCDC changefeed topic | reset, `test_schema_change4` created/inserted/updated, renamed to `test_schema_change3` with DML after it, `r1`/`r2` created and renamed by one statement — which TiCDC split into one message per pair — `ADD COLUMN` |
| `tidb-debezium/` | TiCDC 8.5.1, `debezium` protocol (`enable-tidb-extension`) | TiCDC changefeed topic | the same DML as above — and **no DDL at all**: TiCDC's `debezium` protocol emits no schema-change records in 8.5.1 (see the README one level up) |

`mysql-debezium/16`-`18` are the swap statement as Debezium splits it: one record per pair, each with
its own rewritten `ddl`.

## What the captures settled

- canal (both canal-server and TiCDC) announces the **post**-rename name in `table`, and for a
  multi-pair statement the first pair's new name — never the old name, and never a usable hash.
- TiCDC rewrites a multi-pair `RENAME TABLE` into **one message per pair**, each carrying a
  single-pair statement of its own, so a TiCDC source can never deliver a swap as one event.
- canal-server delivers a multi-pair `RENAME TABLE` as **one** message with the whole statement.
- canal reports `ALTER TABLE … RENAME TO` as `type: "RENAME"`, indistinguishable from `RENAME TABLE`.
- Debezium MySQL splits a multi-pair statement into one schema-change record per pair, and its
  `source.table` is a comma-joined list of every name of the statement (`"t2_new,t1_new"`,
  `"a,a_tmp,b"`) — a value that is no table name at all and cannot be looked up.
- A Debezium schema-change record may carry an **empty** `tableChanges` (`16-…-swap-1`) even for a
  table the connector tracks, so a record is not guaranteed to describe the schema it changed.
- canal's `ROWS_QUERY` messages (`isDdl:false`, `data:null`, `database:""`) must convert to zero
  records.
- MySQL refuses `RENAME TABLE a TO b, b TO a` and `RENAME TABLE a TO b, b TO c` with
  `ERROR 1050 Table 'b' already exists`, so the temporary-name chain is the only way a producer can
  express a swap — verified on the same `cap-mysql` by running both statements by hand.

## Re-capturing

The scaffolding used for these sessions is kept outside the repository. To redo a session: start the
producer and the console consumer on a **fresh** topic, apply the SQL, stop the consumer, then slice
the dump from its last database reset and write one message per file, single-line, byte-exact. Name
the files `NN-step-name.json` in topic order: the test replays them in that order.
