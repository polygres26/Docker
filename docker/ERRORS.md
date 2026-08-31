# Error translation across every wire protocol

A client that speaks Oracle, MySQL, SQL Server, MongoDB, DynamoDB, SQS, OpenSearch, InfluxDB, or GraphDB (Neo4j) expects a
real failure — a duplicate key, a missing table, a dead connection — to come back in *that*
protocol's own native error shape, with the exact code/name a driver's own retry, reconnect, and
error-handling logic keys off. Underneath every one of them, PolyWire is talking to real Postgres,
which raises its own SQLSTATE for the same failure. This page documents how each protocol
translates that real Postgres SQLSTATE into a genuine native error — not a generic
`internal error` that leaves the client's own driver logic (retry-on-duplicate-key, reconnect-on-
connection-loss, etc.) with nothing to key off.

Every mapping below was verified — against the vendor's own official documentation, or empirically
against that protocol's real client library (the AWS SDK, `opensearch-java`, `mongodb-driver-sync`,
a real Oracle/MySQL/SQL Server JDBC driver) actually parsing PolyWire's response — not guessed.
Where a vendor genuinely has no dedicated code for a condition, this page says so plainly rather
than inventing a plausible-looking one.

---

## The shape each protocol's error takes

| Protocol | Native error shape |
|---|---|
| **Oracle** (orawire) | `ORA-NNNNN` number + real Oracle message text + SQLSTATE |
| **MySQL** (mywire) | numeric `errno` + SQLSTATE + real MySQL message text |
| **SQL Server** (mssqlwire) | error number / severity / state + real sys.messages-style text |
| **MongoDB** (mongowire) | `code` (int) + `codeName` (string), on both command errors and per-document `writeErrors` |
| **DynamoDB** (dynamowire) | HTTP status + AWS exception name, in the real AWS JSON error envelope |
| **SQS** (sqswire) | HTTP status + AWS error code, in the real AWS JSON/XML error envelope |
| **OpenSearch** (oswire) | HTTP status + `error.type` + `error.reason` + `error.root_cause[]` |
| **InfluxDB** (influxwire) | HTTP status + a flat `{"error": "..."}` message |
| **GraphDB** (boltwire) | a real Bolt `FAILURE` message: `Neo.*`-shaped code (e.g. `Neo.ClientError.Statement.SyntaxError`) + message text |

Each protocol maps the real Postgres condition to its own native error table, falling back to that
protocol's own generic default for anything unmapped rather than failing to respond at all.
InfluxDB is left out of the per-condition table below on purpose, not by omission: its own real
error responses don't carry a distinct error-code taxonomy the way Oracle's `ORA-NNNNN` or
OpenSearch's `error.type` do -- just an HTTP status and a human-readable message -- so mapping it
into that table's per-condition columns would mean inventing distinctions real InfluxDB itself
doesn't make, not documenting real ones. `InfluxErrorMapper` maps SQLSTATE to the closest real HTTP
status (404/403/400/503) instead; see its own source for the exact table.

GraphDB (boltwire) is left out of the per-condition table for the same honest reason, one step
further: it doesn't translate Postgres SQLSTATEs into a per-condition Neo4j error table at all
yet, only three coarse real Bolt codes -- `Neo.ClientError.Statement.SyntaxError` for a Cypher
parse/translation failure, `Neo.ClientError.Statement.ExecutionFailed` for any other real
`SQLException` from the underlying query, and `Neo.DatabaseError.General.UnknownError` for an
unexpected server-side failure encoding a result row (e.g. a value PackStream has no encoding
for) -- rather than inventing a finer-grained mapping real Neo4j's own error catalog would draw
differently. Every one of these is a message a real Bolt client driver already knows how to parse
into its own typed exception, since the shape (a real `Neo.*`-namespaced code string plus message
text) is exactly what the wire protocol itself defines, not a PolyWire-specific convention.

---

## Canonical condition → per-protocol native error

| Canonical condition | Oracle | MySQL | SQL Server | MongoDB | DynamoDB | SQS | OpenSearch |
|---|---|---|---|---|---|---|---|
| Duplicate / already exists | `ORA-00001` unique constraint violated | `1062` Duplicate entry | `2627` Violation of UNIQUE KEY | `11000 DuplicateKey` | `ConditionalCheckFailedException` | *(no dedicated code — see note)* | `409 version_conflict_engine_exception` |
| Table/index/queue already exists | `ORA-00955` name already used | `1050` Table already exists | `2714` object already exists | `11000 DuplicateKey` | `ResourceInUseException` | *(no dedicated code)* | `resource_already_exists_exception` |
| Conditional / concurrency conflict | `ORA-08177` can't serialize access / `ORA-00060` deadlock | `1213` Deadlock found | `1205` deadlock victim | `112 WriteConflict` | `TransactionConflictException` | *(N/A — no transactions)* | *(N/A — version_conflict above covers this)* |
| Resource not found (table/collection/queue/index) | `ORA-00942` table or view does not exist | `1146` Table doesn't exist | `208` Invalid object name | `26 NamespaceNotFound` | `ResourceNotFoundException` | `400 QueueDoesNotExist` | `404 index_not_found_exception` |
| Invalid input / schema violation | `ORA-01400`/`ORA-01722`/`ORA-12899` (null/numeric/length, by shape) | `1048`/`1366`/`1406` (same, by shape) | `515`/`245`/`8152` (same, by shape) | `121 DocumentValidationFailure` | `ValidationException` | *(N/A)* | `400 mapper_parsing_exception` |
| Permission / auth denied | `ORA-01031` insufficient privileges | `1142` Access denied | `229` SELECT permission denied | `13 Unauthorized` | `403 AccessDeniedException` | `400 AccessDenied` | `403 security_exception` |
| Throttling / resource limit | `ORA-00018` max sessions exceeded | `1040` Too many connections | *(no single documented number — see note)* | `91 ShutdownInProgress`\* | `InternalServerError`\* | *(default — see note)* | `503 no_shard_available_action_exception`\* |
| Timeout / statement canceled | `ORA-01013` user requested cancel | `3024` (statement timeout) | *(no numbered error — signaled via TDS "attention" packet)* | — | — | — | — |
| Backend unreachable / connection lost | `ORA-03113` end-of-file on communication channel | Lost connection to MySQL server during query | Communication link failure | `91 ShutdownInProgress` | `500 InternalServerError` | default `InternalError` | `503 no_shard_available_action_exception` |
| Unsupported operation | *(no PolyWire SQLSTATE maps here today — each protocol's own app-level validation handles this before reaching Postgres)* | | | | | | |

\* `53300` (too_many_connections) reuses the same "backend unreachable" native error as an actual
connection drop in MongoDB/DynamoDB/OpenSearch — none of the three has a dedicated *"the pool is
full"* code distinct from *"the backend is gone"*, so both real conditions surface identically to
the client, matching how each vendor's own driver actually treats them (retry/reconnect).

**Notes on the genuine gaps** (documented here rather than papered over with an invented code):
- **SQL Server** has no single canonical `sys.messages` number for a mid-session transport failure
  or for `too_many_connections` — severity-20 "transport-level error" text isn't consistently
  numbered across versions. `mssqlwire`'s configured default (`50000`) is used deliberately.
- **SQS** has no dedicated AWS error code for a duplicate/conditional/throttling condition distinct
  from a generic backend failure — the real AWS `InternalError` (already `sqswire`'s own default)
  is used, so these conditions are indistinguishable to an SQS client, exactly as they'd be against
  real SQS for those cases.
- **Oracle** has no error code dedicated to "function not found" — real Oracle reuses `ORA-00904`
  "invalid identifier," the same code `undefined_column` maps to.

---

## Connection-loss detail: the code a driver's reconnect logic actually keys off

The most consequential mapping on this page: every wire protocol's driver ecosystem has
reconnect/retry logic that keys off a *specific* code for "the backend connection is gone," not a
generic error. Oracle drivers check for `ORA-03113` specifically; that behavior only works if
PolyWire sends that exact number, not a plausible-sounding stand-in.

PolyWire distinguishes three real Postgres SQLSTATEs that all mean "the backend is unreachable,"
each confirmed live against a real outage (`RealPostgres#stop()`, not assumed):

| SQLSTATE | When it fires | 
|---|---|
| `57P01` (`admin_shutdown`) | Postgres's own SQLSTATE when an **already-open** connection is killed — a graceful backend restart, an operator running `pg_terminate_backend`, a container stop. This is what a real client mid-session actually gets, confirmed by repeated live runs against a stopped backend. |
| `08006` (`connection_failure`) | The more generic transport-level failure code Postgres itself defines for the same broad class of loss. |
| `08001` (`sqlclient_unable_to_establish_sqlconnection`) | A **new** connection attempt failing to establish — distinct from the two above, which are about an already-open connection dying. Confirmed live via MongoDB's own retryable-reads (on by default): after the first attempt hits `57P01`, the driver's automatic retry hits this code instead, from HikariCP's own connection-pool-exhaustion exception once it can no longer open a fresh physical connection at all. |

All three map to the same native "backend gone" error in every protocol — `ORA-03113` /
MySQL `2013` (Lost connection to MySQL server during query) / SQL Server's configured default /
Mongo's `91 ShutdownInProgress` / DynamoDB's `InternalServerError` / SQS's default `InternalError` /
OpenSearch's `no_shard_available_action_exception` — so a driver written against any of these
protocols gets exactly the signal its own reconnect logic expects, regardless of which of the three
underlying SQLSTATEs actually fired.

**What this does *not* cover:** if PolyWire itself dies (as opposed to Postgres dying underneath a
live PolyWire), the client's own transport layer detects a closed socket directly — no in-protocol
error code is sent or expected, since there's no PolyWire process left to send one. This page is
about translating a real error *from* Postgres, not about PolyWire's own process lifecycle.

Every mapping in this document is exercised end-to-end against that protocol's own real client
library — not just checked for the right JSON shape, but confirmed to raise the exact typed
exception a real application would catch.
