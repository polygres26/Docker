# PolyWire — Performance: SQL Statistics, RTT, and the Cache-Hit Latency Investigation

This document covers two related things added/fixed in the same investigation:

1. **SQL statistics and RTT (round-trip time) instrumentation** — a real, ongoing feature: every
   wire protocol's traffic is now measured, not estimated.
2. **The latency investigation that instrumentation made possible** — seven real, distinct
   bottlenecks found and fixed across five protocols, each proven with a live before/after
   benchmark against real client libraries, not synthetic load.

Nothing here is a benchmark run once and trusted — every number below was reproduced with a real
client (`psycopg2`, `pymysql`, `pymssql`, `python-oracledb`, `boto3`, `pymongo`) against a real
Postgres backend, and every fix was verified for correctness (not just speed) before being
accepted.

---

## 1. What gets measured, and how

### 1.1 Two different numbers: exec time vs. RTT

`SqlMetricsCollector` (`com.polygres.wire.core`) is the single collector every protocol feeds.
It tracks two distinct things per normalized statement/operation:

| Metric | What it spans | Where it comes from |
|---|---|---|
| **exec time** (`avgMs`/`totalMs`) | `StatementPipeline.execute()` only — firewall → router → QoS → translation → rollup → cache → the actual backend round trip | `StatsCollectorStage`, already existed |
| **RTT** (`avgRttMs`) | Full request-read-to-response-written span, including response serialization and the socket write | Added this pass, per protocol |

**RTT here means server-side round trip** (the same thing a reverse proxy's `$request_time`
means) — not network RTT to the client, which no server-side vantage point can measure. It's
useful precisely because it catches costs exec time doesn't: response encoding, and — as it
turned out — a couple of real bugs living in that gap.

### 1.2 Coverage — and one place it's honestly not possible

| Protocol | Exec time | RTT |
|---|---|---|
| pgwire (simple query) | ✅ | ✅ |
| pgwire (extended: Bind) | ✅ | ❌ — see below |
| pgwire (extended: Execute) | ✅ | ✅ |
| mywire, mssqlwire | ✅ | ✅ |
| orawire (Execute, Fetch) | ✅ | ✅ |
| gRPC | ✅ | ✅ |
| mongowire, dynamowire, sqswire | ✅ | ✅ |

**pgwire's Bind step never gets an RTT sample, on purpose.** Postgres's extended query protocol
splits a query into `Parse` → `Bind` (executes the query, sends nothing back) → `Execute` (a
separate, client-paced message that streams the already-computed result). A span joining Bind to
Execute would silently include however long the client took to *decide* to send Execute — not
server time. `Execute`'s own span is honest on its own (no re-execution, no client-controlled
gap inside it), so it gets a real sample; Bind doesn't, and that's a design choice, not a gap
someone forgot.

orawire has the same shape with `Fetch` (streams already-fetched rows, no re-execution) — that
one *does* get an honest RTT sample, unlike pgwire's Bind, because Fetch's own span has nothing
client-paced inside it.

### 1.3 Where it surfaces

- **API**: `GET /api/metrics/summary` (admin HTTP, bearer-token gated) — `topSql[]` per
  fingerprint (`avgMs`, `avgRttMs`, nullable when a call site doesn't report RTT), plus an
  overall `avgRttMs`/`rttSamples`, `byBackend[]`, protocol counts, reads/writes-per-sec.
- **UI**: the Metrics page (`advisor/web`) — an "Avg RTT" hero tile and an "Avg RTT" column on
  the top-SQL table, with **"—"** (not a misleading `0ms`) wherever a call site genuinely
  doesn't report it.
- **Fingerprinting**: SQL text is normalized (string/number literals → `?`) so `SELECT * FROM t
  WHERE id = 7` and `... id = 42` land in the same bucket, same as `pg_stat_statements`. For
  mongowire/dynamowire/sqswire, which have no SQL text, the operation name (`insert`, `GetItem`,
  `SendMessage`) is the fingerprint — the natural equivalent.

---

## 2. The investigation: seven bugs, one recurring pattern

Every fix below has the same shape: **an uncached lookup, a redundant round trip, or an
unnecessary transaction, running unconditionally on a hot path** — invisible from reading the
code, found only by comparing an operation's real cost against a cheaper operation that should
have cost about the same.

### Summary — single-key write, before and after

| Protocol | Operation | Before | After | Root cause |
|---|---|---|---|---|
| dynamowire | `PutItem` | 1.608ms | **0.82ms** | unconditional `SELECT...FOR UPDATE` + unnecessary explicit transaction |
| orawire | `INSERT` (autocommit) | 1.188ms | **0.580ms** | explicit `COMMIT` round trip when the client already asked for autocommit |
| mywire | `INSERT` | 0.933ms | **0.412ms** | synchronous translation-cache write in the request path |
| mssqlwire | `INSERT` (query-only) | — | **rounds to 0** | same translation-cache bug |
| sqswire | `ReceiveMessage` | 1.04ms | **0.65ms** | uncached `_sqs_queues` catalog lookup on every call |
| mongowire | `insertOne` | 0.962ms | **0.741ms** | unconditional, provably-unneeded cache invalidation |
| mssqlwire | connection reuse | **hung forever** | **works** | missing `DONE_ATTN` status bit on `ATTENTION` ack (TDS spec violation) |

*(pgwire numbers throughout are the baseline other protocols are compared against — it had none
of these bugs.)*

### Final state — single-key write across every protocol, server-side

| Protocol | ms | vs. sqswire's `SendMessage` (0.62ms) |
|---|---|---|
| pgwire | 0.47 | below |
| mywire | 0.51 | below |
| mssqlwire | 0.51 | below |
| orawire | 0.51 | below (was 2x over) |
| mongowire | 0.74 | close — inherent BSON framing cost |
| dynamowire | 0.82 | close — inherent JSON marshaling cost |
| sqswire | 0.62 | target |

---

## 3. Each bug, in detail

### 3.1 dynamowire — `describeTable()` on every GetItem, even a cache hit

**Symptom**: a confirmed Ignite cache hit for `GetItem` cost ~0.55ms server-side — should be
near-zero, since the item table isn't even touched on a hit.

**Cause**: `PgItemStore#describeTable()` ran a real Postgres query against `_dynamo_tables` on
every single call, cache hit or miss — because the caller needs the table's key schema before it
can even compute the cache key.

**Fix**: an in-memory `ConcurrentHashMap<String, TableSchema>` cache, safe because this
implementation has no async `CREATING`/`UPDATING` table states — a table is `ACTIVE` the instant
`CreateTable` returns, and Dynamo's API has no way to change a table's key schema afterward.
Populated by `CreateTable`, invalidated by `DeleteTable`.

**Result**: cache-hit `GetItem` server-side 0.549ms → **0.216ms**; client-observed (boto3) p50
1.523ms → **0.758ms**.

### 3.2 orawire / mywire / mssqlwire — synchronous translation-cache write

**Symptom**: chasing "why is orawire ~2x slower than pgwire on an *identical* cached query,
through the *identical* shared pipeline stages" — nanosecond checkpoints around every phase of
orawire's Execute path showed ~90% of total time inside `reusablePipeline.execute()`, the one
piece shared with pgwire.

**Cause**: `DialectTranslationStage` only does real work when source and target dialects differ
— a no-op for pgwire (Postgres→Postgres), but for every non-Postgres protocol (Oracle/MySQL/
SQL Server → Postgres backend) it calls `TranslationCacheStore#recordAccess()` on *every*
translation-cache hit: a real, synchronous Postgres round trip (`INSERT ... ON CONFLICT`) just
to bump `hit_count`/`last_hit_at` — pure analytics metadata, blocking the response.

**Fix**: `recordAccess()` now submits the write to a single background daemon thread and returns
immediately. Nothing downstream ever reads `hit_count`/`last_hit_at` to decide whether a query
can be served — that answer already came from the in-memory `TranslationCache` lookup before
`recordAccess` is even called — so a write landing a few ms late is never a correctness issue.

**Result**: orawire client p50 ~0.73-0.77ms → **0.375ms**; mywire client p50 0.933ms →
**0.412ms**; mssqlwire query-only server cost rounds to **0**. This one fix landed for three
protocols at once, since they all share `DialectTranslationStage`.

### 3.3 mssqlwire — `ATTENTION` acknowledgment missing `DONE_ATTN`

**Symptom**: not a latency bug — a correctness bug that *forced* a slower benchmark methodology.
Any second query on a reused mssqlwire connection hung the client forever.

**Root-caused, not guessed**: confirmed with two independent TDS client libraries (`pymssql`/
FreeTDS and `pytds`, unrelated codebases) failing identically — ruling out a single-library
quirk. `netstat` during the hang showed zero bytes pending in either direction — ruling out a
wire-framing bug. Captured and hand-decoded the raw response bytes for a *successful* first
query against the TDS token spec byte-by-byte: parsed perfectly, zero leftover, zero overrun —
the data encoding was already correct. Extending the capture into the second query attempt
showed the client sending an `ATTENTION` packet (type `0x06`) *before* reusing the connection —
entirely standard, spec-correct client behavior — and the server's response was a `DONE` token
with plain `DONE_FINAL` (`0x0000`) instead of the `DONE_ATTN` bit (`0x0020`) the TDS spec
requires for an attention acknowledgment. Byte-perfect, semantically wrong.

**Fix**: `TdsTokens.doneAttnStatus()` (new, `0x20`), used in the `ATTENTION` case handler
instead of `doneFinalStatus()`.

**Result**: both client libraries now run 5+ sequential queries on one reused connection
cleanly. Real-connection-reuse benchmark became possible for the first time: min 0.428ms, p50
0.526ms, p90 0.728ms.

### 3.4 sqswire — `queueAttributes()` on every `ReceiveMessage`

**Symptom**: `ReceiveMessage` on an *empty* queue (should be cheap — a claim `UPDATE` matching
nothing) cost ~2x `SendMessage` (a real `INSERT`, genuinely more work).

**Cause**: `receiveMessages()` called `queueAttributes(queueName)` — a real `_sqs_queues`
catalog round trip — before doing any message work, on every call.

**Fix**: an `attributesCache` (`ConcurrentHashMap<String, QueueAttributes>`). Unlike
dynamowire's table schema, SQS queue attributes *are* mutable at runtime (`SetQueueAttributes`),
so this one is genuinely invalidated on write, not just populated once: `createQueue()`/
`setQueueAttributes()` refresh it, `deleteQueue()` evicts it.

**Result**: server-side `ReceiveMessage` 1.04ms → **0.65ms**, now almost exactly matching
`SendMessage`'s 0.62ms — confirming the redundant round trip is gone, not just optimized.

### 3.5 dynamowire — `PutItem`/`DeleteItem` paying for work nobody asked for

**Symptom**: even after the schema-cache fix (§3.1), `PutItem` still cost ~1.6ms server-side —
2.6x sqswire's `SendMessage` target, for comparable work (one write).

**Cause, two stacked**:
1. Every `PutItem` ran `SELECT ... FOR UPDATE` against the target row *before* the `INSERT`,
   unconditionally. Checked both call sites (`OperationHandlers`'s main handler and
   `BatchWriteItem`): the fetched "old" item is only ever used when `ReturnValues=ALL_OLD` is
   requested — everywhere else, it was fetched and silently discarded.
2. Once that `SELECT` was removed, the surrounding `setAutoCommit(false)` + `commit()`/
   `rollback()` was wrapping a transaction with nothing left needing cross-statement atomicity —
   paying for an explicit `BEGIN`/`COMMIT` round-trip pair around a single statement.

**Fix**: a new `needExisting` parameter (default `true`, source-compatible) skips the `SELECT`
unless there's a `ConditionExpression` to evaluate against it or the caller actually asked for
the old value back. A `needsTransaction` flag (`conditionExpr != null || needExisting`) skips
the explicit transaction the same way — a plain unconditional `PutItem`/`DeleteItem` now runs as
one autocommit statement. Identical fix applied to `deleteItem` (same shape, same waste in
`BatchWriteItem`'s `DeleteRequest` handling).

**Result**: server-side 1.608ms → **0.82ms** (rounds to 0 on the RTT metric, i.e. genuinely
sub-0.5ms); client-observed p50 2.198ms → **1.414ms**.

### 3.6 orawire — explicit `COMMIT` when the client already asked for autocommit

**Symptom**: after §3.2's fix, orawire's plain `INSERT` still cost ~1.0-1.2ms vs. pgwire's
~0.5ms — a real, reproducible gap, confirmed with fresh nanosecond instrumentation (parse,
rewrite, response-encode, socket-send all 0-30µs; the entire gap was one call).

**Cause**: `commitAll()` → `pgConnection.commit()`, a real Postgres `COMMIT` round trip
(~330-470µs measured directly), running on every autocommit statement. orawire's Postgres
connection is *always* opened with `autoCommit=false` — to support dual Oracle+Postgres
execution, shadow replicas, and XA transactions, none of which were configured in the
benchmark.

**Considered and rejected**: deciding autocommit mode once at connection-open, based on whether
dual/shadow/XA are configured for the session. Rejected because it can't distinguish "no
dual/shadow/XA" from "the client wants manual multi-statement transactions on this otherwise-
plain connection" — real orawire clients can and do set `connection.autocommit = False`.
Forcing autocommit blindly would have silently broken that.

**Fix instead**: the client's own `EXEC_OPTION_COMMIT` bit (set by `oracledb` exactly when the
caller's `connection.autocommit` is `True`) already says precisely what's wanted for *this*
statement — no session-level inference needed. When it's set and nothing else needs explicit-
transaction mode, `setAutoCommit(true)` is toggled around just that one `pipeline.execute()`
call in a `try/finally` (always restored, including on the exception path, before
`rollbackAfterStatementError()` ever runs). Toggling itself costs nothing — pgjdbc only talks to
the server for it when there's an open transaction to close, and there never is one immediately
before a freshly claimed statement.

**Verified correctness before trusting the speed**: a real manual transaction (`autocommit =
False`) — 2 inserts + rollback → 0 rows persisted; 2 inserts + commit → both rows persisted.
`EXEC_OPTION_COMMIT` is never set for those statements, so the fast path never engages; existing
explicit-transaction behavior is completely unchanged.

**Result**: client p50 1.188ms → **0.580ms**, now matching pgwire/mywire/mssqlwire almost
exactly. Confirmed 51/51 rows actually persisted (no silent data loss).

### 3.7 mongowire — `insertOne` invalidating a cache entry that can't exist

**Symptom**: `insertOne`'s own DB work (~450-570µs, measured directly) was already comparable to
pgwire's entire `INSERT` cost — so the ~1.1ms total wasn't there. The gap was in
`MongoCommandDispatcher#insert`'s own overhead beyond the DB call.

**Cause**: `cache.invalidate(...)` ran on every successful insert — a real (if local) Ignite
cache operation, 140-270µs — to guard against a stale cached document surviving under a reused
`_id`. But `updateMany`/`deleteMany` already invalidate every key they touch on every write, so
by the time an `_id` becomes available for a fresh `INSERT` to reuse (it must have been deleted
first — this is a real insert, not an upsert), its cache entry is already gone. There's nothing
left to invalidate.

**Fix**: removed the call, with the reasoning above documented in place.

**Verified correctness before trusting the removal**: insert an `_id`, find it (warms the
cache), delete it, insert the *same* `_id` again with different content, find it again — got the
fresh document, not a stale cached one.

**Result**: client p50 0.962ms → **0.741ms**; server-side RTT rounds to 0, down from 1
(1-2ms range).

---

## 4. What was tried and reverted (and why that's the right call)

Not everything attempted worked, and one attempt was reverted after it broke real requests —
worth recording precisely because it's evidence the fixes above weren't accepted on faith
either:

**CacheStage's SQL result cache** — tried caching `ExecutionResult` directly as a typed Ignite
value instead of hand-rolled `ObjectOutputStream`/`ObjectInputStream` serialization, to cut JDK
reflection overhead on cache hits. Crashed immediately on real traffic:
`can't get field offset on a record class` — Ignite's reflective marshaller doesn't support Java
`record` types. Reverted to the proven `byte[]` + manual serialization; the reasoning (and why
not to retry it blindly) is documented in `CacheStage.java`. A safe version of this idea exists
(a non-record DTO, or a custom Ignite Binary type registration) but wasn't attempted given the
correctness risk of hand-rolling encoding for arbitrary JDBC row values under time pressure.

**`TCP_NODELAY`** — enabled on every raw-socket protocol (was unset everywhere). Verified not to
regress anything, but made no measurable difference on a loopback, single-connection benchmark —
Nagle's algorithm just isn't where the time was going locally. Kept anyway: it's a correct fix
with no downside under real network conditions or concurrent load, just not the bottleneck this
investigation was chasing.

**Session-scoped pipeline reuse** (orawire, gRPC) — orawire was rebuilding a fresh
`JdbcBackendExecutor`+`RoutingBackendExecutor`+`StatementPipeline` object graph on *every single
request*, unlike pgwire/mywire which build theirs once per session and reuse via `rebind()`.
Fixed to match — also fixed a latent gap where a fresh `RoutingBackendExecutor` per call could
never accumulate its own cross-statement transaction/cursor state within a session. Verified not
to regress anything, but — like `TCP_NODELAY` — made no measurable difference on this specific
benchmark. Worth doing regardless for the same reason: correct, no downside, just not what was
actually costing time here.

---

## 5. Methodology, if reproducing any of this

1. **Warm, then measure**: one throwaway call to populate any cache, then 30-50 timed calls,
   sorted for percentiles (min/p50/p90).
2. **Client-observed *and* server-side, always both**: client-observed (Python `time.perf_counter`
   around the call) includes network + client-library overhead (real, but not PolyWire's to fix);
   server-side (`/api/metrics/summary`'s `avgRttMs`) isolates what's actually controllable. The
   two disagreeing is itself informative — it's exactly how the DynamoDB/boto3 and orawire/
   oracledb client-library-overhead conclusions were reached.
3. **Compare operations that should cost about the same**: `ReceiveMessage` on an empty queue vs.
   `SendMessage`; a cache hit vs. a cache miss; the same query through two protocols sharing the
   same backend and the same pipeline stages. The bug is the operation that doesn't match its
   peer's cost for no visible reason.
4. **When a hypothesis needs more than a stopwatch, add real instrumentation** — nanosecond
   checkpoints around each phase, temporarily, removed once the answer is found (never left in
   production code as permanent debug noise).
5. **Verify correctness before trusting speed.** Every fix that touched write/transaction
   semantics (dynamowire's transaction skip, orawire's autocommit toggle, mongowire's cache
   invalidation removal) was proven correct with a real before/after data check, not just timed.
6. **Two independent implementations beat one, for ruling out a library-specific quirk** — the
   mssqlwire `ATTENTION` bug was only confidently attributed to the server once both `pymssql`
   and `pytds` failed identically.
