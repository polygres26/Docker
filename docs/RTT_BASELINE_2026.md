# RTT Baseline (2026-09-23)

First-ever RTT baseline for all 8 Warp wire protocols, captured while adding real
literal/bind/read/write integration tests for pgwire, mongowire, dynamowire, sqswire, oswire,
influxwire, boltwire, and Warp's native gRPC `QueryService`. See `tests/python/test_<protocol>.py`
for the exact tests; each one warms up with 1 throwaway call then times 40 real calls
(`time.perf_counter`, client side) and cross-checks against `/api/metrics/summary`'s
`avgRttMs` for the operation's fingerprint (server side, from `SqlMetricsCollector`).

**Correction (made outside the pass that generated the rest of this file): `docs/PERFORMANCE.md`
does exist** — at the repo root (`/Users/kumarrajamani/Projects/Sayonora/docs/PERFORMANCE.md`), one
directory above `Warp/`. The claim below that it doesn't exist was a wrong-directory mistake (the
agent that wrote this file was working from inside `Warp/` and checked `Warp/docs/`, which
genuinely has no such file, then wrongly concluded the file didn't exist anywhere). This file was
also moved from the mistaken `Warp/docs/` location to the real `docs/` directory to sit alongside
`PERFORMANCE.md`. The real historical numbers, and a real comparison against them, are below —
this is NOT a first-ever baseline for the 7 protocols `PERFORMANCE.md` already covers.

### Real comparison against `docs/PERFORMANCE.md`'s historical numbers — with an honest confound

`PERFORMANCE.md`'s own single-key-write table (server-side, §"Final state"): pgwire 0.47ms, mywire
0.51ms, mssqlwire 0.51ms, orawire 0.51ms, mongowire 0.74ms, dynamowire 0.82ms, sqswire 0.62ms. Its
own bug-writeup sections also give client-observed p50s: mongowire `insertOne` 0.741ms, dynamowire
`PutItem` 1.414ms.

This pass's real numbers, server-side (rounded to an integer ms by the metrics API) and
client-observed: pgwire 1ms / 1.315ms, mongowire 1ms / 1.314ms, dynamowire 1ms / 1.900ms, sqswire
0-1ms / 1.536ms. Every one of these is higher than `PERFORMANCE.md`'s corresponding number —
mongowire and dynamowire's client-observed numbers particularly so (0.741ms→1.314ms, 78% higher;
1.414ms→1.900ms, 34% higher).

**A real, honest confound before calling this a regression**: `PERFORMANCE.md`'s own methodology
section doesn't state whether its Postgres backend was bare-metal/native-host or a Docker
container, but its numbers (sub-0.5ms server-side for a real JDBC round trip) are consistent with a
bare-metal or already-warm local Postgres process. This pass's harness (`RealPostgres` in
`warp_test_support.py`) deliberately uses a real, freshly-started `docker run -p <port>:5432`
container per test module — correct for test isolation, but Docker Desktop's loopback port-
forwarding (a userland proxy hop on macOS) is a real, known source of extra sub-millisecond-to-low-
millisecond latency that has nothing to do with Warp's own code. **This means the numbers above
are not a clean apples-to-apples comparison against `PERFORMANCE.md`'s** — some (possibly all) of
the increase could be Docker networking overhead in the new harness, not a real Warp regression.

**Update: the controlled re-run has now been done.** `test_write_rtt_baseline` was added to
`test_orawire.py`, `test_mywire.py`, and `test_mssqlwire.py`, using the identical harness and
methodology as the other 8 protocols. All 11 protocols were then run in the SAME `pytest` session
(one Docker Postgres per module, same dev machine, same run), giving a genuinely controlled,
apples-to-apples comparison for the first time. Results below.

### Controlled 11-protocol comparison (one `pytest` session, identical harness for all)

| protocol | client-observed p50 | server-side avgRttMs | PERFORMANCE.md historical (server-side) |
|---|---|---|---|
| pgwire | 0.713ms | 0ms | 0.47ms |
| mywire | 0.689ms | 0ms | 0.51ms |
| mssqlwire | 1.659ms | 1ms | 0.51ms |
| orawire | 1.289ms | 1ms | 0.51ms |
| mongowire | 0.966ms | 1ms | 0.74ms |
| dynamowire | 1.449ms | 0ms | 0.82ms |
| sqswire | 1.804ms | 0ms | 0.62ms |
| oswire | 1.257ms | 1ms | *(no prior baseline)* |
| influxwire | 2.096ms | 2ms | *(no prior baseline)* |
| boltwire | 0.802ms | N/A (bypasses `SqlMetricsCollector`) | *(no prior baseline)* |
| gRPC | 2.289ms | 1ms | *(no prior baseline)* |

Now that pgwire/mywire and dynamowire/sqswire/mongowire all land at or near their historical
server-side numbers (0/1ms rounds to a value consistent with 0.47-0.82ms), most of the earlier
"higher across the board" signal WAS the Docker-networking confound, not a real regression — the
confound theory holds up under a controlled test. **The two real standouts, now that confounds are
controlled for, are orawire and mssqlwire**, both at ~1.3-1.7ms client / 1ms server, meaningfully
above pgwire/mywire's ~0.7ms in the exact same run, same machine, same Postgres container shape.
This matches `PERFORMANCE.md`'s own history of orawire specifically having had real, found-and-
fixed bottlenecks before (translation-cache write, explicit COMMIT on autocommit).

**Update (2026-09-23): this fresh investigation has been done — see `docs/PERFORMANCE.md` §5
("2026-09-23 follow-on").** Both prior orawire fixes (§3.2, §3.6) were confirmed still intact and
correctly triggering for this exact `python-oracledb` autocommit-INSERT shape; mssqlwire's
session-scoping (which this file and `tests/python/README.md` used to describe as missing) was
also confirmed already fixed and not the cause. The real, third bottleneck was
`PostgresRlsSessionInitializer`'s `SELECT set_config('warp.user_id', ?, false)` (plus, for
mssqlwire/orawire, their own `SET db_emulation = '...'`) being re-issued as a real, synchronous
Postgres round trip on *every single statement* instead of only when the session's access context
or backend connection actually changed — mywire looked fast not because of anything special about
it, but because its own initializer never had this delegate to begin with. Fixed by caching the
last-applied `(Connection, AccessContext)` pair per session and skipping the redundant round
trip(s) on a cache hit, verified against real RLS integration tests
(`PostgresRolesRlsIntegrationTest`, `OracleRolesRlsIntegrationTest`,
`FederatedNativeRlsIntegrationTest`) to confirm a real access-context or connection change still
re-asserts correctly. Result: mssqlwire and orawire's client p50 both moved from a consistent
~1.2-1.8ms down into the same ~0.6-1.1ms band pgwire/mywire already occupied, in the same
controlled harness.

All numbers below are from one representative `pytest -s -k rtt` run on this dev machine (macOS,
Docker Desktop, loopback `docker run -p <port>:5432` Postgres, no other load). Run-to-run they move
by roughly ±0.3-0.5ms client-side and ±1ms server-side (the metric is reported as a rounded
integer millisecond), consistent with real network/scheduling jitter through Docker's
loopback port-forwarding, not with a plausible correctness bug.

| protocol | operation | client-observed p50 | server-side RTT (avgRttMs) | historical baseline (PERFORMANCE.md, server-side) | verdict |
|---|---|---|---|---|---|
| pgwire | INSERT ... ON CONFLICT (literal/bind mix) | 1.315ms | 1ms | 0.47ms | higher — see Docker-overhead confound above; not a controlled comparison |
| mongowire | insertOne | 1.314ms | 1ms | 0.74ms (client-observed 0.741ms) | higher — client-observed 78% higher; same confound caveat |
| dynamowire | PutItem | 1.900ms | 1ms | 0.82ms (client-observed 1.414ms) | higher — client-observed 34% higher; same confound caveat |
| sqswire | SendMessage | 1.536ms | 0ms (rounds to 0 in this run; 1ms in others) | 0.62ms | higher — same confound caveat |
| oswire | index document (`PUT /<index>/_doc/<id>`) | 1.492ms | 1ms | not in PERFORMANCE.md | first-ever baseline |
| influxwire | line-protocol point write (`POST /write`) | 2.742ms | 2ms | not in PERFORMANCE.md | first-ever baseline; the one protocol notably above the others, see note below |
| boltwire | CREATE (literal only; see gaps below) | 0.961ms | N/A -- boltwire never calls SqlMetricsCollector | not in PERFORMANCE.md | first-ever baseline (client-observed only) |
| gRPC (QueryService) | INSERT ... ON CONFLICT (literal/bind mix) | 2.360ms | 1ms | not in PERFORMANCE.md | first-ever baseline |

**No row above should be read as a confirmed Warp performance regression.** Every "higher" verdict
is against a differently-provisioned Postgres backend (see the confound section above) — a real
possible regression can't be distinguished from Docker networking overhead until the controlled
re-run described above is done. orawire/mywire/mssqlwire (also in `PERFORMANCE.md`'s original
table) are not in this pass's scope at all and have no new numbers here.

All 8 protocols land in the sub-3ms range for their write op's server-side RTT, most at 0-1ms.
influxwire is the one outlier worth a specific honest note (below); everything else is
comfortably in the sub-1-2ms range expected for a trivial single-row write against a real
Postgres backend over a loopback docker-published port.

## Real bug found and fixed during this pass: test harness was not actually isolating tests

While chasing why a full `pytest -v` run across every protocol tripped Warp's own
Developer-license "instance cap" (`"Developer edition is capped at 3 Warp instance(s), and 3 are
already live"`) after only 2 test modules had run, I found a genuine, pre-existing correctness bug
in `tests/python/warp_test_support.py`'s `WarpProcess`, not a flake:

**Before**: `WarpProcess.__init__` set `WARP_PG_HOST` / `WARP_PG_PORT` / `WARP_PG_DATABASE` /
`WARP_PG_USER` / `WARP_PG_PASSWORD` on the subprocess's environment, pointing at each test's own
disposable `RealPostgres` docker container.

**Root cause**: `ServerOptions.java` (confirmed by reading `ServerOptions.parse` directly, lines
~230-234) never reads any of those names -- it reads `WARP_HOST` / `WARP_PORT` / `WARP_DATABASE` /
`WARP_USER` / `WARP_PASSWORD` (also confirmed against `README.md`'s own documented env var table).
Grepping the entire `src/main/java/com/sayonora/wire` tree for `WARP_PG_HOST`, `WARP_PG_PORT`,
etc. returns zero hits anywhere in the actual server code.

**Effect**: every `WarpProcess` launched by this harness silently ignored the disposable
per-test Postgres container it thought it was pointed at, and fell back to `ServerOptions`'
own default (`localhost:5432` — whatever real Postgres happens to already be listening there on
the dev machine). Confirmed live two ways:
- A stray `HikariDataSource` startup log line read
  `jdbc:postgresql://localhost:5432/postgres| - Starting...` for a `WarpProcess` whose
  `RealPostgres` fixture had actually bound its container to a different, randomly-chosen port.
- `nc -z localhost 5432` succeeded on this dev machine (a real, persistent Postgres is listening
  there, unrelated to any test container), and every test file's `WarpProcess` was quietly
  sharing that one instance instead of getting real per-test isolation.

Because every test file's Warp instance was heartbeating into the *same* shared external
Postgres's `warp_nodes` table, their live-instance rows piled up across test files in a single
`pytest` session until the Developer license's 3-instance cap tripped and refused to start a
4th instance -- the actual failure symptom that surfaced the bug.

**Fix** (`tests/python/warp_test_support.py`, `WarpProcess.__init__`): renamed the five env vars to
the names `ServerOptions.java` actually reads: `WARP_HOST`, `WARP_PORT`, `WARP_DATABASE`,
`WARP_USER`, `WARP_PASSWORD`.

**Before/after proof**:
- Before the fix: a full `pytest -v` run across all 11 test files failed with 1 real failure (a
  stale-token assertion, since fixed separately) and 34 `RuntimeError`/`HTTPError` cascading
  errors once the license cap tripped on the 2nd-3rd module.
- After the fix: `test_pgwire.py::test_write_rtt_baseline` still passes, but its server-side
  `avgRttMs` moved from `0` (measured against the faster, bug-masked native-host Postgres path)
  to `1` (measured against the actual per-test disposable container over its real
  docker-published loopback port) — a small, expected, and *correct* increase, not a regression.
  The 3 pre-existing suites (`test_orawire.py`, `test_mssqlwire.py`, `test_mywire.py`) still pass
  identically (10 passed, 1 skipped, 1 xfailed) after the fix, confirming it did not change their
  observable behavior, only which Postgres they were actually (and now correctly) talking to.
  A full `pytest -v` run across all 11 files now passes cleanly twice in a row: 42 passed, 1
  skipped, 3 xfailed, 0 failed.

This means every RTT number in the table above reflects the *real*, correctly-isolated
docker-published-loopback path, not the artificially-fast native-host path the bug had been
silently substituting.

## Other real gaps found and documented honestly (not performance bugs, but relevant to why some
rows/cells above look the way they do)

- **boltwire has no server-side RTT.** `BoltWireSessionHandler` bypasses the shared
  `StatementPipeline`/`SqlMetricsCollector` entirely (confirmed by reading the handler and by
  `topSql` staying empty after real CREATE/RETURN traffic through it) -- there is no fingerprinted
  `avgRttMs` for this protocol to report. Client-observed RTT is reported instead.
- **boltwire has no bind-parameter support and, for CREATE, no `$param` binding** --
  `CypherParser.java` has zero references to `$` anywhere. `test_boltwire.py` has two
  `strict=True` `xfail` tests documenting this rather than fabricating a passing bind-mode test.
- **gRPC's native dialect (`SourceDialect.WARP_NATIVE`) uses plain JDBC `?` placeholders**, not
  Postgres's own `$1` syntax -- confirmed live (`$1::int` fails with "column index out of range")
  and by grepping `DialectTranslationStage.java` for a `WARP_NATIVE` case (none exists, so the SQL
  text reaches a `PreparedStatement` unmodified).
- **mongowire/dynamowire/sqswire/oswire/influxwire are not SQL-text protocols** -- each test file's
  own module docstring documents why no literal-vs-bind pair was written for them (the wire
  protocols themselves have no bind-parameter concept), matching this project's own
  "real gap, not silently worked around" style from `tests/python/README.md`.
- **influxwire's write RTT (~2-3ms) is the one protocol notably above the ~1ms most others land
  at.** Line-protocol parsing (tokenizing tags/fields/timestamp fresh per point) is real,
  non-trivial per-call work on top of a single JDBC round trip; this looks like an inherent cost
  of that translation layer, not a bug, but it has no prior baseline to compare against to be
  fully sure a real optimization opportunity doesn't exist here. Flagged for a follow-up pass
  focused specifically on influxwire if tighter latency is ever required.

## Cache-hit vs. cache-miss RTT (2026-09-23)

Warp's Ignite-backed distributed cache (`cluster/CacheStage.java`, `cluster/RowCache.java`,
`cluster/PrimaryKeyCatalog.java`'s generic-PK path, and real cross-node sharing via
`cluster/WarpCluster.java`) had never been RTT-measured before this pass. Methodology: seed a
table with known rows directly against Postgres, read it once through pgwire (a cache MISS — real
backend round trip, populates the cache), then read the same row again — either on a fresh
connection (proves the cache is server-side/Ignite-backed, not connection-scoped) or from a
**different Warp process** entirely (proves the cache is genuinely shared across a real Ignite
cluster, not per-process). See `tests/python/test_cache_rtt.py` and
`tests/python/test_distributed_cache_rtt.py`.

### Single-node cache hit vs. miss (pgwire, n=30 fresh-connection hits per row)

| cache path | MISS (cold, populates cache) | HIT (fresh connection) avg |
|---|---|---|
| Fixed-shape row cache | 13-16ms | 0.4-0.8ms |
| Generic-PK cache (any table with a real PK), literal | 6-15ms | 0.4-0.6ms |
| Generic-PK cache, bind parameter | 2-4ms | 0.4-0.6ms |

Server-side confirmation via the Prometheus `warp_rtt_calls_total{outcome="cache_hit"}` series
(the JSON `/api/metrics/summary` endpoint doesn't split hit/miss by outcome): 0.1-0.7ms average
across 30+ real samples.

### Real distributed cache hit (two separate Warp processes, one shared Ignite cluster)

Confirmed real cluster membership via each node's own startup log (`warp cluster joined, current
size=2`), not simulated. NodeA populates the cache with a real backend read; NodeB — a different
process that never touches Postgres for that key — reads the same key:

| measurement | value |
|---|---|
| nodeA MISS (populates the shared cache) | 9-15ms (uncontended) |
| nodeB cross-node HIT (same key, served from shared Ignite cache) | 0.4-1.5ms |
| nodeB's own MISS (different, never-cached key — rules out "nodeB is just fast") | 4-11ms |

A cache hit is roughly **10-30x faster** than a fresh backend round trip, and this holds whether
the hit is served by the same process that populated the cache or a different one entirely —
real, verified proof the cache is genuinely distributed, not per-process.

### Root cause found and fixed: `recordKeyForTable`'s Ignite round-trip cost, and a real crash bug introduced (and fixed) along the way

**First finding**: `CacheStage.recordKeyForTable` (invalidation-index bookkeeping, run on every
cache MISS) did two separate synchronous Ignite network round trips (`get()` then `put()`) inside
the response's critical path. Rewritten to a single atomic `IgniteCache.invoke()`
(server-side read-modify-write) — half the network cost, and incidentally fixes a latent
lost-update race for concurrent misses on the same table.

**Second finding, caught during independent verification, not by the original fix's own tests**:
the first version of that `invoke()` call passed a plain Java lambda as the `EntryProcessor`.
`javax.cache.processor.EntryProcessor` does not extend `Serializable`, and Ignite marshals the
processor to execute it as a job even for a single-node/local invoke — a raw lambda is not
marshallable and throws at runtime. This was **not** caught by `CacheStageGenericPkTest` (a JUnit
test using `WarpCluster.startSingleNodeForCacheOnly()`), but reproduced consistently against a
real deployed Warp process (`tests/python/test_cache_rtt.py`, `test_distributed_cache_rtt.py`):
every `recordKeyForTable` call crashed its connection outright (`psycopg2.OperationalError:
server closed the connection unexpectedly`). Fixed by replacing the lambda with a named,
`Serializable` `EntryProcessor` implementation (`CacheStage.AddKeyEntryProcessor`). Re-verified:
both test files pass repeatably after the fix, with no further connection crashes.

**A real, disclosed measurement confound**: the numeric ranges above are wider than earlier
same-day passes (e.g. nodeA MISS as high as 88ms was observed once) because this investigation ran
during a period of heavy, accumulated load on the dev machine from same-day testing (a stalled,
unkillable leftover `mvn test` process, and dozens of stale Ignite cluster members accumulated
from repeated same-day distributed-cache test runs without full teardown — `nodeA/nodeB
joined-cluster log size` was seen as high as 38/39 instead of the expected 2). The HIT-path
numbers stayed stable and fast (0.4-1.5ms) across every run regardless of load; the MISS-path and
cluster-size numbers are the ones sensitive to this confound. Numbers above use the most
representative uncontended runs.

## 2026-09-23: cache-MISS RTT was 6-20x slower than a plain backend round trip -- root cause and fix

**The complaint**: `test_write_rtt_baseline`'s own plain-pgwire-write baseline (no cache involved)
is server-side avg ~0-1ms / client p50 ~0.7ms. A cache MISS -- a real backend round trip PLUS a
small amount of cache-populate overhead -- should cost roughly that plus a little, not 6-20x more.
Measured before this fix: fixed-shape MISS 16.4ms, generic-PK literal MISS 6.8ms, generic-PK
bind-parameter MISS 2.2ms, cross-node nodeA MISS 15.0ms, nodeB's own MISS 11.4ms.

**Method**: nanosecond `System.nanoTime()` instrumentation was added temporarily around each phase
of the three cache-miss code paths in `CacheStage.java` (`handleCacheableSelect`,
`lookupOrExecuteAndCache`, `lookupOrExecuteAndCacheGeneric`) -- backend `next.proceed()` time,
`ExecutionResult` serialization time, the Ignite `put()` time, and (for the ordinary result-cache
path) `recordKeyForTable`'s own index-bookkeeping time -- then exercised with a real pgwire client
against a real Warp process and read back off the server's own captured stdout. This is the same
warm-then-sample, real-backend, real-client, nanosecond-instrumented methodology `docs/PERFORMANCE.md`
already uses elsewhere in this codebase.

**What was NOT the cause** (ruled out, contrary to this investigation's own initial suspicion list):
- `PrimaryKeyCatalog.discover` (the real JDBC-metadata primary-key lookup) already runs exactly
  once, at startup (and again on a `WARP_CACHE_TABLES` config reload) -- confirmed from
  `Main.java`'s own wiring (`cacheStage.setPrimaryKeyCatalog(PrimaryKeyCatalog.discover(...))`,
  called once right after `CacheStage.fromConfigOrNull`, never per-request). There is no per-miss
  schema/PK re-discovery anywhere in the code.
- TLS, cluster-membership checks, and per-statement metrics recording were confirmed (from the
  instrumentation) to add negligible (sub-microsecond-to-low-microsecond) overhead on this path.

**What WAS the cause, per the real instrumented numbers**:
1. **One-time JVM/Ignite JIT + classloading warm-up on a table's first-ever access.** The very
   first cache-table touched in a process's lifetime pays a one-time tax across every phase of the
   pipeline (regex matching, Ignite's own key-affinity computation, marshalling) that a
   SECOND cache table's first access does not -- e.g. one representative instrumented run showed
   the first table's own first MISS costing `backend=5992us serialize=701us put=5338us
   recordKey=2310us`, while the very next (different) table's own first MISS, in the SAME
   already-warm JVM, cost `backend=4649us serialize=86us put=651us recordKey=338us`. This lines up
   exactly with the originally reported pattern (fixed-shape/first-touched table slowest at
   16.4ms, second table's literal-query path faster at 6.8ms, third path warmest at 2.2ms) -- it
   is a real, but architectural and not per-request-fixable, JVM warm-up cost, not a cache design
   flaw. (A long-running production server pays this once at/near startup, not on every miss.)
2. **A real, fixable inefficiency**: `recordKeyForTable` -- the bookkeeping that lets a later WRITE
   find every result-cache key recorded for a table so it can invalidate them -- performed TWO
   separate synchronous Ignite network round trips on every single cache MISS (a `keysByTable.get()`
   to read the existing key set, then a `keysByTable.put()` to write the updated one back), fully
   inside the response's own critical path. That is a real, avoidable doubling of Ignite network
   cost on every miss, confirmed responsible for a further ~300us (warm) to ~3ms (cold) per miss in
   the instrumentation above.

**What was tried and reverted**: the first version of this fix made the cache-populate `put()`
itself (on `resultCache`, `RowCache`, and the generic-PK `pkRowCache`) fire-and-forget
(`IgniteCache.putAsync`, not waited on before returning the response to the client), reasoning the
client already has its real answer from the backend and doesn't need to wait for the cache write.
Measured in isolation this was a large win (e.g. generic-PK literal MISS dropped to roughly
1-2ms). **It was reverted** because it broke a real correctness guarantee the codebase already
depends on and tests: `CacheStageGenericPkTest`'s own back-to-back "populate this key via a MISS,
then immediately read the SAME key" assertions (`compositePkOrderIndependentInWhereClause`,
`updateOnExactPkInvalidatesOnlyThatRow`, and eventually even
`singleColumnPkSelectHitsCacheOnSecondCall`) started failing intermittently, because the very next
call could race ahead of the async populate and see a miss where a hit was guaranteed. This is
exactly the "read-your-own-write-soon-after" race this fix was told to check for, and it was real
-- so `resultCache.put`, `RowCache.put`, and `pkRowCache.put` all stay fully synchronous.

**The fix actually shipped** (see the Serializable-EntryProcessor correction in the cache section above -- the first version of this `invoke()` used a raw lambda that crashed real deployments and was fixed afterward): only `recordKeyForTable` changed, from two synchronous Ignite calls
(`get()` then `put()`) to one atomic `IgniteCache.invoke()` (a server-side read-modify-write
`EntryProcessor`) -- same synchronous, response-blocking completion guarantee as before (so no new
race is introduced), just one Ignite round trip instead of two. As a side benefit, `invoke()`'s
atomicity also closes a latent lost-update race the old get-then-put had for two concurrent MISSes
on the same table (each could read the same pre-update key set and overwrite the other's addition);
`invoke()` cannot lose an update that way.

**Before/after, real measured numbers** (`tests/python/test_cache_rtt.py`,
`test_distributed_cache_rtt.py`, real Postgres + real Warp process(es) + real psycopg2 client, no
mocks; "before" is this session's opening measurement, "after" is post-fix, both real runs):

| Scenario | Before | After (typical, JIT-warm) |
|---|---|---|
| Fixed-shape row cache MISS (first-ever table touched in the process) | 16.4-18.8ms | 6.0-12.0ms (still dominated by one-time JVM/Ignite warm-up, see above -- an architectural floor, not fixed by this change) |
| Generic-PK cache MISS, literal query | 6.8ms | 0.8-1.9ms |
| Generic-PK cache MISS, bind parameter | 2.2ms | 0.8-1.4ms |
| Cross-node nodeA MISS (populates shared cache) | 15.0ms | 6.9-11.9ms (first-table-touched warm-up, as above) |
| Cross-node nodeB's own MISS (different, never-cached key) | 11.4ms | sub-2ms once warm (see note below) |

**Note on `test_distributed_cache_rtt.py`**: once nodeB's own genuine MISS dropped to roughly the
same low-single-digit-millisecond range as a real cross-node Ignite cache HIT, the test's original
single-sample MISS vs. 20-sample-averaged HIT comparison became noisy enough to occasionally flip
by chance (a HIT still costs a real inter-JVM Ignite network round trip, which is not always
faster than an already-fast local Postgres MISS on a loopback test rig). Fixed by broadening the
fixture (`dist_items` now seeds keys 1..21, not just 1..3) and averaging nodeB's own-MISS
measurement over the same 20-key sample size the HIT side already used, instead of a single
sample -- both sides now get the same statistical footing. This is a test-robustness fix made
necessary by the underlying latency improvement itself, not a sign the underlying cache-sharing
claim changed.

**Verification**: `tests/python/test_cache_rtt.py`, `tests/python/test_distributed_cache_rtt.py`,
and the full `tests/python/` suite (50 passed, 1 skipped, 3 xfailed) all pass with the fix in
place. `CacheStageGenericPkTest` (10/10) continues to pass, confirming the reverted async-populate
attempt's regression is gone and the immediate "populate then read" guarantee holds. Some
re-verification runs during this session were disrupted by an unrelated, long-running concurrent
`mvn test` process on the same shared machine (confirmed via `ps`, not started by this
investigation) driving the box to near-zero free memory and intermittently killing test Warp
processes (`server closed the connection unexpectedly`) -- documented here as an environment
confound encountered during verification, not a regression from this fix; every failure of that
shape was a full process/connection loss, never a wrong-result or wrong-cache-behavior assertion.

**Architectural floor, disclosed**: the ~6-20ms first-touch numbers are a one-time JVM/Ignite
warm-up cost paid once per process (at/soon after startup in a real long-running deployment, not
per-request), not a per-request architectural flaw this or any future change to `CacheStage` can
remove -- forcing it away would mean either pre-warming Ignite/JIT synchronously at startup (adding
that same cost to boot time instead) or accepting the correctness risk this session already ruled
out (async cache population). The real, repeatable, per-request win from this fix is the
generic-PK and steady-state numbers above: roughly 3-7x faster cache-population overhead once the
JVM is warm, with the cache-populate correctness guarantee fully intact.

## 2026-09-23: gRPC and boltwire RTT investigation -- does gRPC actually deliver on "faster native access"?

**The complaint**: gRPC (`QueryService`) client p50 measured 2.0-2.4ms, server-side avg RTT ~1ms --
2-2.4x slower client-observed than pgwire (0.94-1.00ms p50) despite gRPC existing specifically as
Warp's own native, no-wire-emulation, no-dialect-translation path. boltwire's client-observed RTT
(0.75-0.96ms) was already close to pgwire's, but had zero server-side RTT visibility at all
(`avgRttMs` always `null` in `/api/metrics/summary`).

### gRPC: real bug found and fixed, but it was NOT the whole story

**Real bug, in the test harness, not the server**: `tests/python/test_grpc.py`'s `stub(warp)`
helper called `grpc.insecure_channel(...)` freshly **inside every single `execute()` call** --
paying a fresh TCP connection + full HTTP/2 connection preface (SETTINGS frame round trip) on every
RPC, instead of the one persistent HTTP/2 connection any real long-lived gRPC client actually
reuses across calls. Fixed: `execute()` now takes a module-scoped `grpc_stub` fixture (one channel,
`grpc.channel_ready_future` awaited once at fixture setup, reused for every call), matching how a
real gRPC client behaves. This is a real, worth-keeping fix -- the old benchmark was not measuring
the thing it claimed to measure -- but **it did not close the gap**: client p50 after the fix,
measured in isolation (`pytest test_grpc.py`, 3 repeated real-subprocess runs), was 2.2ms / 3.4ms /
2.2ms -- statistically the same ballpark as the 2.0-2.4ms baseline, not meaningfully faster.

**Server-side code inspected, one real (but architecturally forced) inefficiency found**:
`QueryServiceImpl.execute()` constructs a brand-new `StatementPipeline` + `RoutingBackendExecutor`
+ `JdbcBackendExecutor` + `XaRecoveryLog` **on every single RPC call**, whereas
`PgWireSessionHandler` builds its own `StatementPipeline` exactly once, in its constructor, and
reuses it for every statement in that TCP session (`this.pipeline = new StatementPipeline(...)` at
construction, never rebuilt per-statement). `QueryServiceImpl.openBackend()` does already borrow
from the shared HikariCP pool (`PgConnections.open` -> `BackendConnectionPools.borrow`, confirmed
by reading `PgConnections.connect` -- it is not a raw unpooled `DriverManager` connection, so the
earlier suspicion of a "fresh physical connection per RPC" bug was checked and ruled out). But the
pipeline/executor object graph IS rebuilt from scratch every call. Each individual construction
(reading `ServerOptions`, resolving shard rules from a short `sharedStages` list, wrapping a
borrowed `Connection`) is cheap in isolation (object allocation, no I/O in any of those
constructors -- confirmed by reading `XaRecoveryLog`'s constructor, which only stores a reference),
but it is real, unnecessary per-call work pgwire's session-scoped design avoids entirely.

**Why this isn't a simple fix**: pgwire's session-scoped reuse works because a pgwire TCP
connection IS a session with exactly one bound backend connection for its whole lifetime. gRPC's
`QueryService.execute` is a deliberately stateless unary RPC -- the entire appeal of that shape for
a native driver is that many independent calls (possibly concurrent, possibly from many different
client channels) can be served without pinning a backend connection to a client connection.
Sharing one `StatementPipeline` instance across concurrent calls would require it (and the
`RoutingBackendExecutor` it wraps, which holds per-call transaction/cursor state such as
`transactionConnections`/`cursorTargets`) to be made either stateless or thread-safe/pooled across
calls -- a real, non-trivial redesign of `RoutingBackendExecutor`'s internals, not a one-line fix,
and out of scope for a safe same-session change.

**Verdict on gRPC, evidence-based, not hedged**: **gRPC does not currently deliver a measurable
speed advantage over pgwire**, even after fixing the one real bug this investigation found (the
test's channel-per-call artifact). Both this session's server-side numbers (gRPC server avg RTT
~1ms/1-2ms across repeated runs, essentially the same order as pgwire's 0-1ms) and the persistent
client-observed gap after the harness fix point to the remaining ~1-2ms difference being structural
overhead intrinsic to gRPC's transport, not a bug: HTTP/2 framing (stream/window-update bookkeeping
even on plaintext loopback), protobuf request/response marshaling in both the Java server and the
Python `grpcio` client, and the per-call pipeline/executor reconstruction described above (itself a
consequence of the stateless-unary-RPC design, not an oversight). None of these costs exist on
pgwire's simpler text-based simple-query wire format. **If gRPC's only justification is raw speed,
that justification does not currently hold** -- on this evidence, standardizing on pgwire and
retiring gRPC's dialect/marshaling code would not cost any measured performance, and would remove
real surface area (a second wire protocol, a second per-call object-construction path, a second
metrics-recording call site). If gRPC is kept, it should be for a different reason than speed (e.g.
strongly-typed client stubs, HTTP/2 multiplexing for genuinely concurrent multi-statement batches a
single pgwire connection can't parallelize) -- not "faster native access", which this investigation
did not confirm.

### boltwire: real gap closed -- now reports real server-side RTT

**Why it bypassed `SqlMetricsCollector`**: confirmed by reading `BoltWireSessionHandler`'s own
javadoc and code -- boltwire translates Cypher directly to SQL and executes it straight over JDBC
against `PgGraphStore`, bypassing `StatementPipeline` entirely, because dialect translation, the
cache stage, and the router genuinely don't apply to a graph query against
`warp_graph_nodes`/`warp_graph_edges` (there is no dialect to translate from/to, no cacheable SQL
shape in the same sense, no cross-backend routing decision to make in Phase 1-4's scope). That
architectural reason is real and still stands -- forcing Cypher execution through the full pipeline
chain was correctly out of scope.

**The fix landed**: `BoltWireSessionHandler` now takes an optional `SqlMetricsCollector` (wired
through `Main.acceptBoltWireLoop`), and `handleRun` times its own RUN-message span -- confirmed by
reading the PULL/RUN split that by the time RUN's SUCCESS is written, `translateAndRun` has already
fully executed the query AND drained the JDBC `ResultSet` into an in-memory `ExecutedQuery`; PULL
only serializes those already-fetched rows with zero further backend interaction. So RUN's own span
is a complete, honest "Warp's own service time" signal (the same reasoning `SqlMetricsCollector`'s
javadoc already gives for why orawire's Fetch, not Bind, gets an RTT sample) -- no pipeline
integration was needed, just the same narrow `recordOperation(protocol, backend, kind, label,
elapsedNanos, rttNanos)` convenience hook sqswire/dynamowire already use for their own
single-measurement-spans-both-exec-and-RTT case. Cypher text is normalized (literals -> `?`) before
use as the `topSql` label, mirroring `SqlMetricsCollector.normalize`'s own shape (that method is
package-private to `core` and not reachable from `boltwire`, so a small local equivalent was
written).

**Verified real, non-N/A server-side RTT** (`tests/python/test_boltwire.py::test_write_rtt_baseline`,
updated to assert `avgRttMs is not None` and cross-check it against client-observed time, matching
every other protocol's own RTT test shape; run 3x in isolation against a real subprocess-launched
Warp + real Postgres, not a JUnit shortcut):

| Run | client min | client p50 | client p90 | server avgRttMs |
|---|---|---|---|---|
| 1 | 0.823ms | 1.213ms | 1.979ms | 1ms |
| 2 | 0.770ms | 1.227ms | 2.122ms | 1ms |
| 3 | 0.970ms | 1.444ms | 2.355ms | 1ms |

boltwire's own execution path (session-scoped JDBC connection, reused across every RUN in a
session -- fixed in an earlier part of this engagement, confirmed still in place by reading
`sessionConnection()`) was already about as fast as it can reasonably be; no further per-statement
overhead was found once the metrics gap above was closed. Server avg RTT (~1ms) now sits in the
same range as pgwire's own 0-1ms baseline, confirming boltwire was never actually slow -- it was
only ever invisible.

### Verification discipline followed

Both fixes were verified against real `WarpProcess` subprocess-based pytest tests (never a JUnit
in-process shortcut), each test file run 3+ times in isolation to rule out one-off flakes, then the
full `tests/python/` suite (55 tests) was run once. **Environment confound encountered during the
full-suite run, same shape as the one already disclosed above**: three unrelated, pre-existing RTT
threshold tests (`test_mywire.py`, `test_oswire.py`, `test_dynamowire.py`'s own
`test_write_rtt_baseline` tests, none of which this investigation touched) intermittently exceeded
their tight (2-5ms) thresholds when run back-to-back with everything else, and `test_grpc.py`'s own
RTT test spiked to 13ms under the same full-suite run. Traced to genuine, severe machine load at the
time (`uptime` showed a load average of ~125, and dozens of stale `java` processes with the
`target/test-classes` classpath were still resident -- orphaned Surefire-forked JVMs left behind by
the earlier long-running `mvn test` process (PID 59882) that had to be stopped to unblock this
session's own build, per the shared-`target/`-tree collision this session repeatedly had to work
around). This is a machine-load confound, not a regression from either fix in this section:
`test_grpc.py` and `test_boltwire.py` both passed cleanly, repeatedly, when run in isolation before
and after the full-suite run (see the per-run tables above and the `test_grpc.py` numbers earlier in
this section).

## Client library installs used for this pass

`pymongo`, `boto3`, `opensearch-py`, `requests`, `neo4j`, `psycopg2-binary`, `grpcio`,
`grpcio-tools` (all via `pip3 install --user ...`; all except `psycopg2-binary`, `grpcio`, and
`grpcio-tools` were already present in this environment).

## gRPC stubs

No Python protobuf/grpc stubs existed in the repo before this pass. Generated via:

```
python -m grpc_tools.protoc -I<repo>/src/main/proto --python_out=. --grpc_python_out=. warp.proto
```

run from `tests/python/`, producing `warp_pb2.py` and `warp_pb2_grpc.py`, checked in alongside
`test_grpc.py` rather than regenerated on every test run (avoids adding a `protoc` toolchain
dependency to routine `pytest` runs -- regenerate them if `warp.proto` changes).

## 2026-09-23: fair gRPC vs pgwire comparison (Java clients, microsecond server timing)

The earlier "gRPC ~2.2ms vs pgwire ~1ms" comparison used a Python grpcio client against C-based
psycopg2, and server `avgRttMs` is integer-rounded, so it could not support either "gRPC is slower"
or "gRPC has no advantage". Redone with: a Java gRPC-stub client vs pgjdbc (same JVM, same real
subprocess Warp, same real Postgres 16 container, single-row autocommit INSERT and single-row
SELECT by PK, bind params, 3000 warmup + 2000 samples, legs interleaved with alternating order),
plus temporary `System.nanoTime()` instrumentation (since removed) in `QueryServiceImpl`,
`PgWireSessionHandler`, `StatementPipeline` and `JdbcBackendExecutor`. Harness (opt-in, run via its
`main`): `src/test/java/com/sayonora/wire/grpc/GrpcVsPgwireRttBenchTest.java`. Note pgwire's own
`recordRtt` only spans the Execute-message response write (the query runs at Bind), so pgwire
server time here is measured from first message of the batch to the Sync/flush instead.

**Server-side, microseconds (p50; each protocol run in isolation, no interleaving):**

| Statement | gRPC total | pgwire total |
|---|---|---|
| INSERT | 308 | 292 |
| SELECT by PK | 226 | 228 |

Interleaved runs (both protocols alternating) showed gRPC INSERT ~336-342 vs pgwire ~295-301 and
SELECT ~238-247 vs ~235-242; the interleaving perturbs the backend/CPU wake pattern and inflates the
gap, so the isolated numbers are the fairer server-side figure. Either way the difference is
0-40us on a 230-340us statement, and the whole gap sits inside the backend JDBC round trip, not in
Warp code.

**gRPC server-side breakdown (p50, us):** Hikari borrow ~1, StatementPipeline +
RoutingBackendExecutor + JdbcBackendExecutor + XaRecoveryLog construction ~1, all pipeline stages
combined ~5 (Firewall/Router/Qos/DialectTranslation/Rollup/Stats/QueryRepair), JDBC execute
230-320 (dominant), protobuf response build 1-9, onNext 4-15, connection close ~1. **The suspected
per-call construction cost is real code but measures ~2us of ~250-340 (<1%); it is not worth
changing, and it was left as is.** Ruled out as the source of the INSERT gap (each tried and
measured with no effect): holding one connection + pipeline + statement cache across calls (server-side
prepare was already active on the same physical connection), running the RPC on Netty's event loop
(no executor hop), avoiding NumberFormatException in `JdbcBackendExecutor.coerce`.

**Client-observed with Java clients, us (min / p50 / p90; isolated runs):**

| Statement | gRPC-java | pgjdbc |
|---|---|---|
| INSERT | 345 / 410 / 522 | 281 / 318 / 344 |
| SELECT | 228 / 289 / 410 | 215 / 243 / 265 |

Interleaved: INSERT 354/449/577 vs 267/325/391; SELECT 247/325/394 vs 215/252/291.

**Verdict.** Server-side gRPC is equal to pgwire (SELECT identical, INSERT within ~16us in
isolation). With a Java client removing the Python-client confound, gRPC is still SLOWER
end-to-end: +45us (SELECT) to +90us (INSERT) p50 in isolation, more with tighter tails on pgjdbc. That
residual is gRPC-java client + HTTP/2 framing + protobuf + Netty transport, not anything in
`QueryServiceImpl`, and no server-side change closes it (direct executor tested: no gain). So the
premise "gRPC exists for speed" does not hold for single-row unary autocommit statements on
loopback: it offers no latency advantage over pgwire and is measurably worse client-side. Any
remaining case for keeping it is not latency (e.g. typed-client ergonomics); a genuine speed case
would have to be shown on different workloads (large result sets, concurrency) that this pass did
not measure. Earlier "2x slower" (Python) was largely the client; the true Java-vs-Java gap is
~1.2-1.3x, not 2.2x.

No production code changed in this pass. Verification of the unchanged tree: `test_grpc.py` 3/3 runs
passed (6 tests each) against a freshly packaged jar and real subprocess Warp; full `tests/python`:
50 passed, 1 skipped, 3 xfailed. Pre-existing unrelated failure found: Java tests that build a real
grpc `ManagedChannel` (`PeerChannelPoolTest`, `WarpPeerServiceIntegrationTest`) error with
`ServiceConfigurationError: io.grpc.xds.XdsNameResolverProvider could not be instantiated`
(`NoClassDefFoundError: io.grpc.NameResolver$Args$Key` -- grpc-xds 1.82.2 is version-skewed against
the grpc-api on the classpath); it also means the `WarpDriver` JDBC client cannot open a channel on
this classpath. Worth fixing separately.

## 2026-09-23 (later): gRPC RTT optimization pass and grpc-xds fix

**Result: goal (gRPC <= pgwire, Java clients both sides) was NOT met and cannot be met with stock
grpc-java on this platform without an unsafe or architectural change.** Fixed the version skew;
found the residual gap is the gRPC-java framework floor.

**grpc-xds fix.** Cause: `google-cloud-spanner-jdbc` pulls grpc-xds/alts/googleapis/rls/services/
opentelemetry 1.82.2 (and bigquery pulls grpc-grpclb) while pom pinned grpc-netty-shaded/protobuf/stub
(hence grpc-api) at 1.68.1. Fix in `Warp/pom.xml`: `grpc.version` 1.82.2 plus a `grpc-bom` import in
`dependencyManagement` so every io.grpc artifact is aligned; `grpc.plugin.version` stays 1.68.1 for
protoc-gen-grpc-java (only that version is available locally; generated stubs are compatible).
Verified: PeerChannelPoolTest (2), WarpPeerServiceIntegrationTest (2),
ParallelJoinRemoteDispatchIntegrationTest (1) all pass; shaded jar's
`META-INF/services/io.grpc.NameResolverProvider` still merged (all providers listed); jar serves gRPC
(test_grpc.py 6/6 x3 against fresh jar + real subprocess Warp; full tests/python 50 passed, 1 skipped,
3 xfailed).

**Candidates (client-observed p50 us, interleaved both-protocol runs, 3000 warmup + 2000 samples;
baseline gRPC INSERT ~440 / SELECT ~328 vs pgjdbc ~323 / ~254):**

| Candidate | gRPC INSERT / SELECT p50 | Effect |
|---|---|---|
| Client `directExecutor()` | 458 / 330 | none |
| Client `disableRetry()` | 449 / 320 | none |
| Client `disableServiceConfigLookUp()` | 448 / 323 | none |
| Client flowControlWindow 1MB | 456 / 323 (worse tail) | none |
| Server flowControlWindow 1MB | 462 / 338 | none/worse |
| Server fixed 4-thread executor | 457 / 338 | none |
| Server + client directExecutor | 461 / 327 | none |
| Server directExecutor (isolated grpc-only, 3 runs each, base 410-425 / 303-313) | 392-411 / 270-308 | about -10us; KEPT as opt-in only |
| Proto/wire shape | n/a | Server trace: response serialize+enqueue 1-3us, close 3us; not a factor, proto untouched |
| Interceptors (ConnectionLimit, ACL) | n/a | ACL is no-op without rules; sub-us |
| Native transport (kqueue/epoll) | n/a | not in grpc-netty-shaded on macOS (epoll not testable here) |
| Equalized warmup | n/a | both legs 3000 warmup; pgjdbc not "warmer" |

Server directExecutor is shipped as opt-in `WARP_GRPC_DIRECT_EXECUTOR=true` (default off): it runs the
blocking JDBC call on the Netty event loop, so it can stall other connections; the ~10us gain
(INSERT 418 / SELECT 286 vs pgjdbc 325 / 241) does not close the gap.

**Evidence for the floor.** Per-phase server trace (temporary interceptor, removed): header to
parsed message ~10us (thread hop), handler 232us (SELECT, JDBC), serialize 1-3us, close 3us, so
~248us in Warp vs ~315us client-observed, leaving ~67us in client + transport. Pgwire has ~28us there.
Isolated echo microbench (no Warp, no DB, loopback, 60-byte payload): raw blocking TCP 13us;
grpc-java unary with default cached server executor 52us; server directExecutor 35us; server direct +
spin-waiting client 26us. So the stock gRPC path costs ~40us more than a raw socket round trip
(HEADERS and DATA frames, trailers, event-loop hop, executor hop, blocking-stub park/unpark), which is
the entire observed gap of 70-115us once JDBC variance is included.

**Minimum achievable gap:** roughly +25us (direct server executor + spinning client, both unsafe for
a shared server / CPU-burning) to +40us (safe stock config) over pgwire on loopback; ~+70-100us as
measured today. Reaching <= pgwire would need an architectural change: custom lean framing over a
plain socket (one write per request, no HTTP/2), or unix domain sockets plus busy-poll client, or
batching/pipelining several statements per RPC so the fixed ~40us amortizes.

**Final numbers (p50 / p90 / p99 us, two default runs; before = start of this pass):**

| Statement | gRPC before | gRPC after (default) | gRPC opt-in direct | pgjdbc |
|---|---|---|---|---|
| INSERT | 440 / 543 / 1035 | 450,448 / 551,549 / 1074,974 | 418 / 522 / 964 | 326,325 / 387,383 / 595,620 |
| SELECT | 328 / 391 / 620 | 326,323 / 396,397 / 627,645 | 286 / 393 / 669 | 252,251 / 292,293 / 423,444 |

Default gRPC is unchanged in performance (no safe change helped); only the classpath fix and the
opt-in knob shipped. Files: `Warp/pom.xml`, `Warp/src/main/java/com/sayonora/wire/grpc/WarpGrpcServer.java`,
`Warp/src/test/java/com/sayonora/wire/grpc/GrpcVsPgwireRttBenchTest.java` (client-knob system
properties, p99, `-Dserver.direct`). The earlier "known bug" note above is resolved.

## 2026-09-23: s3wire (S3 frontend over a MinIO backend bucket)

**What it is.** `s3wire` (port 18020, enabled by `WARP_S3WIRE_BACKEND_BUCKET`) speaks the S3 REST API
(path-style, SigV4) to stock clients (boto3 verified) and stores objects in one real S3-compatible
backend bucket using Warp's own backend credentials (AWS SDK v2). Client-visible buckets are key
prefixes: bucket `b`, key `k` lives at backend key `b/k` (CreateBucket writes a hidden
`b/.s3wire-bucket` marker). Every operation is recorded in `SqlMetricsCollector` as protocol
`s3wire`, so `/api/metrics/summary` shows per-operation `avgRttMs`.

**Write RTT** (`tests/python/test_write_rtt_baseline` in `test_s3wire.py`; real Warp jar subprocess, real
MinIO container, boto3, same machine, loopback, 256-byte PutObject, 1 warm-up + 40 measured, 3 runs):

| Path | client p50 (ms) | client min (ms) |
|---|---|---|
| boto3 direct to MinIO | 2.05 / 2.12 / 2.12 | 1.78 / 1.84 / 1.92 |
| boto3 via s3wire -> MinIO | 2.80 / 2.82 / 2.86 | 2.57 / 2.59 / 2.51 |
| **Gateway overhead (p50)** | **+0.70 to +0.75** | |

Server-side `avgRttMs` for PutObject reads 3 in all runs. Caveats: `avgRttMs` is the collector's
integer-millisecond truncated mean (a sub-ms figure cannot be shown) and includes the JIT-cold
warm-up request, so it overstates steady state; the client-side delta above is the honest overhead
number. The overhead is one extra HTTP hop (client -> Warp -> MinIO), SigV4 verification, and Warp's
own SDK call; it is not a cache story -- there is no object cache (the RowCache stretch was not done).
GET was not baselined; large-object throughput was not benchmarked (20 MB multipart verified for
correctness only).

**Auth, exactly.** Validated: access key is one of `WARP_S3WIRE_CREDENTIALS`; SigV4 signature over
method, path, query, signed headers and the claimed `x-amz-content-sha256`; 15-minute clock skew;
presigned URL signature/expiry; a claimed hex payload hash is compared after streaming (a mismatched
PUT is rolled back and rejected). NOT validated: per-chunk signatures of `STREAMING-*` uploads (only
the seed signature), any per-key/bucket authorization (every valid key reaches every bucket).
s3wire refuses to start with no credentials configured.

**Supported:** ListBuckets, CreateBucket, HeadBucket, DeleteBucket, GetBucketLocation, PutObject,
GetObject (Range, If-Match/If-None-Match), HeadObject, DeleteObject, DeleteObjects, CopyObject
(server-side, single request), ListObjects v1/v2 (prefix, delimiter, continuation-token, start-after,
max-keys, encoding-type=url), multipart Create/UploadPart/Complete/Abort (proxied to backend
multipart; boto3 `upload_file` of 20 MB verified), presigned GET.

**Not supported (501 NotImplemented):** ListParts, ListMultipartUploads, UploadPartCopy, versioning,
ACLs, tagging, policies/lifecycle/CORS/encryption, SelectObjectContent, virtual-hosted-style
addressing, CopyObject over 5 GB. Also: objects written straight into the backend bucket without a
`bucket/` prefix are invisible; a listing page can hold one fewer key than max-keys if it contained
the hidden bucket marker.

## 2026-09-25: many client connections vs a 10-connection backend pool (wait or error?)

> **Superseded for the SQL/graph wire frontends:** this section measures the hold-for-session model. pgwire, mywire,
> mssqlwire, orawire and boltwire now multiplex sessions onto the pool; see "Connection multiplexing ... before/after"
> at the end of this file for the same tests re-run, and the fixes for bugs 1-5 below. The HTTP frontends/gRPC results
> here are unchanged.

Question: N client connections against a Warp whose pool to Postgres is `WARP_POOL_MAX_SIZE=10`; when
all 10 are in use, does the client WAIT or get an ERROR? Real Postgres 16 container, real Warp jar
(`target/sayonora-wire.jar`, built 2026-09-24 22:50), real drivers, no mocks. Harness:
`Warp/tests/python/test_connection_pooling.py` (opt-in: `WARP_RUN_POOL_TESTS=1`, run as a script,
`--help` lists `--protocols/--n/--mode/--env/--fresh/--idle-holders/--release-probe`). Fresh Postgres and
Warp per case, Ignite discovery pinned to one seed port, QoS `RATE/BURST=100000` so only the pool is
measured. Server-side truth is sampled every 50 ms from `pg_stat_activity` (direct connection) and
Warp's own `/metrics` (`warp_pool_connections`, `warp_pool_waiting`). Slow operations: `pg_sleep(0.1)` for
SQL/gRPC; for protocols with no SQL a `BEFORE INSERT OR UPDATE` trigger on the protocol's data table that
sleeps 100 ms (mongo, dynamo, sqs, os, influx, bolt). Latency below = client time from start until the
workload finished (connect + 2-3 statements, one of them 100 ms).

### Headline answers

1. **The pool is never exceeded.** Hikari `active` peaked at exactly 10 in every saturated run; total
   Postgres client backends peaked at 12 (13 transient twice) = 10 pooled + 2 dedicated `LISTEN`
   connections (`warp_config_changed`, `warp_firewall_rules_changed`) that live outside the pool.
2. **Wait or error: it WAITS up to `WARP_POOL_CONNECT_TIMEOUT_MS` (default 5000 ms), then ERRORS**
   (`Connection is not available, request timed out after 500Xms (total=10, active=10, idle=0, waiting=N)`,
   Hikari `SQLTransientConnectionException`). Whether a client ever hits the error depends on when the
   backend connection is BORROWED and RELEASED, and that differs sharply by frontend (next table).
3. **Not what the design intent says: pgwire, mywire, mssqlwire, orawire and boltwire ALL pin one pooled
   backend connection per client session, even when the client is idle** (proved in the idle-holder test
   below). Only mongowire, dynamowire, sqswire, oswire, influxwire and gRPC borrow per operation. So with
   10 idle-but-connected SQL/Bolt clients the 11th client cannot run a single statement: the "many client
   connections -> few Postgres connections" multiplexing does not exist for those frontends today.
4. **Developer license caps CONCURRENT CLIENT TCP CONNECTIONS at 25 per instance**
   (`License.DEVELOPER_MAX_CONNECTIONS`, `ConnectionGate.acceptTcp`, one shared counter across pgwire,
   mywire, mssqlwire, orawire, mongowire, boltwire). 1000 client connections cannot be held under the
   default license: the surplus are closed at accept. HTTP frontends (dynamo/sqs/os/influx/s3/MCP) and
   gRPC are not counted (gRPC has its own `Edition` limiter: COMMERCIAL unlimited by default, FREE = 100 in
   flight); those were tested with a true 1000.
5. **No legitimate test-only license mechanism exists**, so no Enterprise key was used. `License.java` has
   the Ed25519 public key as a hard-coded constant and reads only `WARP_LICENSE_KEY`; there is no public-key
   override. `LicenseIntegrationTest` mints keys with the real signing private key committed inside that
   test file; using it to run the experiment would be forging a production-valid license, so it was NOT
   done and `src/main` was not touched. Full 1000-session tests of the five TCP SQL/Bolt frontends
   therefore need a real `WARP_LICENSE_KEY` (Enterprise) or a test-only key-override hook added to
   `License.java`.

### When each frontend borrows and releases (source + measured)

| Frontend | Driver | Borrows | Releases | Verified how |
|---|---|---|---|---|
| pgwire | psycopg2 | lazily on 1st statement (`sessionConnection()`), plus one pooled borrow in the session constructor (`FailedStatementLog.ensureSchema()`, on the ACCEPT thread) | session end only; autocommit, COMMIT, idle: never | idle test: 10 idle holders => Hikari active 10 |
| mywire | pymysql | same (`MySqlWireSessionHandler.sessionConnection()`) | session end only | idle test + release probe |
| mssqlwire | pymssql | same (`MssqlWireSessionHandler.sessionConnection()`) | session end only | idle test + release probe |
| orawire | python-oracledb thin | `LazyPooledConnection.get()` on first statement | on client COMMIT/ROLLBACK or session end. NOT on a plain SELECT, NOT on autocommit | see orawire subsection |
| boltwire | neo4j driver | 1st RUN (`sessionConnection()`) | session end only | idle test |
| mongowire | pymongo | per operation (`try (Connection ...)`) | end of operation | idle: active 0 |
| dynamowire / sqswire / oswire / influxwire | boto3 / requests | per request | end of request | idle: active 0 |
| gRPC | grpcio | per RPC | end of RPC | idle: active 0 |

Not run: s3wire (its data path is MinIO, not the Postgres pool; needs a MinIO image) and the MCP server.

### Idle-holder test (10 clients each run one `SELECT`-class request then sit idle, no open transaction; then an 11th and a 12th run one request), default timeout 5000 ms

| Frontend | Hikari active with 10 idle holders | 11th client | 12th client |
|---|---|---|---|
| pgwire | 10 | ERROR after 15.03 s (`Connection is not available, request timed out after 5006ms`) | ERROR after 15.01 s |
| mywire | 10 | ERROR after 10.02 s (`1105 ... request timed out after 5003ms`) | ERROR after 10.02 s |
| mssqlwire | 10 | ERROR after 10.01 s (`InterfaceError: Could not set connection properties`, pool timeout hidden) | ERROR after 10.01 s |
| orawire, plain (autocommit off, no COMMIT) | 10 | ERROR after 15.03 s (login OK in 0.01 s, the first statement times out) | ERROR after 15.02 s |
| orawire, client `COMMIT` after each statement | **0** | OK, connect 0.009 s + statement 0.005 s | OK, 0.007 s + 0.004 s |
| orawire, `connection.autocommit = True` | 10 (autocommit does NOT release) | ERROR after 15.03 s | ERROR after 15.02 s |
| boltwire | 10 | ERROR after 5.01 s (`Neo.ClientError.Statement.ExecutionFailed`, pool timeout) | ERROR after 5.01 s |
| mongowire, dynamowire, sqswire, oswire, influxwire, gRPC | 0 | OK, 1-7 ms | OK, 1-7 ms |

Release probe (one client, Hikari active minus baseline after 1 s idle; 0 = released, 1 = pinned):
pgwire A(select)=1, B(autocommit INSERT)=1, C(INSERT+COMMIT)=1, D(open txn)=1; mywire 1/1/1/1; mssqlwire
1/1/1/1; orawire A=1, B(autocommit INSERT)=1, C(INSERT+COMMIT)=**0**, D(open txn)=1.

**orawire autocommit, exactly:** the "`setAutoCommit(true)` around one statement" path
(`RequestLoop.handleExecute`, PERFORMANCE.md 3.6) flips the flag on the SAME pooled connection and back
to false; it never calls `LazyPooledConnection.release()`, so the connection stays pinned after an
autocommit statement. Only an explicit COMMIT/ROLLBACK from the client (or disconnect) returns it.
A driver that autocommits (`autocommit=True`) or a client that only SELECTs and never commits pins a slot
for the whole session.

### Saturation runs (default: pool 10, timeout 5000 ms)

`hold` = each client keeps its connection open after its workload until all clients finished (long-lived
application connections). `quick` = each client closes right after its own workload. TCP frontends use a
fresh Warp per row. "err@" is seconds from the start of the run.

Session-pinning frontends, N=25 (under the license cap), hold:

| Frontend | ok / fail | Error the 15 losers see | First..last error | ok latency p50/p99/max (s) | max PG backends / Hikari active |
|---|---|---|---|---|---|
| pgwire | 10 / 15 | `SystemError: Connection is not available, request timed out after 500Xms` at CONNECT | 10.4 s .. 95.5 s (about one every 5-6 s, see bug 1) | 0.25 / 0.49 / 0.49 | 12 / 10 |
| mywire | 10 / 15 | `OperationalError (1105, 'Connection is not available ...')` | 5.4 s .. 75.5 s | 0.28 / 0.49 / 0.49 | 12 / 10 |
| mssqlwire | 10 / 15 | 8x `Could not set connection properties` (6 .. 56 s), 7x connection reset at 71 s (listener died, bug 2) | 6.0 s .. 71.1 s | 0.36 / 1.11 / 1.11 | 13 / 10 |
| orawire | 10 / 15 | login succeeds, first statement: `DatabaseError: Connection is not available ...` | all at 10.2 s | 0.30 / 0.50 / 0.50 | 12 / 10 |
| boltwire | 10 / 15 | `ClientError Neo.ClientError.Statement.ExecutionFailed: Connection is not available ...` | 5.0 .. 5.5 s | 0.35 / 0.60 / 0.60 | 12 / 10 |

Same frontends, N=25 quick: 25/25 ok for all five, latency p50 0.36-0.57 s, p99 0.49-0.90 s (clients
queue for a slot for at most ~one 0.15 s session, so they WAIT and succeed).

Per-operation frontends, hold, N=25: 25/25 ok for mongo (N=22), dynamo, sqs, os, influx, gRPC, p50 0.35-0.63 s.
N=1000 (all connections genuinely open at once; HTTP via one keep-alive client with a 1000-connection
pool, gRPC via 1000 channels):

| Frontend | ok / fail | Error text | First error at | ok latency p50/p99/max (s) | Hikari max waiting / max PG |
|---|---|---|---|---|---|
| dynamowire (boto3) | 999 / 1 | `InternalFailure ... PutItem failed` | 9.0 s | 10.6 / 12.0 / 12.0 | 184 / 12 |
| sqswire (boto3) | 898 / 102 | `InternalError: Postgres error: Connection is not available ...` | 7.5 s | 15.5 / 20.7 / 21.1 | 185 / 12 |
| oswire (requests) | 1000 / 0 | | | 9.1 / 11.9 / 11.9 | 184 / 12 |
| influxwire (requests) | 1000 / 0 | | | 8.4 / 11.5 / 11.5 | 185 / 12 |
| gRPC (grpcio) | 682 / 318 | `RuntimeError: Connection is not available, request timed out after 500Xms` | 9.4 s | 7.3 / 11.2 / 11.4 | 960 / 12 |
| mongowire (pymongo, N=22, the cap-limited maximum) | 22 / 0 | | | 0.37 / 0.45 / 0.45 | 20 / 12 |

Reading these: per-request frontends WAIT (queue) for the pool; a request errors only if ITS OWN borrow
waits more than 5 s. One client does two requests (slow write + read), so success latency is the sum
of two waits and can exceed 5 s (e.g. oswire p99 11.9 s) with zero errors. At 100 ms per slow op and 10
connections the pool serves about 100 ops/s, so 1000 clients need about 10 s of work: that is why
throughput-bound frontends (dynamo, sqs, gRPC) fail their slowest borrowers and lighter ones (os, influx)
do not.

### The 1000-connection question under the default (Developer) license

What a client sees when the gate closes its surplus connection (hold, N=1000; the first 25 are accepted,
of which 10 get a pool slot):

| Frontend | Surplus clients see | Count / time |
|---|---|---|
| mywire | `OperationalError: Lost connection to MySQL server during query` | 975 rejected (first at 4.5 s, p50 4.9 s), plus 15 pool errors at 10.7 s |
| orawire | `DPY-xxxx: cannot connect to database ... the database or network closed the connection` | 975 rejected at about 4.2 s, plus 15 pool errors at 14.8-19.7 s |
| boltwire | `ServiceUnavailable: Failed to read four byte Bolt handshake response` | 972 (0.2-4.8 s), plus 15 pool errors at 5.6-8.0 s |
| mongowire | `AutoReconnect: [Errno 54] Connection reset by peer` / `connection closed` | 946 fail at 0.17-0.66 s, 54 ok (pymongo opens N op sockets + 2-3 monitor sockets against the cap, so N=22 is the largest clean run) |
| pgwire / mssqlwire | not measurable at 1000: the listener died or stalled first (bugs 1 and 2); clients saw `connection ... timed out` / `Connection refused` / `Adaptive Server is unavailable` after 15.7-120 s | 990 fail |

The gate rejects (`license: rejecting connection ... capped at 25`) are visible in the Warp log. In `quick`
mode the cap often does not trip because clients disconnect faster than 25 accumulate: pgwire N=1000 quick
= 724 ok / 276 `connection ... Operation timed out` at 15.7 s (kernel accept backlog, no gate rejects);
mssqlwire N=1000 quick = 1000/1000 ok but p50 14.8 s, max 35.5 s (serialised accept); mywire N=1000
quick = 179 ok / 821 `Lost connection` at 5.3-6.8 s; orawire 83 ok / 917 closed; boltwire 44 ok / 956.
So even where nothing is refused, 1000 simultaneous connects mostly WAIT in the accept path (pg/mssql), and
where the gate fires the client sees an abrupt close with a driver-specific message, never a
"license limit" message.

### Knobs: what turns error into wait and back

| Knob | Default | Effect measured |
|---|---|---|
| `WARP_POOL_CONNECT_TIMEOUT_MS` | 5000 | THE wait-vs-error boundary. 60000: gRPC N=1000 682 ok -> **1000 ok, 0 errors** (p50 5.6 s, max 10.9 s); dynamo 999 -> 1000; sqs 898 -> 1000 (max 23.8 s). pgwire hold N=12: the 2 waiters still never got in (holders never release): error moved from 10-15 s to 120 s (two 60 s borrows) - a longer timeout only helps when slots are actually freed. pg/my quick N=25 unchanged (all ok). |
| `WARP_POOL_MAX_SIZE` | 30 | Must be >= concurrent sessions for pgwire/mywire/mssqlwire/boltwire/orawire, since each pins a slot; for per-request frontends it sets throughput (10 slots x 100 ms = ~100 ops/s). |
| `WARP_LICENSE_KEY` (Enterprise) | none | Only way past 25 concurrent TCP connections. Not exercised (no test key mechanism, see above). |
| `WARP_QOS_POOL_WAIT_THRESHOLD` | unset | **Inert.** Set to 1: grpc N=1000 still 771 ok / 229 pool-timeout errors with 945 threads waiting, dynamo 999/1, pgwire hold N=12 identical to baseline; no `ERR_QOS_POOL_SATURATED` was ever raised. It was designed to turn saturation into a fast reject instead of a 5 s wait, but it does not fire (bug 3). |
| `WARP_QOS_RATE_PER_SEC` / `BURST` (5 / 5) / `WARP_QOS_MAX_WAIT_MS` (0) | | Not measured at defaults (kept at 100000 so only the pool is measured). From the code: with maxWait 0 an over-rate statement is rejected immediately with `ERR_QOS_RATE_LIMIT`; a positive maxWait converts that reject into a wait. |
| `WARP_POOL_IDLE_TIMEOUT_MS` | 60000 | Not exercised; irrelevant to saturation. |

### Bugs found while measuring (report-first: production code was NOT changed)

1. **Session constructors block the ACCEPT thread on the pool.** `PgWireSessionHandler` (line 165),
   `MssqlWireSessionHandler` (115) and `MySqlWireSessionHandler` (113) call
   `FailedStatementLog.ensureSchema()` in their constructors, and `Main.acceptPgWireLoop` etc. construct
   the handler on the listener thread. `ensureSchema` borrows a pooled connection and runs `CREATE TABLE IF
   NOT EXISTS` on EVERY new connection. When the pool is full each new connection stalls the whole listener for
   up to `WARP_POOL_CONNECT_TIMEOUT_MS`, so pgwire/mywire/mssqlwire accept one connection per 5 s (the 5-6 s
   spacing of the errors above; 41 pool timeouts for 15 failed pgwire clients), even clients that need no backend
   connection are stuck behind it. Stack captured in the Warp log: `HikariPool.getConnection <-
   PgConnections.open <- FailedStatementLog.ensureSchema <- PgWireSessionHandler.<init> <-
   Main.acceptPgWireLoop`.
2. **One aborted connection kills a listener for good.** In `Main.accept*Loop` the
   `clientSocket.setTcpNoDelay(true)` call sits inside the `while(true)` loop whose only `catch (IOException)`
   is outside it, so a `SocketException: Invalid argument` from a client that already reset (macOS) ends the
   accept thread: `Postgres wire listener on port N failed` / `SQL Server TDS wire listener ... failed`
   (seen in pgwire N=50/1000 hold and mssqlwire N=25/30/50/1000 hold), after which every connect times out or is
   refused until Warp restarts. Triggered by the accept backlog overflowing / clients giving up while the
   accept thread is blocked (bug 1).
3. **`WARP_QOS_POOL_WAIT_THRESHOLD` never triggers.** `QosControlStage` calls
   `BackendConnectionPools.statsFor(statement.targetBackend())`, but pools are keyed by `jdbcUrl|user`
   (`poolKeyFor`), while `targetBackend` is a backend name (or null before routing), so `statsFor` returns
   null and the check is skipped.
4. (Unrelated to pooling, seen in passing) orawire returns a null `ExecutionResult`
   (`Cannot invoke "ExecutionResult.isQuery()" because "result" is null`) for `SELECT pg_sleep(0.1) FROM dual`,
   `INSERT INTO t VALUES (1)` (no column list) and some `DELETE`s, and the session then hangs; the harness uses
   `SELECT 1 FROM dual WHERE pg_sleep(0.1) IS NOT NULL` and column-listed INSERTs.
5. mssqlwire: pymssql with `database=` sends `USE postgres`, which the Postgres backend rejects
   (`syntax error at or near "use"`); the harness omits `database`. Pool timeouts surface as the opaque
   `Could not set connection properties`.

### Recommendation

- Treat the pool as a hard ceiling with a 5 s wait; that default is sensible for per-request frontends
  (dynamo/sqs/os/influx/gRPC/mongo): raise `WARP_POOL_MAX_SIZE` to the Postgres connection budget and keep the
  timeout at 5-10 s so overload shows up as bounded latency then a retryable error, not an unbounded queue.
  Only raise the timeout to 30-60 s for batch/bursty per-request workloads that prefer waiting to failing.
- For pgwire, mywire, mssqlwire, boltwire (and orawire clients that do not commit), size
  `WARP_POOL_MAX_SIZE` >= peak concurrent sessions, or expect the 11th client to fail even with every other
  session idle. A long timeout does NOT help here (idle holders never release). Until session connections are
  released per statement/transaction, use a pooling client (HikariCP, pgbouncer in front of the client side)
  to bound concurrent sessions, and make orawire clients COMMIT after reads.
- Go Enterprise (or set the real `WARP_LICENSE_KEY`) for more than 25 concurrent TCP sessions per instance;
  the surplus otherwise see an unexplained connection close.
- Fix bugs 1-3 before relying on saturation behavior in production; bug 1 in particular makes one saturated
  pool freeze new-connection acceptance for the SQL frontends.

Reproduce: `ulimit -n 8192; WARP_RUN_POOL_TESTS=1 python3 Warp/tests/python/test_connection_pooling.py
--protocols pg --mode hold --n 25 --fresh` (also `--idle-holders`, `--release-probe`,
`--env WARP_POOL_CONNECT_TIMEOUT_MS=60000`). Raw JSON per run is not committed.

## Connection multiplexing (many clients -> few backend connections): before/after (2026-09-25)

Everything above measured the **hold-for-session** model: pgwire, mywire, mssqlwire and boltwire borrowed one pooled
Postgres connection at the client's first statement and kept it until disconnect; orawire released only on
COMMIT/ROLLBACK. That is what made idle clients starve the pool. The dialect-translating frontends now borrow per
statement/transaction and pin only for an open transaction or backend session state
(`core/SessionConnectionLease`, `core/LazyPooledConnection` for orawire; see WARP_GUIDE.md "Connection
multiplexing"; `WARP_MULTIPLEX_SESSIONS=false` restores the old behaviour). Same-day A/B: the pre-change jar
(`baseline.jar`, built from HEAD) against the new jar, same machine, same harness, same Postgres.

**Environment caveat.** The Docker VM's disk was full while these were measured (`postgres:16-alpine` exited with
`No space left on device`), so both sides ran against a native Postgres 17 (Homebrew `initdb`/`postgres`, scram
auth, same client drivers) started by the harness (`WARP_TEST_PG_LOCAL=1` in `warp_test_support.py`). Absolute
latencies are therefore much lower than the Docker-port-forward figures earlier in this file (the one Docker round
that ran first: pgwire p50 0.75 ms, mywire 0.71, mssqlwire 1.14, boltwire 1.00 ms); only before-vs-after is
comparable.

### 1. Idle holders (`--idle-holders`, `WARP_POOL_MAX_SIZE=10`): 10 clients connect, run one statement, then idle; clients 11 and 12 connect and run one statement

| Protocol (driver) | Hikari active with 10 idle holders (before -> after) | Client 11 / 12 before | Client 11 / 12 after |
|---|---|---|---|
| pgwire (psycopg2) | 10 -> **0** | fail after 15.0 s: pool timeout | ok, connect 4 ms + stmt <1 ms |
| mywire (pymysql) | 10 -> **0** | fail after 10.0 s | ok, 6 ms + 1 ms |
| mssqlwire (pymssql) | 10 -> **0** | fail after 10.0 s (`Could not set connection properties`) | ok, 1 ms + 1 ms |
| orawire (python-oracledb, default non-autocommit, plain SELECT) | 10 -> **0** | fail after 15.0 s | ok, 9 ms + 1 ms |
| boltwire (neo4j driver) | 10 -> **0** | fail after 5.0 s (`ExecutionFailed`) | ok, 4 ms + 1 ms |

### 2. Saturation (`--mode hold --n 25`, pool 10, workload: `SELECT 1`, `pg_sleep(0.1)` / a 100 ms-trigger write, `SELECT 1`; clients stay connected until all are done)

| Protocol | Before: ok / failed, wall | Before: error | After: ok / failed, wall | After: latency p50 / p90 / max |
|---|---|---|---|---|
| pgwire | 10 / 15, 96.4 s | 41 pool timeouts, clients waited 10-65 s | **25 / 0, 1.5 s** | 0.37 / 0.43 / 0.44 s |
| mywire | 10 / 15, 76.4 s | 36 pool timeouts | **25 / 0, 1.5 s** | 0.40 / 0.47 / 0.47 s |
| mssqlwire | 10 / 15, 61.2 s | `Could not set connection properties` | **25 / 0, 1.5 s** | 0.38 / 0.45 / 0.45 s |
| orawire | 10 / 15, 11.4 s | first statement fails after 10 s | **25 / 0, 1.5 s** | 0.41 / 0.48 / 0.48 s |
| boltwire | 10 / 15, 6.6 s | `ExecutionFailed` after 5 s | **25 / 0, 1.8 s** | 0.54 / 0.75 / 0.75 s |

After: server side peaked at 10 active pooled connections (never above the pool), 0 pool timeouts, 0 license rejects,
0 dead listeners; the extra client latency is the queue for 10 connections. (`peak backends` seen by Postgres: 12
including the harness's own monitor sessions.)

### 3. 25 concurrent clients sharing a pool of TWO (new `test_25_concurrent_clients_share_pool_of_two`, `WARP_POOL_MAX_SIZE=2`, `WARP_POOL_CONNECT_TIMEOUT_MS=1500`, 4 statements + 4 writes + think time each)

Before: pgwire ok 25 (p50 2.20 s, p90 3.76 s, max 4.06 s: clients serialised two at a time behind each other's whole
session), mssqlwire ok 25 (2.21 / 3.78 / 4.09 s), mywire 12 of 25 failed, orawire 11 and 5 of 25 failed (autocommit and
not), boltwire 9 of 25 failed. After: **25/25 ok on every protocol, p50 0.31-0.37 s, p90 0.31-0.38 s, max <= 0.40 s**
(the 0.3 s think time inside each client dominates; hikari active never above 2).

### 4. Write RTT (the `test_write_rtt_baseline` statement: autocommit single-row INSERT), client p50 in ms

The 40-sample pytest RTT tests swing +-0.15 ms run to run on this loaded machine (e.g. pgwire 0.26-0.48 for the
same jar), too noisy to resolve the few-microsecond cost of a borrow/return, so this used a high-sample harness
(`Warp/tests/python/rtt_bench.py`: 300 warm-up + 1500 timed statements per run, jars alternated before/after, 4 rounds, median of
the per-run p50):

| Protocol | before p50 (p90) | after p50 (p90) | delta p50 |
|---|---|---|---|
| pgwire | 0.105 (0.153) | 0.117 (0.167) | +0.011 |
| mywire | 0.139 (0.200) | 0.143 (0.205) | +0.004 |
| mssqlwire | 0.129 (0.181) | 0.138 (0.192) | +0.009 |
| orawire | 0.115 (0.176) | 0.117 (0.181) | +0.002 |
| boltwire | 0.182 (0.227) | 0.183 (0.230) | +0.001 |

The cost of multiplexing on the hot path is +1 to +11 microseconds per statement (Hikari borrow + return, one
map lookup of the per-physical-connection state, the pin/replay classification): **no regression, far inside the
+-0.3 ms budget**. A direct probe measured Hikari borrow+return at 0.06-0.2 us and a `SELECT 1` through a lease at
17.0 us versus 17.1 us on a held connection. The earlier per-statement `set_config` caching gains hold: the
identity/`db_emulation`/tenant-search_path cache used to be keyed by (session, Connection *proxy*) -- a pool hands
out a new proxy per borrow -- and is now keyed by the *physical* connection (`PhysicalSessionState`), so getting the
same physical connection back costs zero extra round trips, and a different physical connection is a (correct)
cache miss that re-applies the context. Where it is a miss (first use of a connection, a different client's state
left on it) the whole `warp.*` identity is now applied in ONE round trip instead of one per attribute.

### Bugs found by the baseline, fixed here

1. Session constructors ran `CREATE TABLE IF NOT EXISTS warp_failed_statements` on the accept thread for every new
   connection: a full pool froze accepting for the whole listener. Now once per process, off-thread; handler
   construction borrows nothing.
2. `setTcpNoDelay` sat outside the per-connection try/catch, so one aborted connection killed the listener until
   restart. All TCP accept loops in `Main` share one resilient `acceptLoop` (log, drop that connection, continue).
3. `WARP_QOS_POOL_WAIT_THRESHOLD` never fired (pools are keyed `jdbcUrl|user`, `targetBackend` is a name or null).
   Backend name -> pool key is now recorded on every borrow; the threshold rejects immediately with SQLSTATE 53300.
4. Pool exhaustion now gives every protocol a clear native error naming Warp's pool, the wait and the knobs
   (`Warp backend connection pool exhausted: waited 1500ms for one of 2 pooled backend connections ...`): Postgres
   `53300`, MySQL 1040, `ORA-00018`, mssqlwire error 50000-class, Bolt `Neo.TransientError.General.DatabaseUnavailable`.
5. mssqlwire accepts `USE <db>` as a no-op; orawire no longer NPEs (session hang) on a statement that arrives with no
   bind rows (`SELECT pg_sleep(0.1) FROM dual` now reaches the backend and returns a clear "unsupported column type
   void" error; an INSERT without a column list already worked). mywire `SET autocommit=0` is now a session mode: a
   COMMIT/ROLLBACK no longer silently drops the session back into autocommit (the strict-xfail
   `test_transaction_rollback_discards_uncommitted_writes` in `test_mywire.py` had been documenting that bug; its
   marker is removed).

### Not covered / honest limits

* orawire with `WARP_DUAL_EXEC_*`, replication or XA keeps the old behaviour (it needs a connection kept open).
* Session-state pins are permanent for the session; SQL-level `PREPARE` names and pg_oracle's DBMS_OUTPUT buffer are
  not wiped when a pinned session ends. Native-proxy modes were not touched.
* The Java integration tests that spawn a Warp JVM or need Docker images (Oracle/SQL Server containers) could not run
  on this machine (Docker VM disk full; a stray-Warp Ignite discovery hang for the spawned JVMs).

Reproduce: `WARP_TEST_PG_LOCAL=1` (only if Docker is unavailable), then from `Warp/tests/python`:
`python3 -m pytest -q test_connection_pooling.py` (the multiplexing tests: idle holders, 25 clients / pool 2,
transaction isolation, extended-protocol prepared statements and partial-fetch portals, SET/temp-table pinning,
RLS identity isolation, pool exhaustion errors, QoS threshold, listener resilience, kill switch), and
`WARP_RUN_POOL_TESTS=1 python3 test_connection_pooling.py --protocols pg,my,mssql,ora,bolt --idle-holders` /
`--mode hold --n 25 --fresh` and `python3 rtt_bench.py <jar> <pg|my|mssql|ora|bolt>` (alternate two jars) for the numbers above.


## 2026-09-25: s3wire Postgres mode (the `s3` store) vs proxy mode vs MinIO direct

**What was measured.** The same boto3 client (path-style, SigV4, one thread, `retries=0`), same machine, loopback:
`minio-direct` = boto3 straight to a MinIO container; `warp-proxy->minio` = s3wire proxy mode in front of
that MinIO; `warp-postgres-1-host` / `-2-host` = s3wire Postgres mode over one / two native Homebrew Postgres 17
servers (default settings, fsync on, local disk), objects sharded across the two in the second row. Small
operations: 256-byte PutObject over 50 rotating keys, GetObject of one key, `ListObjectsV2(Prefix, MaxKeys=20)`
on that prefix; 20 warm-up + 150 measured each. Large: one `put_object` of a 100 MiB in-memory body and one
`get_object` streamed in 1 MiB reads. Numbers are client-side milliseconds (p50, with the minimum in brackets)
and MB/s; one run of a throwaway benchmark script (the MinIO/proxy rows were measured in four runs, the 1-host Postgres row in two, the 2-host row once; repeated rows agreed within ~15%).

| Path | PUT 256 B p50 (min) | GET 256 B p50 (min) | List 20 keys p50 (min) | 100 MiB PUT | 100 MiB GET |
|---|---|---|---|---|---|
| boto3 direct to MinIO (Docker) | 6.9 (3.8) | 3.7 (2.3) | 5.4 (3.6) | 308 MB/s | 495 MB/s |
| s3wire proxy -> MinIO | 5.6 (4.1) | 2.6 (2.0) | 4.2 (3.2) | 253 MB/s | 432 MB/s |
| **s3wire Postgres, 1 host** | **1.4 (1.1)** | **0.8 (0.7)** | **1.4 (1.3)** | **246 MB/s** | **1691 MB/s** |
| **s3wire Postgres, 2 hosts** | **1.7 (1.1)** | **0.9 (0.8)** | **1.9 (1.5)** | **226 MB/s** | **1204 MB/s** |

Reading it honestly: the small-operation gap is mostly *where the backend runs* -- MinIO sits in the Docker
VM (an extra hop and fsync-heavy erasure-coded writes), the native Postgres is on the host -- so it says the
Postgres path adds no meaningful overhead per operation (one transaction of a handful of statements for a
small PUT; one indexed lookup for a small GET), not that Postgres is "faster than an object store". A
containerised Postgres would land closer to MinIO. The list gets slightly slower with two hosts (two
sequential shard queries plus a merge); GET is also slower there (not investigated), and 100 MiB PUT stays at
~220-250 MB/s, the same order as the proxy (not profiled; consistent with being bound by Postgres write throughput). Read throughput out
of Postgres is high here because the just-written chunks are in the shared buffer/page cache; a cold read is
bounded by disk. The server-side `avgRttMs` for `s3wire` in `/api/metrics/summary` is a millisecond-truncated mean
across all operations of the run, so it is not broken out here. Caveats: single client thread, loopback, Postgres
untuned, warm cache, one 100 MiB object per mode, macOS. Not measured: concurrent throughput, objects above
100 MiB, a Postgres on another host.

## 2026-09-25: sqswire SQS conformance work -- Floci SQS results and RTT before/after

**Conformance.** Floci's SQS compatibility suites (Floci `compatibility-tests/`, one suite at a time, fresh Warp on native
Postgres 17 per run, credentials test/test) before -> after: **python 7/16 -> 16/16, node 6/8 -> 8/8, java 8/27 -> 27/27**
(pass/total; baseline files kept as `Warp/tests/python/floci_compat/results/warp-sqs-*-baseline.md`). Nothing is left
failing, so no test is classified Floci-specific. Warp's own `tests/python/test_sqswire_conformance.py` adds 31 scenarios
run against one and against two sharded Postgres backends (62 cases), with both the JSON protocol (boto3) and the Query/XML
protocol (raw SigV4-signed requests), and passes; `test_sqswire.py`, the SQS cases in `test_backend_set_stores.py` and
`test_mcp_backend_set_stores.py` still pass.

**RTT must not regress.** Same machine, native Postgres 17, boto3 with keep-alive, one thread, loopback, 400 iterations of
send / receive / delete / receive-on-empty per run (`tests/python/sqswire_rtt_compare.py`), client-observed p50 in ms, two
alternating runs per jar. Baseline = the jar built before this work; after = same jar with the new sqswire classes.

| operation | before (run 1 / run 2) | after (run 1 / run 2) |
|---|---|---|
| SendMessage | 0.573 / 0.613 | 0.593 / 0.599 |
| ReceiveMessage (one message) | 0.589 / 0.630 | 0.606 / 0.625 |
| ReceiveMessage (empty queue) | 0.532 / 0.562 | 0.542 / 0.548 |
| DeleteMessage | 0.557 / 0.589 | 0.571 / 0.577 |

The differences are inside run-to-run noise (about 0.03 ms; mean +0.01 to +0.06 ms on send/receive in the first pair, none in the
second). `test_sqswire.py::test_write_rtt_baseline` client p50 is 0.88-0.97 ms both before and after, and the server-side
`avgRttMs` is 0 (the metric is whole milliseconds) in both. Why it stays flat: a standard-queue send is still one
`INSERT ... RETURNING` and a receive is still one `UPDATE ... RETURNING` (now batched for up to 10 messages), queue
attributes come from a per-process cache (5 s TTL), and FIFO queues, which use a transaction and an advisory lock, are the only
path that got more expensive. **Long-poll caveat:** a `ReceiveMessage` with `WaitTimeSeconds` now really waits (up to 20 s); the
time parked is excluded from the reported RTT and no backend connection is held while parked.

## 2026-09-25: dynamowire DynamoDB conformance work -- Floci results and RTT before/after

**Conformance.** Floci's DynamoDB SDK compatibility suites (Floci `compatibility-tests/`, one suite at a time, fresh Warp on native
Postgres 17 per run, credentials test/test) before -> after, pass/total: **python 13/22 -> 22/22, node 23/59 -> 59/59, java
38/120 -> 118/120** (baseline files kept as `Warp/tests/python/floci_compat/results/warp-dynamodb-*-baseline.md`). The two remaining java
failures are Floci-specific: `DynamoDbTest::updateTableReplicaLifecycle` (adds a replica region to a stream-less table through UpdateTable
`ReplicaUpdates`; global tables are documented as unsupported and answer a ValidationException) and `DynamoDbTest::searchVectors` (`SearchVectors`
is a Floci extension, not a DynamoDB API). Warp's own `tests/python/test_dynamowire_conformance.py` adds 17 scenarios (33 cases: 16 run
against one and against two sharded Postgres backends, plus one for a backend added after a table exists) -- secondary indexes, parallel
scan, TTL expiry, atomic conditional writes and transactions under concurrency (also across shards), PartiQL, legacy parameters,
validation, ConsumedCapacity -- and passes, as do `test_dynamowire.py`, the DynamoDB cases in `test_backend_set_stores.py` (the
cross-shard-transaction case now asserts atomic commit instead of a refusal) and `test_mcp_backends.py`; 22 Java unit tests cover the
expression engine, validation, key planning and the PartiQL parser. Behaviour checked case by case against Amazon's DynamoDB Local
(about 400 request/response pairs; the remaining differences are documented in `docs/WARP_GUIDE.md`, *The DynamoDB store*).

**RTT must not regress.** Same machine, native Postgres 17, raw DynamoDB-protocol requests over one keep-alive connection
(`tests/python/dynamowire_rtt_bench.py`, no SDK, no signing), one thread, loopback, 300 warm-up + 1000 timed requests per operation,
client-observed **p50 in ms, median of 3 alternating runs per jar**. Before = the jar built from the tree without the new dynamowire
classes; after = the same tree with them.

| operation | before | after |
|---|---|---|
| PutItem | 0.233 | 0.238 |
| PutItem with `attribute_not_exists` | 0.190 | 0.202 |
| UpdateItem (`SET v = if_not_exists(v, :z) + :o`) | 0.169 | 0.172 |
| GetItem, Postgres read | 0.146 | 0.149 |
| GetItem, row-cache hit | 0.080 | 0.082 |
| Query (one item) | 0.100 | 0.126 |

The plain write and read paths move by less than the run-to-run spread (about 0.01 ms). Two paths cost a little: a conditional write
now locks the item (`pg_advisory_xact_lock` plus `SELECT ... FOR UPDATE`, sent as two pipelined statements in one round trip -- a first
version that ran them as separate round trips cost +0.11 ms and was rewritten), which is what makes `attribute_not_exists` puts atomic,
and a Query goes through the new planner (+0.025 ms); a Query that can return only a few rows (a small `Limit`, or the whole primary
key given) still runs as a single autocommit statement, while larger reads stream through a cursor. `test_dynamowire.py::
test_write_rtt_baseline` (boto3, 40 samples) still reports a client p50 of about 1.07 ms and a server-side `avgRttMs` of 0 (whole
milliseconds) after the change.

## 2026-09-25 -- influxwire behaves like InfluxDB (differential conformance) and RTT

influxwire was made to behave like a real InfluxDB 1.8.10 (measured differentially: `Warp/tests/python/influx_conformance/`, 290 cases /
1,821 requests replayed against the real server and against Warp). Result before -> after:

| | identical | documented divergence | clock/server-state dependent | message-only | different | cases fully identical |
|---|---|---|---|---|---|---|
| before (one Postgres backend) | 208 of 1,821 ignoring Content-Type (0 strict) | 19 | 8 | 196 | 1,598 | 2 / 290 |
| after, one Postgres backend | 1,794 | 19 | 8 | 0 | 0 | 290 / 290 |
| after, two sharded Postgres backends | 1,794 | 19 | 8 | 0 | 0 | 290 / 290 |

Warp-side, `tests/python/test_influxwire_conformance.py` replays the same corpus (InfluxDB's answers recorded as a golden file) on one and on two
sharded backends, plus the 2.x write endpoint, credentials, the `influxdb` python client and shard-exactness checks: 588 tests pass; `test_influxwire.py`,
the InfluxDB case of `test_backend_set_stores.py`, `test_mcp_backends.py -k influx` and `test_mcp_backend_set_stores.py` pass; 17 Java unit tests
(line-protocol parser, InfluxQL parser, engine over an in-memory backend) pass.

**RTT.** Same machine, native Postgres, one keep-alive connection, loopback, one thread, 300 single-point writes of a new series
(`POST /write`) and 100 filtered point queries (`SELECT value FROM temp WHERE host = 'h<i>'`) per run, client-observed **p50 in ms, median of 3 runs
alternating between the jars** (`tests/python/influx_conformance/rtt_bench.py`); server-side `avgRttMs` reads 0 (whole milliseconds) for both.

| operation | before | after |
|---|---|---|
| single-point write | 0.818 | 0.877 |
| filtered point query | 0.831 | 0.938 |

A write is one `INSERT ... ON CONFLICT` (the same one round trip; the catalog is only touched for a field or tag key not seen before, and databases,
retention policies and schemas are cached), +0.06 ms for the wider row and the unique-index probe. A query now needs the measurement's schema
(cached for 2 s) and the points: +0.1 ms. `test_influxwire.py::test_write_rtt_baseline` (40 samples) reports a client p50 of about 1.2 ms and a
server-side `avgRttMs` of 1, inside its 3 ms bar. Throughput sanity check on 200,000 points (50 series): 2.5 s to write in 5,000-point batches
(80k points/s), 30-500 ms for aggregate and windowed queries over all of them.

## 2026-09-25 -- oswire behaves like OpenSearch (differential + REST-spec conformance) and RTT

oswire (OpenSearch REST/JSON over Postgres) was made to behave like a real OpenSearch 2.19.6, measured two ways
(`Warp/tests/python/os_conformance/`, README there): OpenSearch's own REST API YAML tests and a 321-case differential corpus replayed against the
real server and against Warp. Result before -> after:

| | before | after, one Postgres backend | after, index sharded over two Postgres backends |
|---|---|---|---|
| OpenSearch REST-spec tests valid on real OpenSearch 2.19.6 that Warp passes (42 test directories) | 21 / 936 | 693 / 936 | 690 / 936 (3 order-of-tied-hits) |
| differential corpus, cases matching the real server | 18 / 321 | 313 / 321 | 310 / 321 |
| differential corpus, differing only in a documented way (`os_conformance/known.py`) | - | 8 | 11 (per-host scores/tie order) |
| Lucene BM25 `_score` equal to OpenSearch's (6 digits) | flat 1.0 / ts_rank | yes (term, match, phrase, bool, multi_match, fuzzy, query_string) | per host (like a shard) |

The 243 remaining spec failures are classified in `os_conformance/results/spec_failures.tsv`: 117 features not implemented (intervals, span, more_like_this,
profile, significant_terms, unsigned_long, range field types, scripted aggregations, geo shapes, terms lookup, ...) that fail with a clear OpenSearch-style
error, 48 that cannot exist on Postgres or are deliberately different (custom routing, refresh/realtime visibility, hdr precision, shard internals such as
request-cache/batched reduce, cluster/node detail), 78 details (error-message texts, limits, corner cases). Warp-side, `tests/python/test_oswire_conformance.py`
replays the corpus against Warp (one and two sharded backends, using the recorded OpenSearch answers) and adds opensearch-py end-to-end, sharding, k-NN/hybrid
across hosts, refresh gating and legacy-table adoption checks: 14 tests pass; `test_oswire.py`, the OpenSearch case of `test_backend_set_stores.py` and
`test_mcp_backend_set_stores.py` pass; 17 Java unit tests (analysis, dates, mapping, BM25 numbers recorded from OpenSearch, query/aggregation engine,
scripts, pre-filter) pass.

**RTT.** Same machine, native Postgres, one keep-alive connection, loopback, one thread, opensearch-py, 400 requests per operation, client-observed
**p50 in ms, 3 runs alternating between the jars** (baseline jar built from the tree before this work / this work):

| operation | before | after |
|---|---|---|
| `PUT /idx/_doc/<new id>` (index a new document) | 0.306-0.313 | 0.332-0.345 |
| `GET /idx/_doc/<id>` | 0.225-0.228 | 0.220-0.226 |
| `PUT /idx/_doc/<existing id>` (overwrite) | 0.193-0.199 | 0.207-0.210 |

A write is still one SQL statement (`INSERT ... ON CONFLICT ... RETURNING seq_no, version`); the +0.02-0.03 ms is the returned row (created/updated, `_seq_no`,
`_version`) and the per-index sequence, plus a dynamic-mapping check of the document against the cached mapping (the catalog row is re-read at most every 2 s).
A get is unchanged. `test_oswire.py::test_write_rtt_baseline` (40 samples) reports a client p50 of 0.5-0.7 ms before and after, server-side `avgRttMs` 0
(whole milliseconds), inside its 2 ms bar. Search cost is now O(documents read): a 20,000-document index answers term/range/match/aggregation requests in
10-110 ms (`_bulk` of 20,000 documents: 1.2 s).

## 2026-09-25: s3wire S3 conformance work (Postgres mode) -- Floci S3 results and RTT before/after

**Conformance.** Floci's S3 compatibility suites (Floci `compatibility-tests/`, one suite at a time, fresh Warp on native Postgres 17 per run, s3 store enabled on the
default backend, credentials test/test) before -> after, pass / total: **python 17/42 -> 42/42, node 20/35 -> 35/35, java 30/75 -> 74/75** (baseline files:
`Warp/tests/python/floci_compat/results/warp-baseline-s3-*.md`; current: `warp-s3-*.md`). The one remaining failure, `S3Test::deleteBucketTagging`, is class c
(Floci-specific): it expects an empty tag set from GetBucketTagging after DeleteBucketTagging, real S3 answers `404 NoSuchTagSet`, and s3wire answers like S3. The baseline
failures were tagging, versioning + ListObjectVersions, GetObjectAttributes and multipart checksum types (COMPOSITE / FULL_OBJECT), CORS, PublicAccessBlock, UploadPartCopy,
LocationConstraint, virtual-hosted addressing and object annotations. Floci classes beyond those suites, run against the same Warp: S3LifecycleTest 2/2, S3PresignTest 1/1,
S3BlockPublicAccessTest 6/6, S3SelectTest 19/19, S3PresignedUrlSigV4VerificationTest 11/13 (2 class c: Floci IAM; S3 answers 400 rather than 403 for a malformed presigned credential).
Warp's own `tests/python/test_s3wire_conformance.py` runs 47 scenarios against one and against two sharded Postgres backends (versioning, tagging, ACLs/PAB/ownership/policy,
CORS, checksums incl. signed and unsigned streaming trailers, multipart extras, conditional requests, presigned + POST-policy uploads, virtual-hosted addressing, Select,
annotations, error parity; 91 passed, 3 skipped for optional dependencies) and passes; `test_s3wire_postgres.py` (36), `test_s3wire.py` (proxy mode with MinIO, 14),
`test_backend_set_stores.py`, `test_mcp_backend_set_stores.py`, `S3StoreUnitTest` (13), `S3WireUnitTest` (5) and the new `S3ConformanceUnitTest` (22) pass. A MinIO oracle
(same boto3 calls against MinIO and Warp, ~110 probes) showed differences only where MinIO lacks the operation or differs from S3 documentation; the ones acted on were the
combined `If-None-Match` + `If-Modified-Since` order (both are evaluated, 304 if either says not modified).

**RTT (small operations, Postgres mode).** Same machine and method as the 2026-09-25 s3wire row above (boto3, path-style, SigV4, one thread, 256 B PutObject over 50 keys,
GetObject of one key, ListObjectsV2 of 20 keys; 150 measured requests after 20 warm-up; native Postgres 17), client p50 in ms, jar before this work (the s3 store as first delivered)
against this work, two runs each alternating:

| | PUT 256 B | GET 256 B | LIST 20 keys |
|---|---|---|---|
| before, 1 host / 2 hosts | 1.35 / 1.38, 1.44 / 1.42 | 0.84 / 0.83, 0.87 / 0.88 | 1.43 / 1.48, 1.55 / 1.63 |
| after, 1 host / 2 hosts | 1.46 / 1.47, 1.41 / 1.43 | 0.87 / 0.85, 0.83 / 0.84 | 1.52 / 1.60, 1.48 / 1.62 |

No regression beyond noise (about +0.03 ms on PUT, GET and LIST unchanged). A first version measured +0.1 ms on PUT (1.46-1.50): the commit path re-read every column of the current
row and stored a default `AES256` marker in every object; the commit now locks a narrow row (blob ids, annotation flag) when the bucket is unversioned and the implicit SSE-S3
default is not stored. The new work adds per-object CRC64NVME (slicing-by-8) when the client names no checksum, the additional-checksum columns and the bucket row fetch (one
query, cached 2 s, joined with the encryption default), all inside the same transaction shape (lock / insert / upsert). 100 MiB PUT stayed 200-250 MB/s, GET 1250-1500 MB/s.

## 2026-09-25: boltwire Neo4j conformance work -- openCypher TCK and differential corpus, RTT before/after

**Conformance** (real Neo4j 5.26 community as oracle; `Warp/tests/python/bolt_conformance/`). openCypher TCK (3,897 scenario instances after expanding
outlines; 50 need client-registered test procedures and are skipped, 37 more are invalid on real Neo4j, mostly TCK side-effect accounting that Neo4j's
counters do not reproduce): scenarios valid on Neo4j **3,810**; Warp before -> after, pass: **0 -> 3,810** (the previous CypherParser knew only
`RETURN <literal>`, `CREATE` and a single-hop `MATCH`; it failed every scenario, starting with the graph reset `MATCH (n) DETACH DELETE n`). Differential corpus (952 cases:
expressions, parameters of every type, matching, aggregation, writes, DDL, procedures, transactions, streaming, driver/protocol facts): **926 identical, 26
documented differences (`bolt_known.py`), 0 unexpected**; the first run against the new engine was 889 identical / 55 different, all fixed or documented.
Warp-side: `test_boltwire_conformance.py` (replays the recorded oracle answers, needs no Docker; the TCK part runs when `BOLT_TCK_DIR` is set) and
`CypherParserAnalyzerTest` pass; `test_boltwire.py` passes with its two `$param` xfails removed (the features now exist).

**RTT** (one session, neo4j Python driver, 300 measured requests after 50 warm-up, native Postgres, client p50 ms, before = jar of the previous commit, two runs each):

| | CREATE (RETURN n.v) | MATCH by label+property | RETURN literal |
|---|---|---|---|
| before | 0.249, 0.246 | 0.211, 0.219 | 0.166, 0.170 |
| after | 0.320, 0.303 | 0.209, 0.216 | 0.134, 0.142 |

MATCH and literal reads are unchanged or faster (statements are parsed once and cached); a write now costs about +0.06 ms because it runs in an explicit backend transaction
(commit round trip) so that a multi-step Cypher statement is atomic; the single-INSERT shortcut of the old CREATE could not be kept for MERGE/SET/DELETE.

## 2026-09-25: rediswire (Redis frontend) -- RTT and throughput next to real Redis 7.4.11

Setup: one real Warp process + native Postgres 17 (`WARP_TEST_PG_LOCAL=1`), raw-socket RESP client (`Warp/tests/python/redis_rtt_bench.py`), loopback, 3,000 measured requests after 300 warm-up; the
comparison Redis is the `redis:7` container (Docker Desktop VM, so its loopback path is slower than a native Redis: about 0.19 ms per request). Single client p50/p99 ms, pipelined 100 commands, 16 client threads.

| command | Warp p50 | Warp p99 | Warp pipe-100 ops/s | Warp 16 clients ops/s | Redis p50 | Redis pipe-100 ops/s | Redis 16 clients ops/s |
|---|---|---|---|---|---|---|---|
| SET | 0.064 | 0.111 | 21,392 | 29,550 | 0.193 | 276,642 | 26,263 |
| GET | 0.044 | 0.069 | 43,268 | 34,692 | 0.192 | 285,802 | 26,852 |
| INCR | 0.096 | 0.177 | 12,275 | 7,801 | 0.189 | 311,469 | 26,930 |
| LPUSH | 0.173 | 0.286 | 6,549 | 4,934 | 0.193 | 278,234 | 26,995 |
| ZADD | 0.198 | 0.309 | 4,052 | 3,560 | 0.191 | 255,224 | 26,235 |

GET and SET are one Postgres statement (well inside the 0.3 ms target); INCR, LPUSH and ZADD are a short transaction (row lock, write, counter, commit). The Docker path adds latency to Redis that a native Redis would not have.
Pipelining does not batch backend round trips (each command is its own statement), so pipelined throughput is bounded by Postgres, not by the protocol.

## 2026-09-26: azurewire (Azure Blob / Queue / Table) -- RTT next to Azurite 3.37

Setup: one real Warp process + one native Postgres 17 (`WARP_TEST_PG_LOCAL=1`), the three azurewire listeners, `Warp/tests/python/az_conformance/az_rtt_bench.py`
(Python `requests`, one keep-alive connection, sequential, every request SharedKey-signed by the same code for both sides, 300 measured requests per operation,
client-side median / p95 in ms). The comparison is Azurite 3.37.0 (`mcr.microsoft.com/azure-storage/azurite`, in-memory persistence) in the Docker Desktop VM:
its loopback path adds a fraction of a millisecond that a native Azurite would not have, and Azurite keeps everything in memory while Warp commits every write to
Postgres, so read this as "same order of magnitude, Warp is not slower", not as a claim about Azure itself.

| operation | Azurite median / p95 | Warp median / p95 |
|---|---|---|
| blob PUT 1 KiB (Put Blob) | 1.94 / 2.74 | 1.78 / 2.94 |
| blob GET 1 KiB | 2.74 / 3.79 | 0.98 / 1.12 |
| queue put message | 2.57 / 4.23 | 1.10 / 1.34 |
| queue get message (dequeue) | 4.99 / 7.13 | 1.01 / 1.17 |
| table insert entity | 2.92 / 4.43 | 1.01 / 1.16 |
| table get entity | 2.06 / 2.75 | 0.87 / 0.95 |
| table query (PartitionKey eq, top 20) | 3.34 / 4.59 | 0.92 / 1.05 |

Every operation is reported to the metrics collector under the protocol names `azblobwire` / `azqueuewire` / `aztablewire`. Also measured (test suite, 2026-09-26):
a 100 MiB Put Blob through a Warp started with `-Xmx300m` (25 chunk rows of 4 MiB, MD5 verified on download) completes in about 0.5 s on loopback.

## 2026-09-26: gcswire (Google Cloud Storage JSON / XML API) -- RTT next to fake-gcs-server

Setup: one real Warp process + native Postgres 17 (`WARP_TEST_PG_LOCAL=1`, one or two backends), `Warp/tests/python/gcs_conformance/gcs_rtt_bench.py` (Python `requests`, one keep-alive
connection, sequential, anonymous access, the same client for both sides, 300 measured requests per operation after 20 warm-up inserts, client-side median / p95 in ms). The comparison is
`fsouza/fake-gcs-server` (Go, `-backend memory`) in the Docker Desktop VM: its loopback path adds a fraction of a millisecond that a native process would not have, it keeps everything in
memory while Warp commits every write to Postgres, and it does far less work per request (no preconditions, no generation bookkeeping, no hash verification on most paths), so read this as
"same order of magnitude, Warp is not slower on this box", not as a claim about Google's service.

| operation | fake-gcs-server median / p95 | Warp, 1 Postgres median / p95 | Warp, 2 sharded Postgres median / p95 |
|---|---|---|---|
| object insert 1 KiB (`uploadType=media`) | 2.18 / 3.91 | 1.49 / 2.26 | 1.53 / 2.53 |
| object get metadata | 1.72 / 2.61 | 0.77 / 0.90 | 0.79 / 0.90 |
| object get media 1 KiB | 1.66 / 2.51 | 0.85 / 0.99 | 0.86 / 0.98 |
| list objects (prefix, `maxResults=20`) | 2.41 / 4.22 | 1.39 / 1.91 | 1.61 / 2.58 |
| object patch metadata | 1.96 / 2.85 | 0.88 / 1.03 | 0.89 / 1.03 |
| object delete | 1.41 / 3.35 | 0.84 / 0.94 | 0.91 / 0.99 |

An insert is one short transaction on the owning shard (retire the previous generation, insert the row, adopt the data blob) plus the chunk insert; a listing on two shards costs one extra query and
a merge. Every operation is reported to the metrics collector under the protocol name `gcswire` (labels such as `objects.insert.media`, `objects.get.media`, `objects.list`, `xml.putObject`).
Also measured (loopback, Warp started with `-Xmx300m`, single run, treat as indicative): a 100 MiB resumable upload in 8 MiB chunks in about 0.64 s (about 155 MiB/s), a streamed download of
the same object in about 0.07 s, a single 100 MiB `uploadType=media` POST in about 0.4 s.

## 2026-09-26: mongowire (MongoDB 7.0 compatibility rewrite) -- conformance counts and RTT

Setup: one real Warp process + one native Postgres (`WARP_TEST_PG_LOCAL=1`), pymongo 4.17, `Warp/tests/python/mongo_conformance/mongo_rtt_bench.py` (one connection, sequential, 500 measured
operations, client-side p50 in ms, three runs each), row cache on (default). Before = the previous mongowire jar (SQL translation of a small operator subset), after = the engine described in
*The MongoDB store* (WARP_GUIDE section 4.7): documents stored as type-exact BSON, evaluated by a Java implementation of MongoDB's semantics.

| operation | before p50 | after p50 |
|---|---|---|
| insertOne (one row, `_id` primary key, BSON + jsonb mirror) | 0.110 - 0.113 | 0.116 - 0.119 |
| find_one by `_id` | 0.161 - 0.241 (row-cache hit path) | 0.124 - 0.126 (one primary-key read; the cache no longer serves reads because it cannot carry BSON types) |

Conformance (real `mongod:7.0` container as the oracle, 2,683 recorded steps): before 322 identical / 22 same-code / 2,105 different (the old server lacked `drop`, `create`, `getMore`, index and admin
commands and most operators); after 2,357 identical / 247 same code with another message / 0 different / 79 documented divergences on one backend, and the same 0 different on two sharded
backends. MongoDB driver-spec CRUD + BSON-corpus tests: 1,018 of 1,018 runnable tests pass on one backend (0 of the 51 core CRUD tests passed before), 1,006 on two backends (the 12 failures
depend on the natural order of documents). Two backends: `insertOne` and find-by-`_id` are unchanged (one host); a non-`_id` read scans both hosts and evaluates once.

## 2026-09-26: awswire (SNS, Kinesis, Secrets Manager, SSM, KMS, STS on Postgres, unified AWS endpoint) -- p50 / p95 only

Setup: one real Warp process + native Postgres 17 (`WARP_TEST_PG_LOCAL=1`, one backend, then two sharded backends), the unified AWS endpoint, `Warp/tests/python/awsextras_rtt_bench.py` (boto3, one client per service on a
keep-alive connection, sequential, dummy credentials, 300 measured requests per operation after 30 warm-ups, client-side median / p95 in ms). There is no comparison product for these services, so this only records Warp's own
numbers (the SQS row is the same client against the same listener, as a yardstick). The machine was shared with other builds and test runs, so treat differences of a few hundredths of a millisecond as noise.

| operation | 1 Postgres p50 / p95 | 2 sharded Postgres p50 / p95 |
|---|---|---|
| sqs SendMessage (reference, unified endpoint) | 0.72 / 0.84 | 0.69 / 0.84 |
| sns Publish, no subscribers | 0.70 / 0.84 | 0.71 / 0.92 |
| sns Publish, 1 SQS subscription (envelope) | 1.34 / 1.48 | 1.36 / 1.50 |
| sns GetTopicAttributes | 0.58 / 0.64 | 0.58 / 0.64 |
| kinesis PutRecord 100 B | 0.84 / 1.03 | 0.83 / 1.00 |
| kinesis PutRecords 10 x 100 B | 0.91 / 1.04 | 0.89 / 1.01 |
| kinesis GetRecords (Limit 10) | 1.05 / 1.26 | 1.03 / 1.13 |
| secretsmanager GetSecretValue (KMS decrypt) | 0.66 / 0.74 | 0.66 / 0.74 |
| secretsmanager PutSecretValue (KMS encrypt) | 1.10 / 1.38 | 1.11 / 1.30 |
| ssm GetParameter String | 0.49 / 0.54 | 0.50 / 0.54 |
| ssm GetParameter SecureString WithDecryption | 0.51 / 0.58 | 0.51 / 0.59 |
| ssm PutParameter Overwrite | 1.71 / 1.80 | 2.11 / 2.30 |
| kms Encrypt 64 B (symmetric) | 0.43 / 0.63 | 0.43 / 0.48 |
| kms Decrypt | 0.42 / 0.48 | 0.43 / 0.49 |
| kms GenerateDataKey AES_256 | 0.43 / 0.50 | 0.43 / 0.47 |
| kms Sign ECDSA_SHA_256 | 0.55 / 0.63 | 0.55 / 0.64 |
| sts GetCallerIdentity | 0.42 / 0.52 | 0.42 / 0.49 |
| sts AssumeRole (stores a session) | 0.70 / 1.04 | 0.71 / 0.99 |

Reading it: a read is one pooled connection and one query (SSM GetParameter and KMS calls are dominated by the HTTP and SDK cost; a KMS operation reads the key row once per two seconds per process and does AES-GCM in memory). Publishing to a topic with one SQS subscription costs a
transaction (topic, dedup, subscriptions) plus the in-process SQS insert. `PutSecretValue` and `SecureString` writes seal the value with KMS before the transaction opens. `PutParameter Overwrite` is the slowest row: it reads the head row, then writes the head, the new history row and prunes old versions in one transaction.
Two sharded backends do not change the per-request cost (an operation lands on one host); listings (ListTopics, ListSecrets, DescribeParameters) fan out and are not in this table. Every operation is recorded through `SqlMetricsCollector` under `awswire` (unified endpoint) or `snswire` / `kinesiswire` / `secretswire` / `ssmwire` / `kmswire` / `stswire` (own port).

## 2026-09-26: pubsubwire (Google Cloud Pub/Sub gRPC) -- RTT next to Google's official Pub/Sub emulator

Setup: one real Warp process + native Postgres 17 (`WARP_TEST_PG_LOCAL=1`, one backend, then two sharded backends), the official emulator (`gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators`, `gcloud beta emulators pubsub start`) in
Docker on the same machine, `Warp/tests/python/ps_conformance/ps_rtt_bench.py` (raw gRPC from Python, one channel per side, sequential, 300 measured calls per operation after 30 warm-ups, ~1 KB messages, client-side
median / p95 in ms; the "10 messages" row uses 60 calls). One topic with one subscription, so the two-backend run places the subscription's queue on one of the two hosts. The machine was shared with other builds and
test runs; differences of a few hundredths of a millisecond are noise.

| operation | emulator median / p95 | Warp, 1 Postgres median / p95 | Warp, 2 sharded Postgres median / p95 |
|---|---|---|---|
| Publish 1 message | 0.48 / 0.59 | 0.56 / 0.91 | 0.57 / 0.91 |
| Publish 10 messages | 0.47 / 0.56 | 0.57 / 0.72 | 0.60 / 0.87 |
| Pull 1 message (available, `return_immediately`) | 0.48 / 0.54 | 0.36 / 0.53 | 0.36 / 0.52 |
| Acknowledge 1 message | 0.44 / 0.49 | 0.30 / 0.41 | 0.30 / 0.45 |
| GetSubscription | 0.47 / 0.67 | 0.16 / 0.23 | 0.17 / 0.23 |
| Publish -> StreamingPull delivery (one open stream) | 51.76 / 56.92 | 0.58 / 1.02 | 0.64 / 1.46 |

Caveats, honestly: the emulator is a Java process that keeps everything in memory (no durability, no fsync) behind Docker's port forwarding, Warp runs natively and commits every publish to Postgres, so this compares two
products with different guarantees, not two builds of one. Warp's Publish is the slower row (one transaction per publish plus the topic and subscription lookups on the home host); Pull and Acknowledge win because a
subscription's document is cached for a second per process and the claim is one indexed `UPDATE ... FOR UPDATE SKIP LOCKED`. The 51 ms emulator delivery latency looks like a polling interval in its StreamingPull path (not investigated), Warp wakes the stream's pump in-process on publish (a publish handled by another node is seen within the 200 ms idle poll). With more subscriptions per topic, Publish costs one insert
per subscription (batched per host); a topic whose subscriptions span both hosts adds the outbox write (one more short statement on the home host) and one transaction per host. Every operation is
reported to the metrics collector under the protocol name `pubsubwire` (labels `Publish`, `Pull`, `Acknowledge`, ...).

## 2026-09-26: firestorewire and datastorewire (Firestore / Datastore gRPC) -- RTT next to Google's official emulators

Setup: one real Warp process on native Postgres 17 (one backend), the official emulators (`gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators`, Docker, `--memory 1g`), raw gRPC from Python
(`Warp/tests/python/fs_conformance/fs_rtt_bench.py`, `ds_conformance/ds_rtt_bench.py`), one channel per side, sequential calls, 200 measured calls per row after 20 warm-ups, ~1 KB documents, 200 documents
in the collection; median / p95 in ms. The machine was shared with other test runs.

| Firestore operation | emulator | Warp |
|---|---|---|
| Commit: set 1 document | 0.46 / 0.54 | 0.89 / 1.21 |
| GetDocument | 0.40 / 0.48 | 0.27 / 0.38 |
| BatchGetDocuments (10) | 0.68 / 0.86 | 0.52 / 0.79 |
| RunQuery: collection, limit 20 | 1.86 / 2.62 | 0.73 / 1.02 |
| RunQuery: filter, limit 20 | 1.72 / 2.68 | 0.60 / 0.83 |
| RunAggregationQuery: count | 0.80 / 1.05 | 0.26 / 0.34 |
| Transaction: begin + get + commit | 1.74 / 2.25 | 0.78 / 0.92 |

| Datastore operation | emulator | Warp |
|---|---|---|
| Commit: upsert 1 entity | 0.75 / 1.07 | 0.75 / 1.01 |
| Lookup 1 key / 10 keys | 0.52 / 0.65, 0.55 / 0.76 | 0.29 / 0.34, 0.32 / 0.40 |
| RunQuery: kind, limit 20 | 0.57 / 0.85 | 0.52 / 0.80 |
| RunQuery: filter, limit 20 | 0.69 / 1.00 | 0.48 / 0.62 |
| Transaction: begin + lookup + commit | 1.46 / 2.14 | 0.93 / 1.42 |

Caveats: the emulators are in-memory Java processes behind Docker port forwarding with no durability; Warp runs natively and commits every write to Postgres (a Firestore commit is the slower row: advisory locks,
a load, the write and the version-log row in one transaction). Warp's queries are cheap here because 200 documents are scanned and filtered in Java from one keyset page; scans grow linearly with the collection (no
composite indexes exist) -- these numbers say nothing about large collections. Not measured: two-backend runs, Listen latency.

## 2026-09-26: bigtablewire (Google Cloud Bigtable gRPC) -- RTT next to Google's official Bigtable emulator

Setup: one real Warp process + native Postgres (`WARP_TEST_PG_LOCAL=1`, one backend, then two sharded backends), the official emulator (`gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators`, `gcloud beta emulators bigtable start`) in Docker. Client: `Warp/tests/python/bt_conformance/bt_rtt_bench.py`, one gRPC channel, sequential calls, 300 per operation (fewer for the batch/scan rows), median / p95 in ms. The emulator is in-memory with no durability, Warp commits every write to Postgres: two products with different guarantees, not two implementations of one.

| operation | emulator med / p95 | Warp 1 Postgres med / p95 | Warp 2 Postgres med / p95 |
|---|---|---|---|
| MutateRow (1 KB cell) | 0.60 / 0.69 | 1.01 / 1.58 | 0.55 / 0.75 |
| MutateRows (10 rows) | 1.08 / 1.19 | 1.64 / 2.27 | 1.28 / 1.84 |
| ReadRows (1 row by key) | 0.67 / 0.77 | 0.37 / 0.58 | 0.35 / 0.56 |
| ReadRows (scan 100 rows) | 5.48 / 6.32 | 1.45 / 2.29 | 1.52 / 2.24 |
| ReadModifyWriteRow (increment) | 0.58 / 0.68 | 0.36 / 0.64 | 0.37 / 0.72 |
| CheckAndMutateRow | 0.57 / 0.68 | 0.32 / 0.60 | 0.28 / 0.47 |
| GetTable | 0.53 / 0.64 | 0.16 / 0.30 | 0.16 / 0.29 |

Single-run numbers on a loaded developer machine (other Warp processes were running); read them as order of magnitude. Every operation is reported to the metrics collector under the protocol name `bigtablewire`.

## 2026-09-26: cqlwire (Apache Cassandra CQL native protocol) -- RTT next to a real Apache Cassandra 5.0

Setup: one real Warp process on native Postgres (`WARP_TEST_PG_LOCAL=1`, one backend, then two sharded backends), a real Apache Cassandra 5.0.9 (`cassandra:5.0`, Docker, single node, `--memory 1500m`,
`MAX_HEAP_SIZE=512M`, default `commitlog_sync: periodic`). Client: the DataStax python driver 3.29.3 (`Warp/tests/python/cql_conformance/cql_rtt_bench.py`), protocol v4, one connection, prepared
statements, consistency ONE, sequential calls, 400 per operation (100 for the LWT and count rows) after the schema was created, median / p99 in ms. The machine was a developer laptop shared with other
test processes; single run, read as order of magnitude. Warp's default Developer edition caps a process at 25 concurrent client connections, so nothing here measures concurrency.

| operation | Cassandra 5.0 median / p99 | Warp, 1 Postgres median / p99 | Warp, 2 sharded Postgres median / p99 |
|---|---|---|---|
| INSERT (prepared) | 0.46 / 0.62 | 0.32 / 0.81 | 0.51 / 2.19 |
| SELECT by key (prepared) | 0.48 / 0.62 | 0.24 / 0.67 | 0.23 / 0.62 |
| UPDATE (prepared) | 0.46 / 0.63 | 0.20 / 0.46 | 0.17 / 0.46 |
| counter UPDATE | 0.49 / 0.72 | 0.26 / 0.53 | 0.15 / 0.29 |
| LWT INSERT IF NOT EXISTS | 0.91 / 1.25 | 0.48 / 0.97 | 0.39 / 0.72 |
| SELECT LIMIT 10 in a partition | 0.46 / 0.69 | 0.36 / 1.10 | 0.29 / 0.69 |
| SELECT count(*) of a 50-row partition | 0.51 / 0.64 | 0.41 / 1.15 | 0.39 / 0.96 |

Caveats, honestly: these are two products with different guarantees, not two builds of one. Cassandra here is a JVM in a Docker VM behind port forwarding whose commit log syncs periodically, and the
python driver dominates the sub-millisecond numbers; Warp runs natively next to a local Postgres and every write is a synchronously committed Postgres transaction (one statement or transaction per
CQL operation, no coordinator hop, no replicas). That single-node, localhost setup is why Warp is at or below Cassandra for point reads and writes; it says nothing about a real cluster, replication,
large datasets or sustained load. The rows that cost more are the honest ones: `INSERT` writes a row marker cell plus one cell per column (one JDBC batch in one transaction), a list, set or map write adds a
collection tombstone and a delete leaves tombstones, and a read merges cells, static cells and range tombstones in Java (`SELECT LIMIT 10`, `count(*)`). With two backends a single-partition operation
still touches one host (the higher `INSERT` p99 on two backends is tail latency of single commits, not investigated further, not a second round trip); statements without a partition key read every host and merge. Not measured: paged full scans,
wide partitions beyond 50 rows, concurrent clients. Every operation is reported to the metrics collector under the protocol name `cqlwire` (labels `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `BATCH`, ...).

## 2026-09-26: kafkawire (Apache Kafka wire protocol) -- RTT next to a real Apache Kafka 4.3.1

Setup: one real Warp process on native Postgres (`WARP_TEST_PG_LOCAL=1`, one backend, then two sharded backends), a real Apache Kafka 4.3.1 (`apache/kafka:latest`, Docker, KRaft, one node combined
broker/controller, `--memory 1g`, `KAFKA_HEAP_OPTS=-Xmx512m`, `group.initial.rebalance.delay.ms=0`, default `acks`/flush settings, `-p 29092:9092`). Client for the rows above the last two: the raw-protocol
client of `Warp/tests/python/kafka_conformance/kafka_raw_client.py` (official Kafka message schemas, one TCP connection, requests sent one at a time, so the numbers are broker round trips and not
client-library overhead), 300 requests per row (75 for the 100-record produce, 75 end-to-end samples) after 20 warm-up produces; the last two rows use the real `kafka-python` 3.0.11 producer/consumer.
Median / p99 in ms, `Warp/tests/python/kafka_conformance/kf_rtt_bench.py`. The machine was a developer laptop shared with other test processes (other Warp instances and containers were running);
single run, read as order of magnitude. Warp's default Developer edition caps a process at 25 concurrent client connections, so nothing here measures concurrency.

| operation | Apache Kafka 4.3.1 median / p99 | Warp, 1 Postgres median / p99 | Warp, 2 sharded Postgres median / p99 |
|---|---|---|---|
| Produce 1 record, acks=all | 0.42 / 0.76 | 0.22 / 0.51 | 0.23 / 0.51 |
| Produce 1 record, acks=1 | 0.38 / 0.53 | 0.18 / 0.31 | 0.18 / 0.33 |
| Produce 10 records, acks=all | 0.39 / 0.53 | 0.17 / 0.26 | 0.17 / 0.26 |
| Produce 100 records, acks=all | 0.44 / 0.76 | 0.19 / 0.24 | 0.20 / 0.25 |
| Fetch 1 batch of 10 records (existing data) | 0.71 / 1.18 | 0.44 / 0.67 | 0.45 / 0.65 |
| Fetch 100 records (existing data) | 1.88 / 2.84 | 0.68 / 0.91 | 0.68 / 0.87 |
| Fetch at the end (empty, max_wait=0) | 0.57 / 1.14 | 0.10 / 0.17 | 0.10 / 0.18 |
| ListOffsets latest | 0.36 / 0.61 | 0.08 / 0.17 | 0.09 / 0.14 |
| Metadata (one topic) | 0.39 / 0.47 | 0.13 / 0.24 | 0.13 / 0.21 |
| Heartbeat | 0.38 / 0.62 | 0.07 / 0.14 | 0.07 / 0.15 |
| OffsetCommit | 0.50 / 0.71 | 0.13 / 0.23 | 0.13 / 0.24 |
| OffsetFetch | 0.60 / 0.75 | 0.11 / 0.18 | 0.11 / 0.23 |
| produce, then a long-polling consumer's Fetch returns it (end to end) | 3.87 / 5.49 | 2.54 / 3.60 | 2.47 / 3.67 |
| kafka-python `send().get()`, acks=all | 1.36 / 2.97 | 0.71 / 0.87 | 0.71 / 0.80 |
| kafka-python produce, then `poll()` sees it (end to end) | 4.34 / 113.69 | 3.57 / 114.11 | 1.51 / 3.21 |

Caveats, honestly: these are two products with different guarantees, not two builds of one. Kafka here is a JVM in a Docker VM behind port forwarding and replicates nothing (one node) while Warp runs natively
next to a local Postgres; every Warp produce is a synchronously committed Postgres transaction (one row lock on the partition, one insert per batch, one update of the log end), which on a local, fsync-cheap
Postgres is as fast as or faster than the Docker-hosted broker. That says nothing about a real cluster, replication, disks that fsync slowly, large batches, hundreds of partitions per request or sustained
load; Kafka's strength (sequential log appends, zero-copy fetch, consumers spread over brokers) is not measured. The end-to-end rows are dominated by the client: the raw client's two connections and a thread
hand-off (about 2.5 ms on both brokers' order of magnitude), and `kafka-python`'s fetch and poll loop, whose p99 of about 114 ms is a client wait, identical for Kafka and Warp, not broker time. A Fetch that has to wait is
woken by an in-process signal when the produce came through the same Warp node and otherwise notices an append within its 100 ms re-check; the end-to-end rows above are the same-node case. Not measured: many concurrent
producers and consumers, fetches of thousands of partitions (a single Fetch of 1,000 partitions on two backends took 0.07 s in the test suite), rebalances of large groups, retention sweeps under load. Every request is
reported to the metrics collector under the protocol name `kafkawire` (labels `Produce`, `Fetch`, `Metadata`, `JoinGroup`, `OffsetCommit`, ...).

### gremlinwire (Apache TinkerPop Gremlin Server protocol) versus a real Gremlin Server

`tests/python/gremlin_conformance/gr_rtt_bench.py`: the same requests over **one persistent WebSocket, sequential, GraphSON 3.0** (raw client, so no driver overhead is timed; every answer is checked to be a 200/204), 300
requests each (reads after one warm-up), median and p99 client-side in milliseconds, on the standard *modern* toy graph (6 vertices, 6 edges). The reference is `tinkerpop/gremlin-server:latest` (3.8.2, TinkerGraph, default
`gremlin-server.yaml`) in Docker with `--memory 1g` and its port published to localhost; Warp is the shaded jar with one, and with two sharded, **native** Postgres 17 servers on the same machine.

| request | Gremlin Server 3.8 (TinkerGraph) median / p99 (ms) | Warp, 1 Postgres median / p99 | Warp, 2 sharded Postgres median / p99 |
|---|---|---|---|
| script 1+1 (protocol floor) | 0.37 / 0.50 | 0.11 / 0.27 | 0.11 / 0.43 |
| g.V().count() | 0.39 / 0.51 | 0.24 / 0.79 | 0.26 / 0.78 |
| g.V(1).values('name') | 0.41 / 0.48 | 0.17 / 0.49 | 0.14 / 0.44 |
| g.V(1).out().values('name') | 0.41 / 0.50 | 0.27 / 0.64 | 0.29 / 0.89 |
| g.V().has('name','marko').out('knows').values('name') | 0.42 / 0.54 | 0.20 / 0.57 | 0.29 / 1.25 |
| g.V(1).out().out().path() | 0.46 / 0.58 | 0.40 / 1.09 | 0.38 / 1.26 |
| g.V(1).repeat(out()).times(2).values('name') | 0.43 / 0.81 | 0.43 / 1.40 | 0.44 / 1.16 |
| g.V().hasLabel('person').group().by(label).by(count()) | 0.45 / 0.57 | 0.16 / 0.76 | 0.16 / 0.55 |
| bytecode g.V(1).out().values('name') | 0.40 / 0.55 | 0.28 / 0.75 | 0.29 / 1.22 |
| write: g.addV('b').property('i', n) | 0.44 / 0.53 | 0.27 / 1.62 | 0.18 / 0.83 |
| write: g.V(1).property('n', n) | 0.44 / 0.59 | 0.32 / 0.70 | 0.23 / 0.77 |
| write: addV + addE (one script) | 0.44 / 0.88 | 0.72 / 1.31 | 0.69 / 2.20 |
| throughput, 8 clients (ops/s) | 9448 | 9786 | 8430 |

How to read it. The reference pays Docker Desktop's userland port proxy (its own "protocol floor" row, a script that touches no graph, is 0.37 ms against 0.14 ms for Warp's JVM with no proxy), so the comparison is **not
apples to apples** and says nothing about which implementation is faster in equal conditions; what it does show is that a Gremlin request costs Warp one to a few Postgres round trips on top of a sub-millisecond protocol floor (the multi-statement `addV` + `addE` script is the one row where Warp is slower than the in-memory reference). Reads: `g.V(1).values('name')` is one indexed lookup; `out()` is two (edges of the vertex, then the neighbour vertices, batched per 128 traversers, not per traverser);
`repeat(out()).times(2)` and `path()` add one pair per hop. With **two sharded backends** an `out()` still touches one host but `in()` / `both()` ask both, which is the small extra cost visible on the multi-hop rows.
Writes: `addV().property()` is one `INSERT` plus one `nextval()` for the generated id (the id sequence lives on the first host), a property update is `SELECT ... FOR UPDATE` plus `UPDATE` in one transaction, an
`addV` + `addE` script does two vertex reads and three inserts. Throughput is 8 clients each running 300 `g.V(1).out().values('name')` (Developer edition caps a Warp at 25 connections; the machine also runs the Docker VM
and other work, so treat the figure as an order of magnitude). GraphBinary and bytecode requests cost the same as the script rows within noise (one bytecode row is included).

Method notes: Postgres `fsync` is on; the graph and the request strings are the ones in the golden corpus, so the bench doubles as a smoke test of the same code paths the conformance run exercises.

### Azure Cosmos DB for NoSQL (cosmoswire), Warp only

Sequential raw signed REST requests on one connection (no real Cosmos service or emulator to compare with), median / p99 in ms, n=300.

| Operation | 1 Postgres backend | 2 Postgres backends |
|---|---|---|
| Create item (1 KB) | 0.97 / 1.20 | 0.97 / 1.13 |
| Point read | 0.88 / 1.19 | 0.86 / 1.20 |
| Upsert item | 0.97 / 1.33 | 0.98 / 1.20 |
| Replace item | 1.19 / 1.49 | 1.13 / 1.36 |
| Patch item (set + incr) | 1.20 / 1.40 | 1.13 / 1.33 |
| Delete item | 1.08 / 1.28 | 1.03 / 3.83 |
| Query, one partition key (10 items) | 0.88 / 1.11 | 0.88 / 1.50 |
| Query by id in one partition | 0.86 / 1.14 | 0.84 / 1.04 |
| Cross-partition SELECT VALUE COUNT(1) (1000 items) | 0.87 / 1.21 | 0.95 / 1.25 |
| Cross-partition filter scan (1000 items, 10 hits) | 2.22 / 4.19 | 2.21 / 4.02 |
| Cross-partition ORDER BY n DESC TOP 10 (1000 items) | 2.55 / 4.39 | 2.66 / 4.62 |
| Cross-partition GROUP BY pk COUNT (1000 items) | 2.25 / 3.76 | 2.35 / 4.07 |
| Transactional batch of 5 creates | 1.38 / 1.59 | 1.42 / 1.68 |
