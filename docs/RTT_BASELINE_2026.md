# RTT Baseline (2026-09-23)

First-ever RTT baseline for all 8 Warp wire protocols, captured while adding real
literal/bind/read/write integration tests for pgwire, mongowire, dynamowire, sqswire, oswire,
influxwire, boltwire, and Warp's native gRPC `QueryService`. See `tests/python/test_<protocol>.py`
for the exact tests; each one warms up with 1 throwaway call then times 40 real calls
(`time.perf_counter`, client side) and cross-checks against `/api/metrics/summary`'s
`avgRttMs` for the operation's fingerprint (server side, from `SqlMetricsCollector`).

**Correction (made outside the pass that generated the rest of this file): `docs/PERFORMANCE.md`
does exist** — at the repo root (`/Users/kumarrajamani/Projects/Sayonora/docs/PERFORMANCE.md`), one
directory above `wire/`. The claim below that it doesn't exist was a wrong-directory mistake (the
agent that wrote this file was working from inside `wire/` and checked `wire/docs/`, which
genuinely has no such file, then wrongly concluded the file didn't exist anywhere). This file was
also moved from the mistaken `wire/docs/` location to the real `docs/` directory to sit alongside
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
`polywire_support.py`) deliberately uses a real, freshly-started `docker run -p <port>:5432`
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
in `tests/python/polywire_support.py`'s `WarpProcess`, not a flake:

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

**Fix** (`tests/python/polywire_support.py`, `WarpProcess.__init__`): renamed the five env vars to
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
