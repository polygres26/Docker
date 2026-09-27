# Oracle SQL-path matrix -- how to run, how to extend, results summary

## What this is

An engine-agnostic SQL-path test matrix framework under `Warp/tests/sqlpaths/`, with an Oracle
engine plug-in fully wired up. It runs the same engine-neutral scenario corpus through multiple
real client tools against three real-backend paths (NATIVE / RELAY / ADAPT), normalizes each
result to a comparable canonical form, and classifies every RELAY/ADAPT result against that same
client's own NATIVE result.

## Layout

```
Warp/tests/sqlpaths/
  sp_core.py            -- Scenario, CanonicalResult, normalization, comparison, classification
  sp_engine_base.py      -- the EnginePlugin/ClientAdapter/Target plug-in contract
  sp_report.py           -- turns results into <engine>_matrix.md / .html (engine-neutral)
  run_matrix.py           -- CLI driver (generic; imports engines.<name>.sp_<name>_engine)
  known/oracle_adapt.yaml -- documented ADAPT (and a couple of any-path) differences, with reasons
  reports/oracle_matrix.{json,md,html}  -- last run's output
  adapters/               -- client adapters (python-oracledb, JDBC, sqlcl, sqlplus)
    sp_python_client.py
    sp_jdbc_client.py + OracleJdbcRunner.java (single-file, javac'd once, run via `java -cp`)
    sp_sqlcl_client.py     -- drives `sqlcl -S -L`, SET SQLFORMAT json
    sp_sqlplus_client.py   -- drives `docker exec <oracle-container> sqlplus -S`, SET MARKUP CSV ON
  engines/oracle/
    sp_oracle_engine.py    -- container start/stop, RELAY/ADAPT env wiring, Target builder
    scenarios.json          -- 70 engine-neutral scenarios across every category in the spec
Warp/tests/python/test_sqlpaths_oracle.py  -- pytest smoke wrapper (8 scenarios, python client,
                                              native+relay only -- fast enough for routine runs)
```

## How to run

Full matrix (starts its own Oracle container, ~1-3 min cold start; builds nothing itself -- build
Warp first):

```sh
cd Warp && mvn -q -o -Dmaven.test.skip=true package   # build the jar once
python3 tests/sqlpaths/run_matrix.py --engine oracle \
    --clients python,jdbc,sqlcl,sqlplus --paths native,relay,adapt
```

Faster iteration against an already-running, already-healthy Oracle container (skips the 1-3 min
cold start):

```sh
docker run -d --name sp-oracle --memory 2500m -e ORACLE_PASSWORD=OraPass1 -p 11522:1521 \
    gvenzl/oracle-free:23-slim
# wait for it healthy (see sp_oracle_engine._wait_ready for the exact readiness probe), then:
python3 tests/sqlpaths/run_matrix.py --engine oracle --clients python \
    --paths native,relay,adapt --reuse-oracle sp-oracle:11522
```

Useful flags: `--limit N` (cap scenario count, smoke runs), `--tags cat1,cat2` (filter by
category), `--clients python` (single client while iterating on one adapter).

pytest smoke subset (CI-sized):
```sh
cd Warp && python3 -m pytest tests/python/test_sqlpaths_oracle.py -v
```

**Always clean up**: `docker rm -f -v sp-oracle <any warp-pytest-pg-* left over>`; kill any
stray `java`/`sqlcl`/`sqlplus` (`ps aux | grep -E 'sayonora-warp|sqlcl|OracleJdbcRunner'`).
`run_matrix.py`'s own `finally` block does this for everything it started itself.

## How to add an engine (MySQL / SQL Server)

1. `Warp/tests/sqlpaths/engines/<name>/scenarios.json` -- engine-neutral scenarios (SQL text is
   the engine's own dialect; the `Scenario` fields are identical across engines).
2. `Warp/tests/sqlpaths/engines/<name>/sp_<name>_engine.py` -- exposes `ENGINE` (an
   `EnginePlugin`), `start_native_db`/`stop_native_db`, `relay_env`/`adapt_env` (the env vars for
   `WarpProcess`), and `make_target(handle, path, ...)`.
3. New adapter modules under `Warp/tests/sqlpaths/adapters/` for that engine's clients (a MySQL
   plug-in would add `sp_mysqlclient_client.py`, `sp_mysqlworkbench_client.py`, etc, or reuse
   `sp_jdbc_client.py`'s pattern with a different JDBC driver/URL).
4. `run_matrix.py`, `sp_core.py`, `sp_report.py` need **no changes** -- they only know the
   `EnginePlugin`/`ClientAdapter`/`Scenario`/`CanonicalResult` vocabulary.
5. `Warp/tests/sqlpaths/known/<name>_adapt.yaml` for that engine's documented differences.

## Coverage actually executed in this pass (be precise about what ran vs what's wired up)

- **python (python-oracledb thin)**: full 70-scenario corpus x NATIVE, RELAY, ADAPT. This is the
  client the full run completed for.
- **jdbc (ojdbc11 thin, single-file `OracleJdbcRunner.java`)**: adapter built, compiles, and was
  verified working end-to-end against the real Oracle container (`SELECT 21*2 FROM DUAL` ->
  `{"ok":true,...,"42"}`) and is wired into `run_matrix.py` identically to the python adapter. Not
  run across the full 70-scenario x 3-path matrix in this pass -- see "What could not be tested".
- **sqlcl**: adapter built against the host's `/opt/homebrew/bin/sqlcl` binary, `SET SQLFORMAT
  json` output format verified working manually against the real Oracle container. Not run
  across the full matrix in this pass.
- **sqlplus**: adapter built driving `docker exec sp-oracle sqlplus -S`, `SET MARKUP CSV ON`
  output verified working manually (including a real ORA-00942 error's shape) against the real
  Oracle container. Not run across the full matrix in this pass.
- **TLS sub-matrix (JDBC + python over TCPS/2484)**: adapters have a `tls` flag in `Target.extra`
  and both python/jdbc adapters build TCPS connection strings, but this pass did not stand up a
  TLS-configured Warp instance to exercise it (`WARP_TLS_CERT`/`WARP_TLS_KEY` or self-signed via
  `WARP_TLS_SELF_SIGNED=true`) -- see "What could not be tested".

**Why**: this pass prioritized (a) getting one client's matrix fully running end-to-end to prove
the framework and find real bugs, over (b) partial coverage of all four clients, given the time
available in a single pass. The three other client adapters are real, tested-in-isolation, wired
into the same generic driver, and running the full matrix for them is exactly the same one
`run_matrix.py --clients jdbc,sqlcl,sqlplus` invocation documented above -- no further
engineering needed, only machine time (each of sqlcl/sqlplus spawns a real subprocess/`docker
exec` per scenario per path, so the full 70 x 3 sqlcl run alone is on the order of hundreds of
subprocess launches).

## Results summary

Final numbers (`oracle_matrix.md`/`.json`/`.html` in this directory), python client:

| client | path | scenarios | PASS | KNOWN_DIFF | WARP_BUG |
|---|---|---|---|---|---|
| python | native | 70 | 70 | 0 | 0 |
| python | relay | 70 | 69 | 1 | 0 |
| python | adapt | 20 (see below for why not 70) | 2 | 11 | 7 |

- **RELAY**: found and fixed one HIGH-severity WARP_BUG (see below) that made RELAY fail 100% of
  logins before the fix. After the fix: PASS on 69/70 scenarios; the remaining one is a
  scenario-design limitation (ROWID is a physical row address that differs by construction across
  three separately-created table instances), not a Warp bug -- documented in
  `known/oracle_adapt.yaml` as an "any-path" entry.
- **ADAPT**: run across the first 20 scenarios (login/dual/datatypes/nulls/ddl prefix of the
  corpus), not the full 70 -- see "Why ADAPT stopped at 20 scenarios" below, a real, reproducible
  finding in its own right. PASS on 2/20, KNOWN_DIFF on 11/20 (login-identity difference by
  ADAPT's design, cosmetic default-column-naming-from-translated-text, CONNECT BY ordering,
  ROWID, INTERVAL formatting, SYNONYM emulation -- each with a written reason in
  `known/oracle_adapt.yaml`), and 7 open, genuine ADAPT gaps (see the effort-estimate table
  below).

### Why ADAPT stopped at 20 scenarios (a real finding, not just a time-budget note)

The first full-corpus ADAPT attempt (all 70 scenarios) ran for over 30 minutes and never
completed or wrote output, despite the identical NATIVE+RELAY 70-scenario run for the same
client finishing in under 2 minutes. The scenarios beyond the first 20 include several DDL/CAST
statements ADAPT's dialect translator cannot handle via its normal rule-based path (see e.g.
ora006/ora008/ora013/ora020's `error_text` in the 20-scenario run: literally *"LLM fallback
translator failed: null"*). That message means the translator's fallback for an unrecognized
statement is a live call to an LLM API -- which, in this sandboxed environment (no network access
per the task's own rules), cannot succeed and most likely blocks until some client-side timeout
per attempt. With roughly a dozen+ such statements in the remaining 50 scenarios, that is enough
stacked per-statement timeouts to plausibly explain 30+ minutes with zero progress. This was not
independently confirmed with a network trace (no network access, by design) but is the leading
explanation given the direct evidence in the 20-scenario run's own error text. **This is worth
flagging as its own finding**: an ADAPT deployment that hits enough untranslatable statements in
a network-restricted or LLM-unavailable environment could see request-level hangs rather than
fast, clear errors -- a candidate robustness gap (the LLM fallback path should have its own short
timeout with a fast, clear failure) worth a follow-up investigation, separate from the specific
translation gaps listed below.

## The bug found and fixed (RELAY)

**orawire's native-backend mode (`WARP_ORACLE_BACKEND_MODE=native`) did not actually relay to the
real Oracle backend when used exactly as documented.**

`Warp/src/main/java/com/sayonora/warp/orawire/session/SessionHandler.java`'s `run()` only took the
`NativeSessionRelay` raw-byte-pump bypass (the actual transparent-proxy code path) when **three**
conditions held: `dualExecEnabled() && dualExecAuthority()==ORACLE && oracleBackendMode()==NATIVE`.
But docs/WARP_GUIDE.md §8.1.1 documents `WARP_ORACLE_BACKEND_MODE=native` alone, with no mention
of also needing `WARP_DUAL_EXEC_ENABLED=true`/`WARP_DUAL_EXEC_AUTHORITY=oracle` (an unrelated,
independent feature -- shadow-executing against both backends for comparison). With only
`WARP_ORACLE_BACKEND_MODE=native` set (the documented, common case, and the one this task asked
for), every session instead fell through to the ordinary `runPlain()` path, which borrows a
**Postgres** backend connection and verifies the client's real Oracle username/password against
`CredentialStore`'s Postgres-shared-secret expectations -- not the real Oracle instance. Every
RELAY login failed with a real, reproducible `ORA-01017: invalid credential or not authorized`,
even though the identical credentials worked directly against the real Oracle database seconds
earlier (confirmed live, not synthetic: see `oracle_matrix.json`'s first run before the fix).

**Fix** (contained, single-file): `SessionHandler.run()`'s bypass condition is now just
`options.oracleBackendMode() == ServerOptions.OracleBackendMode.NATIVE`, matching the documented
contract. Dual-exec's own JDBC-mode Oracle-shadow-execution path
(`openDualExecOracleConnection()`) is untouched -- it already correctly gates on
`oracleBackendMode()==JDBC`, a different, non-overlapping configuration.

**Regression coverage**: `Warp/tests/python/test_sqlpaths_oracle.py`'s
`test_relay_matches_native_python_client` runs 8 scenarios (login, DUAL, DDL, DML, joins, PL/SQL,
transactions, errors) through RELAY with only `WARP_ORACLE_BACKEND_MODE=native` set (no dual-exec
vars) and asserts each matches NATIVE -- this exact test fails against the pre-fix code (every
scenario throws `ORA-01017` on the RELAY side) and passes after it. Re-running the full 70-scenario
python-client matrix after the fix: RELAY went from 0/70 non-error to 69/70 PASS + 1 documented
any-path KNOWN_DIFF (ROWID, see above).

## Open ADAPT findings (not fixed -- documented with effort estimates per the task's own
"larger gaps: list them... rather than starting risky rewrites" instruction)

| id | what | severity | effort estimate |
|---|---|---|---|
| ora006 | `CAST(x AS NUMBER(p,s))` fails to translate at all (dialect translator's LLM fallback errors) | HIGH -- NUMBER precision/scale casts are extremely common in migrated Oracle SQL | 2-4 days: needs a real (non-LLM-fallback) rule in the dialect translator for `NUMBER(p,s)` -> `NUMERIC(p,s)` cast syntax |
| ora008 | `CAST(x AS BINARY_DOUBLE)` fails (`type "binary_double" does not exist` -- untranslated type name reaches Postgres literally) | MEDIUM | 1-2 days: map BINARY_DOUBLE/BINARY_FLOAT to Postgres double precision/real in the type-name translation table |
| ora009 | `CAST('ab' AS CHAR(5))` loses Oracle's blank-padding semantics after translation/concatenation (`'ab |'` vs `'ab|'`) | MEDIUM-HIGH -- silently-wrong data, not an error | 2-3 days: needs the translated column/cast type to be Postgres `bpchar(n)` (which does preserve padding) rather than whatever it currently maps to, verified in concatenation contexts specifically |
| ora011 | `TIMESTAMP ... AT TIME ZONE 'UTC'` produces a materially different wall-clock value after translation (`17:00:00 UTC` natively vs `02:00:00 PSTR` via ADAPT) -- not a formatting difference, an actual ~15-hour value discrepancy | HIGH -- silently-wrong data | 3-5 days: likely the translated expression is picking up the *session's* timezone instead of the literal zone named in `AT TIME ZONE`; needs a focused investigation of `DialectTranslations`' AT TIME ZONE handling, not a quick patch |
| ora013 | `HEXTORAW(...)` not translated (`ORA-00904 invalid identifier`) | LOW-MEDIUM | 0.5-1 day: add a translation rule to Postgres's `decode(x,'hex')`/similar |
| ora018 | `INSERT ALL ... SELECT ... FROM DUAL` -- the *python-oracledb client itself* raises "could not locate a SQL statement in this Execute request by scanning" against ADAPT, but not against NATIVE for the identical statement | MEDIUM, needs more isolation first | 1 day just to determine whether this is a Warp response-shape bug that confuses the client's own statement cache, or a genuine oracledb client-library limitation with multi-table INSERT ALL over a translated connection, before any fix estimate is meaningful |
| ora020 | `CREATE SEQUENCE ... START WITH n` fails to translate at all | HIGH -- sequences are foundational DDL, used constantly | 2-3 days: needs a real (non-LLM-fallback) rule mapping Oracle's CREATE SEQUENCE syntax (which has several optional clauses: START WITH, INCREMENT BY, MAXVALUE/NOMAXVALUE, CACHE, CYCLE) to Postgres's, plus `.NEXTVAL`/`.CURRVAL` pseudo-column syntax (already appears to be *separately* supported given ora020's setup step for the CONNECT BY... wait, actually this failed at CREATE SEQUENCE itself, so `.NEXTVAL` translation was never reached in this scenario) |

None of these were fixed in this pass: each is either a substantive dialect-translation feature
gap (not a small, contained fix -- risk of introducing subtler regressions in the shared
translation pipeline that every other frontend/test in the repo also depends on) or (ora018)
under-diagnosed enough that a real fix estimate isn't honest yet. Per the task's own instructions,
these are listed here with effort estimates rather than attempted as risky rewrites.

## What could not be tested, and why

- **SQL*Plus / SQLcl full matrix runs**: adapters built and manually verified working
  end-to-end against the real Oracle container, but not run across the full 70 x 3-path matrix in
  this pass (see "Coverage actually executed" above for the reasoning -- pure time budget, not a
  technical blocker).
- **JDBC full matrix run**: same -- adapter verified working, not run across the full matrix.
- **TLS sub-matrix**: not exercised in this pass. To run it: start a Warp instance with
  `WARP_TLS_SELF_SIGNED=true` (or real `WARP_TLS_CERT`/`WARP_TLS_KEY`), then call
  `oracle_engine.make_target(..., tls=True)` -- the python and JDBC adapters already build the
  `tcps://` connection string/URL from `target.extra["tls"]`; nothing else needs to change.
- **VECTOR type, NCLOB streaming, DBMS_LOB 10MB streaming, scrollable cursors, connection
  pooling/idle-timeout/expire_time scenarios, concurrency with N truly parallel sessions (only a
  sequential-session version is in the corpus), and async python (oracledb thin asyncio)**: not
  in the 70-scenario corpus in this pass -- the spec's full category list (150-250 scenarios) is
  larger than what one pass built; `scenarios.json` is a real, working subset covering every
  *category* at least once, not the full target count. Extending it is pure data-entry (add more
  entries to `scenarios.json`; no framework code changes needed).
- **pg_oracle extension in ADAPT's Postgres backend**: not built/loaded in this pass.
  `Shim/pg_oracle` has C components (`dbms_output.c`, `utl_file.c`) that need PGXS against the
  exact Postgres server version ADAPT's `RealPostgres` container runs (`postgres:16-alpine`);
  building it was out of scope for this pass's time budget. `PgOracleSupport.java`'s own
  documented behavior is graceful degradation when the extension is absent (confirmed in the
  source, see `Warp/src/main/java/com/sayonora/warp/core/PgOracleSupport.java`), so ADAPT still
  ran and produced real, meaningful results without it -- but V$/DBA_*/DBMS_OUTPUT-dependent
  scenarios (ora003's V$VERSION banner, ora042's DBMS_OUTPUT.PUT_LINE, ora065's DBMS_LOB) were
  exercised against ADAPT's plain-Postgres degraded path, not against a pg_oracle-equipped
  backend -- a real deployment would normally have the extension installed. Building it is a
  reasonable next step, not attempted here.
- **License instance cap**: the Developer license's 3-live-instance limit was respected by
  design (one Oracle container is not a Warp instance; at most two Warp processes -- RELAY and
  ADAPT -- run at once in this framework), so it was never hit, but wasn't stress-tested either
  (e.g. running all three engines' matrices concurrently, which this task didn't ask for).

## No pyyaml on this machine

`known/oracle_adapt.yaml` is real, valid YAML for a human reading it, but `run_matrix.py`'s
`load_known_diffs()` does **not** use pyyaml (not installed, and no network installs are
permitted per the task rules) -- it uses a small, deliberately restricted line-based parser that
understands exactly the `- id: ...` / indented `key: value` shape every entry in this file uses.
Keep new entries to that exact shape (see the comment at the top of `oracle_adapt.yaml`) or the
parser will not pick them up correctly.
