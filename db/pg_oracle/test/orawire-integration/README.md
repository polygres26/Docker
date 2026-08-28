# orawire + pg_oracle live integration test

The first real end-to-end run: `sqlcl` (Oracle's actual client) -> Polywire's orawire (TNS/TTC)
-> Postgres 17 with `pg_oracle` installed. Not simulated -- a real `sqlcl` process connected
over the real Oracle wire protocol port (11521) to a real running Polywire instance.

`test.sql` is the exact script run; `output-after-fix.log` is `sqlcl`'s real output, captured
*after* the one critical bug below was found and fixed (see git history for the before/after).

## The critical bug this run found and got fixed

**`db_emulation` was never actually being set on a plain username/password orawire
connection at all.** `JdbcBackendExecutor` skips its session initializer entirely for an
`AccessContext.ANONYMOUS` connection (no `POLYWIRE_AUTH_MODE`/OAuth/RBAC configured) -- a real,
correct optimization for RLS/VPD-context propagation, but wrong for `db_emulation`, which is a
protocol-level requirement of every orawire session, not a per-user concern. Confirmed live: with
the bug present, `V$VERSION`/`V$INSTANCE`/`V$SESSION` and the new one-argument `to_char()`
overload all failed with `ORA-00942`/`ORA-00904` ("table or view does not exist"/"invalid
identifier"), while schema-qualified calls like `DBMS_RANDOM.STRING(...)` (which don't depend on
`search_path` at all) worked throughout -- that contrast is what pointed at the real cause instead
of a pg_oracle bug. Fixed with a `runEvenWhenAnonymous()` override
(`wire/src/main/java/com/nexagres/wire/core/access/NativeRlsSessionInitializer.java` and
`OraclePgEmulationSessionInitializer.java`) -- see that commit for the full story. Reverified
after the fix: `V$VERSION`/`V$INSTANCE`/`V$SESSION` all return real data.

## Remaining real findings from this same run, not yet fixed

- **`SYS_CONTEXT('USERENV', 'CURRENT_USER')` -> `ORA-00904: "sys_context(unknown, unknown)":
  invalid identifier`**, even after the db_emulation fix. `pg_stat_statements` shows orawire
  forwards the two string literals as untyped bind parameters Postgres can't resolve an overload
  for. Needs a fix in orawire's bind-parameter typing/translation for this call shape (likely
  `BindVariableRewriter` or the dialect translator emitting an explicit `::text` cast), not in
  `pg_oracle` itself.
- **Bare, one-argument `TO_CHAR(SYSDATE)` doesn't reach `pg_oracle`'s new one-arg overload at
  all** -- `pg_stat_statements` shows orawire's own translator rewrites it into a two-argument
  Postgres call (`TO_CHAR(CURRENT_TIMESTAMP, $1)`, synthesizing its own default format string)
  before it ever reaches Postgres. Since `pg_catalog` already has an exact two-argument
  `to_char(timestamptz, text)` overload, that one wins the tie every time (matches the
  already-documented two-argument-form limitation in `db/pg_oracle/README.md`) -- but it means
  the new one-argument capability this session added to `pg_oracle` is currently unreachable
  through orawire specifically, only through a direct schema-qualified call or plain `psql`.
  Fixing this means either teaching orawire's translator to leave a genuinely bare
  `TO_CHAR(SYSDATE)` alone (let `pg_oracle`'s one-arg overload handle it), or accepting the
  two-argument-only path and improving that path's `RR`/`FF`/`X` fidelity instead.
- **`CREATE TABLE emp_test (id NUMBER, name VARCHAR2(50))` fails outright**: "statement cannot
  be translated from ORACLE to POSTGRES (needs manual migration): LLM fallback translator
  failed: null". orawire's built-in (non-LLM) dialect translator doesn't map basic Oracle DDL
  types (`NUMBER`, `VARCHAR2`) to Postgres equivalents, and the LLM fallback isn't configured in
  this environment, surfacing a bare "null" rather than a clear "LLM not configured" message.
  This is a significant, high-value gap: none of `pg_oracle`'s V$/DBMS_*/UTL_* work matters for
  a migrated app that can't even `CREATE TABLE` its own schema. Squarely orawire's dialect
  translator, not `pg_oracle`.
- **An anonymous `BEGIN ... this_procedure_does_not_exist(); END; /` PL/SQL block** hit
  `unsupported TTC function code: 59` server-side (`RequestLoop.java`) on an earlier run of this
  same test -- a genuine, unimplemented Oracle wire-protocol function code gap in orawire's TTC
  layer, separate from the (already well-documented) anonymous-PL/SQL-block architecture
  limitation itself.
- `BEGIN DBMS_OUTPUT.PUT_LINE(...); END; /` fails with a plain Postgres syntax error at
  `DBMS_OUTPUT` -- exactly the already-documented anonymous-PL/SQL-block limitation
  (`db/pg_oracle/README.md`'s own section), reproduced here for the first time over the real
  wire protocol rather than just reasoned about.
- `DBMS_CRYPTO.HASH(UTL_RAW.CAST_TO_RAW(...), ...)` and `DBMS_METADATA.GET_DDL(...)` correctly
  fail with "schema does not exist" -- expected, both `UTL_RAW` and `DBMS_METADATA` are
  documented as not-yet-implemented in `pg_oracle`'s "Remaining top-20 scope", not a surprise.
- Division by zero (`ORA-01476`) and a genuine SQL syntax error both surfaced with sensible,
  real error messages -- working as expected, included for completeness of the log.

## How to reproduce

```bash
# 1. Postgres with pg_oracle installed (see db/pg_oracle/README.md's Building section)
# 2. Build and run Polywire pointed at it (see wire/scripts/run.sh)
# 3. Real Oracle client against orawire's port (11521 by default):
sql -S "postgres/postgres@127.0.0.1:11521/postgres" @test.sql
```
