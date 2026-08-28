# orawire + pg_oracle live integration test

The first real end-to-end run: `sqlcl` (Oracle's actual client) -> Polywire's orawire (TNS/TTC)
-> Postgres 17 with `pg_oracle` installed. Not simulated -- a real `sqlcl` process connected
over the real Oracle wire protocol port (11521) to a real running Polywire instance.

`test.sql` is the original battery run; `ddl-test.sql` is the follow-up that specifically
exercises Oracle DDL types; `output-after-fix.log` is `sqlcl`'s real output from the first pass,
captured after the first (of two) real bugs below was found and fixed.

## Bug 1: db_emulation never applying on anonymous orawire connections

**`db_emulation` was never actually being set on a plain username/password orawire connection at
all.** `JdbcBackendExecutor` skips its session initializer entirely for an
`AccessContext.ANONYMOUS` connection (no `POLYWIRE_AUTH_MODE`/OAuth/RBAC configured) -- a real,
correct optimization for RLS/VPD-context propagation, but wrong for `db_emulation`, which is a
protocol-level requirement of every orawire session, not a per-user concern. Confirmed live: with
the bug present, `V$VERSION`/`V$INSTANCE`/`V$SESSION` and the new one-argument `to_char()`
overload all failed with `ORA-00942`/`ORA-00904` ("table or view does not exist"/"invalid
identifier"), while schema-qualified calls like `DBMS_RANDOM.STRING(...)` (which don't depend on
`search_path` at all) worked throughout -- that contrast is what pointed at the real cause instead
of a pg_oracle bug. Fixed with a `runEvenWhenAnonymous()` override
(`wire/src/main/java/com/nexagres/wire/core/access/NativeRlsSessionInitializer.java` and
`OraclePgEmulationSessionInitializer.java`).

## Bug 2: the SAME db_emulation GUC was still unreliable across separate connections

Bug 1's fix made a single session's V$ views work, but a *second*, separate `sqlcl` connection
(fresh `LazyPooledConnection` wrapper, but possibly a *pool-reused physical Postgres backend*)
would sometimes still fail with the exact same `ORA-00942`/`ORA-00904` errors, even though
`current_setting('db_emulation')` correctly reported `'oracle'`. Root cause (found by adding
temporary debug logging around the `SET db_emulation = 'oracle'` call, not by further guessing):
`db_emulation_mode` is a C-level static tied to the *physical Postgres backend process*, which
can outlive what Polywire's own code thinks is one logical connection. `LazyPooledConnection`
issues its own unconditional `SET search_path TO "<tenant>", public` the first time its Java
wrapper opens a connection -- with no idea `pg_oracle`'s own search_path append exists -- and if
that physical backend was reused, `db_emulation_mode`'s enum value was *already* `oracle` from a
prior logical session, so `pg_oracle`'s `db_emulation_assign_hook` (gated on "did the enum value
change") wrongly treated the repeat `SET` as a no-op and never re-appended `oracle_catalog`/
`dbms_output`/etc. onto the just-reset `search_path`.

Fixed in `db/pg_oracle/src/pg_oracle.c`: the hook now reconciles against the *actual current*
`search_path` text on every call, not the enum-value transition -- idempotent either way, so a
genuinely repeated `SET db_emulation = 'oracle'` where nothing external touched `search_path` is
just a cheap string-suffix check that changes nothing, while a `search_path` that got reset out
from under it (as `LazyPooledConnection` does) gets correctly re-appended. Verified live: five
separate, fresh `sqlcl` connections in a row (the exact scenario that previously failed
intermittently) all correctly resolved `V$VERSION`.

**This one root cause turned out to explain three of the four originally-reported findings**:
`SYS_CONTEXT('USERENV', 'CURRENT_USER')`'s `ORA-00904` and bare `TO_CHAR(SYSDATE)`'s `ORA-00904`
were *not* separate bind-parameter-typing bugs as first hypothesized from a single failing run --
re-tested after the search_path fix, both work correctly. The original hypothesis (untyped JDBC
bind parameters) was wrong; always re-verify against fresh ground truth rather than trusting a
plausible-sounding first theory.

## Bug 3: CREATE TABLE with ordinary Oracle datatypes had no deterministic translation

`CREATE TABLE emp_test (id NUMBER, name VARCHAR2(50))` failed outright: "statement cannot be
translated from ORACLE to POSTGRES (needs manual migration): LLM fallback translator failed:
null". `DialectTranslations`'s deterministic rule set had no type-mapping rule for basic Oracle
DDL types at all -- `containsUnhandledOracleConstruct()` correctly detected `NUMBER`/`VARCHAR2`/
etc. as untranslated and routed to the LLM fallback (working as designed), but the LLM wasn't
configured in this environment, and its failure surfaced a bare "null" rather than a clear
message.

Fixed with a real deterministic type mapper (`mapOracleDdlTypes()` in `DialectTranslations.java`),
scoped to `CREATE TABLE`/`ALTER TABLE` statements specifically: `VARCHAR2(n)`/`NVARCHAR2(n)` ->
`VARCHAR(n)`, `NUMBER(p,s)`/`NUMBER(p)`/bare `NUMBER` -> the matching `NUMERIC` form, `CLOB` ->
`TEXT`, `BLOB` -> `BYTEA`, `RAW(n)`/bare `RAW` -> `BYTEA` (the length bound is dropped, not
translated -- `BYTEA` has no length-bounded form in Postgres, and dropping a bound only ever
narrows what's accepted, never silently truncates data the way keeping a wrong one could).
Verified live: `CREATE TABLE` with all five of these types succeeded, and a subsequent
`INSERT`/`SELECT` round-tripped real data correctly.

**Not fixed, explicitly out of scope for this pass**: Oracle's `DATE` type actually stores a
time-of-day component (unlike Postgres's own `DATE`, which is date-only) -- full fidelity needs
`DATE` mapped to Postgres `TIMESTAMP`, not left as `DATE`. Skipped here because it's a more
consequential semantic change (affects every existing `DATE`-typed schema element's behavior,
not just DDL parsing) that deserves its own dedicated look rather than being bundled into this
pass.

## How to reproduce

```bash
# 1. Postgres with pg_oracle installed (see db/pg_oracle/README.md's Building section)
# 2. Build and run Polywire pointed at it (see wire/scripts/run.sh)
# 3. Real Oracle client against orawire's port (11521 by default):
sql -S "postgres/postgres@127.0.0.1:11521/postgres" @test.sql
sql -S "postgres/postgres@127.0.0.1:11521/postgres" @ddl-test.sql
```
