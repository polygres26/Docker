# pg_oracle

Oracle compatibility for stock Postgres -- the first extension in the
**Polygres** collection (`db/pg_oracle`, `db/pg_mysql`, `db/pg_sqlserver`
planned; see the top-level plan discussion for why MongoDB is out for now
and why this is a plain `CREATE EXTENSION`, not a forked Postgres binary
the way IvorySQL/Babelfish are).

## What it does

`SET db_emulation = 'oracle'` puts this extension's schemas on
`search_path`, so unqualified references the way real Oracle client code
writes them resolve without the caller spelling out a schema:

```sql
SET db_emulation = 'oracle';

SELECT * FROM v$session;                    -- V$SESSION
SELECT * FROM dba_tables WHERE owner = 'X';  -- DBA_TABLES
SELECT dbms_output.put_line('hi');           -- DBMS_OUTPUT.PUT_LINE
```

Polywire's orawire frontend issues that one `SET` per session on connect;
nothing else on its side needs to change. Plain `psql` against a Postgres
instance with `pg_oracle` installed gets the identical behavior -- this
extension has no dependency on Polywire and doesn't know it exists.

## Scope (v1)

- **V$/GV$ views**: thin views over `pg_stat_activity`/`pg_locks`/
  `pg_settings`/`pg_stat_statements`, Oracle column names. `V$VERSION`,
  `V$INSTANCE`, `V$DATABASE`, `V$PARAMETER`, `V$SESSION`/`GV$SESSION`,
  `V$SQL`, `V$SQL_PLAN`, `V$LOCK`, `V$TRANSACTION` so far -- the rest of
  the top-20 list from the original plan is follow-up work, not a
  redesign. `V$SQL_PLAN` is a function, not a bare view -- see below.
- **DBA_\*/USER_\*/ALL_\* views**: thin views over `pg_catalog`/
  `information_schema`. `DBA_TABLES`, `DBA_TAB_COLUMNS`, `DBA_INDEXES`,
  `DBA_OBJECTS`, `DBA_VIEWS`, `DBA_CONSTRAINTS`, `DBA_SEQUENCES`,
  `DBA_USERS` plus `USER_*`/`ALL_*` for the two most-queried
  (`*_TABLES`, `*_TAB_COLUMNS`) as the pattern proof; the rest of each
  triad is mechanical repetition of that pattern, not new design.
- **DBMS_OUTPUT**: the first of the top-20 `DBMS_*` packages, done in C
  (`src/dbms_output.c`) because it needs session-lifetime state a plain
  SQL/plpgsql function can't hold. `ENABLE`/`DISABLE`/`PUT_LINE`/`PUT`/
  `NEW_LINE`/`GET_LINE`, faithful to Oracle's "silently dropped unless
  enabled" behavior.
- **`db_emulation` GUC** (`src/pg_oracle.c`): the session-activation
  mechanism described above. It also does one more thing: on `SET
  db_emulation = 'oracle'`, it auto-creates a schema named after the
  current role (`CREATE SCHEMA <role> AUTHORIZATION <role>`), if one
  doesn't already exist -- an Oracle-native pattern (schema *is*
  username in real Oracle), and it needs no new search_path logic here
  since Postgres's own `"$user"` search_path entry already resolves to a
  same-named schema whenever one exists. This is what makes
  `USER_TABLES`/`USER_TAB_COLUMNS` (owner-filtered) actually see a role's
  own objects instead of always coming back empty because Postgres's
  default schema is `public`, not something named after the role.
  Isolated in its own subtransaction: a role without `CREATE` privilege
  on the database (the common case for a non-superuser app role) gets a
  `NOTICE` and falls back to ordinary shared-`public` resolution --
  `SET db_emulation = 'oracle'` itself never fails because of this.

## V$SQL_PLAN's syntax delta and fidelity limits

Real Oracle serves `V$SQL_PLAN` straight from the shared pool's already-
computed plan for a cursor. Stock Postgres has no cache of physical plans
keyed by statement id -- `pg_stat_statements` tracks execution *stats*,
not the plan -- so this re-derives one on demand via `EXPLAIN`:

```sql
SET db_emulation = 'oracle';
SELECT id, operation, plan_line FROM v$sql_plan('<sql_id from v$sql>');
```

Called as a function (`v$sql_plan('...')`), not queried as a bare view
the way Oracle's `WHERE sql_id = '...'` works -- a per-row correlated
`EXPLAIN` doesn't fit a plain view. Two real fidelity limits, stated
rather than hidden: a freshly-derived plan can differ from whatever plan
actually ran a given execution (parameter sniffing, stats drift since);
and `pg_stat_statements` normalizes literals to `$1`-style placeholders,
which `EXPLAIN` can't run without real values -- a normalized statement
returns an explanatory row instead of silently guessing. Verified live:
a literal-valued `GROUP BY` query explains cleanly (`HashAggregate` /
`Seq Scan` plan lines returned); a bind-parameterized `WHERE dept = $1`
query correctly returns the explanatory row instead of a wrong plan.

## Explicitly not in v1

The remaining ~19 of the top-20 `DBMS_*`/`UTL_*` packages (`DBMS_UTILITY`,
`DBMS_RANDOM`, `DBMS_CRYPTO`, `UTL_FILE`, ...) -- planned to build on
[orafce](https://pgxn.org/dist/orafce/) (PostgreSQL-licensed) rather than
reimplement from scratch; real PL/SQL stored-procedure support (a
syntax-subset transpiler to `plpgsql`, the hardest and highest-effort
piece of the whole plan); `GV$*` across more than one instance (needs
Polywire's own sharding fan-out, not just this extension); full
privilege-accurate `ALL_*` views (today: owned-by-you or on-search_path,
not real grant-checking).

## A real bug this design already caught

The first working version *prepended* `dbms_output, utl_file,
oracle_catalog` onto `search_path`. That's wrong: `search_path`'s first
entry is also where an unqualified `CREATE TABLE`/`CREATE FUNCTION`/etc.
lands, so a plain `CREATE TABLE t1(...)` issued right after `SET
db_emulation='oracle'` landed in the `dbms_output` schema instead of
`public` -- found live, not by inspection, while smoke-testing. Fixed by
*appending* instead: unqualified lookups (`DBMS_OUTPUT.PUT_LINE`,
`V$SESSION`) still resolve since `search_path` is scanned in full for
reads, but object *creation* keeps landing wherever it would have without
this extension loaded at all.

## Building

Standard PGXS extension -- needs Postgres server dev headers
(`pg_config` on `PATH`) and, for `V$SQL`, the `pg_stat_statements`
extension available (declared as a hard dependency in the `.control`
file; `CREATE EXTENSION pg_oracle CASCADE` installs it automatically).

```bash
cd db/pg_oracle
make
make install
```

`db_emulation` is a custom GUC, which Postgres only recognizes once this
extension's library is loaded -- add it to `session_preload_libraries`
(session-level) or `shared_preload_libraries` (cluster-wide) in
`postgresql.conf` and restart:

```
session_preload_libraries = 'pg_oracle'
```

Then, per-database:

```sql
CREATE EXTENSION pg_oracle CASCADE;
```

## A second real bug this design caught: grants

The first working version created every schema/view/function owned by
whoever ran `CREATE EXTENSION` (normally a superuser or admin role) with
no grants to anyone else. Since `db_emulation` is `PGC_USERSET` --
any role can `SET` it, by design, since Polywire needs to set it for
whatever role a client authenticates as -- a plain application role could
successfully `SET db_emulation = 'oracle'` and then hit `permission
denied for schema oracle_catalog` on its very first query. Found live
with a real non-superuser test role, not by inspection. Fixed with
`GRANT USAGE`/`SELECT`/`EXECUTE ... TO PUBLIC` on this extension's
schemas/views/functions at install time (see the end of
`sql/pg_oracle--0.1.sql`) -- safe to grant broadly since everything here
is either read-only (the views) or scoped to the caller's own session
(`DBMS_OUTPUT`'s buffer is per-backend).

## Verified

Built and smoke-tested end to end against a real local Postgres 17
instance (not just compiled): `CREATE EXTENSION pg_oracle CASCADE`
succeeds; `SET db_emulation = 'oracle'` correctly appends the package
schemas to `search_path` and unqualified `v$version`/`v$instance`/
`dba_tables`/`user_tab_columns` all resolve and return correct data;
`dbms_output.enable()`/`put_line()`/`get_line()` round-trip correctly
including the buffer-disabled-by-default and empty-buffer (`status = 1`)
cases; `SET db_emulation = 'postgres'` correctly restores the original
`search_path` and unqualified object creation (`CREATE TABLE`) lands back
in `public`, confirming the prepend-vs-append fix above; the auto-created
user schema lands unqualified `CREATE TABLE` in it (not `public`) and
`USER_TABLES`/`USER_TAB_COLUMNS` immediately see it, re-running `SET
db_emulation` twice in a row is a safe no-op (no duplicate-schema error);
and a real non-superuser role with no `CREATE` privilege gets the
graceful `NOTICE`-and-fallback path instead of a broken `SET`, then
successfully uses `V$PARAMETER`, `DBA_TABLES`, and `DBMS_OUTPUT` after
the grants fix above. No `pg_regress` test suite yet (tracked as
follow-up in the `Makefile`).
