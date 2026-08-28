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
- **DBMS_RANDOM**, **DBMS_UTILITY** (partial), **DBMS_ASSERT**: packages
  2-4, plain SQL/plpgsql -- no session state needed, unlike DBMS_OUTPUT.
  `DBMS_RANDOM`: `VALUE`/`VALUE(low,high)`/`RANDOM`/`SEED`/`STRING`.
  `DBMS_UTILITY`: `GET_TIME`, `DB_VERSION` -- `FORMAT_ERROR_STACK`/
  `FORMAT_CALL_STACK` deliberately not here, see the function's own
  comment in the SQL file for why a zero-argument version can't be
  faithful. `DBMS_ASSERT`: `SIMPLE_SQL_NAME`, `QUALIFIED_SQL_NAME`,
  `SCHEMA_NAME`, `ENQUOTE_NAME`, `ENQUOTE_LITERAL`, `NOOP` -- the
  SQL-injection-defense helpers real Oracle dynamic-SQL code calls.
- **UTL_FILE**: package 5, in C (`src/utl_file.c`) for the same
  session-lifetime-state reason as `DBMS_OUTPUT`, plus the file I/O
  itself isn't reachable from plain SQL at all. `FOPEN`/`PUT`/`PUT_LINE`/
  `NEW_LINE`/`GET_LINE`/`FCLOSE`/`FCLOSE_ALL`/`IS_OPEN`, gated on
  Postgres's own `pg_read_server_files`/`pg_write_server_files`
  predefined roles instead of a second, invented permission system --
  see "UTL_FILE's security model" below. `FILE_TYPE` is simplified to a
  plain integer handle rather than Oracle's opaque record type --
  documented, not hidden.
- **DBMS_NETWORK_ACL_ADMIN + UTL_HTTP**: package 6, real ACL-gated
  outbound HTTP -- see the dedicated section below for the model and the
  one deliberate exception to "grant everything to PUBLIC" this package
  makes.
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

## UTL_FILE's security model

Deliberately *not* a new, invented permission system: Oracle's directory-
object model (`CREATE DIRECTORY`, then `GRANT READ/WRITE ON DIRECTORY ...
TO ...`) is reused conceptually but implemented on top of Postgres's own
predefined roles `pg_read_server_files`/`pg_write_server_files` (built in
since PG 11 for exactly this "let a non-superuser touch the filesystem
safely" purpose). `utl_file.create_directory()`/`FOPEN` both check
membership in one of those (superuser always passes, same as every other
Postgres filesystem-access check). `utl_file.directories` itself only
maps a name to a path -- no privilege lives in that table, so it's
`GRANT SELECT ... TO PUBLIC` (every role needs to read it for `FOPEN`'s
internal lookup to work) while writes to it are only reachable through
`create_directory()`/`drop_directory()`'s own privilege check. A filename
containing `/` or `..` is rejected outright -- no legitimate `FOPEN` call
needs either, and allowing them would let a caller escape the registered
directory entirely.

## DBMS_NETWORK_ACL_ADMIN + UTL_HTTP

Implemented, not deferred: earlier in this project's history `UTL_HTTP`
was held back specifically because network egress needed its own
access-control design first, not because it needed nothing at all. That
design is real Oracle's own: `DBMS_NETWORK_ACL_ADMIN` (`CREATE_ACL`,
`ADD_PRIVILEGE`, `ASSIGN_ACL`, `UNASSIGN_ACL`, `DROP_ACL`,
`CHECK_PRIVILEGE`), reused faithfully rather than inventing a simpler,
unfamiliar policy layer. `UTL_HTTP.REQUEST` raises `ORA-24247` unless a
role has been granted `CONNECT` on an ACL assigned to the target
host/port -- exactly Oracle's own 11g+ behavior, deny-by-default, no
"just let PL/SQL call out" mode.

Model: an ACL is a named, *ordered* list of `(principal, privilege,
grant-or-deny)` entries -- ordered because Oracle's own semantics are
"first matching entry in the list wins", not "any deny anywhere beats
any grant" (`acl_privileges.id` preserves that order). An ACL is assigned
to one or more host/port patterns (exact host, `*.subdomain` wildcard, or
`*` catch-all); resolving a request picks the *most specific* matching
assignment's ACL first, then walks that ACL's entries in order.

Split of responsibility, same principle used everywhere else in this
extension: the policy (host/port specificity, list-order resolution)
lives entirely in plpgsql (`dbms_network_acl_admin.*`), independently
testable and inspectable; `src/utl_http.c` is C only for what SQL
genuinely cannot do -- parsing a URL for its host/port, and calling
libcurl to actually open the socket -- and it calls back into that
plpgsql policy via SPI before ever doing either.

**The one place this extension deliberately does NOT grant `PUBLIC`
the way every other package does**: `dbms_network_acl_admin`'s
*mutating* functions (`create_acl`/`add_privilege`/`assign_acl`/
`unassign_acl`/`drop_acl`) stay owner-only. Granting those to `PUBLIC`
the way `dbms_output`/`utl_file`/etc. are would let any role grant
itself network access and defeat the entire feature -- real Oracle draws
the same line (`DBMS_NETWORK_ACL_ADMIN` isn't `PUBLIC`-executable there
either). Only the three read-only lookup functions
(`resolve_acl_for_host`/`check_privilege`/`check_privilege_for_host`) are
`PUBLIC`, and even those needed `SECURITY DEFINER` with a pinned
`search_path` to work for an ordinary role at all -- see the bug below.

`UTL_HTTP.REQUEST`'s scope is simplified from real Oracle's full
request/response handle API (`BEGIN_REQUEST`/`GET_RESPONSE`/
`READ_TEXT`, which supports streaming and per-header access) down to a
single synchronous call returning the response body as `text`, plus
`utl_http.last_status()` for the most recent call's HTTP status code --
covers the extremely common "make a call, check status, use the body"
pattern, not multi-request pipelining or custom header inspection.

A third real bug, found live: the three read-only ACL-lookup functions
initially had no `SECURITY DEFINER`. plpgsql functions are
`SECURITY INVOKER` by default, so granting `EXECUTE` on them to `PUBLIC`
wasn't enough -- an ordinary role still needed direct `SELECT` on
`acls`/`acl_privileges`/`host_assignments` itself, which would have
meant handing out broad read access to the ACL tables just so
`utl_http.request()` could check them. Fixed with `SECURITY DEFINER` (so
the check runs with the function owner's rights, the same way real
Oracle's own ACL checks run against `SYS`-owned structures) and a pinned
`search_path` (standard hygiene against a caller shadowing
`host_assignments` with an object of their own).

## Anonymous PL/SQL blocks (`DECLARE ... BEGIN ... END;`): an architecture
## finding, not a feature gap in this extension

Stock Postgres's grammar has **no top-level `DECLARE`/`BEGIN`/`END`
statement** -- that syntax only exists wrapped as `DO $$ ... $$;`, and
there is no supported extension hook that adds new top-level grammar
productions without patching Postgres's own parser (which is exactly the
IvorySQL/Babelfish fork-the-core approach this whole plan chose to avoid
-- see the top-level plan discussion). So a raw Oracle anonymous block
cannot be handed to stock Postgres as text and just work; it has to be
rewritten to `DO $$ ... $$;` (plus syntax deltas like bare procedure
calls needing `PERFORM`) *before* it reaches Postgres at all.

That rewrite is a text-transformation step that can only live in front
of Postgres -- i.e. in Polywire's orawire dialect-translation stage
(which already exists for exactly this kind of protocol/dialect
adaptation), not inside this extension. `pg_oracle`'s job is making sure
the *rewritten* block actually runs correctly once it gets there, and
that much is already proven: every package above was verified via real
`DO $$ ... $$` blocks during this session, including the Oracle idiom of
catching `UTL_FILE.GET_LINE`'s end-of-file with `EXCEPTION WHEN
NO_DATA_FOUND` (works today, unchanged from plpgsql's own native
behavior) and `DBMS_ASSERT` rejecting bad input inside `EXCEPTION WHEN
OTHERS`. The concrete, scoped follow-up is the orawire-side rewriter,
not more work here.

## Remaining top-20 scope

`DBMS_CRYPTO`, `DBMS_LOB`, `DBMS_SQL`, `DBMS_SESSION`,
`DBMS_APPLICATION_INFO`, `DBMS_METADATA` (subset), `DBMS_JOB`/
`DBMS_SCHEDULER` (subset), `DBMS_STATS` (shim), `DBMS_LOCK`,
`DBMS_ALERT`, `DBMS_PIPE`, `UTL_RAW`, `UTL_ENCODE` -- planned to build on
[orafce](https://pgxn.org/dist/orafce/) (PostgreSQL-licensed) where it
already covers one, rather than reimplement from scratch; `GV$*` across
more than one instance (needs Polywire's own sharding fan-out, not just
this extension); full privilege-accurate `ALL_*` views (today:
owned-by-you or on-search_path, not real grant-checking).

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

## Two more real bugs this pass caught

`DBMS_RANDOM.SEED(integer)`'s first version computed
`(val % 2147483647) + 2147483647` in `int4` arithmetic -- overflows for
completely ordinary seed values (`SEED(42)` was the very first call that
hit it: `ERROR: integer out of range`, not a boundary case). Fixed by
normalizing in `double precision` from the start
(`val::double precision / 2147483647.0`, clamped) instead of doing
modular arithmetic in a type too narrow for the intermediate result.

`utl_file.create_directory(directory_name text, path text)`'s first
version named its own parameters the same as the table's columns,
making `ON CONFLICT (directory_name) DO UPDATE SET path = ...` genuinely
ambiguous to plpgsql ("could refer to either a PL/pgSQL variable or a
table column") -- found on the first real call, not by inspection. Fixed
by prefixing the parameters (`p_directory_name`, `p_path`).

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
the grants fix above.

`DBMS_RANDOM`/`DBMS_UTILITY`/`DBMS_ASSERT`: `VALUE()`/`VALUE(low,high)`
stay in range; `STRING('U', 8)` returns 8 uppercase letters; `SEED()`
handles both an ordinary value and `-2147483648` (the boundary that broke
the pre-fix formula's sibling); `SIMPLE_SQL_NAME` correctly rejects an
injection-shaped string, and does so in a way `EXCEPTION WHEN OTHERS`
catches exactly like real Oracle code expects.

`UTL_FILE`, end to end: `create_directory` + `fopen(..., 'w')` +
`put_line`/`put`/`new_line` + `fclose` produced a real file on disk with
exactly the expected bytes; reopening for read and looping
`get_line()`/`EXCEPTION WHEN NO_DATA_FOUND` correctly read all three
lines back and stopped at EOF; a filename containing `..` was rejected
before ever touching the filesystem; a real non-superuser role with no
file-role membership got a clean permission error on `FOPEN`, then
succeeded after being granted `pg_read_server_files` -- and, separately,
still correctly failed to open the same file for *write* until also
granted `pg_write_server_files`, confirming the read/write split actually
holds and isn't just "has any file role, do anything."

`DBMS_NETWORK_ACL_ADMIN`/`UTL_HTTP`, end to end against real network
requests (not mocked): with no ACL at all, `utl_http.request()` correctly
raises `ORA-24247` (including correct default-port inference: 443 for
`https`); after `create_acl`+`assign_acl` for `example.com`, a real HTTPS
request to `example.com` succeeds and returns real response body and
status 200, while a request to an unrelated domain (`postgresql.org`)
still correctly denies -- the grant doesn't leak beyond its host.
Specificity resolution verified directly: an exact-host `DENY` correctly
overrides a same-ACL-unrelated `*.example.com` wildcard `GRANT` for that
one subdomain, while a different subdomain still resolves via the
wildcard grant, and a totally unrelated host correctly resolves to `NULL`
(no ACL covers it at all). List-order semantics verified: adding a `DENY`
entry after an existing `GRANT` for the same principal+privilege does
*not* flip the answer -- the first entry in the list still wins, matching
real Oracle instead of a naive "any deny wins" implementation. Privilege
separation verified with a real non-superuser role: it cannot call
`create_acl` (`permission denied for table acls`, confirmed correctly
blocked even after being granted `EXECUTE` on nothing extra), but once a
superuser grants it `CONNECT` via `add_privilege`, that same role
successfully makes a real HTTPS request to `httpbin.org` and reads back
its actual response -- proving the read-only check path, the
`SECURITY DEFINER` fix, and the owner-only mutation boundary all hold
together correctly, not just in isolation.

No `pg_regress` test suite yet (tracked as follow-up in the `Makefile`).
