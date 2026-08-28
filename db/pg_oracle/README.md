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
- **DBMS_CRYPTO**: package 7, real hashing/HMAC/encryption over
  Postgres's own `pgcrypto` -- see the dedicated section below.
- **DBMS_SCHEDULER**: package 8, real recurring jobs over `pg_cron` --
  see the dedicated section below for a second real privilege-model
  decision (why these functions are deliberately NOT `SECURITY DEFINER`,
  unlike `DBMS_NETWORK_ACL_ADMIN`'s).
- **DBMS_AQADM + DBMS_AQ**: package 9, real message queueing over
  `SELECT ... FOR UPDATE SKIP LOCKED` -- see the dedicated section below,
  including a critical privilege bug this pass found and fixed.
- **DBMS_STATS**: package 10, a real shim over Postgres's own
  `ANALYZE`/`pg_class` stats -- see the dedicated section below.
- **SYS_CONTEXT + DBMS_SESSION**: the VPD-enabling piece -- makes
  `SYS_CONTEXT()` return values real Postgres Row-Level Security
  policies can read. See the dedicated section below for a working,
  verified `CREATE POLICY ... USING (sys_context(...))` example.
- **ALTER SESSION / ALTER SYSTEM + NLS_\*/TO_CHAR/TO_DATE**: `ALTER
  SYSTEM SET` already works natively in stock Postgres (nothing to
  build); `ALTER SESSION SET` needs the same function-call substitute
  pattern as `CREATE CONTEXT`, and a real bug in its first version is
  worth reading about. See the two dedicated sections below.
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

## DBMS_CRYPTO

Package 7 -- a thin adapter over Postgres's own `pgcrypto` contrib
extension (audited, battle-tested), not hand-rolled: crypto is exactly
the category of code where "don't reinvent it yourself" matters most.
`HASH`/`MAC` (HMAC forms)/`RANDOMBYTES` call straight through to
`digest()`/`hmac()`/`gen_random_bytes()` -- verified byte-for-byte
identical to those functions' own output, not just "produces some hash".
`ENCRYPT`/`DECRYPT` wrap `pgcrypto`'s `encrypt_iv()`/`decrypt_iv()`,
AES-128/192/256 with CBC/ECB chaining and PKCS5/none padding only --
`pgcrypto` itself doesn't support Oracle's CFB/OFB chaining or
PAD_ZERO/PAD_ORCL, so those are rejected outright rather than silently
approximated.

Constants (`dbms_crypto.hash_sh256()` etc.) are zero-argument functions,
not Oracle's bare package constants (`DBMS_CRYPTO.HASH_SH256`, no
parens) -- Postgres has no syntax for a bare constant reference at all.
Real migrated code hardcoding the bare form needs the same class of
textual rewrite as anonymous PL/SQL blocks (see that section) -- a much
smaller rewrite (append `()`), but still a rewrite this extension alone
can't paper over.

`HASH_*`/`HMAC_*` constant *values* match Oracle's own documented,
stable-since-10g numbering. `ENCRYPT_*`/`CHAIN_*`/`PAD_*` values are
this extension's **own** numbering, explicitly **not** verified against
real Oracle's actual bit-packed `TYP` encoding (algorithm + chaining +
padding summed into one integer) -- correctness only holds as long as
callers use the symbolic constants, as idiomatic Oracle code always
does, not a hardcoded literal matching real Oracle's numbers.

## DBMS_SCHEDULER

Package 8 -- a thin adapter over [pg_cron](https://github.com/citusdata/pg_cron)
(PostgreSQL-licensed), not a hand-rolled background worker: reliable
cron-style scheduling inside Postgres is exactly the kind of
infrastructure not worth reimplementing. `CREATE_JOB`/`RUN_JOB`/
`ENABLE`/`DISABLE`/`STOP_JOB`/`DROP_JOB`. Requires
`shared_preload_libraries = 'pg_cron'` and `CREATE EXTENSION pg_cron` --
**not** `CASCADE`-installed by `pg_oracle` itself the way
`pg_stat_statements`/`pgcrypto` are, since `pg_cron` needs a
preload-library restart `CREATE EXTENSION` cannot arrange.

`repeat_interval` supports only the four most common Oracle calendar
shapes -- `FREQ=DAILY`/`HOURLY`/`WEEKLY`/`MONTHLY` (with `INTERVAL`,
`BYHOUR`, `BYMINUTE`, `BYDAY`, `BYMONTHDAY`) -- translated to a 5-field
cron expression. Anything else (`FREQ=YEARLY`, `BYYEARDAY`, exclusion
lists, `FREQ=DAILY` with `INTERVAL != 1`, which plain cron can't
represent at all) raises a clear error instead of silently producing a
wrong schedule. `job_action` for `job_type='PLSQL_BLOCK'` inherits the
same anonymous-PL/SQL-block limitation as everywhere else in this
extension (pg_cron executes it as literal SQL text) -- see that section.

**A real privilege-model decision, verified live, not assumed**:
`create_job`/`enable`/`disable`/`drop_job`/`run_job` are deliberately
left plain `SECURITY INVOKER` (plpgsql's default), *not*
`SECURITY DEFINER` the way the `DBMS_NETWORK_ACL_ADMIN` read functions
needed to be. `pg_cron`'s own `cron.job` table records `username =
CURRENT_USER` at schedule time and the job later runs as that role --
making these functions `SECURITY DEFINER` would have silently made every
scheduled job run with this extension's owner's (likely superuser)
privileges instead of the real caller's, a genuine privilege escalation.
Confirmed live with a real non-superuser role: `cron.job.username`
correctly recorded that role, not the extension owner, and the job's
actual `INSERT` later executed with a `current_user` of that same role,
not an elevated one.

This is also why the grants here are broader than everywhere else:
`pg_cron` ships its own row-level security on `cron.job`
(`USING (username = CURRENT_USER)`, confirmed live) -- broadly granting
`SELECT`/`INSERT`/`UPDATE`/`DELETE` on it is safe because a role can only
ever see or touch its own jobs through it regardless. The grants apply
conditionally (`to_regnamespace('cron') IS NOT NULL`) since `pg_cron`
isn't a hard dependency; see `sql/pg_oracle--0.1.sql` for the exact
re-run steps if `pg_cron` is installed *after* `pg_oracle`.

## DBMS_AQADM + DBMS_AQ

Package 9 -- built on `SELECT ... FOR UPDATE SKIP LOCKED`, part of stock
Postgres core since 9.5, not a hand-rolled queue engine: concurrent-safe
"pop the next available message without blocking on rows another backend
already grabbed" is exactly the problem `SKIP LOCKED` exists to solve.
`DBMS_AQADM.CREATE_QUEUE_TABLE` creates a real physical table (one per
Oracle queue table, matching Oracle's own model); `CREATE_QUEUE`/
`START_QUEUE`/`STOP_QUEUE`/`DROP_QUEUE` manage its lifecycle;
`DBMS_AQ.ENQUEUE`/`DEQUEUE` do the actual message passing, with priority
ordering, delay, expiration, correlation filtering, and all three
dequeue modes (`REMOVE`/`BROWSE`/`LOCKED`).

Payload type is `RAW` (bytea) only -- Oracle's object-typed payloads
aren't supported; callers serialize structured data into bytea
themselves. `NEXT_MESSAGE`/`FIRST_MESSAGE` navigation behave identically
(no per-session dequeue cursor is tracked). `WAIT` is a bounded polling
loop (`pg_sleep` between retries, confirmed live to actually poll rather
than return instantly or block the full ceiling), not Oracle's true
OCI-level blocking wait -- functionally equivalent, not the same
mechanism. `DBMS_AQADM.GRANT_QUEUE_PRIVILEGE` (Oracle's own per-queue
ACL) is **not** implemented -- stated plainly: any role that can reach
`dbms_aq` at all can touch every queue in this version.

**Privilege model, matching real Oracle's own split**: queue
*administration* (`CREATE_QUEUE_TABLE` and friends) is `SECURITY
DEFINER` and owner-only -- real Oracle also treats AQ administration as
privileged, separate from ordinary enqueue/dequeue access. `DBMS_AQ`
itself (`ENQUEUE`/`DEQUEUE`) is `PUBLIC`, matching Oracle's own
app-role usage once a DBA has set a queue up. Verified live end to end
with a real non-superuser role: blocked from `CREATE_QUEUE_TABLE`, then
successfully enqueuing and dequeuing on a queue an admin had already
created and started.

## DBMS_STATS

Package 10 -- an honest shim, not a reimplementation of Oracle's
optimizer internals: Postgres has its own, completely different
statistics system (`pg_statistic`, gathered by `ANALYZE`, already kept
fresh by autovacuum) -- there's no Oracle-shaped histogram engine to
port, and pretending to fabricate one would be actively misleading.
`GATHER_TABLE_STATS`/`GATHER_SCHEMA_STATS`/`GATHER_DATABASE_STATS` call
real `ANALYZE`; `SET_TABLE_STATS` does a real, legitimate
`pg_class.reltuples`/`relpages` update (a well-known technique for
fabricating stats to test a query plan, not a risky hack);
`LOCK_TABLE_STATS`/`UNLOCK_TABLE_STATS` combine an internal lock flag
this package checks with `ALTER TABLE ... SET (autovacuum_enabled =
...)`, the closest real Postgres equivalent (doesn't block a manually
issued `ANALYZE` from outside this package, a stated simplification).
`DELETE_TABLE_STATS` is an honest no-op with a clear `NOTICE` -- Postgres
has no supported way to discard statistics the way Oracle does, and
reaching into `pg_statistic` directly to fake it would be genuinely
risky, not just inelegant.

No separate permission layer needed here -- Postgres's own ownership
checks on `ANALYZE`/`ALTER TABLE`/`UPDATE pg_class` are the real gate,
confirmed live: a non-owning role's `gather_table_stats()` call didn't
error (Postgres's own `ANALYZE` emits a `WARNING` and silently skips a
table it can't analyze, rather than a hard privilege error -- a real,
worth-knowing behavioral difference from Oracle, not a security gap:
no unauthorized action occurred either way).

## SYS_CONTEXT + DBMS_SESSION (VPD)

Why this one matters more than its own line in the top-20 list: a
migrated VPD (`DBMS_RLS`) policy predicate calls `SYS_CONTEXT('my_ctx',
'attr')` to find out who's asking. The good news: Postgres already has
a native, arguably more capable VPD-equivalent enforcement mechanism --
real Row-Level Security (`CREATE POLICY ... USING (...)`). This
package's real job isn't reimplementing row filtering, it's making
`SYS_CONTEXT()` return a real value an RLS policy's `USING` clause can
read.

Two kinds of context, matching real Oracle's own split. `USERENV` is
Oracle's built-in namespace, read-only, dispatched to real Postgres
session facts, not stored state: `CURRENT_USER`/`SESSION_USER`/
`CURRENT_SCHEMA`/`DB_NAME`/`INSTANCE_NAME`/`SERVER_HOST`/`HOST`/
`IP_ADDRESS`/`SESSIONID`/`SID`/`ISDBA`/`LANG`/`LANGUAGE`/`CON_NAME`/
`CON_ID`/`CLIENT_IDENTIFIER` are covered; anything else (`MODULE`,
`ACTION`, `PROXY_USER`, `AUTHENTICATION_METHOD`, ...) returns `NULL`,
not implemented yet, stated in the function's own comment rather than
silently absent. `CON_NAME`/`CON_ID` map to `current_database()`/`'1'`
-- Postgres has no CDB/PDB multitenant architecture, so there's no real
container name to report.

Anything else is a user-defined context: `oracle_catalog.create_context`
stands in for Oracle's `CREATE CONTEXT` DDL (exposed as a function call,
not new syntax -- stock Postgres's grammar has no `CREATE CONTEXT`
statement to add without patching the parser, the same class of gap as
anonymous PL/SQL blocks); `DBMS_SESSION.SET_CONTEXT`/`CLEAR_CONTEXT`
then read/write actual per-session values for it, stored in a lazily
created `TEMP TABLE` (`pg_temp`, inherently private and auto-dropped per
session -- no C needed, unlike `DBMS_OUTPUT`'s buffer, since a temp
table already gives "session-lifetime state" for free). Confirmed live:
a value set by one `limited_role` session was correctly invisible in a
brand new `limited_role` connection -- genuinely per-session, not a
shared or leaking store. `CLIENT_IDENTIFIER` (the common
connection-pooling pattern: pool authenticates once as a shared DB role,
then `SET_IDENTIFIER` per logical end user) goes through its own
`SET_IDENTIFIER`/`CLEAR_IDENTIFIER` entry points, not plain
`SET_CONTEXT` -- matching real Oracle exactly, which explicitly rejects
`SET_CONTEXT('USERENV', ...)` with `ORA-01739`, reproduced here and
verified live.

**Privilege model**: `create_context` (the DDL-equivalent operation) is
`SECURITY DEFINER` and owner-only, matching Oracle's `CREATE ANY
CONTEXT` system privilege being DBA-level -- verified live with a real
non-superuser role, denied. `SET_CONTEXT`/`SYS_CONTEXT` themselves stay
`PUBLIC`, since ordinary application code needs to call them. Oracle's
own real enforcement for *who* may call `SET_CONTEXT` for a given
namespace ("only the namespace's trusted package") has no Postgres
object to check -- there are no PL/SQL packages -- so this package
doesn't invent a second access-control layer: the honest, correct
parallel is to wrap `DBMS_SESSION.SET_CONTEXT` calls in your own
`SECURITY DEFINER` function, which is exactly how Oracle's own
enforcement actually works underneath (only that package's compiled
code, running with definer rights, reaches the real primitive).
`create_context`'s `REVOKE EXECUTE FROM PUBLIC` was written *after* the
blanket `oracle_catalog` grant on purpose, applying the lesson from the
`dbms_aqadm` bug directly above: statement order matters for
`GRANT`/`REVOKE`, and reversing the two lines would have silently
re-opened it.

**The real VPD-equivalent demo, verified live end to end** -- not just
the plumbing in isolation:

```sql
SET db_emulation = 'oracle';
SELECT oracle_catalog.create_context('tenant_ctx');   -- as an admin, once

CREATE TABLE orders(id int, tenant_id text, amount numeric);
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON orders
  USING (tenant_id = oracle_catalog.sys_context('tenant_ctx', 'tenant_id'));

-- as an ordinary app role, per request:
SELECT dbms_session.set_context('tenant_ctx', 'tenant_id', '42');
SELECT * FROM orders;   -- only tenant 42's rows, real Postgres RLS enforcing it
```

Confirmed live: with three rows across two tenants, a role with no
context set sees zero rows (`sys_context` correctly returns `NULL`, and
`NULL = tenant_id` is never true); the same role after
`SET_CONTEXT('tenant_ctx','tenant_id','42')` sees exactly its own two
rows, correctly excluding the other tenant's row -- real Postgres RLS
doing the actual enforcement, this package only supplying the value it
reads.

## ALTER SESSION / ALTER SYSTEM

Two completely different stories, each checked live rather than assumed.

**`ALTER SYSTEM SET parameter = value`** -- nothing to build. Postgres
already has this exact statement, native, since 9.4 (`ALTER SYSTEM SET
work_mem = '8MB'` + `SELECT pg_reload_conf()` confirmed live). The only
real gap is Oracle-specific parameter *names* having no Postgres GUC
equivalent -- the same vocabulary gap `V$PARAMETER` already documents,
not a syntax problem.

**`ALTER SESSION SET parameter = value`** -- confirmed live to be a
genuine Postgres syntax error (`syntax error at or near "SESSION"`):
stock Postgres's grammar has no `ALTER SESSION` statement at all, the
same class of gap as anonymous PL/SQL blocks and `CREATE CONTEXT`. What
this extension provides instead: `oracle_catalog.alter_session_set()`,
a function-call substitute (usable directly today, and a natural
rewrite target for an eventual orawire-side transform) covering the
forms that have a real Postgres equivalent -- `CURRENT_SCHEMA` (->
`search_path`), `TIME_ZONE` (-> `timezone`), and `NLS_*` (-> the
session-local store `to_char()`/`to_date()` below read). Anything else
(`RESUMABLE`, parallel-DML degree, and similar Oracle-only session
concepts with no Postgres analog at all) is a `NOTICE`-and-no-op, not a
silent false claim of having applied something.

**A real bug found live**: the first version of `CURRENT_SCHEMA` did a
bare `SET search_path TO <schema>` -- which wholesale *replaced* the
session's search_path, wiping out every Oracle package schema
`db_emulation` had already appended (`dbms_output`, `oracle_catalog`,
...) and immediately breaking unqualified `to_char()`/`sys_context()`
in the same session. Fixed by *prepending* the new schema onto the
existing search_path instead -- the same prepend-vs-replace lesson as
`db_emulation`'s own original bug, rediscovered here in a different
function. Confirmed live after the fix: `search_path` correctly shows
the new schema first with every Oracle package schema still present
after it, and `CREATE TABLE` correctly lands in the new current schema.

## NLS_* / TO_CHAR / TO_DATE

Postgres's own `to_char()`/`to_date()` already understand almost every
Oracle format token identically -- checked live, not assumed:
`YYYY`/`MM`/`DD`/`HH24`/`HH12`/`MI`/`SS`/`MON`/`DY`/`DAY`/`AM`/`PM`/`FM`
and numeric `9`/`0`/`,`/`.` groups all matched real Oracle's own output
exactly (including the overflow-to-`#` behavior for a value too wide
for its format, verified to match Oracle's own convention, not a bug).
Exactly three real, confirmed differences: `RR` (Oracle's
century-rounding 2-digit year) isn't recognized at all by Postgres and
is echoed back literally; bare `FF` (fractional seconds with no digit
count -- `FF1`-`FF9` **do** work natively, checked) isn't recognized;
`X` (Oracle's locale radix/decimal-point token) isn't recognized.
`oracle_catalog.translate_nls_format()` rewrites all three
(`RR`->`YY`, bare `FF`->`FF6`, `X`->`.`) before delegating to real
Postgres `to_char`/`to_date` -- a plain string substitution, not a real
format-string tokenizer, so a literal "RR"/"FF"/"X" inside an Oracle
format string's own quoted-literal-text segment would be incorrectly
rewritten too (stated, not silently risked; real-world Oracle date
formats essentially never do this).

`NLS_DATE_FORMAT`/`NLS_TIMESTAMP_FORMAT` are read from
`oracle_catalog.get_nls_parameter()`, session-settable via
`alter_session_set()` above, defaulting to Oracle's real
`AMERICAN_AMERICA` defaults (`DD-MON-RR`, `DD-MON-RR HH.MI.SSXFF AM`)
when unset -- introspectable via `oracle_catalog."nls_session_parameters"`,
a real Oracle dictionary view name.

**A genuinely new capability, not an override**: Postgres's own
`to_char`/`to_date` have **no** one-argument form for any date/timestamp
type at all (confirmed live: `to_char(current_date)` is a plain "function
does not exist" in stock Postgres) -- so `oracle_catalog.to_char(date)`/
`to_char(timestamp)`/`to_char(timestamptz)` and
`oracle_catalog.to_date(text)` are safe, collision-free additions that
make the extremely common bare `TO_CHAR(some_date)`/`TO_DATE(some_str)`
Oracle idiom (relying on the session's NLS default format) work at all,
not just work better.

**A stated scope limit, not a silent gap**: the explicit-two-argument
form is only translation-aware for the `date` type specifically (no
existing `pg_catalog` overload to collide with there); `pg_catalog`
already has an *exact* two-argument overload for `timestamp`/
`timestamptz`/`text` types, and Postgres always resolves an exact-type
tie in `pg_catalog`'s favor regardless of this extension's search_path
additions -- verified this is real, not assumed (`pg_catalog` is
implicitly searched first unless a caller deliberately reorders it,
which this extension does not attempt, on purpose: reordering
`pg_catalog`'s effective priority globally to win that tie would risk
shadowing unrelated built-ins throughout the whole session for a
narrow gain). A bare, unqualified two-argument
`TO_DATE(str, 'DD-MON-RR')`/`TO_CHAR(some_timestamp, 'DD-MON-RR')` call
still resolves to real Postgres's own function and will not get
`RR`/bare-`FF`/`X` translation -- only the one-argument (NLS-default)
forms, and the explicit-format `date`-typed overload, do.

## Remaining top-20 scope

`DBMS_LOB`, `DBMS_SQL`, `DBMS_APPLICATION_INFO`,
`DBMS_METADATA` (subset), `DBMS_LOCK`, `DBMS_ALERT`, `DBMS_PIPE`,
`UTL_RAW`, `UTL_ENCODE` -- planned to build on
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

Two more real bugs found live in this pass: `dbms_crypto.hash()`'s first
version tried to catch an exception from `digest(src, NULL)` for an
unrecognized type -- except that call doesn't raise at all, it just
returns `NULL`, so `dbms_crypto.hash('x', 999)` silently produced an
empty result instead of failing loudly. Fixed by checking the algorithm
name upfront instead of hoping for an exception that was never going to
happen. Separately, `dbms_scheduler.jobs` (this extension's own job
bookkeeping table) had no row-level security, so a second non-superuser
role could read a first role's scheduled `job_action` SQL text through
it even though `pg_cron`'s own `cron.job` correctly hid that job
entirely -- found live with two real roles, not by inspection. Fixed
with the same RLS policy shape `pg_cron` itself uses.

`DBMS_CRYPTO`, end to end: `hash()` with `HASH_SH256` matches
`digest(..., 'sha256')`'s own output exactly; `mac()` with `HMAC_SH256`
matches `hmac()`'s own output exactly; `randombytes(16)` returns exactly
16 bytes; an unrecognized hash type now correctly raises instead of
returning `NULL`. `encrypt()`/`decrypt()` round-trip correctly for
AES-256/CBC/PKCS5; a key length that doesn't match the requested AES
variant is rejected with a clear error instead of silently doing the
wrong thing; an unsupported chaining mode is rejected outright.

`DBMS_SCHEDULER`, end to end against a real `pg_cron` instance (not
mocked): `oracle_calendar_to_cron()` correctly translates all four
supported `FREQ` shapes (including `BYDAY=MON,WED,FRI` to `1,3,5`) and
correctly rejects `FREQ=YEARLY` instead of guessing; `create_job` +
`run_job` executes `job_action` immediately regardless of
enabled/schedule state; `enable` hands the job to a real `cron.schedule()`
call and `cron.job` shows the exact expected cron expression and command;
`disable` correctly unschedules from `pg_cron` (`cron.job` count back to
0) while preserving the job's own metadata; `drop_job` removes both.
Privilege separation verified with two real non-superuser roles: the
role that scheduled a job is the one recorded in `cron.job.username` and
the one the job's own `INSERT` actually ran as (confirmed via a
`ran_by`-tracking column in the target table) -- not the extension
owner; and, after the RLS fix, a second unrelated role correctly sees
zero rows in both `cron.job` and `dbms_scheduler.jobs` for a job it
didn't create.

**A critical bug this pass found -- not a theoretical concern, an actual
successful exploit against a real non-superuser role, caught before this
ever shipped**: `dbms_aqadm`'s administrative functions were made
`SECURITY DEFINER` (see above) but the install script never explicitly
`REVOKE`d their `EXECUTE` privilege from `PUBLIC` -- and Postgres grants
`EXECUTE` on every newly created function to `PUBLIC` by default. Simply
never writing a `GRANT` line does *nothing*; the privilege was already
there from `CREATE FUNCTION` itself. Live proof: a real non-superuser
test role successfully called `create_queue_table()` -- which, being
`SECURITY DEFINER`, ran with this extension owner's rights -- and
created a real table it had no business creating. Fixed with explicit
`REVOKE EXECUTE ... FROM PUBLIC` on all six `dbms_aqadm` admin functions,
reverified: the same role is now cleanly denied. The equivalent
`dbms_network_acl_admin` mutating functions got the same explicit
`REVOKE` added too, even though they weren't actually exploitable today
(they're plain `SECURITY INVOKER`, so they currently only fail because
the calling role also lacks table-level `INSERT` -- true today, but an
accident of table grants, not a real statement of intent about those
functions, until this fix made it one).

**The takeaway for anything added to this extension going forward**: a
`SECURITY DEFINER` function is un-safe-by-default the moment it's
created, not the moment someone remembers to lock it down -- explicit
`REVOKE EXECUTE FROM PUBLIC` has to ship in the same commit as the
`CREATE FUNCTION`, never as a follow-up.

`DBMS_AQADM`/`DBMS_AQ`, end to end against real concurrent sessions, not
simulated: three messages enqueued with priorities 5/1/3 dequeue in
priority order (1, 3, 5), not enqueue order; `NO_WAIT` on an empty queue
raises `ORA-25228` immediately; a delayed message is correctly invisible
until its delay elapses, and a `WAIT`-mode dequeue measurably blocked for
~4 real seconds (not instant, not the full wait ceiling) before
returning it, proving the poll loop actually polls; an expired message
is correctly invisible after its expiration passes; `BROWSE` mode
returns the same message twice without removing it, then `REMOVE`
actually consumes it; `LOCKED` mode, tested with two real concurrent
`psql` sessions, held a message locked for the length of session one's
transaction, correctly made a concurrent dequeue from session two time
out (`SKIP LOCKED` doing its job) rather than double-deliver the
message, and the message was still there, un-deleted, once the lock was
released. Privilege separation verified live (see the critical bug
above): a real non-superuser role is blocked from
`create_queue_table()`, then successfully enqueues and dequeues on a
queue an admin already created and started.

`DBMS_STATS`, end to end: `gather_table_stats()` runs a real `ANALYZE`
and produces real `reltuples`/`relpages`; `set_table_stats()` really
overwrites `pg_class.reltuples`/`relpages` with fabricated values;
`lock_table_stats()` correctly blocks a subsequent `gather_table_stats()`
call with a clear error, `unlock_table_stats()` correctly restores real
`ANALYZE` behavior (confirmed `reltuples` went back to the true row
count); `delete_table_stats()` emits the honest no-op `NOTICE`; a
non-owning role's `gather_table_stats()` call is silently skipped with a
`WARNING` by Postgres's own `ANALYZE`, not a hard error -- a real,
documented behavioral difference from Oracle, not a security gap.

`SYS_CONTEXT`/`DBMS_SESSION`, end to end: every implemented `USERENV`
attribute returns a real value (`CURRENT_USER`, `SESSION_USER`,
`DB_NAME`, `SESSIONID`, `ISDBA`, `IP_ADDRESS` all confirmed correct
against the actual connection), an unset `CLIENT_IDENTIFIER` and a
genuinely unknown attribute both correctly return `NULL` rather than
erroring; `SET_IDENTIFIER`/`CLEAR_IDENTIFIER` round-trip correctly
through `SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')`;
`SET_CONTEXT('USERENV', ...)` correctly raises `ORA-01739`;
`SET_CONTEXT` on an unregistered namespace correctly raises
`ORA-01403`; a real non-superuser role is denied calling
`create_context` directly (owner-only, confirmed via `pg_proc.proacl`
showing no `PUBLIC` entry) -- and the full VPD-equivalent demo above
(three rows, two tenants, real `CREATE POLICY`) produced exactly the
expected row visibility for a real non-superuser role, confirmed
correct with no context set (zero rows) and with context set (exactly
that tenant's rows); a *second, fresh* connection as the same role
showed zero rows again, confirming the context store is genuinely
per-session, not shared or leaking across connections.

`ALTER SESSION` / `NLS_*`/`TO_CHAR`/`TO_DATE`, end to end: confirmed
live that `ALTER SESSION SET`/`ALTER SYSTEM SET` are respectively a
genuine syntax error and already-native in stock Postgres, not assumed
from memory; `alter_session_set('CURRENT_SCHEMA', ...)` correctly
prepends onto search_path (after the bug below was fixed) and
subsequent `CREATE TABLE` lands in the new schema;
`alter_session_set('TIME_ZONE', ...)` correctly changes `SHOW timezone`;
`translate_nls_format()` correctly rewrites all three confirmed-different
tokens (`RR`->`YY`, bare `FF`->`FF6`, `X`->`.`) while leaving every
other token untouched; the one-argument `to_char(date)`/`to_char(timestamp)`
and `to_date(text)` overloads work correctly with the real
`AMERICAN_AMERICA` defaults, and `alter_session_set('NLS_DATE_FORMAT',
...)` correctly changes what they produce; the explicit-format `date`
overload correctly translates `RR` where the bare Postgres function
would echo it literally.

A real bug found live in this pass: the first version of
`alter_session_set('CURRENT_SCHEMA', ...)` did a bare `SET search_path
TO <schema>`, which replaced the entire search_path and wiped out every
Oracle package schema `db_emulation` had already appended -- the very
next unqualified `to_char()` call in the same session failed with
"function does not exist". Fixed by prepending instead of replacing,
reverified: `search_path` now correctly shows the new schema first with
every package schema still present after it.

## Real orawire + sqlcl integration test

`test/orawire-integration/` -- the first real end-to-end run through the actual gateway, not
`psql`: real `sqlcl` (Oracle's own client) over the real Oracle wire protocol, through Polywire's
orawire, to Postgres with `pg_oracle` installed. Found and fixed a critical bug this way that no
amount of direct-`psql` testing could have caught: `db_emulation` was never being set at all for
a plain username/password orawire connection (Polywire's `JdbcBackendExecutor` skips its session
initializer for an anonymous `AccessContext`, correct for RLS/VPD-context propagation but wrong
for `db_emulation`, a protocol-level requirement of every orawire session). Fixed in `wire/` --
see `test/orawire-integration/README.md` for the full story, the exact contrast that pointed at
it, and several more real, still-open findings from that same run (a `SYS_CONTEXT` bind-typing
bug, a translator/overload-collision gap in reaching this extension's own new one-argument
`TO_CHAR`, and a real gap in orawire's basic `CREATE TABLE` DDL translation for Oracle datatypes).

No `pg_regress` test suite yet (tracked as follow-up in the `Makefile`).
