# Warp integration tests (Python)

Real driver, real Postgres, real Warp subprocess -- no mocks, matching this project's own
live-verification style. Covers the three frontends where a real Python driver was faster to get
working than a Java one: orawire (`python-oracledb`), mssqlwire (`pymssql`), mywire (`PyMySQL`).
pgwire has its own JDBC-based suite instead: `../../src/test/java/.../pgwire/PgWireIntegrationTest.java`.

## Running

```bash
cd wire
mvn -DskipTests package        # produces target/sayonora-wire.jar, which these tests launch
pip install pytest oracledb pymssql pymysql
cd tests/python
pytest -v
```

Requires Docker (each test module starts and tears down a real, disposable `postgres:16-alpine`
container via the plain `docker` CLI).

## What's covered per frontend

Each of `test_orawire.py` / `test_mssqlwire.py` / `test_mywire.py` runs the same four checks:
a simple `SELECT`, a `CREATE TABLE`/`INSERT`/`SELECT` round trip, an explicit transaction
rollback, and the `/metrics` admin endpoint reporting the statements just run.

## Known gaps these tests found and document (not silently worked around)

- **mssqlwire has no per-column TDS type mapping** -- every value comes back as a string
  regardless of its real Postgres type (unlike orawire's VARCHAR2/NUMBER/DATE mapping). Assertions
  in `test_mssqlwire.py` compare as strings to reflect this honestly.
- **(Stale as of `docs/PERFORMANCE.md` §3.9/2026-09-23 -- kept here for history) mssqlwire and
  mywire have no session-scoped connection.** This was true when this note was first written, but
  both `MssqlWireSessionHandler.sessionConnection()` and `MySqlWireSessionHandler.sessionConnection()`
  now give each session one reused pooled `Connection` for its whole lifetime (same shape as
  orawire's own session-scoped `LazyPooledConnection`), specifically so `BEGIN`/`COMMIT`/
  `ROLLBACK` (SQL-verb or `setAutoCommit`-driven) have real cross-statement transaction state to
  act on. See `docs/PERFORMANCE.md` §3.9 for how this was reconfirmed live (a per-call
  `sessionConnection()` checkpoint measuring 0.04-0.08us once warm -- a field read, not a fresh
  borrow) while investigating a separate, still-real performance gap in the same call path. The
  rollback test is `skip`ped for mssqlwire (the client call itself hangs rather than erroring --
  a TDS response-shape mismatch, not yet root-caused) and `xfail(strict=True)` for mywire (fails
  cleanly) -- neither of those is about session-scoping itself, which now works for both.

## Bugs this test suite found and fixed along the way

- **`RoutingBackendExecutor` silently bypassed session transactions** for the common
  single-backend deployment (no `WARP_BACKENDS` configured) -- `RouterStage` explicitly
  assigns the synthetic `"default"` backend as every statement's routing target, which routed
  execution through a brand-new pooled connection (`autoCommit=true`) instead of the session's own
  connection, silently discarding every real client's explicit `COMMIT`/`ROLLBACK`. Fixed in
  `RoutingBackendExecutor.execute()`.
- **mssqlwire had no `BEGIN`/`COMMIT`/`ROLLBACK TRAN` translation at all** -- any driver that
  issues them (most do, on connect) got a hard Postgres syntax error. Fixed in
  `DialectTranslations.normalizeSqlServer`, with the added subtlety that a literal `BEGIN`
  translation leaks an open transaction into the connection pool (mssqlwire has no session to
  close it from) -- translated to a harmless no-op instead.
