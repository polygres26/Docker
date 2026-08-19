# PolyWire integration tests (Python)

Real driver, real Postgres, real PolyWire subprocess -- no mocks, matching this project's own
live-verification style. Covers the three frontends where a real Python driver was faster to get
working than a Java one: orawire (`python-oracledb`), mssqlwire (`pymssql`), mywire (`PyMySQL`).
pgwire has its own JDBC-based suite instead: `../../src/test/java/.../pgwire/PgWireIntegrationTest.java`.

## Running

```bash
cd wire
mvn -DskipTests package        # produces target/polygres-wire.jar, which these tests launch
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
- **mssqlwire and mywire have no session-scoped connection** -- `MssqlWireSessionHandler` and
  `MySqlWireSessionHandler` both open a fresh pooled Postgres connection per statement and close
  it immediately after (unlike orawire's session-scoped `LazyPooledConnection`), so there is no
  cross-statement transaction state for an explicit `COMMIT`/`ROLLBACK` to act on yet. The
  rollback test is `skip`ped for mssqlwire (the client call itself hangs rather than erroring --
  a TDS response-shape mismatch, not yet root-caused) and `xfail(strict=True)` for mywire (fails
  cleanly).

## Bugs this test suite found and fixed along the way

- **`RoutingBackendExecutor` silently bypassed session transactions** for the common
  single-backend deployment (no `POLYWIRE_BACKENDS` configured) -- `RouterStage` explicitly
  assigns the synthetic `"default"` backend as every statement's routing target, which routed
  execution through a brand-new pooled connection (`autoCommit=true`) instead of the session's own
  connection, silently discarding every real client's explicit `COMMIT`/`ROLLBACK`. Fixed in
  `RoutingBackendExecutor.execute()`.
- **mssqlwire had no `BEGIN`/`COMMIT`/`ROLLBACK TRAN` translation at all** -- any driver that
  issues them (most do, on connect) got a hard Postgres syntax error. Fixed in
  `DialectTranslations.normalizeSqlServer`, with the added subtlety that a literal `BEGIN`
  translation leaks an open transaction into the connection pool (mssqlwire has no session to
  close it from) -- translated to a harmless no-op instead.
