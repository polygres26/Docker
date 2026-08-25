# PolyWire smoke tests

Real client libraries, real protocols, one basic smoke test per wire protocol frontend —
verifying `ghcr.io/polygres26/polywire` actually works, with **no source checkout required**.
Both a Python (pytest) and a Java (JUnit 5) suite cover the same seven protocols, so pick
whichever fits your stack — they're not duplicates of each other, just two independent proofs.

Every frontend translates to the *same* Postgres underneath, so there's no real Oracle/SQL
Server/MongoDB/DynamoDB behind any of these ports — `docker-compose.test.yml` only needs one
Postgres container plus PolyWire itself.

## 1. Start PolyWire

```bash
docker compose -f docker-compose.test.yml up -d
```

Wait for both services to report healthy (`docker compose -f docker-compose.test.yml ps`), or
just re-run the test suite below a couple of times — each test connects fresh.

Already have PolyWire running some other way? Skip this step and set `POLYWIRE_HOST` (and the
per-protocol `POLYWIRE_*_PORT` env vars, if you remapped any ports) instead of using the compose
file's defaults.

## 2. Run the tests

**Python:**

```bash
cd python
pip install -r requirements.txt
pytest -v
```

**Java:**

```bash
cd java
mvn test
```

## What's covered per frontend

| Frontend | Client | Covers |
|---|---|---|
| pgwire | psycopg2 / JDBC | simple `SELECT`, create/insert/select round trip, transaction rollback |
| mywire | PyMySQL / JDBC | simple `SELECT`, create/insert/select round trip |
| mssqlwire | pymssql / JDBC | simple `SELECT`, create/insert/select round trip |
| orawire | python-oracledb / ojdbc | simple `SELECT FROM DUAL`, create/insert/select round trip, transaction rollback |
| mongowire | pymongo / MongoDB Java driver | insert + find by `_id`, update, delete |
| dynamowire | boto3 / AWS SDK v2 | create table, put item, get item |
| sqswire | boto3 / AWS SDK v2 | create queue, send/receive/delete message |

**Known gaps these tests reflect honestly, not silently work around** (see each test file's own
docstring/javadoc for detail):
- mywire and mssqlwire have no session-scoped connection (a fresh pooled Postgres connection per
  statement), so neither has a rollback test.
- mssqlwire has no per-column TDS type mapping yet — every value comes back as a string, so its
  simple-`SELECT` test compares as a string.
- mongowire doesn't implement the aggregation pipeline — these tests stick to find/insert/
  update/delete.
- orawire's DDL type translation doesn't cover Oracle-specific type syntax yet (`NUMBER`,
  `VARCHAR2`) — these tests use ANSI-standard `INTEGER`/`VARCHAR` instead.
- **orawire, Java suite only**: `OraWireTest.createInsertSelectRoundtrip` and
  `.transactionRollback` are `@Disabled` — a real, confirmed, currently-reproducible bug found
  while writing this suite: a real ojdbc client's `SELECT` against an actual table gets
  `ORA-01403: no data found` even when the row is there and committed. Deterministic, and also
  breaks wire's own private `OracleJdbcIntegrationTest` — unrelated to this test suite or the
  published image. python-oracledb (the Python suite) is unaffected. `SELECT ... FROM DUAL` is
  unaffected too — the bug is specific to real-table `SELECT`s via ojdbc's combined
  describe+execute call.
- **mywire, Java suite only**: both `MyWireTest` tests are `@Disabled` — MySQL Connector/J 9.x
  gets "Access denied" even after forcing the `mysql_native_password` plugin explicitly; PyMySQL
  authenticates fine with the same credentials. Suspected client-specific protocol-compat gap,
  not fully root-caused — server-side auth code looked correct on inspection but the exact byte
  exchange wasn't instrumented.

## Cleaning up

```bash
docker compose -f docker-compose.test.yml down -v
```
