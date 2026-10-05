# What the JDBC statement style costs through each protocol

Measured on 2026-10-05 with `StatementStyleBenchmarkLiveTest` (laptop, localhost, Warp Adapt mode over Postgres, admission
limit raised). One connection and one thread run 3,000 inserts and then 3,000 primary-key selects per style, sequentially,
after a 300-operation warm-up. Real drivers: pgjdbc, MySQL Connector/J (`useServerPrepStmts=true`), mssql-jdbc, ojdbc11.
"ops/s" is sequential throughput, so it is just the inverse of latency; p50/p99 are in ms. The tables below are the run
**after** the orawire bind fix described under "Found and fixed"; they were run twice and the two runs differ by up to
about 30% in a cell, so read differences under ~25% as noise.

| style | insert ops/s | insert p50 / p99 ms | select ops/s | select p50 / p99 ms |
|---|---|---|---|---|
| new Statement per call | 7732 | 0.11 / 0.32 | 9472 | 0.09 / 0.23 |
| reused Statement | 10015 | 0.09 / 0.20 | 8632 | 0.09 / 0.24 |
| reused PreparedStatement | 11234 | 0.08 / 0.18 | 15173 | 0.06 / 0.15 |
| new PreparedStatement per call | 12609 | 0.08 / 0.13 | 16602 | 0.06 / 0.10 |

## mywire

| style | insert ops/s | insert p50 / p99 ms | select ops/s | select p50 / p99 ms |
|---|---|---|---|---|
| new Statement per call | 7804 | 0.12 / 0.27 | 6910 | 0.12 / 0.29 |
| reused Statement | 10651 | 0.09 / 0.19 | 8751 | 0.10 / 0.23 |
| reused PreparedStatement | 12056 | 0.08 / 0.14 | 8117 | 0.10 / 0.26 |
| new PreparedStatement per call | 6679 | 0.14 / 0.29 | 5320 | 0.18 / 0.31 |

## mssqlwire

| style | insert ops/s | insert p50 / p99 ms | select ops/s | select p50 / p99 ms |
|---|---|---|---|---|
| new Statement per call | 10840 | 0.09 / 0.16 | 12209 | 0.07 / 0.17 |
| reused Statement | 11690 | 0.08 / 0.12 | 15008 | 0.06 / 0.13 |
| reused PreparedStatement | 11809 | 0.08 / 0.15 | 14781 | 0.06 / 0.14 |
| new PreparedStatement per call | 11732 | 0.08 / 0.15 | 18172 | 0.05 / 0.11 |

## orawire

| style | insert ops/s | insert p50 / p99 ms | select ops/s | select p50 / p99 ms |
|---|---|---|---|---|
| new Statement per call | 8877 | 0.10 / 0.24 | 10047 | 0.09 / 0.20 |
| reused Statement | 9459 | 0.09 / 0.21 | 12372 | 0.07 / 0.16 |
| reused PreparedStatement | 11083 | 0.08 / 0.18 | 13853 | 0.06 / 0.15 |
| new PreparedStatement per call | 9299 | 0.10 / 0.20 | 16490 | 0.06 / 0.13 |


## Reading it

- **A new `Statement` per call is somewhat slower than reusing one**, by roughly 5-30% depending on protocol and run
  (inserts, this run: pgwire 7.7k vs 10.0k ops/s, mywire 7.8k vs 10.7k, mssqlwire 10.8k vs 11.7k, orawire 8.9k vs
  9.5k). The first run measured orawire at 6.8k vs 10.0k, so that cell moves a lot. In absolute terms it is 10-30
  microseconds per call, small next to a real network round trip.
- **Bound parameters are the fastest style** for both writes and selects on pgwire, mssqlwire and orawire; a new
  `PreparedStatement` per call is as fast as a reused one there. mywire is the exception: a new `PreparedStatement` per
  call is clearly slower than a reused one (6.7k vs 12.1k inserts/s, 5.3k vs 8.1k selects/s).
- Caveats: one connection (no concurrency), localhost (no network latency to hide Warp's per-call cost), Postgres backend
  only, a table of a few tens of thousands of rows.

## Found and fixed: orawire bound selects were 20-30x slower

In the first run `select n from t where id = ?` through orawire ran at 547-581 ops/s (p50 1.7-1.8 ms) against 12.7k ops/s for
the same query with a literal. Cause: orawire decoded an Oracle NUMBER bind as a `BigDecimal`, which reaches Postgres as
`numeric`; `bigint_col = numeric` is planned as `id::numeric = $1` (a parallel sequential scan, confirmed with
`EXPLAIN`) instead of an index scan on the primary key. It only shows once the table is more than a few thousand rows:
with 5,000 rows the bound select cost 0.30 ms, with 300,000 rows **7.3 ms against 0.37 ms** for the literal. Whole-number
binds that fit a long are now handed to the backend as a `Long`; fractions and larger numbers stay `BigDecimal`. After the
fix, 300,000 rows: bound 0.20 ms, literal 0.36 ms; in this benchmark the orawire bound selects run at 13.9k and 16.5k
ops/s. `OrawireBoundKeyLookupLiveTest` fails without the fix (5.47 ms vs 0.39 ms) and passes with it.

Re-run: `WARP_TEST_STYLE_PG_PORT=<port> mvn test -Dtest=StatementStyleBenchmarkLiveTest` against a Postgres with
`create table t(id bigint primary key, proto text, n int)`; `WARP_TEST_STYLE_PROTOCOLS=orawire` narrows it.
