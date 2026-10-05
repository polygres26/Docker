# What the JDBC statement style costs through each protocol

Measured on 2026-10-05 with `StatementStyleBenchmarkLiveTest` (one run, laptop, localhost, Warp Adapt mode over Postgres,
admission limit raised). One connection and one thread run 3,000 inserts and then 3,000 primary-key selects per style,
sequentially, after a 300-operation warm-up. Real drivers: pgjdbc, MySQL Connector/J (`useServerPrepStmts=true`),
mssql-jdbc, ojdbc11. "ops/s" is sequential throughput, so it is just the inverse of latency; p50/p99 are in ms.

| style | insert ops/s | insert p50 / p99 ms | select ops/s | select p50 / p99 ms |
|---|---|---|---|---|
| new Statement per call | 8281 | 0.11 / 0.29 | 9956 | 0.09 / 0.23 |
| reused Statement | 9528 | 0.09 / 0.24 | 8952 | 0.09 / 0.23 |
| reused PreparedStatement | 12118 | 0.08 / 0.16 | 16178 | 0.06 / 0.13 |
| new PreparedStatement per call | 12927 | 0.08 / 0.13 | 17006 | 0.06 / 0.08 |

## mywire

| style | insert ops/s | insert p50 / p99 ms | select ops/s | select p50 / p99 ms |
|---|---|---|---|---|
| new Statement per call | 7944 | 0.11 / 0.29 | 7348 | 0.12 / 0.28 |
| reused Statement | 10545 | 0.09 / 0.19 | 8836 | 0.10 / 0.24 |
| reused PreparedStatement | 11647 | 0.08 / 0.17 | 8847 | 0.09 / 0.23 |
| new PreparedStatement per call | 6706 | 0.14 / 0.30 | 5525 | 0.17 / 0.32 |

## mssqlwire

| style | insert ops/s | insert p50 / p99 ms | select ops/s | select p50 / p99 ms |
|---|---|---|---|---|
| new Statement per call | 10349 | 0.09 / 0.18 | 12086 | 0.07 / 0.18 |
| reused Statement | 11881 | 0.08 / 0.13 | 14902 | 0.06 / 0.13 |
| reused PreparedStatement | 11813 | 0.08 / 0.16 | 15467 | 0.06 / 0.15 |
| new PreparedStatement per call | 12130 | 0.08 / 0.14 | 18367 | 0.05 / 0.10 |

## orawire

| style | insert ops/s | insert p50 / p99 ms | select ops/s | select p50 / p99 ms |
|---|---|---|---|---|
| new Statement per call | 6824 | 0.11 / 0.41 | 10278 | 0.09 / 0.21 |
| reused Statement | 10029 | 0.09 / 0.21 | 12718 | 0.07 / 0.16 |
| reused PreparedStatement | 11995 | 0.08 / 0.15 | 581 | 1.71 / 1.84 |
| new PreparedStatement per call | 9816 | 0.09 / 0.20 | 547 | 1.82 / 1.91 |


## Reading it

- **A new `Statement` per call costs about 15-30% of throughput on writes** compared with reusing one (orawire 6.8k vs
  10.0k ops/s, mywire 7.9k vs 10.5k, pgwire 8.3k vs 9.5k, mssqlwire 10.3k vs 11.9k). In absolute terms that is roughly
  20-50 microseconds per call (p50 0.11 ms vs 0.09 ms), invisible next to a network round trip to a real database.
  Selects show the same direction with smaller gaps (and pgwire reads were noise-level).
- **Bound parameters are the fastest style for writes everywhere**; a new `PreparedStatement` per call is as fast as a
  reused one on pgwire and mssqlwire, but clearly slower on mywire (6.7k vs 11.6k inserts/s, 5.5k vs 8.8k selects/s).
- **Anomaly, not fixed: orawire selects with a bind variable are about 20x slower.** `select n from t where id = ?` runs at
  547-581 ops/s with a p50 of 1.7-1.8 ms and a p99 within 0.1 ms of it, against 12.7k ops/s (0.07 ms) for the same query
  with a literal. Inserts with binds on orawire are fine (12k ops/s). A constant ~1.6 ms extra on every call looks like
  a fixed delay or an extra round trip in the bound-select path, not parse cost; not diagnosed.
- Caveats: one run per cell, no confidence intervals, localhost (no network latency to hide Warp's per-call cost), one
  connection (no concurrency), Postgres backend only.

Re-run: `WARP_TEST_STYLE_PG_PORT=<port> mvn test -Dtest=StatementStyleBenchmarkLiveTest` against a Postgres with
`create table t(id bigint primary key, proto text, n int)`; `WARP_TEST_STYLE_PROTOCOLS=orawire` narrows it.
