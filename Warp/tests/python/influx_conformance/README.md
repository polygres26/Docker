# influxwire differential conformance

Defines "behaves like InfluxDB" for Warp's InfluxDB frontend (`Warp/src/main/java/com/sayonora/wire/influxwire/`) by replaying the **same**
request sequences against a real InfluxDB 1.8 (the oracle) and against Warp and diffing the normalised answers.

| file | purpose |
|---|---|
| `influx_corpus.py` | the corpus: 290 cases / 1,821 requests -- line protocol (types, escapes, precisions, duplicates, partial writes, conflicts, gzip, CRLF, ...), endpoints, SHOW/DDL/DELETE/INTO, SELECT (fields, tags, casts, WHERE, time bounds, GROUP BY tags/time/fill, every aggregate/selector/transformation/math function, subqueries, LIMIT/OFFSET/SLIMIT, chunked/CSV/epoch/pretty/params) and ~120 syntax/semantic errors |
| `harness.py` | runner: `--oracle URL --warp URL` (or `--start-warp [--shards 2]`), `--dump` (oracle only), `--record golden.json.gz` |
| `warp_start.py` | starts Warp (`WARP_TEST_JAR`) on local Postgres process(es), one or N sharded backends |
| `golden.json.gz` | the oracle's answers, recorded twice per case (non-deterministic steps dropped); replayed offline by `../test_influxwire_conformance.py` |
| `influx_rtt_bench.py` | single-point write / point-query round trips, for before/after comparisons |
| `results/` | `summary.txt` (before -> after counts), `remaining_differences.txt` (every accepted/skipped step and why) |

## Running it

```sh
docker run -d --name warp-influx-oracle -m 2g -p 0:8086 -e INFLUXDB_HTTP_AUTH_ENABLED=false influxdb:1.8   # note the mapped port
export WARP_TEST_JAR=/path/to/sayonora-warp.jar WARP_TEST_PG_LOCAL=1    # local Postgres instead of Docker
# Warps started from a shell that has other Warps running need a free Ignite seed port:
export WARP_CLUSTER_ENABLED=true WARP_CLUSTER_DISCOVERY=static WARP_CLUSTER_SEED_NODES=127.0.0.1:<free port in 47500..47599>
python3 harness.py --oracle http://localhost:<port> --start-warp [--shards 2] [--filter agg_] [--show diff|msg|all] [--out results.json]
python3 harness.py --oracle http://localhost:<port> --record golden.json.gz      # re-record after corpus changes
docker rm -f -v warp-influx-oracle
```

Warp is started with `WARP_INFLUXWIRE_STRICT_DB=true` (InfluxDB's 404 for unknown databases; the Warp default creates the database on first write).
Corpus safety: real InfluxDB materialises every bucket between the first point and `now()` for a `GROUP BY time()` without bounds (millions of
rows for 2020 data, the container is OOM-killed), so `influx_corpus.py` bounds every such query to a two-minute window.

## Verdicts and classes

`same` byte-identical after normalisation (status, content type, JSON body; the per-case database name and clock values are masked) ·
`msg` same status and JSON shape, only an error/message text differs · `diff` different · `accepted` a documented divergence ·
`skipped` output depends on the clock or server-global state.
Classes of remaining differences: **a** not implemented (Flux, HOLT_WINTERS, msgpack, `/debug`, `_internal`), **b** wrong behaviour (none left),
**c** InfluxDB-engine specific and infeasible (Go `math` last digits, Go error texts, shard ids in deletion errors, unstable series/tie order),
**d** harness (clock, server-global users/queries). See `results/remaining_differences.txt` and `docs/WARP_GUIDE.md` (*The InfluxDB store*).
