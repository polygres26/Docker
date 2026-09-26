# mongowire conformance

Defines "behaves like MongoDB" for Warp's MongoDB frontend (`Warp/src/main/java/com/sayonora/warp/mongowire/`) two ways: the same pymongo
operation sequences are run against a **real mongod 7.0** (the oracle) and against Warp and the normalised answers are diffed,
and MongoDB's own **driver-spec tests** (`mongodb/specifications`, Apache-2.0) are run against both.

| file | purpose |
|---|---|
| `mongo_corpus.py` + `mongo_corpus_{query,write,agg,expr,admin}.py` | the differential corpus (find/query operators, sort, projection, count/distinct, insert/update/delete/findAndModify/bulk/upsert, every update operator, pipeline updates, positional + arrayFilters, aggregation stages and ~800 expression operators, indexes and unique enforcement, collection/database admin, validators, cursors, server commands, handshake, error shapes) |
| `mongo_typed.py` | type-exact BSON -> JSON conversion (int32/int64/double/decimal/date/binary/... stay distinguishable; key order is kept) |
| `mongo_harness.py` | runner: `--oracle URI --warp URI` (or `--start-warp [--shards 2]`), `--record golden/oracle_7.0.json.gz`, `--dump CASE`, `--filter RE`, `--show diff|msg|all` |
| `mongo_run_spec.py` | spec-suite runner: `source/crud/tests/unified/*.json` (subset unified-format runner) and every `valid` case of `source/bson-corpus/tests/*.json` (canonical BSON inserted and read back through pymongo, must be byte-identical) |
| `mongo_known.py` | documented divergences (class c) |
| `mongo_launch_warp.py` | starts a throwaway Warp (`WARP_TEST_JAR`) on local Postgres process(es); 2 shards = one backend set with the `mongodb` store sharded |
| `mongo_rtt_bench.py` | insertOne / find-by-_id round trips (before/after comparisons) |
| `golden/oracle_7.0.json.gz` | the oracle's answers, recorded twice per case (non-deterministic steps dropped); replayed offline by `../test_mongowire_conformance.py` |
| `results/` | `summary.txt` (before -> after), `remaining_differences.txt`, `spec_*.json` |

## Running

```sh
docker run -d --name warp-mongo-oracle -m 1g -p 0:27017 mongo:7.0        # note the mapped port
git clone --depth 1 https://github.com/mongodb/specifications /tmp/mongo-specs
export WARP_TEST_JAR=/path/to/sayonora-warp.jar WARP_TEST_PG_LOCAL=1
export WARP_CLUSTER_ENABLED=true WARP_CLUSTER_DISCOVERY=static WARP_CLUSTER_SEED_NODES=127.0.0.1:<free port in 47500..47599>
python3 mongo_harness.py --oracle mongodb://localhost:<port> --start-warp [--shards 2] [--filter agg_] [--show diff|msg|all]
python3 mongo_harness.py --oracle mongodb://localhost:<port> --record golden/oracle_7.0.json.gz        # re-record after corpus changes
python3 mongo_run_spec.py --specs /tmp/mongo-specs --uri <warp uri> --oracle mongodb://localhost:<port> --out results/spec_after.json
docker rm -f -v warp-mongo-oracle
```

Never rebuild the jar while a Warp started from it is running (the JVM loads classes lazily; a replaced jar drops connections).

## Verdicts

`same` identical after normalisation - `msg` same error code / shape, only the message text or codeName differs - `diff` different -
`accepted` a documented divergence (`mongo_known.py`; the rule-based one accepts "both fail, different error code" for aggregation
expressions/stages). With two backends, steps whose answer depends on natural document order (limit/skip/first match without a sort,
`$first`/`$last`, a multi-document update that fails midway) are only compared on one backend; everything else, including sort/limit/
group/lookup/distinct/count, must be exact on two.

Transactions and change streams are not covered by the oracle run because a standalone mongod refuses them too: Warp answers exactly like a
standalone mongod (`IllegalOperation 20` "Transaction numbers are only allowed on a replica set member or mongos", `40573` for `$changeStream`).
