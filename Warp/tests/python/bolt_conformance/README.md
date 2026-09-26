# Neo4j / Bolt conformance harness for boltwire

Warp's Neo4j frontend (`boltwire`: Bolt over Postgres) is checked against a **real Neo4j** two ways. Nothing third-party is vendored.

## 1. openCypher TCK (`bolt_tck.py`)
A small Gherkin runner for `opencypher/openCypher` `tck/` (Apache-2.0) driven through the `neo4j` Python driver. Side effects are checked from the
result summary (nodes/relationships) and net label / property differences; error scenarios must return the same status code the oracle did.
```sh
git clone --depth 1 --filter=blob:none --sparse https://github.com/opencypher/openCypher /tmp/openCypher
git -C /tmp/openCypher sparse-checkout set tck/features tck/graphs
docker run -d --name warp-neo4j-oracle -p 7687:7687 -e NEO4J_AUTH=none -e NEO4J_server_memory_heap_max__size=512m neo4j:5
python3 bolt_tck.py --uri bolt://localhost:7687 --features /tmp/openCypher/tck --out oracle.json              # which scenarios are valid on Neo4j
python3 bolt_tck.py --uri bolt://<warp bolt> --features /tmp/openCypher/tck --only-valid golden/tck_oracle_neo4j_5.json.gz --out warp.json
```
## 2. Differential corpus (`bolt_harness.py` + `bolt_corpus.py`)
~950 Cypher / driver scenarios replayed against Neo4j and Warp with normalised comparison (values, columns, update counters, status codes).
```sh
python3 bolt_harness.py --oracle bolt://localhost:7687 --warp bolt://<warp> --record golden/oracle_neo4j_5.json.gz
python3 bolt_harness.py --golden golden/oracle_neo4j_5.json.gz --warp bolt://<warp>      # no Neo4j needed
```
`bolt_known.py` lists the documented differences. `bolt_launch_warp.py` starts a throwaway Warp (`WARP_TEST_PG_LOCAL=1 WARP_TEST_JAR=...`);
`bolt_rtt_bench.py` measures CREATE / MATCH round trips. `tests/python/test_boltwire_conformance.py` runs the replay in the normal suite.
