# OpenSearch conformance harness for oswire

Warp's OpenSearch frontend (`oswire`, HTTP/JSON over Postgres) is checked against a **real OpenSearch** two ways. Nothing
third-party is vendored here.

## 1. OpenSearch's own REST API tests (`run_spec.py`)

`run_spec.py` is a small runner for the YAML tests in `opensearch-project/OpenSearch`
(`rest-api-spec/src/main/resources/rest-api-spec/{api,test}`, Apache-2.0): `do`/`catch`/`headers`, `match`/`length`/
`is_true`/`is_false`/`gt`/`gte`/`lt`/`lte`/`contains`/`close_to`/`set`, `skip` (version + features), `setup`/`teardown`,
`$stash`, `_arbitrary_key_`. Needs PyYAML and requests.

```sh
git clone --depth 1 --filter=blob:none --sparse https://github.com/opensearch-project/OpenSearch.git /tmp/os-specs/OpenSearch
git -C /tmp/os-specs/OpenSearch sparse-checkout set rest-api-spec/src/main/resources/rest-api-spec
SPEC=/tmp/os-specs/OpenSearch/rest-api-spec/src/main/resources/rest-api-spec
DIRS=index,create,get,get_source,delete,update,bulk,search,msearch,count,mget,exists,indices.create,indices.delete,indices.get,\
indices.put_mapping,indices.get_mapping,indices.exists,indices.refresh,cluster.health,cat.indices,cat.health,cat.count,\
search.aggregation,search.highlight,scroll,info,ping,indices.get_alias,indices.put_alias,indices.update_aliases,\
indices.get_settings,indices.analyze,indices.get_field_mapping,field_caps,explain,pit,indices.flush,indices.exists_alias,\
cluster.stats,nodes.info,mlt

# 1. establish which tests are valid for this OpenSearch version (skip the ones that fail on the real thing)
python3 run_spec.py --specs $SPEC --url http://localhost:9200 --dirs $DIRS --out oracle.json
# 2. run only those against Warp
python3 run_spec.py --specs $SPEC --url http://localhost:<oswire port> --dirs $DIRS --only-passing oracle.json --out warp.json
```

`results/` holds the recorded counts and the classified remaining failures.

## 2. Differential corpus (`diff_harness.py` + `corpus.py`)

About 330 request sequences (index CRUD, versioning, bulk, update, scripts, dynamic mapping, query DSL, sorting, paging,
aggregations, highlighting, analyzers, aliases, templates, cat/cluster, error shapes) replayed against a real OpenSearch
and against Warp; status codes, error types and normalised response bodies (volatile fields such as `took`, uuids and
sequence numbers removed, floats rounded, `_score` compared where the case asks for it) are compared.

```sh
docker run -d --name warp-os-oracle -p 9200:9200 -e discovery.type=single-node -e DISABLE_SECURITY_PLUGIN=true \
  -e DISABLE_INSTALL_DEMO_CONFIG=true -e OPENSEARCH_JAVA_OPTS='-Xms512m -Xmx512m' opensearchproject/opensearch:2

python3 diff_harness.py --oracle http://localhost:9200 --warp http://localhost:<oswire> --record golden/oracle_2.19.6.json
python3 diff_harness.py --golden golden/oracle_2.19.6.json --warp http://localhost:<oswire>     # no Docker needed
docker rm -f -v warp-os-oracle
```

`golden/oracle_2.19.6.json` is the recorded, normalised oracle output (OpenSearch 2.19.6); it is what
`tests/python/test_oswire_conformance.py` replays against a real Warp (one Postgres, and two sharded Postgres).
A case can carry a `known` reason: it is then reported as `known` instead of failing.

## Launching Warp for these runs

`launch_warp.py --state state.json [--shards 2]` starts a throwaway Warp (oswire on a free port) on 1 or 2 local Postgres
servers (`WARP_TEST_PG_LOCAL=1`; two servers = one backend set, the `opensearch` store sharded across both) and
prints the URL. `build.sh <dir>` builds Warp in a private copy of the tree.
