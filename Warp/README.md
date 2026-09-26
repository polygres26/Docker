# Warp

A mid-tier database gateway. It speaks Oracle TNS/TTC, MySQL client/server protocol, SQL Server
TDS, Postgres wire protocol v3, MongoDB wire protocol, DynamoDB's HTTP/JSON API, Amazon SQS's
HTTP/JSON API, gRPC, and MCP to clients — translating and routing every one of them to a real
backend. Postgres was the original, still-primary backend; Oracle, SQL Server, and MySQL/MariaDB
are now real backend engines too, not just protocols Warp imitates for clients — see
[`../docs/WARP_GUIDE.md` §4.4](../docs/WARP_GUIDE.md#44-multiple-backend-engines-top-5-by-db-engines-ranking-alongside-postgres).
It's wire-protocol compatibility for a pre- or post-migration cutover, not a schema/data migration
tool itself. mywire, orawire, mssqlwire, and MCP can each also run in **native-backend mode**
instead of translating — proxying straight through to a real Oracle/MySQL/SQL Server database of
your own with nothing rewritten in transit, for keeping the engine you already run rather than
migrating off it. See [`../docs/WARP_GUIDE.md` §8.1.1](../docs/WARP_GUIDE.md#811-native-backend-mode-proxy-straight-to-oracle-mysql-or-sql-server-instead-of-translating).

New here? Start with [`../docs/USER_GUIDE.md`](../docs/USER_GUIDE.md) — what Warp does, why
you'd use it, and how to point your app at it. [`../docs/WARP_GUIDE.md`](../docs/WARP_GUIDE.md)
and [`../docs/PERFORMANCE.md`](../docs/PERFORMANCE.md) are technical/internal references (pipeline
internals, security, HA, the latency investigation) for operators and contributors.

Point an existing app's connection string at Warp instead of its original database, and it
translates and routes to real Postgres. Run it indefinitely as a permanent compatibility shim
(e.g. legacy MongoDB driver code not worth rewriting), or as a temporary cutover bridge while a
migration tool moves schema/data behind the scenes.

## Architecture

Every protocol frontend feeds the same pipeline: frontends → cross-backend JOIN federation →
firewall → router → QoS admission control → dialect translation → rollup → cache → stats
collection → backend execution. Config lives in Postgres itself (`warp_config`,
`warp_firewall_rules`), hot-reloaded to every running process via `LISTEN/NOTIFY` — no
restart to change a firewall rule, routing topology, or SQL rewrite rule.

A `JOIN` spanning two shards or two functionally-separated backends is planned and executed for
real via Apache Calcite (predicate/column pushdown, real-statistics-driven cost-based join
ordering, exact semi-join pushdown to cut what crosses the wire) — not the correctness-limited
scatter-gather path's own broadcast-and-merge. See
[`../docs/WARP_GUIDE.md` §4.3](../docs/WARP_GUIDE.md#43-cross-shard--cross-backend-join-federation).

![Warp architecture: nine client protocols (OraWire, MySQL, SQL Server, Postgres wire, MongoDB, DynamoDB, Amazon SQS, gRPC, MCP) feed a shared eight-stage pipeline -- frontends, firewall, router, QoS, dialect translation, rollup, cache, stats collector -- each paired with the customer outcome it drives, backed by a Postgres control plane over LISTEN/NOTIFY and executing against horizontally-sharded Postgres backends](docs/architecture.png)

The full architecture, security, HA, and deployment guide with more diagrams lives at
[`warp/index.html`](https://polygres26.github.io/warp/) (or open it directly:
[warp/index.html](https://github.com/polygres26/polygres26.github.io/blob/main/warp/index.html)).

## Quick start

```bash
mvn package -DskipTests
scripts/run.sh
```

No `WARP_*` env vars set defaults to `localhost:5432`; see [Configuration](#configuration)
below for pointing it at a real backend.

## Protocol frontends

| Frontend | Protocol | Default port |
|---|---|---|
| pgwire | Postgres wire protocol v3 | 15432 |
| mywire | MySQL client/server protocol | 13306 |
| orawire | Oracle TNS/TTC | 11521 (plaintext), 2484 (TCPS/TLS) |
| mssqlwire | SQL Server TDS | 14333 |
| mongowire | MongoDB wire protocol | 27017 |
| dynamowire | DynamoDB HTTP/JSON API | 18000 |
| sqswire | Amazon SQS HTTP/JSON API | 9324 |
| s3wire | Amazon S3 REST API (path-style, SigV4); Postgres mode (objects stored in the Postgres backends that enable the `s3` store, sharded by key) or proxy mode over an S3-compatible bucket (MinIO); off unless the `s3` store is enabled, `WARP_S3WIRE_BACKEND_BUCKET` is set or `WARP_S3WIRE_ENABLED=true` | 18020 |
| rediswire | Redis RESP2/RESP3 (strings, hashes, lists, sets, sorted sets, streams, pub/sub, MULTI/EXEC, blocking, HyperLogLog, bitmaps, geo; no Lua); data in the Postgres backends that enable the `redis` store, sharded by Redis Cluster hash slot; off unless the `redis` store is enabled, `WARP_REDISWIRE_PORT` is set or `WARP_REDISWIRE_ENABLED=true` | 16379 |
| azurewire | Azure Storage REST: Blob, Queue and Table (OData JSON) with SharedKey/SharedKeyLite/SAS auth; data in the Postgres backends that enable the `azblob` / `azqueue` / `aztable` stores; verified against Azurite; each listener is off unless its store is enabled, its port is set or `WARP_AZ<BLOB\|QUEUE\|TABLE>WIRE_ENABLED=true` | 10000 / 10001 / 10002 |
| gcswire | Google Cloud Storage: JSON API (buckets, objects, media / multipart / resumable uploads, copy/rewrite/compose, generations, ACLs, HMAC keys, batch) and XML API (S3-interoperable, SigV4 with HMAC keys, XML multipart and resumable); data in the Postgres backends that enable the `gcs` store, sharded by bucket/object name; verified against fake-gcs-server; off unless the `gcs` store is enabled, `WARP_GCSWIRE_PORT` is set or `WARP_GCSWIRE_ENABLED=true` | 4443 |
| pubsubwire | Google Cloud Pub/Sub: gRPC (Publisher, Subscriber with StreamingPull, SchemaService, IAMPolicy) and REST/JSON v1; topics, subscriptions, ack deadlines, ordering keys, filters, dead letters, retry policy, exactly-once delivery, push, snapshots and seek, Avro schemas; data in the Postgres backends of a backend set (the `pubsub` store); default ports 8085 (gRPC) and 8086 (REST) |
| firestorewire | Google Cloud Firestore (native mode): v1 gRPC (incl. bidirectional Write and Listen) and REST/JSON on one port; documents, structured queries with Firestore's value order, transactions, transforms, aggregations, vector search, collection groups; `FIRESTORE_EMULATOR_HOST` works. Postgres-backed, verified against the official Firestore emulator |
| datastorewire | Google Cloud Datastore: v1 gRPC and REST/JSON (incl. GQL) on one port; entities, keys and entity groups, queries, projections, transactions, aggregations, id allocation; `DATASTORE_EMULATOR_HOST` works. Postgres-backed, verified against the official Datastore emulator |
| awswire | one unified AWS endpoint: DynamoDB, SQS, S3 (in process) plus SNS (Query/JSON, SQS and HTTP(S) delivery, filter policies, FIFO), Kinesis (JSON/CBOR, HTTP/2, SubscribeToShard), Secrets Manager, SSM Parameter Store, KMS and STS/IAM basics on Postgres (`sns`, `kinesis`, `awsparams` stores); verified against Floci's SDK suites; off unless `WARP_AWSWIRE_PORT` is set or `WARP_AWSWIRE_ENABLED=true` | 4566 |
| oswire | OpenSearch 2.x REST/JSON API (documents, `_bulk`, `_search` with query DSL/aggregations/highlight, scroll, k-NN, index management, cat/cluster probes; Lucene BM25 scoring; verified against a real OpenSearch, see `tests/python/os_conformance/`) | 9200 |
| bigtablewire | Google Cloud Bigtable: gRPC data API (ReadRows with row sets, reversed scans and the full filter language, MutateRow/MutateRows, CheckAndMutateRow, ReadModifyWriteRow, SampleRowKeys) and table admin API (tables, column families, GC rules, DropRowRange); rows in the Postgres backends of a backend set (the `bigtable` store) sharded by hash of table and row key; verified against Google's Bigtable emulator (`tests/python/bt_conformance/`); env `WARP_BIGTABLEWIRE_PORT` / `_SET` / `_ENABLED` / `_TOKENS` / `_GC_INTERVAL_SECONDS` / `_SCAN_PAGE_CELLS` / `_SAMPLE_BYTES` | 8088 |
| cqlwire | Apache Cassandra CQL native protocol v3/v4 (what Amazon Keyspaces and Cosmos DB's Cassandra API expose): unmodified Cassandra drivers connect; keyspaces, tables (partition/clustering keys, static columns, clustering order), UDTs, secondary indexes, INSERT/UPDATE/DELETE with TTL/TIMESTAMP, lightweight transactions, counters, batches, paging, collections/tuples/UDTs, JSON, `system.local` / `system_schema.*` for driver discovery; partitions in the Postgres backends of a backend set (the `cql` store) sharded by Murmur3 hash of the partition key, cross-partition scans merged in Cassandra's token order; verified against a real Apache Cassandra 5.0 (`tests/python/cql_conformance/`); env `WARP_CQLWIRE_PORT` / `_SET` / `_ENABLED` / `_AUTH` / `_SWEEP_MS` / `_SCHEMA_TTL_MS` / `_CLUSTER_NAME` / `_RELEASE_VERSION` | 19042 |
| gremlinwire | Apache TinkerPop Gremlin Server protocol (what Azure Cosmos DB's Gremlin API exposes): unmodified Gremlin drivers connect over WebSocket (GraphSON 3.0/2.0 and GraphBinary 1.0 negotiated by mimetype; `eval` scripts, `bytecode` traversals, sessions, `batchSize` chunking with 206/200/204, SASL PLAIN) or HTTP (`POST /` with `{"gremlin": ...}`); a real traversal interpreter written for Warp (no embedded Groovy or TinkerPop: about 100 steps incl. predicates, repeat/until/emit, group/project/select, path/tree, sack, side effects, addV/addE/property/drop/mergeV/mergeE, a Groovy-subset script language with closures); a property graph of its own in the Postgres backends of a backend set (the `gremlin` store), vertices sharded by hash of the id and edges stored with their out-vertex, in-edge and edge-by-id lookups scatter-gathered so traversals across hosts stay correct; verified against a real Gremlin Server 3.8 (`tests/python/gremlin_conformance/`: about 1500 recorded cases incl. 700 seeded random traversals, plus the reference's own Java serializers); env `WARP_GREMLINWIRE_PORT` / `_SET` / `_ENABLED` / `_AUTH` / `_BATCH_SIZE` / `_EVAL_TIMEOUT_MS` / `_SESSION_TIMEOUT_MS` / `_MAX_CONTENT_LENGTH` / `_READ_ONLY` | 8182 |
| kafkawire | Apache Kafka wire protocol: unmodified Kafka clients (Java, librdkafka / confluent-kafka, kafka-python, kcat, the console tools) produce, fetch and consume; Produce (acks 0/1/all, record batch v2 with CRC32C check, gzip / snappy / lz4 / zstd stored as sent, idempotent producers with sequence checks), Fetch (long poll, `max_bytes`, watermarks), ListOffsets, consumer groups (classic protocol, any assignor, static membership only minimally), OffsetCommit / OffsetFetch, topic / partition / config administration, DeleteRecords, retention sweeper, SASL/PLAIN; the log lives in the Postgres backends of a backend set (the `kafka` store), a partition on the host owning hash(topic, partition), topic metadata / groups / offsets on the first host; transactions and log compaction are not implemented; verified against a real Apache Kafka 4.3 (`tests/python/kafka_conformance/`); env `WARP_KAFKAWIRE_PORT` / `_SET` / `_ENABLED` / `_ADVERTISED_HOST` / `_ADVERTISED_PORT` / `_AUTH` / `_AUTO_CREATE` / `_NUM_PARTITIONS` / `_SWEEP_MS` / `_GROUP_INITIAL_REBALANCE_DELAY_MS` | 19092 |
| cosmoswire | Azure Cosmos DB for NoSQL (SQL/Core) REST API over plain HTTP: the official SDKs work (databases, containers with partition key paths incl. hierarchical keys, indexing policy stored, default TTL, unique keys; item create/upsert/replace/patch/delete with ETags and session tokens; SQL queries with joins, subqueries, aggregates, GROUP BY, ORDER BY, OFFSET/LIMIT and the system functions; cross-partition queries, query plan and pkranges endpoints, transactional batch, incremental change feed, TTL sweeper); master-key HMAC auth. Stored procedures, triggers and UDFs are stored but never executed. Port 18081 (`WARP_COSMOSWIRE_PORT`), store `cosmos`; see `tests/python/cosmos_conformance/cosmos_known.md` |
| gRPC | gRPC | 7070 (plaintext), 17071 (TLS) |
| MCP | JSON-RPC 2.0 over Streamable HTTP | 18010 |
| Admin / metrics | HTTP | 19090 |

Every frontend feeds the same shared pipeline: `FirewallStage → RouterStage → QosControlStage →
DialectTranslationStage → RollupStage → CacheStage → StatsCollectorStage`.

## Configuration

Every setting is readable from **either** an env var or the `warp_config` Postgres table
(hot-reloaded via `LISTEN/NOTIFY`, no restart required). Key env vars:

| Variable | Purpose |
|---|---|
| `WARP_HOST` / `_PORT` / `_DATABASE` / `_USER` / `_PASSWORD` | The config-primary Postgres — holds `warp_config`, `warp_firewall_rules`, and control-plane state |
| `WARP_AUTH_USER` / `_PASSWORD` | Default credential for wire-protocol frontend auth |
| `WARP_STANDBY_HOST` / `_PORT` | Optional standby for automatic config-primary failover |
| `WARP_BACKENDS` / `WARP_SHARD_BACKENDS` | Additional named Postgres data-plane targets and shard groups. Backends are managed inside **backend sets** (admin UI *Backend sets*, `/api/backend-sets`); `WARP_BACKEND_GROUPS` are the sets (a backend with no group is in the implicit `default` set) |
| `WARP_CONNECT_ROUTING` | Connect-time backend routing: the database / service name a client sends selects a backend or backend set. `implicit` (default: routes + backend/set names, unknown names behave as before), `strict` (reject unknown names with the protocol's native error), `off`. See WARP_GUIDE §4.8 |
| `WARP_CONNECTION_ROUTES` / `warp_config.connectionRoutes` | JSON array of explicit routes `{protocol?, database, user?, target, defaultBackend?}` (`/api/connection-routes`) |
| `WARP_BACKEND_HEALTH_PROBE_TIMEOUT_SECONDS` | Per-backend health-probe timeout (default 10); all backends are probed concurrently. With many backends also lower `WARP_POOL_MAX_SIZE` (pools are per backend) |
| `WARP_BACKEND_STORES` / `WARP_BACKEND_SET_NAMES` | Env spelling of the enabled stores per Postgres backend (`pg2=mongodb,sqs\|default=dynamodb`; stores: `influxdb`, `mongodb`, `sqs`, `neo4j`, `opensearch`, `dynamodb`) and the declared set names (`a,b`); persisted in `warp_config` (`backendStores`, `backendSetNames`), hot-reloaded |
| `WARP_DYNAMOWIRE_SET` / `WARP_SQSWIRE_SET` / `WARP_MONGOWIRE_SET` / `WARP_INFLUXWIRE_SET` / `WARP_OSWIRE_SET` / `WARP_BOLTWIRE_SET` | The backend set a protocol frontend serves (default: the set holding the `default` backend). A store enabled on backend(s) of that set is hosted there, sharded by key hash when several backends enable it (Neo4j: one backend per set). See `docs/WARP_GUIDE.md` §4.7 |
| `WARP_ROUTER_SCHEMA_RULES` | Routes a schema-qualified table to a named backend — 2+ rules also enables cross-backend `JOIN` federation |
| `WARP_FEDERATION_PLAN_HISTORY` | Capacity of the federated-query SQL plan cache/history (0/unset disables) |
| `WARP_TRUSTED_BACKEND_HOSTS` | Allowlist gating what hosts `WARP_BACKENDS` can register — env-var only, never DB-writable |
| `WARP_ACL_RULES` | IP/CIDR allow-deny rules |
| `WARP_ACL_PPV2_ENABLED` / `WARP_ACL_TRUSTED_PROXIES` | PROXY protocol v2 / X-Forwarded-For support behind a load balancer |
| `WARP_OAUTH_ISSUER` / `_AUDIENCE` | OAuth2/OIDC bearer-token auth (Okta, EntraID, any standard issuer) for HTTP frontends |
| `WARP_AWS_IAM_CREDENTIALS` | AWS SigV4 request verification for dynamowire |
| `WARP_S3WIRE_BACKEND_BUCKET` / `_ENDPOINT` / `_ACCESS_KEY` / `_SECRET_KEY` / `_REGION` / `_PATH_STYLE` | s3wire backend: the one S3-compatible bucket (required to enable s3wire), its endpoint (unset = AWS), Warp's own credentials, region (default us-east-1), path-style (default true) |
| `WARP_S3WIRE_CREDENTIALS` / `WARP_S3WIRE_PORT` | s3wire client SigV4 pairs `key=secret;key2=secret2` (required; falls back to `WARP_AWS_IAM_CREDENTIALS`) and port (default 18020) |
| `WARP_S3WIRE_SET` | Backend set s3wire serves in Postgres mode (default: the set holding `default`). Enable the `s3` store on its Postgres backend(s) to store objects in Postgres instead of proxying; Postgres mode wins when `WARP_S3WIRE_BACKEND_BUCKET` is also set (WARN logged) |
| `WARP_S3WIRE_ENABLED` | `true` = start the s3wire listener even when neither the store nor the proxy bucket is configured yet (it adopts the `s3` store when enabled later). Needs `WARP_S3WIRE_CREDENTIALS` |
| `WARP_S3WIRE_CHUNK_BYTES` | Postgres mode chunk (bytea row) size, default 4194304 (64 KiB–64 MiB); stored per object, so changing it is safe |
| `WARP_S3WIRE_MAX_OBJECT_BYTES` / `WARP_S3WIRE_MAX_MULTIPART_BYTES` | Largest single PUT or part (default 5 GiB) / largest completed multipart object (default 50 GiB); over → `EntityTooLarge` |
| `WARP_S3WIRE_GC_INTERVAL_SECONDS` / `_GC_GRACE_SECONDS` / `_GC_UPLOADING_AGE_SECONDS` / `_GC_MULTIPART_AGE_SECONDS` | Postgres mode collector: period (60, 0 = off), how long replaced/deleted chunks stay for in-flight readers (600), idle age after which an unfinished upload is discarded (3600), age after which an uncompleted multipart upload is aborted (604800) |
| `WARP_S3WIRE_BUCKET_CACHE_MILLIS` / `WARP_S3WIRE_PROBE_OTHER_SHARDS` | Bucket-exists cache per Warp process (default 2000, 0 = off) / on a GET/HEAD miss also look on the other hosts of the set (default false; for data written before a topology change) |
| `WARP_REDISWIRE_PORT` / `_SET` / `_PASSWORD` / `_ENABLED` | rediswire port (default 16379), backend set it serves (default: the set holding `default`), AUTH password (empty = none), `true` = start even before the `redis` store is enabled |
| `WARP_REDISWIRE_ADVERTISE_HOST` / `_SWEEP_MS` / `_DATABASES` / `_MAX_CLIENTS` | host reported by `CLUSTER SLOTS` (default: the address the client connected to), expiry sweeper period (1000 ms), number of `SELECT` databases (16), client limit (10000) |
| `WARP_AZBLOBWIRE_PORT` / `WARP_AZQUEUEWIRE_PORT` / `WARP_AZTABLEWIRE_PORT` | azurewire listener ports (defaults 10000 / 10001 / 10002) |
| `WARP_AZBLOBWIRE_SET` / `WARP_AZQUEUEWIRE_SET` / `WARP_AZTABLEWIRE_SET` / `WARP_AZ<BLOB\|QUEUE\|TABLE>WIRE_ENABLED` | backend set each Azure store serves (default: the set holding `default`); `true` = start before the store is enabled |
| `WARP_AZURE_ACCOUNTS` / `WARP_AZURE_DEV_ACCOUNT` | storage accounts `account:base64key;account2:base64key` clients sign with (required); `true` also enables Azurite's public `devstoreaccount1` and its public dev key |
| `WARP_AZURE_DOMAIN` / `WARP_AZURE_BEARER_TOKEN` / `WARP_AZQUEUEWIRE_SWEEP_SECONDS` / `WARP_AZUREWIRE_MAX_THREADS` | domain for host-style addressing `<account>.blob.<domain>` (default `localhost`); one static token accepted as an Entra ID bearer token (unset = bearer auth refused); expired-message sweeper period (10); request threads per listener (400) |
| `WARP_GCSWIRE_PORT` / `_SET` / `_ENABLED` | gcswire listener port (default 4443, like fake-gcs-server), backend set the `gcs` store serves (default: the set holding `default`), `true` = start before the store is enabled |
| `WARP_GCSWIRE_TOKENS` / `WARP_GCSWIRE_ALLOW_ANONYMOUS` | accepted OAuth2 bearer tokens (comma list, not validated as JWTs); `true` accepts requests without credentials (how fake-gcs-server runs). With neither, only HMAC-signed XML requests and signed URLs succeed (HMAC keys are created through the JSON API, so bootstrap with a token or anonymous access) |
| `WARP_GCSWIRE_SIGNING_KEYS` | `email=/path/key.pem;email2=/path/sa.json` public keys (PEM public key / certificate / PKCS#8 private key / service-account JSON) that verify V4 (`GOOG4-RSA-SHA256`) and V2 signed URLs |
| `WARP_GCSWIRE_DOMAIN` / `_LOCATION` / `_CHUNK_BYTES` / `_PROBE_OTHER_SHARDS` / `_GC_GRACE_SECONDS` / `_SESSION_TTL_SECONDS` / `_MAX_THREADS` | virtual-host suffix (`storage.googleapis.com`), default bucket location (`US`), data chunk row size (4 MiB), also look on the other hosts on a GET miss (false), how long deleted data lingers for readers mid-stream (600), abandoned resumable / multipart session lifetime (7 days), Jetty threads (400) |
| `WARP_PUBSUBWIRE_PORT` / `_REST_PORT` / `_SET` / `_ENABLED` | pubsubwire gRPC port (default 8085, like the official Pub/Sub emulator) and REST/JSON port (default 8087, `0` disables it), backend set the `pubsub` store serves (default: the set holding `default`), `true` = start before the store is enabled |
| `WARP_PUBSUBWIRE_TOKENS` | accepted bearer tokens (comma list); when set every gRPC call needs `authorization: Bearer <token>` and every REST request `Authorization: Bearer <token>` (or `access_token=`); unset = no authentication, like the emulator. Clients use `PUBSUB_EMULATOR_HOST=host:8085` |
| `WARP_PUBSUBWIRE_PULL_WAIT_MS` / `_MAX_STREAMS` / `_REST_THREADS` | how long a Pull without `return_immediately` waits (20000), cap of open StreamingPull streams (100000), REST worker threads (300) |
| `WARP_FIRESTOREWIRE_PORT` / `_SET` / `_ENABLED` | firestorewire listener (gRPC and REST on one port, default 8080, like the official emulator), backend set the `firestore` store serves (default: the set holding `default`), `true` = start it even when the store is not enabled |
| `WARP_FIRESTOREWIRE_TOKENS` / `_HISTORY_SECONDS` | accepted bearer tokens (comma list; none = no auth, like the emulator, which `Authorization: Bearer owner` also satisfies); how long document versions are kept for `read_time` reads, read-only transactions and Listen resume tokens (default 3600) |
| `WARP_DATASTOREWIRE_PORT` / `_SET` / `_ENABLED` | datastorewire listener (gRPC and REST on one port, default 8081, like the official emulator), backend set the `datastore` store serves, `true` = start it even when the store is not enabled |
| `WARP_DATASTOREWIRE_TOKENS` | accepted bearer tokens (comma list; none = no auth) |
| `WARP_AWSWIRE_PORT` / `WARP_AWSWIRE_ENABLED` / `WARP_AWSWIRE_MAX_THREADS` | unified AWS endpoint (`awswire`) port (default 4566 when enabled), `true` = start on the default port, Jetty threads (400). Off unless one is set. It dispatches by `X-Amz-Target` / SigV4 credential-scope service / Query `Action` / S3 default to dynamowire, sqswire and s3wire in process and to the new services (`docs/WARP_GUIDE.md` §4.7 *The unified AWS endpoint*) |
| `WARP_SNSWIRE_PORT` / `WARP_KINESISWIRE_PORT` / `WARP_SECRETSWIRE_PORT` / `WARP_SSMWIRE_PORT` / `WARP_KMSWIRE_PORT` / `WARP_STSWIRE_PORT` | start one AWS service on its own port (HTTP/1.1 and cleartext HTTP/2); off unless set. The data lives in the `sns`, `kinesis` and `awsparams` (Secrets, SSM, KMS, STS, IAM) stores enabled on Postgres backends of the set |
| `WARP_SNSWIRE_SET` / `WARP_KINESISWIRE_SET` / `WARP_AWSPARAMSWIRE_SET` | backend set each of those stores serves (default: the set holding `default`); sharded by topic / stream / secret, parameter and key hash across the backends that enable the store |
| `WARP_AWS_ACCOUNT_ID` / `WARP_AWS_REGION` | account id and region in every ARN of the new services (defaults `WARP_SQSWIRE_ACCOUNT_ID` / `WARP_SQSWIRE_REGION`, else `000000000000` / `us-east-1`) |
| `WARP_AWS_IAM_CREDENTIALS` | `accessKey=secret;...`: when set, SNS, Kinesis, Secrets Manager, SSM, KMS, STS, IAM and (through the unified endpoint) SQS requests must carry a valid SigV4 signature made with a pair or with an STS temporary credential issued by Warp; DynamoDB validates with the same variable. Unset = any credentials accepted. Authorization is never evaluated |
| `WARP_KMS_MASTER_KEY` / `WARP_KMS_INSECURE_DEV_KEY` | secret from which the KMS key-sealing key is derived (PBKDF2). KMS, Secrets Manager values and SecureString parameters **fail closed** (`KMSInternalException`) without it; `WARP_KMS_INSECURE_DEV_KEY=true` uses a fixed public key for development only. KMS here is an emulator, not an HSM |
| `WARP_SNSWIRE_HTTP_ATTEMPTS` / `WARP_SNSWIRE_HTTP_BACKOFF_MS` | delivery attempts to an HTTP(S) subscriber (default 3) and the linear backoff between them (1000 ms) before the subscription's redrive policy |
| `WARP_KINESISWIRE_SWEEP_SECONDS` / `WARP_KINESISWIRE_EFO_LINGER_MS` | retention sweeper period (default 60) / how long a SubscribeToShard stream stays open once caught up (default 2000; 300000 mimics Kinesis's 5 minutes) |
| `WARP_AWSWIRE_DEBUG_SIGV4` / `WARP_AWSWIRE_DEBUG_H2` | `true` logs the canonical request of every SigV4 mismatch on the new services (debugging clients that sign differently) / every HTTP/2 frame and read of the cleartext HTTP/2 support |
| `WARP_MCP_TOOLS` | Postgres functions/procedures to expose as individually-named MCP tools |
| `WARP_BACKEND_DESCRIPTIONS` / `WARP_BACKEND_GROUP_DESCRIPTIONS` | JSON objects `{"<backend>":"text"}` / `{"<group>":"text"}` describing backends and backend groups/sets; persisted in `warp_config` (`backendDescriptions`, `backendGroupDescriptions`, editable with `PUT /api/config`), hot-reloaded; shown by MCP `list_backends`/`describe_backend`, `inspect_schema` and `GET /api/backends` |
| `WARP_MCP_EMULATED_STORES` | Fallback: opt the Warp-emulated stores into MCP as logical backends of `default` (`default.dynamodb`, `default.mongodb`, `default.influx`): comma list or `all` (default none). Stores enabled on a backend through config (§4.7) are listed automatically as `<backend>.<kind>` and need no env |
| `WARP_MCP_READ_ONLY` | Hide and refuse every non-relational MCP write tool (`put_item`, `insert-many`, `put_object`, `write_line_protocol`, ...) |
| `WARP_MCP_ALLOW_SECRET_READS` | With `WARP_MCP_READ_ONLY=true`: still allow tools that return secret material (`secrets_get_secret_value`, decrypted SSM values, `kms_decrypt`); writes stay blocked |
| `WARP_MCP_GCP_PROJECT` / `WARP_MCP_BIGTABLE_INSTANCE` | Default Google project / Bigtable instance of the gcs, pubsub, firestore, datastore and bigtable MCP tools (defaults `warp-project` / `warp-instance`) |
| `WARP_MCP_REQUIRE_ENDPOINT` | `true` = the MCP listener serves only user-created endpoints (`/e/<id>` + bearer token) |
| `WARP_MCP_KIND` | **Legacy override/filter only** (tools are now associated automatically from the backend types in scope): restrict an endpoint to families (`relational`, `dynamodb`, `influx`, `mongodb`, `s3`, ...; comma list); several families → `<kind>_`-prefixed tools, `/kinds/<kind>` paths |
| *(admin API)* `/api/mcp-endpoints` | Create/list/get/PATCH/DELETE MCP endpoints with optional expiry (`expiresAt` ISO-8601 with offset, or `ttlSeconds`; null = never); stored in `warp_config.mcpEndpoints`, hot-reloaded — see WARP_GUIDE §8.5.2 |
| `WARP_ORACLE_BACKEND_MODE` / `WARP_MYWIRE_BACKEND` / `WARP_MSSQLWIRE_BACKEND` / `WARP_MCP_BACKEND` | Native-backend mode per frontend — proxy straight to a real Oracle/MySQL/SQL Server backend instead of dialect-translating into Postgres (§8.1.1) |
| `WARP_TLS_KEYSTORE` | Shared keystore for orawire TCPS / gRPC TLS |

## Security

- **SQL Firewall** — DBA-managed `warp_firewall_rules` table (priority, action, statement
  type, table-pattern glob or raw regex), matched before every statement executes.
- **ACL + PPv2/XFF** — IP/CIDR allow-deny, trusted-proxy-aware so a real client IP survives
  behind a load balancer without allowing header spoofing.
- **Backend-poisoning allowlist** — `WARP_TRUSTED_BACKEND_HOSTS` closes a config-driven SSRF
  vector where DB write access to `warp_config` could otherwise register an arbitrary
  routing target.
- **OAuth2/OIDC + AWS SigV4** — for the HTTP-based frontends (gRPC, MCP, dynamowire, admin API).
- **TLS** — dedicated listeners for orawire (TCPS) and gRPC, one shared keystore.

## High availability

Config-primary failover (`WARP_STANDBY_HOST`) with automatic failback probing. Sharding via
`WARP_SHARD_BACKENDS` with scatter-gather query fan-out.

![Warp multi-AZ cloud deployment: client applications behind a hyperscaler Network Load Balancer, fanning out to stateless Warp instances in three availability zones, each zone holding a primary or backup copy of cached entries with backup-copy replication across zones, a config-primary Postgres with a standby for automatic failover pushing LISTEN/NOTIFY config to every zone, and a data-plane Postgres shard/replica per zone](docs/deployment.png)

Every piece of this diagram is real today, including the cross-zone cache backup replication:
cloud-native cluster discovery (`WARP_CLUSTER_DISCOVERY=static|s3|gcs|azure`), AZ-aware
backup placement (a cache entry's backup never lands in the same AZ as its primary -- live-proven
by `WarpClusterAzBackupPlacementTest`, three real Ignite nodes, not a simulation), a
configurable backup count (`WARP_CLUSTER_CACHE_BACKUPS`, default 1), and TLS between cache
nodes (`WARP_TLS_KEYSTORE`) are all implemented and tested. What's genuinely still open: the
S3/GCS/Azure discovery finders are verified against the real Ignite classes but not yet exercised
against real cloud storage (no cloud credentials available to test with), and AZ is
operator-supplied (`WARP_AVAILABILITY_ZONE`) rather than auto-detected from cloud
instance-metadata. See the full deployment guide for the complete verification detail.

## Building

```bash
mvn package -DskipTests
```

Produces `target/sayonora-wire.jar` (shaded, runnable with `java -jar`). Requires the
`--add-opens` flags in `scripts/run.sh` for the embedded Ignite distributed cache.

## License

MIT — see [LICENSE](LICENSE).
