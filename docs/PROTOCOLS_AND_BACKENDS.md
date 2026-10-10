# Protocols, services and backends

What Warp speaks to clients, what is behind each protocol, what real databases it can connect to, and what each of them cannot do.

**How to read this.** Every statement comes from [WARP_GUIDE.md](WARP_GUIDE.md), [REPLICAS_AND_FAILOVER.md](REPLICAS_AND_FAILOVER.md) or the code, and was
extracted on 2026-10-10. It was not re-tested for this document. "Verified against" repeats what the guide says was run, and names the reference
implementation where there was one. Where the guide and the code disagreed, the code was checked and the guide corrected (see the last section). Where
neither says, this document says "not stated" rather than guessing. The per-protocol sections link to the guide's own section for the full list of limits.

## 1. Three things that get confused

1. **A front end** is a port Warp listens on and a protocol it speaks to clients (PostgreSQL, Oracle TNS, MySQL, DynamoDB, S3, Kafka ...).
2. **Emulated** means Warp implements the protocol server itself and keeps the data in tables on **Postgres** backends (a "store"). No Cassandra, Kafka,
   S3 or RabbitMQ process exists. Stores can be hosted on Postgres backends only; enabling one elsewhere is refused (HTTP 400). The SQL front ends have an
   emulated mode too: the client's Oracle, MySQL or SQL Server SQL is translated and run on Postgres.
3. **A real backend** is a database engine Warp connects to: the SQL front ends in `bridge` or `relay` mode talk to a real Oracle, MySQL or SQL Server;
   any registered backend can be a routed, sharded or federated target; and a few non-JDBC systems (Cassandra, DynamoDB, MongoDB, S3, Kafka, Splunk) can be
   mounted read-only into a federated query. Section 6 lists them.

The **default backend** also holds Warp's control plane (`warp_config`, firewall rules, failover leases) and has to be Postgres.

## 2. Front ends at a glance

`Port` is the plaintext default; the TLS port is in brackets. `Store` is the name used to enable the emulated store on a backend. Sharding, replicas and
failover are described for the emulated store unless the row says otherwise. Section numbers refer to this document.

| Front end | Port (TLS) | Emulates / serves | Where the data is | Sharding | Replicas and failover | Atomic across hosts | Verified against |
|---|---|---|---|---|---|---|---|
| pgwire | 15432 (same port) | PostgreSQL v3 | Postgres (pass-through) | Table and value sharding, slots | Yes, all four engines' mechanisms apply to its backends | 2PC where the shards support it, else commit-last | Real pgwire client, live |
| orawire | 11521 (2484) | Oracle TNS/TTC | `emulate`: Postgres; `bridge`/`relay`: real Oracle | As for SQL (emulate/bridge) | Emulate: none for reads; relay: reader port | As pgwire | Real Python, JDBC, SQLcl; Relay also SQL\*Plus |
| mywire | 13306 (same port) | MySQL | `emulate`: Postgres; `bridge`/`relay`: real MySQL | As for SQL | Replica reads live; relay reader port live | As pgwire | Real MySQL 8.4 |
| mssqlwire | 14333 (same port) | SQL Server TDS | `emulate`: Postgres; `bridge`/`relay`: real SQL Server | As for SQL | Availability-group reads live; relay reader port stand-in only | As pgwire (XA not available in a stock Linux container) | Real SQL Server / Azure SQL Edge |
| gRPC | 7070 (17071) | Warp's own `QueryService.Execute` | Shared pipeline | As for SQL | Not stated | Not stated | Not stated |
| MCP | 18010 (18443) | Model Context Protocol | Postgres, or native Oracle/MySQL/SQL Server | Not stated | Not stated | Not stated | Not stated for MCP as a whole |
| A2A | 18030 (18444) | Agent2Agent, one method | Uses MCP's `query_natural_language` | n/a | n/a | n/a (read-only) | Not stated |
| mongowire | 27017 (same port) | MongoDB 7.0 | Postgres, `jsonb`+`bson` per collection | `_id` | Via the store's backend set | Not atomic across hosts | Real mongod 7.0, 2,692 steps |
| dynamowire | 18000 (18445) | DynamoDB JSON API | Postgres | Partition key | Via the backend set | `TransactWriteItems` per host, partial window | DynamoDB Local, AWS docs, Floci suites |
| oswire | 9200 (18447) | OpenSearch 2.x REST | Postgres | `_id` | Via the backend set | Not stated | Real OpenSearch 2.19.6 |
| influxwire | 8086 (18448) | InfluxDB 1.x, 2.x write | Postgres (optional TimescaleDB) | Measurement + tag set | Via the backend set | Batch applied host by host | Real InfluxDB 1.8.10 |
| boltwire | 7687 (same port) | Neo4j Bolt 4.4/5.x + Cypher | One Postgres backend | None (one host) | Via the backend | n/a | Real Neo4j 5.26, openCypher TCK |
| gremlinwire | 8182 (18461) | Gremlin Server (WebSocket+HTTP) | Postgres | Vertex key | Via the backend set | Vertex drop is one transaction per host | Real TinkerPop 3.8.2 |
| rediswire | 16379 (17379) | Redis RESP2/RESP3 | Postgres | CRC16 hash slot | Via the backend set | CROSSSLOT without a hash tag | Real Redis 7.4.11 |
| cosmoswire | 18081 (18457) | Cosmos DB for NoSQL | Postgres | Partition key | Via the backend set | One partition key only | Not compared against real Cosmos DB |
| cqlwire | 19042 (same port) | Cassandra CQL v3/v4 | Postgres | Murmur3 partition token | Via the backend set | Batch not atomic across hosts | Real Cassandra 5.0.9, 6,329 steps |
| bigtablewire | 8088 (18456) | Bigtable gRPC | Postgres | Table + row key | Via the backend set | Not stated | Google's Bigtable emulator, 503 steps |
| datastorewire | 8081 (18455) | Datastore v1 gRPC and REST | Postgres | Entity group | Via the backend set | Per host, failure-window caveat | Google's Datastore emulator, 330 steps |
| firestorewire | 8080 (18454) | Firestore gRPC and REST | Postgres | Document path | Via the backend set | Per host, failure-window caveat | Google's Firestore emulator, 418 steps |
| sqswire | 9324 (18446) | Amazon SQS (JSON and Query) | Postgres (reduced core on Oracle/SQL Server/MySQL) | Queue name | Via the backend set | Queue is one host; DLQ move is two-step | Floci suites, conformance tests |
| kafkawire | 19092 (19093) | Apache Kafka wire protocol | Postgres | Topic + partition | Via the backend set | Per partition batch | Real Kafka 4.3.1, 435 steps |
| amqpwire | 5672 (5671) | AMQP 0-9-1 and 1.0 | Postgres | Vhost + queue | Via the backend set | Outbox, at-least-once | Real RabbitMQ 4.3.6 |
| pubsubwire | 8085 gRPC, 8087 REST (18453, 18452) | Google Pub/Sub | Postgres | Subscription | Via the backend set | Outbox, at-least-once | Google's Pub/Sub emulator, 269 steps |
| awswire | 4566 (18451) | SNS, Kinesis, Secrets Manager, SSM, KMS, STS, IAM (+ DynamoDB, SQS, S3) | Postgres | Per service key | Via the backend set | Not stated | Floci SDK suites; no real AWS |
| s3wire | 18020 (18449) | Amazon S3 | Postgres chunks, or proxy to a real S3/MinIO bucket | Bucket + key | Via the backend set | No cross-shard atomicity | MinIO where it implements the operation |
| gcswire | 4443 (18450) | Google Cloud Storage JSON + S3-style XML | Postgres chunks | Bucket + object name | Via the backend set | Not stated | fake-gcs-server, 394 steps |
| azurewire | 10000 / 10001 / 10002 (18458 / 18459 / 18460) | Azure Blob / Queue / Table | Postgres | Per service key | Via the backend set | Table batch: one PartitionKey, atomic | Azurite 3.37, 529 steps |

"Via the backend set" means the store follows whichever Postgres backends of its set enabled it; replica reads and failover for those backends are the
SQL features in [REPLICAS_AND_FAILOVER.md](REPLICAS_AND_FAILOVER.md), not something the protocol adds. Adding a backend to a set never moves existing
data; the admin API reports `rebalanceRequired`. Online rebalancing exists only for SQL tables with the `slots` strategy (WARP_GUIDE §8).

## 3. Limits that apply to every emulated store

- Stores live on **Postgres** backends only (project decision: emulations are Postgres-only; sharding, failover, switchover and replica routing are the parts that must work on all four SQL engines).
- Adding a backend to a store's set never moves existing data; the admin API reports `rebalanceRequired`.
- Cross-host atomicity is per protocol (see the table). Where it says "failure-window caveat", a crash between host commits can leave a partial result; the guide's section for that protocol describes the window.
- Emulation is conformance-tested against the named reference implementation, not a claim of full parity. Each protocol's own unsupported list is in [WARP_GUIDE.md](WARP_GUIDE.md) (the section for that protocol).
- Anything marked "not stated" above was not found in the guide or the code summary; treat it as unaudited, not as supported.

## 4. SQL front ends: three modes

| Mode | Applies to | What happens | Notes |
|---|---|---|---|
| `emulate` (default) | orawire, mywire, mssqlwire | The client's dialect is translated and run on Postgres | Oracle/MySQL/SQL Server features without a Postgres equivalent are refused or approximated; Shim (PL/SQL and dictionary views) widens the reach |
| `bridge` | same three | Warp parses and pools, then sends to a real Oracle/MySQL/SQL Server through the shared execution pipeline | `WARP_<ORACLE\|MYWIRE\|MSSQLWIRE>_BACKEND_MODE`; same pipeline as Adapt/JDBC |
| `relay` | same three | One client connection is one backend connection, bytes pass through | Outside the pipeline: no routing, firewall or sharding; a separate reader port can serve read-only traffic to a replica |

pgwire is always pass-through to Postgres. Per-engine HA verification is in the table at the top of [REPLICAS_AND_FAILOVER.md](REPLICAS_AND_FAILOVER.md).

## 5. Messaging, cloud and object services

Cloud-service front ends (sqswire, kafkawire, amqpwire, pubsubwire, awswire, s3wire, gcswire, azurewire) keep queues, topics, objects and secrets in Postgres.
They are for development, testing and consolidation, not a drop-in for the managed service's scale, IAM or durability guarantees. s3wire is the one
exception that can also proxy to a real S3-compatible bucket. awswire covers SNS, Kinesis, Secrets Manager, SSM, KMS, STS and IAM, and also serves
DynamoDB, SQS and S3 on its port. Object stores for OCI are not supported.

## 6. Real backends

### 6.1 JDBC engines

| Engine | Routing/sharding target | XA (atomic multi-shard write) | HA (replicas, failover) | Protocol-store hosting | Live-tested in this repo |
|---|---|---|---|---|---|
| PostgreSQL | Yes | Yes | Yes | Yes (the only supported store host for emulations) | Yes |
| MySQL / MariaDB | Yes | Yes | Yes | Yes (SQL tables) | Yes (MySQL) |
| SQL Server | Yes | Yes (needs the XA procedures; absent in the Linux test container) | Yes | Yes (SQL tables) | Yes |
| Oracle | Yes | Yes (live in `ShardedWriteEnginesLiveTest`) | Yes, promotion/switchover/rejoin scripted only | Yes (SQL tables) | Yes, except those HA paths |
| 16 other registered dialects (Databricks, Spanner, DB2, HANA, Teradata, Pinot, SingleStore, SQLite, Avatica and others) | Registered with a driver | No | No | No | **No**: no test starts a real instance; treat as "registered, unverified" |

Only the four engines in the top rows have `EngineHa`, XA and DDL templates (`resources/ddl/<engine>/`).

### 6.2 Federation-only connectors (read-only)

Cassandra, DynamoDB, MongoDB, S3, Kafka and Splunk can be mounted into a federated query and read through `query_federated`. They cannot be
sharded, replicated or written through direct SQL: a direct statement returns `ERR_<CASSANDRA|DYNAMODB|MONGO|S3|KAFKA|SPLUNK>_ROUTING_UNSUPPORTED`. Multi-table
transactional writes against them are therefore refused, not emulated or compensated.

## 7. Known gaps and corrections

- **Unaudited here:** gRPC, MCP and A2A sharding/HA behavior; cosmoswire and awswire parity with the real services.
- **Unverified HA:** Oracle promotion, switchover and rejoin (fake only); failover during a rebalance on MySQL, SQL Server and Oracle; relay reader port on Oracle and SQL Server (stand-in servers).
- **Open conflict:** the guide says dynamowire supports CRUD on non-Postgres hosts but also that `PutItem` fails on MySQL; not resolved in this pass.
- **Corrected in the guide while writing this:** influx sharding (Postgres hosts of its backend set only, series hash-sharded), Cosmos "no TLS" (TLS exists), Kafka TLS (a second `SSL` listener).
- **Site:** the public site covers about 11 of the 27 protocols; the rest are documented only here and in the guide.
