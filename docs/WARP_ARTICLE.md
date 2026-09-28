# Warp: Postgres, Speaking Every Database's Language

Most companies do not run one database. They run an Oracle system from the last decade, a MySQL
behind the web tier, a SQL Server that came with an acquisition, a MongoDB somebody's team chose
in 2016, and a DynamoDB table or two in the cloud. Each has its own driver, its own wire
protocol, its own SQL dialect and its own operational tooling.

Warp started from a simple question: what if all of those clients could talk to one database, and
never notice?

## Where it came from

Warp grew out of Omnigate, an earlier gateway built to work across many database technologies
at once. Omnigate's job was to sit between applications and a mixed estate of engines.

Working that way surfaced the real cost of a mixed estate. Every extra technology means another
adapter, another set of semantics to reconcile and another system to secure, monitor and fail
over. Adapters between N engines multiply quickly. The hard part isn't reaching each engine. It
is having no single place where the data lives, is governed, and can be made fast and correct.

The insight behind Warp was to stop treating every engine as an equal backend and to choose one.
Postgres is mature, extensible, transactional and well understood, and it has a large ecosystem.
So Warp inverts the problem. Instead of connecting to every database, it makes Postgres look like
every database. Whatever protocol the client speaks, the data ends up in Postgres.

Warp began as a layer on top of Postgres. It has since become its own product, with a query
pipeline, a distributed cache, sharding, federation and distributed transactions that no longer
depend on where it started.

## One gateway, eleven ways in

Warp is a mid-tier gateway. Applications keep their existing drivers and connection code and
change only the host and port in their connection string.

| Client speaks | Warp frontend | What the client sees |
|---|---|---|
| Postgres wire protocol v3 | pgwire | Postgres |
| MySQL client/server protocol | mywire | MySQL |
| SQL Server TDS | mssqlwire | SQL Server |
| Oracle TNS/TTC | orawire | Oracle |
| MongoDB wire protocol | mongowire | MongoDB |
| DynamoDB HTTP/JSON | dynamowire | DynamoDB |
| Amazon SQS HTTP/JSON | sqswire | SQS |
| OpenSearch HTTP/JSON | oswire | OpenSearch |
| InfluxDB line protocol | influxwire | InfluxDB |
| Neo4j Bolt / Cypher | boltwire | Neo4j |
| gRPC | Warp's native QueryService | Warp itself |

An MCP frontend also lets AI agents query through the same governed pipeline.

Cloud SDKs need only an endpoint override. A team using the AWS SDK for DynamoDB or SQS points it
at Warp, and their code does not change.

## How Postgres emulates all of them

The design works because every frontend does two jobs, and only the first one is protocol
specific.

**1. Speak the protocol faithfully.** Each frontend implements enough of its wire protocol that
the real, unmodified client driver connects, authenticates and exchanges messages without
noticing. That means the Oracle TNS/TTC handshake and its data types, the MySQL packet format, the
SQL Server TDS token stream, MongoDB's wire messages and DynamoDB's JSON API. We test each one
with the real client library for that ecosystem, not a hand-written imitation.

**2. Translate to SQL and hand off to one shared pipeline.** Each request becomes a SQL statement
and enters the same eight-stage pipeline, whichever protocol it came from:

frontends, then cross-backend join federation, firewall, router, QoS admission control, dialect
translation, rollup, cache, statistics, and finally backend execution.

Because everything converges on one pipeline, every capability is written once and applies to
every protocol. An Oracle client, a MongoDB client and a DynamoDB client all get the same
firewall, the same caching, the same rate limiting and the same metrics.

Dialect translation is where the emulation earns its keep. An Oracle `NUMBER` becomes a Postgres
`NUMERIC`. SQL Server's `TOP` and MySQL's backtick quoting become the Postgres equivalents.
Session behaviours specific to each engine are emulated on the Postgres connection. Document and
key-value operations from MongoDB and DynamoDB map onto tables Postgres already handles well.

Some protocols are not SQL at all, and Warp says so plainly. MongoDB, DynamoDB, SQS, OpenSearch
and InfluxDB are not SQL-text protocols. Their operations are mapped to fixed, well-understood
physical shapes rather than parsed from arbitrary SQL, and there is no such thing as a bind
variable in their wire formats. Warp does not pretend otherwise.

## It's more than a translator

Once every request flows through one pipeline, the gateway becomes a place to put capabilities
that individual databases handle unevenly.

- **A distributed cache.** Warp's cache is Ignite-backed and cluster-wide. Cached reads are served
  without touching the backend, including by a different Warp node from the one that populated the
  entry. Invalidation keeps it correct, including for writes made outside Warp.
- **Sharding with real routing.** Tables can be sharded across multiple Postgres backends by
  value. Warp routes on literal values and on bind parameters (the prepared-statement traffic that
  dominates real workloads), and prunes to a single shard when it can.
- **Correct cross-backend joins.** A join spanning shards or backends is planned and executed for
  real, using Apache Calcite with predicate pushdown and cost-based ordering, with a parallel join
  engine for large inputs. It is not a silent scatter-gather that is wrong when matching rows live
  on different backends.
- **Distributed transactions.** A client transaction that spans backends is coordinated with real
  two-phase commit (XA), backed by a durable recovery log replayed at startup. It works across
  Postgres, Oracle, SQL Server and MySQL.
- **Security and governance.** A SQL firewall, IP allow-listing, and row-level security passed
  through to the backend so the database itself enforces it, all changeable at runtime without a
  restart.
- **Native-backend mode.** Where you want to keep an engine you already run, the Oracle, MySQL and
  SQL Server frontends can proxy straight to a real backend of that engine, adding pooling, ACLs
  and observability without translating a statement.
- **Beyond Postgres.** A broad JDBC dialect catalog and connectors let Warp federate over other
  systems too, such as Snowflake, BigQuery, ClickHouse, Kafka, Cassandra and S3.

Warp is not a schema or data migration tool. It is the compatibility and runtime layer. Teams can
run it permanently for legacy client code that isn't worth rewriting, or as a bridge while data
moves to Postgres behind the scenes.

## What it costs: round-trip times

A gateway is only credible if it is fast, because it sits in the path of every query. We measured
round-trip time (RTT), the time from the client sending a request to receiving the response, for
every protocol, with real client drivers and a real Postgres backend, and no mocks.

**How we measured.** Each test warms up with a throwaway call, then times 40 real single-row
writes, and also reads Warp's own server-side timing. Numbers below are client-observed p50 on a
laptop, with Postgres in a Docker container on the loopback interface. That is a deliberately
unflattering setup, since Docker's port forwarding on macOS adds latency that has nothing to do
with Warp. Expect production hardware to do better. Run-to-run variation is roughly ±0.3–0.5 ms.

### Without the cache: a single-row write

| Protocol | p50 (ms) |
|---|---|
| mywire (MySQL) | 0.81 |
| boltwire (Neo4j) | 0.86 |
| orawire (Oracle) | 0.88 |
| **pgwire (Postgres)** | **0.94** |
| mongowire (MongoDB) | 1.05 |
| mssqlwire (SQL Server) | 1.13 |
| sqswire (SQS) | 1.63 |
| oswire (OpenSearch) | 1.65 |
| influxwire (InfluxDB) | 2.20 |
| gRPC (Python client) | 2.20 |
| dynamowire (DynamoDB) | 1.45 (3.56 in one run under heavy machine load) |

The SQL-speaking frontends, including the ones emulating Oracle, MySQL and SQL Server, land
within about 0.3 ms of native Postgres, so the emulation costs very little. The higher figures
belong to protocols that also do JSON or line-protocol marshalling on top of the database call.

### With the cache: a single-row read

| Case | Cache miss (ms) | Cache hit (ms) |
|---|---|---|
| Row cache, fixed shape | 13–16 | 0.4–0.8 |
| Generic primary-key cache, literal | 6–15 | 0.4–0.6 |
| Generic primary-key cache, bind parameter | 2–4 | 0.4–0.6 |
| Across nodes: node B serves what node A cached | 9–15 first read | **0.4–1.5** |

A cache hit is served in well under a millisecond, roughly 10 to 30 times faster than a miss.
The cross-node case matters most. Two separate Warp processes in one cluster share the cache, and
the second node answered from it without ever contacting Postgres for that key. The cache is
genuinely distributed, not per process.

The miss numbers are dominated by one-time JVM and cache warm-up on the first table touched. They
fall to roughly 1–2 ms once warm, and a long-running server pays that cost once. Miss numbers
were also the most sensitive to machine load, and hits were stable across every run.

## What the measurements taught us

Measuring honestly turned up real problems, and fixing them is part of the story.

- **Emulation was paying a hidden per-statement tax.** The Oracle and SQL Server frontends were
  re-issuing session-setup queries on every statement instead of once per session, costing up to
  a millisecond per call. Caching the last-applied context brought both into line with Postgres,
  with row-level security verified to still re-apply correctly when the context changes.
- **The cache was over-communicating on every miss.** Its invalidation bookkeeping made two
  network round trips where one atomic operation suffices. Fixing that surfaced a second bug, a
  non-serializable processor that crashed real deployments, which unit tests alone did not catch.
  It was found only by testing a real running server.
- **gRPC does not beat Postgres on speed.** We expected Warp's native gRPC interface to be the
  fastest way in. With Java clients on both sides and microsecond timing, its server-side time
  matched pgwire, but end to end it stayed about 1.2 to 1.3 times slower for single-row calls,
  because of HTTP/2 framing and thread handoffs rather than anything in Warp's code. It remains
  useful for typed clients and batching, and we do not claim a speed advantage we could not
  measure.

## Why this matters

Consolidating on Postgres is easy to want and hard to do, because the blocker is never the data.
It's the years of application code written against other databases' drivers and quirks. Warp
removes that blocker. Teams keep their code, standardise on one durable, open backend, and get
caching, sharding, transactions, security and observability in one place, at latency that stays
close to native Postgres.

It started as a gateway across many technologies, and it is now a database layer of its own.
