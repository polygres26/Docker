# Warp — Support Lifecycle and Reference Deployments

> **This is a technical/internal reference** for operators and contributors planning a deployment
> or evaluating protocol stability. For architecture limits and upgrade/rollback, see
> [`ARCHITECTURE_LIMITS.md`](ARCHITECTURE_LIMITS.md); for security posture, see
> [`SECURITY.md`](SECURITY.md); for the positioning context, see
> [`COMPETITIVE_POSITIONING_ROADMAP.md`](COMPETITIVE_POSITIONING_ROADMAP.md) (Phase H).

Every claim below is derived from reading the current implementation and its own existing
documentation, cited by file and line — not a generic support-tiering template. Two honest facts
shape this whole document and are stated up front rather than glossed over: Warp is currently
versioned as a single `0.1.0-SNAPSHOT` with no tagged releases
([`Warp/pom.xml:9`](../Warp/pom.xml)) and no `CHANGELOG.md`, so there is no real release history to
build a version-support-window table from yet; and there is no Kubernetes/Helm deployment manifest
anywhere in the repo today, only a single-node Docker Compose stack. This document publishes what's
real now (protocol maturity, working HA/sharding mechanisms, the one real deployment manifest) and
states plainly what doesn't exist yet, rather than inventing either.

---

## Part 1 — Support lifecycle

### 1.1 Protocol maturity tiers — already real, now published in one place

Warp already maintains a real, hand-curated maturity classification for every protocol/interface,
surfaced in the admin UI (`MaturityTag`/`MaturityLegend`) and driven from
[`Warp/web/src/api/maturity-data.json`](../Warp/web/src/api/maturity-data.json) via
[`Warp/web/src/api/maturity.ts`](../Warp/web/src/api/maturity.ts). This section is that same data,
published as a document rather than only living in the admin UI.

**This is an explicit editorial judgment call, not a live-measured metric** — `maturity.ts`'s own
comment states it plainly: a "product/documentation judgment call about verification depth,"
hand-assigned and revised as protocols mature or gaps are found, not derived from test pass rates
or the AWS-protocol-only compatibility scorecard (`CompatScorecard.java`), which is a separate,
narrower system covering only the AWS-family wire protocols against third-party Floci SDK suites.

| Tier | Meaning |
|---|---|
| **Verified** | Real official client/server, a large/quantified conformance suite, no major open gaps found — Warp's own verification, not a third-party certification. |
| **Production** | Real backend/emulator verification exists, with real, disclosed gaps documented in the guide. |
| **Preview** | Verification relies on an imperfect oracle, or a real, disclosed feature gap blocks production use end-to-end. |
| **Experimental** | Newest, still-settling, or no verification-oracle evidence found for this protocol yet. |

| Tier | Protocols |
|---|---|
| **Verified** | pgwire, mongowire, dynamowire, boltwire, cqlwire, kafkawire, amqpwire |
| **Production** | mywire, orawire, mssqlwire, sqswire, oswire, firestorewire, datastorewire, rediswire, mcp |
| **Preview** | s3wire, gcswire, pubsubwire, azurewire, gremlinwire, awswire, influxwire |
| **Experimental** | a2a, cosmoswire, bigtablewire, grpc |

The concrete evidence behind each placement lives in [`WARP_GUIDE.md`](WARP_GUIDE.md) per protocol
(e.g. mongowire's 2,683 recorded conformance steps against real `mongod`, boltwire's full openCypher
TCK pass rate against real Neo4j) — this table is the index, not a replacement for that detail.

### 1.2 What "stable vs. experimental" means for support purposes

Given this project has no tagged release history yet, "support" here means **how much a breaking
change to that protocol should worry you today**, not a promised backward-compatibility window
tied to a version number:

- **Verified/Production**: a breaking change to these protocols' wire behavior would be treated as
  a real regression requiring the same investigation discipline this project applies everywhere
  (a live before/after benchmark or conformance re-run, not just "looks fine") before shipping.
- **Preview**: expect real, disclosed gaps to still be closing — a behavior change here is more
  likely to be a genuine bug fix than a regression, but isn't yet held to Production's bar.
- **Experimental**: no compatibility expectation at all — these are the newest additions or ones
  with no verification-oracle evidence yet; behavior can change without notice.

### 1.3 Version support windows — honestly not yet applicable

There is no tagged release (`git tag -l` shows exactly one archival marker,
`archive/main-pre-dms-2026-08-31`, not a semantic release) and no `CHANGELOG.md`. A real "N and N-1
minor versions supported" policy requires a real release train to attach it to — this document does
not invent one. **Once the first tagged release ships, this section should be replaced** with a
real version-support table (e.g. "latest minor + one prior minor supported for security fixes");
until then, "supported" means "whatever is on the `main` branch," and an operator tracking Warp
today should expect to track `main` directly rather than a release cadence that doesn't exist yet.

### 1.4 Per-protocol test coverage (as of this writing)

A rough proxy for how much real automated verification backs each protocol — Java test file count
and Python integration test file count, by protocol:

| Protocol | Java tests | Python tests | Maturity |
|---|---|---|---|
| pgwire | 3 | 1 | Verified |
| mongowire | 1 | 2 | Verified |
| dynamowire | 2 | 3 | Verified |
| boltwire | 2 | 2 | Verified |
| cqlwire | 1 | 0 | Verified |
| kafkawire | 1 | 0 | Verified |
| amqpwire | 2 | 0 | Verified |
| mywire | 1 | 1 | Production |
| orawire | 1 | 1 | Production |
| mssqlwire | 1 | 1 | Production |
| sqswire | 1 | 3 | Production |
| oswire | 3 | 2 | Production |
| firestorewire | 1 | 0 | Production |
| datastorewire | 1 | 0 | Production |
| rediswire | 2 | 2 | Production |
| s3wire | 2 | 3 | Preview |
| gcswire | 2 | 0 | Preview |
| pubsubwire | 2 | 0 | Preview |
| azurewire | 2 | 0 | Preview |
| gremlinwire | 2 | 0 | Preview |
| awswire | 1 | 0 | Preview |
| influxwire | 2 | 2 | Preview |
| cosmoswire | 1 | 0 | Experimental |
| bigtablewire | 1 | 0 | Experimental |

Worth noting honestly: file *count* is a weak proxy (a single large test file can cover more than
several small ones), and several Verified/Production-tier protocols show zero Python integration
tests in this count — their verification evidence lives in Java integration tests instead (see
each protocol's own section in `WARP_GUIDE.md` for what actually backs its tier, not this table
alone).

### 1.5 Native-backend mode — a separate maturity axis for SQL protocols

Four protocols/interfaces can run in **native-backend mode** (no dialect translation, proxying
straight to a real Oracle/MySQL/SQL Server of your own) instead of the default
translated-to-Postgres mode:

| Protocol | Env var | Modes |
|---|---|---|
| mywire | `WARP_MYWIRE_BACKEND_MODE` | `relay` (native) / `bridge` |
| orawire | `WARP_ORACLE_BACKEND_MODE` | `native` (Relay) / `bridge` |
| mssqlwire | `WARP_MSSQLWIRE_BACKEND_MODE` | `relay` (native) / `bridge` |
| MCP | `WARP_MCP_BACKEND` | `oracle` / `mysql` / `sqlserver` |

The maturity tiers above describe the *default translated* mode; native-backend mode is a distinct
code path with its own PL/SQL support matrix (documented separately in this project's engineering
notes, not in the maturity tiers) and should be evaluated on its own terms, not assumed to inherit
its protocol's translated-mode tier.

---

## Part 2 — Reference deployment topologies

Three topologies, each built only from mechanisms confirmed working in the code and existing docs
— nothing aspirational.

### 2.1 Single-node (development / evaluation)

**The one real deployment manifest that exists today**:
[`docker/warp/docker-compose.yml`](../docker/warp/docker-compose.yml) +
[`docker/warp/Dockerfile`](../docker/warp/Dockerfile) — one Postgres container plus one Warp
container, wired together. Every wire protocol's port is exposed (pgwire 15432, mywire 13306,
orawire 11521, mssqlwire 14333, mongowire 27017, gRPC 7070, dynamowire 18000, sqswire 9324, oswire
9200, influxwire 8086, rediswire 16379, boltwire 7687, s3wire 18020, gcswire 4443, awswire 4566,
pubsubwire 8085, bigtablewire 8088, firestorewire 8080, datastorewire 8081, cqlwire 19042,
gremlinwire 8182, kafkawire 19092, cosmoswire 18081, amqpwire 5672, MCP 18010/18443, A2A 18444,
admin 19090/19443, plus TLS twin ports).

**What this topology is for**: local development, protocol evaluation, and the exact environment
this project's own integration tests run against. **What it is not**: an HA or production
topology — no standby Postgres, no multiple Warp instances, no Ignite cluster configuration. Single
point of failure by design, appropriate for its purpose.

**Honest gap**: no Kubernetes or Helm manifest exists anywhere in the repo today. An operator
wanting to run Warp on Kubernetes has to author their own manifest from this Docker Compose file
and the mechanisms below — this is real, unbuilt work, not something this document should imply
already exists.

### 2.2 HA multi-node (config-primary/standby + multi-AZ cache cluster)

Built from real, working mechanisms in [`WARP_GUIDE.md`](WARP_GUIDE.md) §4 and
[`WarpCluster.java`](../Warp/src/main/java/com/sayonora/warp/cluster/WarpCluster.java):

1. **Config-primary/standby Postgres** — `WARP_STANDBY_HOST`/`WARP_STANDBY_PORT` (WARP_GUIDE.md
   §4.1). Covers both the `warp_config`/firewall control plane and, via `BackendTarget`'s
   `failoverOptions`, the synthetic default backend's query path. Failback is automatic
   (`WARP_FAILBACK_CHECK_SECONDS`, default 10s) — no manual intervention once the primary recovers.
   **Real limit, not a bug**: explicitly-named shard backends (`WARP_BACKENDS`) do NOT get this
   automatic failover — pair them at the infrastructure layer yourself (e.g. a PgBouncer/HAProxy
   VIP per shard) if you need it there too.
2. **Multiple stateless Warp instances** behind a load balancer, each independently sufficient
   (the parallel-execution design's own stated principle: "any instance is independently
   sufficient," see the parallel-execution design doc) — every instance connects to the same
   config-primary/standby pair and the same data-plane backend(s).
3. **Multi-AZ cache clustering** (optional, `WARP_CLUSTER_ENABLED=true`) — real Ignite clustering,
   not just the single-node cache-only mode that runs by default. Requires:
   - `WARP_CLUSTER_DISCOVERY` — `static` (default; `WARP_CLUSTER_SEED_NODES` comma-separated
     `host:port` list, falling back to `127.0.0.1:47500`, Ignite's default TCP discovery port) or
     a real cloud-native finder (`s3`, `gcs`, `azure`, each needing its own bucket/container +
     credential env vars) — the cloud finders are verified against real Ignite discovery classes
     but **not exercised against real cloud storage** (no cloud credentials available in this
     project's own test environment) — a disclosed, real gap, not a hidden one.
   - `WARP_AVAILABILITY_ZONE` — operator-supplied per instance (not auto-detected from the cloud
     provider's own metadata service); feeds `ClusterNodeAttributeAffinityBackupFilter` so a cache
     entry's backup copy is never placed in the same AZ as its primary — live-proven with 3 real
     Ignite nodes (`WarpClusterAzBackupPlacementTest`).
   - `WARP_CLUSTER_CACHE_BACKUPS` — backup copy count per cache entry, default **1**.
   - `WARP_TLS_KEYSTORE`/`WARP_TLS_KEYSTORE_PASSWORD` — optional mutual TLS between cache-cluster
     peers (a trust domain independently configured from the client-facing and peer-gRPC TLS —
     see [`SECURITY.md`](SECURITY.md) §1.9).

   **No minimum node count is enforced anywhere in the code** — `WarpCluster.clusterSize()` simply
   reports the live Ignite node count with no quorum gating. A 2-node cluster runs, just without
   the redundancy a 3+-node cluster gives against a single node's failure taking the only backup
   copy with it.

A ready-made sequence diagram for the config-primary/standby failover flow already exists at
[`WARP_GUIDE.md`](WARP_GUIDE.md) §4.1 — reuse it rather than re-drawing it.

### 2.3 Sharded (horizontal scatter-gather across backend shards)

`WARP_SHARD_BACKENDS` (WARP_GUIDE.md §4.2) names a subset of registered backends as a shard group;
`RoutingBackendExecutor` fans a matching query to all of them and merges results
(`ShardJoinExecutor` for cross-shard `JOIN`s). This is the topology for horizontal partitioning of
a single logical dataset across multiple real backend databases, distinct from §2.2's HA
replication of one logical backend.

Real, disclosed constraints:
- Combine with §2.1/§2.2 as needed — sharding is an independent axis from HA; a sharded deployment
  still wants config-primary/standby for the control plane and, optionally, `WARP_CLUSTER_ENABLED`
  for cross-instance cache/statistics sharing.
- Cross-shard `JOIN` planning benefits from statistics (`WARP_STATS_TTL_MS`, default 24h;
  `WARP_STATS_REFRESH_INTERVAL_MINUTES` for a background refresh) and, for genuinely large build
  sides, semi-join pushdown (`WARP_SEMIJOIN_MAX_KEYS`, default 20,000) — both real, already
  documented in WARP_GUIDE.md §4.3, not new claims made here.
- Declarative per-table sharding (`WARP_TABLE_SHARDS`, format `table:strategy:column:params`) and
  named backend sets (`WARP_BACKEND_SETS`, format `name=backend1,backend2`) are the two real
  configuration surfaces for defining which tables shard and how.
- **Multiple real backend engines** (not just Postgres) can participate in a `WARP_BACKENDS`
  topology, including real distributed XA transactions — but with disclosed, engine-specific gaps:
  SQL Server's XA path is real code but not live-verified (the test environment lacks the
  `sqljdbc_xa`/MSDTC support procedures it needs), Postgres 2PC requires the operator to raise
  `max_prepared_transactions` above its default of 0, and Oracle 2PC needs a DBA grant on
  `DBA_2PC_PENDING`/`PENDING_TRANS$`/`DBMS_SYSTEM`. None of these are Warp bugs — they're real
  requirements of the underlying engine's own distributed-transaction support that an operator
  must provision.

---

## Critical files (for whoever extends this document)

- [`Warp/web/src/api/maturity-data.json`](../Warp/web/src/api/maturity-data.json) / [`maturity.ts`](../Warp/web/src/api/maturity.ts) — the maturity tier source of truth (§1.1); update there first, then reflect the change here
- [`Warp/pom.xml`](../Warp/pom.xml) — versioning (§1.3); revisit this document's version-support section once a real release train exists
- [`docker/warp/docker-compose.yml`](../docker/warp/docker-compose.yml) / [`Dockerfile`](../docker/warp/Dockerfile) — the one real deployment manifest (§2.1)
- [`Warp/src/main/java/com/sayonora/warp/cluster/WarpCluster.java`](../Warp/src/main/java/com/sayonora/warp/cluster/WarpCluster.java) — multi-AZ cache clustering mechanics (§2.2)
- [`WARP_GUIDE.md`](WARP_GUIDE.md) §4 — the HA/sharding mechanisms both §2.2 and §2.3 are built from; this document doesn't duplicate its detail, only indexes and contextualizes it as deployment topologies
