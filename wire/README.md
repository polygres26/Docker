# PolyWire

A mid-tier, Postgres-only database gateway. It speaks Oracle TNS/TTC, MySQL client/server
protocol, SQL Server TDS, Postgres wire protocol v3, MongoDB wire protocol, DynamoDB's HTTP/JSON
API, Amazon SQS's HTTP/JSON API, gRPC, and MCP to clients — translating and routing every one of
them to real Postgres backend(s). It's wire-protocol compatibility for a pre- or post-migration
cutover, not a schema/data migration tool itself.

New here? Start with [`../docs/USER_GUIDE.md`](../docs/USER_GUIDE.md) — what PolyWire does, why
you'd use it, and how to point your app at it. [`../docs/POLYWIRE_GUIDE.md`](../docs/POLYWIRE_GUIDE.md)
and [`../docs/PERFORMANCE.md`](../docs/PERFORMANCE.md) are technical/internal references (pipeline
internals, security, HA, the latency investigation) for operators and contributors.

Point an existing app's connection string at PolyWire instead of its original database, and it
translates and routes to real Postgres. Run it indefinitely as a permanent compatibility shim
(e.g. legacy MongoDB driver code not worth rewriting), or as a temporary cutover bridge while a
migration tool moves schema/data behind the scenes.

## Architecture

Every protocol frontend feeds the same 9-stage pipeline: frontends → firewall → router → QoS
admission control → dialect translation → rollup → cache → stats collection → backend
execution. Config lives in Postgres itself (`polywire_config`, `polywire_firewall_rules`),
hot-reloaded to every running process via `LISTEN/NOTIFY` — no restart to change a firewall
rule, routing topology, or SQL rewrite rule.

![PolyWire architecture: nine client protocols (OraWire, MySQL, SQL Server, Postgres wire, MongoDB, DynamoDB, Amazon SQS, gRPC, MCP) feed a shared eight-stage pipeline -- frontends, firewall, router, QoS, dialect translation, rollup, cache, stats collector -- each paired with the customer outcome it drives, backed by a Postgres control plane over LISTEN/NOTIFY and executing against horizontally-sharded Postgres backends](docs/architecture.png)

The full architecture, security, HA, and deployment guide with more diagrams lives at
[`polywire/index.html`](https://polygres26.github.io/polywire/) (or open it directly:
[polywire/index.html](https://github.com/polygres26/polygres26.github.io/blob/main/polywire/index.html)).

## Quick start

```bash
mvn package -DskipTests
scripts/run.sh
```

No `POLYWIRE_*` env vars set defaults to `localhost:5432`; see [Configuration](#configuration)
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
| oswire | OpenSearch HTTP/JSON API (`_search`/documents/`_bulk`) | 9200 |
| gRPC | gRPC | 7070 (plaintext), 17071 (TLS) |
| MCP | JSON-RPC 2.0 over Streamable HTTP | 18010 |
| Admin / metrics | HTTP | 19090 |

Every frontend feeds the same shared pipeline: `FirewallStage → RouterStage → QosControlStage →
DialectTranslationStage → RollupStage → CacheStage → StatsCollectorStage`.

## Configuration

Every setting is readable from **either** an env var or the `polywire_config` Postgres table
(hot-reloaded via `LISTEN/NOTIFY`, no restart required). Key env vars:

| Variable | Purpose |
|---|---|
| `POLYWIRE_HOST` / `_PORT` / `_DATABASE` / `_USER` / `_PASSWORD` | The config-primary Postgres — holds `polywire_config`, `polywire_firewall_rules`, and control-plane state |
| `POLYWIRE_AUTH_USER` / `_PASSWORD` | Default credential for wire-protocol frontend auth |
| `POLYWIRE_STANDBY_HOST` / `_PORT` | Optional standby for automatic config-primary failover |
| `POLYWIRE_BACKENDS` / `POLYWIRE_SHARD_BACKENDS` | Additional named Postgres data-plane targets and shard groups |
| `POLYWIRE_TRUSTED_BACKEND_HOSTS` | Allowlist gating what hosts `POLYWIRE_BACKENDS` can register — env-var only, never DB-writable |
| `POLYWIRE_ACL_RULES` | IP/CIDR allow-deny rules |
| `POLYWIRE_ACL_PPV2_ENABLED` / `POLYWIRE_ACL_TRUSTED_PROXIES` | PROXY protocol v2 / X-Forwarded-For support behind a load balancer |
| `POLYWIRE_OAUTH_ISSUER` / `_AUDIENCE` | OAuth2/OIDC bearer-token auth (Okta, EntraID, any standard issuer) for HTTP frontends |
| `POLYWIRE_AWS_IAM_CREDENTIALS` | AWS SigV4 request verification for dynamowire |
| `POLYWIRE_MCP_TOOLS` | Postgres functions/procedures to expose as individually-named MCP tools |
| `POLYWIRE_TLS_KEYSTORE` | Shared keystore for orawire TCPS / gRPC TLS |

## Security

- **SQL Firewall** — DBA-managed `polywire_firewall_rules` table (priority, action, statement
  type, table-pattern glob or raw regex), matched before every statement executes.
- **ACL + PPv2/XFF** — IP/CIDR allow-deny, trusted-proxy-aware so a real client IP survives
  behind a load balancer without allowing header spoofing.
- **Backend-poisoning allowlist** — `POLYWIRE_TRUSTED_BACKEND_HOSTS` closes a config-driven SSRF
  vector where DB write access to `polywire_config` could otherwise register an arbitrary
  routing target.
- **OAuth2/OIDC + AWS SigV4** — for the HTTP-based frontends (gRPC, MCP, dynamowire, admin API).
- **TLS** — dedicated listeners for orawire (TCPS) and gRPC, one shared keystore.

## High availability

Config-primary failover (`POLYWIRE_STANDBY_HOST`) with automatic failback probing. Sharding via
`POLYWIRE_SHARD_BACKENDS` with scatter-gather query fan-out.

![PolyWire multi-AZ cloud deployment: client applications behind a hyperscaler Network Load Balancer, fanning out to stateless PolyWire instances in three availability zones, each zone holding a primary or backup copy of cached entries with backup-copy replication across zones, a config-primary Postgres with a standby for automatic failover pushing LISTEN/NOTIFY config to every zone, and a data-plane Postgres shard/replica per zone](docs/deployment.png)

Every piece of this diagram is real today, including the cross-zone cache backup replication:
cloud-native cluster discovery (`POLYWIRE_CLUSTER_DISCOVERY=static|s3|gcs|azure`), AZ-aware
backup placement (a cache entry's backup never lands in the same AZ as its primary -- live-proven
by `PolyWireClusterAzBackupPlacementTest`, three real Ignite nodes, not a simulation), a
configurable backup count (`POLYWIRE_CLUSTER_CACHE_BACKUPS`, default 1), and TLS between cache
nodes (`POLYWIRE_TLS_KEYSTORE`) are all implemented and tested. What's genuinely still open: the
S3/GCS/Azure discovery finders are verified against the real Ignite classes but not yet exercised
against real cloud storage (no cloud credentials available to test with), and AZ is
operator-supplied (`POLYWIRE_AVAILABILITY_ZONE`) rather than auto-detected from cloud
instance-metadata. See the full deployment guide for the complete verification detail.

## Building

```bash
mvn package -DskipTests
```

Produces `target/polygres-wire.jar` (shaded, runnable with `java -jar`). Requires the
`--add-opens` flags in `scripts/run.sh` for the embedded Ignite distributed cache.

## License

MIT — see [LICENSE](LICENSE).
