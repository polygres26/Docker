# PolyWire

A mid-tier, Postgres-only database gateway. It speaks Oracle TNS/TTC, MySQL client/server
protocol, SQL Server TDS, Postgres wire protocol v3, MongoDB wire protocol, DynamoDB's HTTP/JSON
API, gRPC, and MCP to clients — translating and routing every one of them to real Postgres
backend(s). It's wire-protocol compatibility for a pre- or post-migration cutover, not a
schema/data migration tool itself.

Point an existing app's connection string at PolyWire instead of its original database, and it
translates and routes to real Postgres. Run it indefinitely as a permanent compatibility shim
(e.g. legacy MongoDB driver code not worth rewriting), or as a temporary cutover bridge while a
migration tool moves schema/data behind the scenes.

## Quick start

```bash
mvn package -DskipTests
scripts/run.sh
```

No `POLYWIRE_PG_*` env vars set defaults to `localhost:5432`; see [Configuration](#configuration)
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
| `POLYWIRE_PG_HOST` / `_PORT` / `_DATABASE` / `_USER` / `_PASSWORD` | The config-primary Postgres — holds `polywire_config`, `polywire_firewall_rules`, and control-plane state |
| `POLYWIRE_AUTH_USER` / `_PASSWORD` | Default credential for wire-protocol frontend auth |
| `POLYWIRE_PG_STANDBY_HOST` / `_PORT` | Optional standby for automatic config-primary failover |
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

Config-primary failover (`POLYWIRE_PG_STANDBY_HOST`) with automatic failback probing. Sharding via
`POLYWIRE_SHARD_BACKENDS` with scatter-gather query fan-out. See the full deployment guide for
what's still open before a real multi-AZ production deploy.

## Building

```bash
mvn package -DskipTests
```

Produces `target/polygres-wire.jar` (shaded, runnable with `java -jar`). Requires the
`--add-opens` flags in `scripts/run.sh` for the embedded Ignite distributed cache.

## License

MIT — see [LICENSE](LICENSE).
