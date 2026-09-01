# Warp — Docker

Multi-stage build: a `node:22-alpine` stage builds the admin SPA (`wire/web`), a
`maven:3.9-eclipse-temurin-21` stage builds the shaded jar (`target/nexagres-wire.jar`), then an
`eclipse-temurin:21-jre-jammy` runtime stage runs both — nothing beyond a JRE is needed at
runtime, everything's already bundled into the shaded jar plus the built SPA's static files.

## Quick start

From the **repo root** (the build context has to be the repo root, not this directory — `wire/`
is a standalone Maven module with no parent pom, but the Dockerfile still needs `wire/` as a
subdirectory it can `COPY` from):

```bash
docker compose -f docker/warp/docker-compose.yml up --build
```

This starts a real Postgres backend plus Warp itself, pointed at each other. Once healthy:

```bash
psql -h localhost -p 15432 -U postgres -d postgres
```

Every other frontend (mywire 13306, orawire 11521, mssqlwire 14333, mongowire 27017, gRPC 7070,
dynamowire 18000, MCP 18010) is published the same way — see `docker-compose.yml`'s port list.
The admin app (Metrics, Topology, SQL Firewall, ACL, OAuth, LLM configuration, and more) is on
19090 — open it directly in a browser; `/metrics` and `/config` are separate JSON endpoints on
the same port.

## Connecting to a real Postgres

The compose file above stands up its own Postgres for a self-contained demo. To point Warp at
an existing Postgres instead — on-prem, Supabase, Amazon RDS, Google Cloud SQL, or Azure Database
for PostgreSQL — see **[../CONNECTING.md](../CONNECTING.md)** for a copy-pasteable `docker run`/
`docker-compose.yml` per target, plus the real, provider-specific gotchas each one hits (Supabase's
IPv6-only direct connection and pooler username shape, Azure's mandatory TLS, RDS/Cloud SQL's
network allowlisting, and more).

## How the admin app gets served (no nginx)

The image build has two stages that feed the runtime stage: a `node:22-alpine` stage builds
`wire/web` to static files, and a `maven:3.9-eclipse-temurin-21` stage builds the backend jar.
Both outputs land in the final `eclipse-temurin:21-jre-jammy` image; `WARP_ADMIN_WEB_DIR=/app/web`
tells `MetricsServer` where to find the built SPA at startup, serving it via `SpaResourceHandler`
on the same port as the admin JSON API (19090). Unset that env var (or point it at a directory
that doesn't exist) and the same image runs API-only.

## Building the image standalone

```bash
docker build -f docker/warp/Dockerfile -t warp:latest .
```

(Still run from the repo root — same reason as above.)

## Configuration

Every Warp setting is an env var — see `Main.java`'s and `ServerOptions.java`'s own javadoc
for the complete, authoritative list (QoS, cache, ACL, OAuth, AWS IAM SigV4, TLS, cluster/
sharding, ...). The compose file above only sets the minimum needed to connect to a real backend
(`WARP_*`) plus the shared dev auth credential (`WARP_AUTH_*`) — everything else uses its
documented default.

**Config that lives in Postgres itself, not env vars**: `warp_config` (QoS/backends/router/
cache/ACL/OAuth/AWS IAM, hot-reloadable, no restart) and `warp_firewall_rules` (a DBA-managed
table — plain `INSERT`/`UPDATE`/`DELETE`, same mental model as `GRANT`/`REVOKE`) both live on
whichever Postgres `WARP_HOST` points at, auto-created on first boot. See `ConfigStore`'s and
`FirewallRuleStore`'s class javadoc for the full schema and examples.

## Primary/standby failover

Set `WARP_STANDBY_HOST`/`WARP_STANDBY_PORT` (a real physical-replica pair of the primary
above, same credentials) to enable automatic failover — covers both real query traffic and every
control-plane store (`warp_config`, `warp_firewall_rules`, translation cache, failed-
statement log). See `PgConnections`' class javadoc for exactly what this does and doesn't cover.

## Why the `--add-opens` flags in the `ENTRYPOINT`

Embedded Apache Ignite (used by `CacheStage`, only actually started when `WARP_CACHE_TABLES`
is set) reflectively opens several `java.base` packages the JVM's module system blocks from Java
17 onward. Harmless when caching is off (the default) — see `scripts/run.sh`'s own comment for
how this was found live (an `InaccessibleObjectException` during real testing on JDK 25, only once
caching was enabled).
