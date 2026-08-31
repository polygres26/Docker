# Nexagres Use Case & Deployment Guide

Covers both modules — **Nexagres DMS** (migration assessment) and **Polywire** (protocol
gateway) — plus the Docker packaging for each. Each section is self-contained; jump to what
you need.

> **On screenshots**: this guide doesn't include UI screenshots. Nexagres DMS's web UI only has
> real content once it's pointed at an actual source database, and generating images here would
> mean either fabricating a fake UI or a blank shell — neither is useful. Once you run `docker
> compose up` (see the Docker section), tell me and I'll drive the running UI in a browser and
> capture real screenshots of your actual data.

---

## 1. What each module is for

| Module | Question it answers | Output |
|---|---|---|
| **Nexagres DMS** | "How hard is it to migrate this Oracle/MySQL/SQL Server database to Postgres, and can you do the easy parts for me?" | Assessment report (schema/feature/workload scoring) + automated migration for low-risk objects |
| **Polywire** | "Can my existing app, written against Oracle/MySQL/SQL Server/MongoDB/DynamoDB wire protocols, talk to Postgres without a rewrite?" | A gateway process that speaks the client's native wire protocol on one side and real Postgres SQL on the other |

They're complementary, not sequential-only: a team can run Polywire indefinitely as a
permanent compatibility shim (e.g. legacy MongoDB driver code that's not worth rewriting) or
as a temporary cutover bridge while Nexagres DMS migrates schema/data behind the scenes.

---

## 2. Architecture

### 2.1 Nexagres DMS

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'primaryColor':'#f0e9f7','primaryTextColor':'#2c1f3d','primaryBorderColor':'#7c5aa6','lineColor':'#7c5aa6','secondaryColor':'#fde9e4','secondaryTextColor':'#2c1f3d','tertiaryColor':'#e2f3ef','tertiaryTextColor':'#2c1f3d','noteBkgColor':'#fde9e4','noteTextColor':'#2c1f3d','noteBorderColor':'#d97a5f','fontSize':'18px','fontFamily':'-apple-system, Helvetica, Arial, sans-serif'}}}%%
flowchart LR
    subgraph Client["Your browser"]
        UI["dms/web\nReact + Vite SPA"]
    end
    subgraph Advisor["Nexagres DMS process"]
        HTTP["DmsHttpServer\n(REST API)"]
        Profiler["Schema / feature\nprofiler"]
        Workload["Workload capture\n& scorer"]
        Migrator["Easy-tier\nmigration engine"]
        Store["Embedded HSQLDB\n(NEXAGRES_DATA_DIR)\nconnections, LLM config, reports"]
    end
    Source[("Source DB\nOracle / MySQL /\nMariaDB / SQL Server")]
    PG[("Target Postgres")]
    LLM["LLM provider\n(optional, for narrative\nreport sections)"]

    UI -->|REST/JSON| HTTP
    HTTP --> Profiler --> Source
    HTTP --> Workload --> Source
    HTTP --> Migrator
    Migrator -->|reads| Source
    Migrator -->|writes| PG
    HTTP --> Store
    HTTP -.optional.-> LLM
```

- **Stateless compute, stateful store**: the process itself holds nothing between requests;
  all durable state (saved connections, LLM provider keys, uploaded/generated reports) lives
  in the embedded HSQLDB file store — one directory (`NEXAGRES_DATA_DIR`), trivially backed up.
- **Read-mostly against the source**: profiling and workload capture are read-only against the
  source database. Only the migration engine writes, and only to the Postgres target — it
  never mutates the source.

### 2.2 Polywire

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'primaryColor':'#f0e9f7','primaryTextColor':'#2c1f3d','primaryBorderColor':'#7c5aa6','lineColor':'#7c5aa6','secondaryColor':'#fde9e4','secondaryTextColor':'#2c1f3d','tertiaryColor':'#e2f3ef','tertiaryTextColor':'#2c1f3d','noteBkgColor':'#fde9e4','noteTextColor':'#2c1f3d','noteBorderColor':'#d97a5f','fontSize':'18px','fontFamily':'-apple-system, Helvetica, Arial, sans-serif'}}}%%
flowchart TB
    subgraph Clients["Existing client drivers — unmodified"]
        OraCli["Oracle TNS/TTC\nclient (JDBC OCI etc.)"]
        MyCli["MySQL client/\nserver protocol"]
        MsCli["SQL Server\nTDS (JDBC)"]
        PgCli["Postgres wire v3\n(psql, JDBC, etc.)"]
        MongoCli["MongoDB wire\nprotocol"]
        DdbCli["DynamoDB\nHTTP/JSON API"]
        GrpcCli["gRPC"]
        McpCli["MCP client\n(AI agent tools)"]
    end

    subgraph Polywire["Polywire process — one shared pipeline"]
        direction TB
        FE["Frontends\norawire:1521/2484 · mywire:3306\nmssqlwire:1433 · pgwire:5432\nmongowire:27017 · dynamowire:8000\ngRPC:7070/17071 · MCP:8010"]
        FW["FirewallStage\n(policy from Postgres)"]
        RT["RouterStage\n(shard/backend selection)"]
        QOS["QosControlStage\n(admission control)"]
        DT["DialectTranslationStage\n(SQL rewrite per protocol)"]
        RU["RollupStage"]
        CA["CacheStage"]
        ST["StatsCollectorStage"]
        FE --> FW --> RT --> QOS --> DT --> RU --> CA --> ST
    end

    Cfg[("polywire_config /\npolywire_firewall_rules\n(control-plane Postgres)")]
    PG1[("Postgres shard 1")]
    PG2[("Postgres shard 2 / N")]

    OraCli & MyCli & MsCli & PgCli & MongoCli & DdbCli & GrpcCli & McpCli --> FE
    ST -->|SQL| PG1
    ST -->|SQL| PG2
    Cfg -.LISTEN/NOTIFY\nhot reload.-> FW
    Cfg -.LISTEN/NOTIFY.-> RT
```

- **One pipeline, many faces**: every frontend (Oracle, MySQL, SQL Server, Mongo, DynamoDB,
  gRPC, MCP, native Postgres) feeds the *same* `StatementPipeline` instance — firewall,
  routing, QoS, translation, caching, and stats are protocol-agnostic.
- **Config lives in Postgres, not just env vars**: `polywire_config` (versioned, insert-only)
  and `polywire_firewall_rules` (mutable, DBA-managed) are real tables in a designated
  "config-primary" Postgres. `LISTEN/NOTIFY` pushes changes to every running Polywire process
  within milliseconds — no restart to change a firewall rule or add a backend.
- **Config-primary vs. data plane**: the `POLYWIRE_*` env vars point at the single Postgres
  that holds control-plane tables. `POLYWIRE_BACKENDS` / `POLYWIRE_SHARD_BACKENDS` are the
  separate, explicitly-registered data-plane shard targets actual queries are routed to — the
  config-primary is never automatically one of the shards.

---

## 3. Security

### 3.1 ACL (IP / CIDR allowlisting) & PPv2 / X-Forwarded-For

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'primaryColor':'#f0e9f7','primaryTextColor':'#2c1f3d','primaryBorderColor':'#7c5aa6','lineColor':'#7c5aa6','secondaryColor':'#fde9e4','secondaryTextColor':'#2c1f3d','tertiaryColor':'#e2f3ef','tertiaryTextColor':'#2c1f3d','noteBkgColor':'#fde9e4','noteTextColor':'#2c1f3d','noteBorderColor':'#d97a5f','fontSize':'18px','fontFamily':'-apple-system, Helvetica, Arial, sans-serif'}}}%%
flowchart LR
    Raw["Raw TCP peer\n(could be a load balancer)"] --> PPv2{"PPv2 header present\nAND peer is a\ntrusted proxy?"}
    PPv2 -->|yes| RealIP["Use the real client IP\ncarried inside PPv2/XFF"]
    PPv2 -->|no| PeerIP["Use the raw TCP\npeer IP as-is"]
    RealIP --> Gate["ClientAcl\nallow/deny by IP or CIDR"]
    PeerIP --> Gate
    Gate -->|allowed| Pipeline["StatementPipeline"]
    Gate -->|denied| Drop["Connection dropped,\nlogged"]
```

**ACL (`ClientAcl`)** — a plain, ordered allow/deny rule list evaluated per inbound
connection, on every frontend (pgwire, mywire, orawire, mssqlwire, mongowire, gRPC, dynamowire,
MCP, admin/metrics HTTP):

- Rule grammar: `allow <ip-or-cidr>` / `deny <ip-or-cidr>`, one per line/`;`-separated entry —
  e.g. `allow 10.0.0.0/8; allow 192.168.1.50; deny 0.0.0.0/0` (allow the private ranges you
  name, deny everything else).
- Sourced from **either** the env var (`POLYWIRE_ACL_RULES`) **or** `polywire_config.aclRules`
  — same dual-source convention as every other setting; the DB-stored version hot-reloads via
  `LISTEN/NOTIFY` with zero restart.
- Default (unset) is fully open — no behavior change until you opt in.
- A rejected connection is dropped immediately, before it reaches the firewall/router stages,
  and logged with the offending IP — visible in Polywire's own logs for audit.

**PPv2 (PROXY protocol v2) / `X-Forwarded-For`** — solves the problem that, once you put a
load balancer or connection pooler in front of Polywire, every connection's raw TCP peer IP
*is the load balancer*, not the real client — so a naive ACL would only ever see one IP.

- `POLYWIRE_ACL_PPV2_ENABLED` (or `polywire_config.aclPpv2Enabled`) turns on parsing of the
  PPv2 header (binary, used by TCP-level proxies — HAProxy, many cloud NLBs) or the
  `X-Forwarded-For` HTTP header (for the HTTP-based frontends) to recover the real client IP.
- **Trust is opt-in per proxy, not global**: `POLYWIRE_ACL_TRUSTED_PROXIES` (or
  `polywire_config.aclTrustedProxies`) is a separate IP/CIDR list — the forwarded-IP header is
  only honored when the *direct* TCP peer is itself in this list. Without this check, any
  client could simply forge its own `X-Forwarded-For` header and impersonate an allowlisted IP
  — this is the exact spoofing vector the trusted-proxy check exists to close.
- Typical setup: `POLYWIRE_ACL_TRUSTED_PROXIES=10.0.0.0/24` (your load balancer's subnet),
  `POLYWIRE_ACL_RULES=allow 203.0.113.0/24` (the actual office/VPN range you want to allow) —
  Polywire then correctly evaluates the ACL against the client's real IP even though every
  packet physically arrives from the load balancer.

### 3.2 Backend-poisoning protection

Because `POLYWIRE_BACKENDS` can be set via the DB-writable `polywire_config` table, anyone
with write access to that table could otherwise register an arbitrary host and have real
client traffic silently routed to it (a config-driven SSRF). `TrustedBackendHosts`
(`POLYWIRE_TRUSTED_BACKEND_HOSTS`) closes this:

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'primaryColor':'#f0e9f7','primaryTextColor':'#2c1f3d','primaryBorderColor':'#7c5aa6','lineColor':'#7c5aa6','secondaryColor':'#fde9e4','secondaryTextColor':'#2c1f3d','tertiaryColor':'#e2f3ef','tertiaryTextColor':'#2c1f3d','noteBkgColor':'#fde9e4','noteTextColor':'#2c1f3d','noteBorderColor':'#d97a5f','fontSize':'18px','fontFamily':'-apple-system, Helvetica, Arial, sans-serif'}}}%%
flowchart TB
    Cfg["polywire_config.backends\n(DB-writable)"] --> Check{"Host in\nPOLYWIRE_TRUSTED_BACKEND_HOSTS?\n(env var only)"}
    Check -->|yes| Register["Registered — routable"]
    Check -->|no| Reject["Refused, logged,\nrest of config unaffected"]
```

Deliberately **env-var only, never itself in `polywire_config`** — if the allowlist lived in
the same DB-writable surface it gates, the protection would be circular. Entries accept IPs,
CIDR blocks, or literal hostnames (docker-compose service names, internal DNS).

### 3.3 SQL Firewall

Runs as its own pipeline stage (`FirewallStage`), first in line — every statement on every
frontend is checked before routing, translation, or execution. Rules live in a real,
DBA-managed Postgres table, **not** an env var or app config file, so a DBA can change policy
with an `UPDATE`/`INSERT` statement and see it apply in milliseconds, no Polywire redeploy.

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'primaryColor':'#f0e9f7','primaryTextColor':'#2c1f3d','primaryBorderColor':'#7c5aa6','lineColor':'#7c5aa6','secondaryColor':'#fde9e4','secondaryTextColor':'#2c1f3d','tertiaryColor':'#e2f3ef','tertiaryTextColor':'#2c1f3d','noteBkgColor':'#fde9e4','noteTextColor':'#2c1f3d','noteBorderColor':'#d97a5f','fontSize':'18px','fontFamily':'-apple-system, Helvetica, Arial, sans-serif'}}}%%
flowchart LR
    Stmt["Incoming statement\n(any protocol)"] --> Match["Match rules in\npriority order"]
    Match -->|first ALLOW match| Pass["Forwarded to\nRouterStage"]
    Match -->|first DENY match| Block["Rejected —\nprotocol-native error\nreturned to client"]
    Match -->|no rule matches| Default["Fail open or closed\n(deployment choice)"]
    DBA["DBA: INSERT/UPDATE\npolywire_firewall_rules"] -.LISTEN/NOTIFY,\nno restart.-> Match
```

**Table schema** (`polywire_firewall_rules`, auto-created, own `LISTEN/NOTIFY` trigger):

| column | meaning |
|---|---|
| `id` | primary key |
| `priority` | lower evaluates first; first matching rule wins |
| `action` | `ALLOW` or `DENY` |
| `statement_type` | `SELECT` / `INSERT` / `UPDATE` / `DELETE` / `DDL` / `*` (any) |
| `table_pattern` | glob against the target table, schema-qualification optional (`orders` matches both `orders` and `public.orders`) |
| `sql_pattern` | optional raw regex escape hatch — matched against the full SQL text when a table-level rule isn't expressive enough |
| `enabled` | boolean — disable a rule without deleting it |
| `description` | free text, shows up in denial logs |

**Example policy:**

| priority | action | statement_type | table_pattern | description |
|---|---|---|---|---|
| 10 | DENY | DELETE | `*orders*` | block bulk order deletes from any app |
| 20 | DENY | `*` | `*pii_*` | block all access (read or write) to PII-prefixed tables |
| 30 | DENY | `*` | `*` *(sql_pattern: `(?i)DROP\s+TABLE`)* | block DDL drops regardless of table name |
| 100 | ALLOW | `*` | `*` | default allow — everything not matched above |

**Config surface** — configured, like every other feature, from **either**:

- a plain SQL `INSERT INTO polywire_firewall_rules (...)` (the intended day-to-day DBA path —
  no application deploy involved at all), or
- the equivalent Postgres stored procedure Polywire ships (wraps the same insert/update with
  validation), for teams that prefer calling a procedure over hand-writing DML.

Changes are pushed to every running Polywire process instantly via the table's `NOTIFY`
trigger — matches the same hot-reload mechanism used by `polywire_config`, `ClientAcl`, and
`TrustedBackendHosts`.

### 3.4 Authentication

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'primaryColor':'#f0e9f7','primaryTextColor':'#2c1f3d','primaryBorderColor':'#7c5aa6','lineColor':'#7c5aa6','secondaryColor':'#fde9e4','secondaryTextColor':'#2c1f3d','tertiaryColor':'#e2f3ef','tertiaryTextColor':'#2c1f3d','noteBkgColor':'#fde9e4','noteTextColor':'#2c1f3d','noteBorderColor':'#d97a5f','fontSize':'18px','fontFamily':'-apple-system, Helvetica, Arial, sans-serif'}}}%%
flowchart LR
    subgraph HTTP["HTTP/gRPC/MCP frontends"]
        Bearer["Bearer JWT"] --> OIDC["AccessContextResolver\nOAuth2/OIDC\n(Okta, EntraID, generic OIDC)\nJWKS discovery + caching"]
    end
    subgraph TCP["TCP protocol frontends\n(Oracle/MySQL/SQL Server/Postgres wire)"]
        PW["Password / driver-native auth"]
    end
    subgraph Dynamo["dynamowire (HTTP)"]
        SigV4["AWS SigV4 request\nsignature verification"]
    end
    OIDC --> Ctx["AccessContext\n(sub, roles)"]
    SigV4 --> Ctx
    Ctx --> Pipeline["Firewall / routing decisions\ncan use roles"]
```

- OAuth2/OIDC bearer-token validation for every HTTP-based frontend (gRPC, MCP, admin API),
  with configurable claim mapping (`sub` → user id, custom claim → roles).
- AWS Signature v4 verification for `dynamowire`, opt-in via `POLYWIRE_AWS_IAM_CREDENTIALS`.
- TCP wire-protocol frontends (Oracle/MySQL/SQL Server/Postgres) authenticate with the
  client driver's own native password exchange, passed through to the real Postgres backend.

### 3.5 TLS

- `orawire` has a dedicated TLS listener (TCPS, port 2484) alongside plaintext TNS (1521).
- gRPC has a dedicated TLS listener (17071) alongside plaintext (7070).
- All built from one shared keystore — one cert to rotate, not one per frontend.

---

## 4. High Availability

### 4.1 Config-primary failover

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'primaryColor':'#f0e9f7','primaryTextColor':'#2c1f3d','primaryBorderColor':'#7c5aa6','lineColor':'#7c5aa6','secondaryColor':'#fde9e4','secondaryTextColor':'#2c1f3d','tertiaryColor':'#e2f3ef','tertiaryTextColor':'#2c1f3d','noteBkgColor':'#fde9e4','noteTextColor':'#2c1f3d','noteBorderColor':'#d97a5f','fontSize':'18px','fontFamily':'-apple-system, Helvetica, Arial, sans-serif'}}}%%
sequenceDiagram
    participant PW as Polywire process
    participant P as Primary Postgres
    participant S as Standby Postgres

    PW->>P: query (normal operation)
    P-->>PW: result
    Note over P: Primary goes down
    PW->>P: query attempt
    P--xPW: connection refused
    PW->>S: failover — retry on standby
    S-->>PW: result
    Note over PW: onStandby=true, background probe\nchecks primary every POLYWIRE_FAILBACK_CHECK_SECONDS
    loop every N seconds
        PW->>P: probe
    end
    Note over P: Primary recovers
    PW->>P: probe succeeds
    Note over PW: onStandby=false — failback,\nnew queries go to primary again
```

- Configured via `POLYWIRE_STANDBY_HOST`/`POLYWIRE_STANDBY_PORT`. Applies to both the
  control-plane connection (`polywire_config`/firewall rules) and, via `BackendTarget`'s
  `failoverOptions`, the actual query-execution path for the synthetic default backend.
- Explicitly-named shard backends (`POLYWIRE_BACKENDS`) do **not** get automatic failover — a
  named shard isn't presumed to be a replica pair of another named shard; pair them yourself
  at the infrastructure layer (e.g. a PgBouncer/HAProxy VIP per shard) if needed.
- Failback is automatic, probed in the background (`POLYWIRE_FAILBACK_CHECK_SECONDS`, default
  10s) — no manual intervention once the primary recovers.

### 4.2 Sharding / scatter-gather

`POLYWIRE_SHARD_BACKENDS` names a subset of the registered backends as a shard group;
`RoutingBackendExecutor` fans a matching query out to all of them and merges results — useful
for read-side aggregate queries across horizontally-partitioned Postgres backends.

### 4.3 What HA does *not* cover yet

Cross-AZ distributed cache (Ignite) placement is verified locally (2-node) but not yet
cloud-native — AZ-aware backup placement, cloud discovery (not static IP lists), and TLS
between cache nodes are open follow-up work before a real multi-AZ production deploy. Treat
single-AZ or single-host as the currently-proven HA envelope; see project memory for the
detailed gap list.

---

## 5. Deploying on a laptop (fastest path)

Both modules have a Docker Compose file that needs nothing but Docker Desktop.

**Polywire** (gateway only, bring your own Postgres or point at one running elsewhere):

```bash
docker compose -f docker/polywire/docker-compose.yml up --build
```

**Nexagres DMS** (one container — API + SPA served together by embedded Jetty):

```bash
docker compose -f docker/dms/docker-compose.yml up --build
```

Then open `http://localhost:8080`. Both commands must run from the **repo root** — the build
context is the repo root even though each Dockerfile lives under `docker/<module>/` (see each
module's own `docker/*/README.md` for why).

For iterative Java development without Docker:

```bash
cd dms && mvn package -DskipTests && ./scripts/run.sh   # if present
cd wire && mvn package -DskipTests && scripts/run.sh
```

No cloud account, no Kubernetes, no external Postgres required to try either module — a local
`sb-postgres`-style container (see either compose file) is enough for a full smoke test.

---

## 6. Cloud deployment

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'primaryColor':'#f0e9f7','primaryTextColor':'#2c1f3d','primaryBorderColor':'#7c5aa6','lineColor':'#7c5aa6','secondaryColor':'#fde9e4','secondaryTextColor':'#2c1f3d','tertiaryColor':'#e2f3ef','tertiaryTextColor':'#2c1f3d','noteBkgColor':'#fde9e4','noteTextColor':'#2c1f3d','noteBorderColor':'#d97a5f','fontSize':'18px','fontFamily':'-apple-system, Helvetica, Arial, sans-serif'}}}%%
flowchart TB
    subgraph LB["Load balancer / ingress"]
        L1["TCP LB\n(orawire/mywire/mssqlwire/pgwire/mongowire\nports — protocol-aware health checks)"]
        L2["HTTP(S) LB\n(dynamowire / gRPC / MCP / admin)"]
    end
    subgraph Cluster["Polywire replica set\n(stateless — safe to scale horizontally)"]
        N1["Polywire pod 1"]
        N2["Polywire pod 2"]
        N3["Polywire pod N"]
    end
    subgraph DataPlane["Data plane"]
        Primary[("Config-primary Postgres\n+ standby")]
        Shard1[("Shard 1")]
        Shard2[("Shard N")]
    end
    IdP["OIDC provider\n(Okta / EntraID / etc.)"]
    IAM["Cloud IAM\n(AWS SigV4)"]

    Clients["Client applications"] --> L1 & L2 --> N1 & N2 & N3
    N1 & N2 & N3 --> Primary
    N1 & N2 & N3 --> Shard1
    N1 & N2 & N3 --> Shard2
    N1 & N2 & N3 -.token validation.-> IdP
    N1 & N2 & N3 -.sig verify.-> IAM
```

- **Polywire itself is stateless** — every pod reads its live config from the same
  config-primary Postgres via `LISTEN/NOTIFY`, so horizontal scaling is just "add more pods
  pointed at the same `POLYWIRE_*`." No sticky sessions needed at the LB beyond normal TCP
  connection affinity for the life of one client session.
- **`POLYWIRE_TRUSTED_BACKEND_HOSTS`** must be set at deploy time (env var / secret /
  Kubernetes `NetworkPolicy`-equivalent) — this is infrastructure config, not something to put
  in application config management that developers can edit.
- **Container images**: build with the Dockerfiles under `docker/polywire/` and
  `docker/dms/`, push to a registry (e.g. `ghcr.io` — see the repo root
  `.env.example` for the token fields needed), deploy via your platform's normal rolling
  update mechanism (ECS service, GKE/EKS Deployment, etc.).
- **Secrets**: `POLYWIRE_PASSWORD`, `POLYWIRE_AWS_IAM_CREDENTIALS`, OAuth client secrets, and
  the registry token belong in your cloud's secret manager (Secrets Manager, Secret Manager,
  Key Vault) or a Kubernetes `Secret`, injected as env vars — never baked into the image.
- **What's still cloud-*agnostic* by design**: Polywire has no hard dependency on any one
  cloud's networking primitives — it only needs TCP reachability to its Postgres backends and,
  optionally, an OIDC issuer and/or AWS IAM for auth. Cross-AZ cache placement is the one piece
  still marked open (§4.3) before treating a specific cloud's multi-AZ topology as fully proven.

---

## 7. Docker packaging reference

| | Polywire | Nexagres DMS |
|---|---|---|
| Images | 1 (`docker/polywire/Dockerfile`) | 1 (`docker/dms/Dockerfile`) — API + SPA in one container |
| Base (build) | `maven:3.9-eclipse-temurin-21` | same, plus a `node:22-alpine` stage to build the SPA |
| Base (runtime) | `eclipse-temurin:21-jre-jammy` | same |
| Published ports | 15432, 13306, 11521, 2484, 14333, 27017, 7070, 17071, 18000, 18010, 19090 | 8090 only |
| Persistent state | none in the image — all state is the external control-plane Postgres | named volume `polyadvisor-data` → `NEXAGRES_DATA_DIR` (embedded HSQLDB) |
| How the SPA is served | n/a | `DmsHttpServer` runs on embedded Jetty (already a dependency) and serves the built `dms/web` SPA itself via `SpaResourceHandler` (`NEXAGRES_DMS_WEB_DIR=/app/web`) — no nginx or second container. Unset that env var to run API-only; `Dockerfile.frontend`/`nginx.conf` are still available in `docker/dms/` for teams that want a separately-scaled static tier instead |
| `.dockerignore` | repo-root only — both compose files set `context: ../..`, and classic Docker only honors a root-level `.dockerignore` | same |

Build standalone (no compose):

```bash
# from repo root
docker build -f docker/polywire/Dockerfile -t polywire:latest .
docker build -f docker/dms/Dockerfile -t nexagres-dms:latest .
```

---

## 8. Complete feature reference

Every end-user-facing feature in both modules, in one place, with its config knob.

### 8.1 Nexagres DMS features

| Feature | What it does | Where |
|---|---|---|
| Source connection management | Save/reuse connections to Oracle, MySQL, MariaDB, SQL Server | web UI + `ConnectionStore` |
| Schema profiler | Inventories tables, columns, types, constraints, indexes on the source | `DmsHttpServer` |
| Feature-usage detection | Flags source-specific features with no direct Postgres equivalent (proprietary types, stored-proc dialects, partitioning schemes, etc.) | profiler |
| Workload capture | Samples real query traffic against the source to weight the assessment by actual usage, not just schema shape | `Workload capture` |
| Migration difficulty scoring | Produces an object-by-object and overall difficulty score | scorer |
| Easy-tier automated migration | Executes the migration itself for objects scored low-risk, writing only to the Postgres target | migration engine |
| LLM-assisted narrative reports | Optional natural-language summary/explanation sections in the report | pluggable LLM provider config |
| Report storage & history | Past assessment/migration reports kept for comparison over time | `ReportStore` (embedded HSQLDB) |
| React/TS web UI | Full workflow — connect, profile, review score, trigger migration, read reports | `dms/web` |

### 8.2 Polywire — protocol frontends

| Frontend | Protocol | Default port | Notes |
|---|---|---|---|
| pgwire | Postgres wire protocol v3 | 15432 | native passthrough, no translation needed |
| mywire | MySQL client/server protocol | 13306 | SQL dialect translated to Postgres |
| orawire | Oracle TNS/TTC | 11521 (plaintext), 2484 (TCPS/TLS) | SQL dialect translated; both plaintext and TLS listeners run together |
| mssqlwire | SQL Server TDS | 14333 | T-SQL dialect translated |
| mongowire | MongoDB wire protocol | 27017 | document ops mapped to SQL |
| dynamowire | DynamoDB HTTP/JSON API | 18000 | AWS SigV4-verifiable, item ops mapped to SQL |
| gRPC | gRPC | 7070 (plaintext), 17071 (TLS) | both listeners run together, one shared keystore |
| MCP | JSON-RPC 2.0 over Streamable HTTP | 18010 | see §8.4 |
| Admin / metrics | HTTP | 19090 | health, metrics, read-only config introspection (never returns passwords) |

### 8.3 Polywire — statement pipeline stages

Every frontend above feeds the same shared pipeline, in this order:

| Stage | Feature |
|---|---|
| `FirewallStage` | SQL Firewall — see §3.3 |
| `RouterStage` | Backend/shard selection per statement |
| `QosControlStage` | Admission control — caps in-flight work per backend to protect it from overload |
| `DialectTranslationStage` | Rewrites source-dialect SQL (Oracle/MySQL/T-SQL) into Postgres SQL |
| `RollupStage` | Aggregates/merges results for scatter-gather (shard-group) queries |
| `CacheStage` | Translation-result and read caching (`polywire_translation_cache`) |
| `StatsCollectorStage` | Per-statement metrics feeding the admin/metrics HTTP endpoint |

### 8.4 Polywire — security features

| Feature | Config knob | Detail |
|---|---|---|
| ACL (IP/CIDR allow-deny) | `POLYWIRE_ACL_RULES` / `polywire_config.aclRules` | §3.1 |
| PPv2 / X-Forwarded-For trusted-proxy resolution | `POLYWIRE_ACL_PPV2_ENABLED`, `POLYWIRE_ACL_TRUSTED_PROXIES` | §3.1 |
| SQL Firewall | `polywire_firewall_rules` table | §3.3 |
| Backend-poisoning allowlist | `POLYWIRE_TRUSTED_BACKEND_HOSTS` (env var only) | §3.2 |
| OAuth2 / OIDC bearer auth | `POLYWIRE_OAUTH_ISSUER`, `POLYWIRE_OAUTH_AUDIENCE`, claim-mapping vars | §3.4 — Okta, EntraID, any standard OIDC issuer |
| AWS SigV4 request verification | `POLYWIRE_AWS_IAM_CREDENTIALS` | §3.4 — for `dynamowire` |
| Native driver password auth | n/a, always on | TCP frontends (Oracle/MySQL/SQL Server/Postgres) |
| TLS listeners | shared keystore config | §3.5 — orawire TCPS, gRPC TLS |

### 8.5 Polywire — configuration & operations features

| Feature | Detail |
|---|---|
| Dual config source | Every setting readable from an env var **or** `polywire_config` — pick per-deployment |
| Hot reload | `LISTEN/NOTIFY` on `polywire_config_changed` and the firewall table's own trigger — no restart for any config change |
| Config-primary designation | `POLYWIRE_*` names the one Postgres holding control-plane tables, separate from data-plane shard backends (§2.2) |
| Config-primary HA failover | `POLYWIRE_STANDBY_HOST`/`_PORT`, automatic failover + failback probe (§4.1) |
| Postgres stored-procedure config API | Wraps `polywire_config`/firewall inserts with validation, for teams that prefer calling a procedure over hand-writing DML |
| Backend registry | `POLYWIRE_BACKENDS` — named additional Postgres targets beyond the implicit default |
| Sharding / scatter-gather | `POLYWIRE_SHARD_BACKENDS` — fan a query to a named group, merge via `RollupStage` (§4.2) |
| Translation cache | `polywire_translation_cache` — avoids re-translating identical statements |
| Failed-statement log | `polywire_failed_statements` — durable record of statements the pipeline rejected or errored on, for audit/debugging |

### 8.6 Polywire — MCP (AI agent tool access)

| Feature | Detail |
|---|---|
| Generic SQL tools | `execute_sql`, `list_tables`, `describe_table` exposed as MCP tools out of the box |
| Registered stored-procedure tools | `POLYWIRE_MCP_TOOLS` names specific Postgres functions/procedures to expose as individually-named MCP tools — only what's explicitly registered is callable, not arbitrary SQL |
| Automatic input-schema generation | Introspects each registered function's real Postgres parameter types and builds the matching JSON Schema (`PgFunctionIntrospector`, `PgTypeToJsonSchema`) |
| OUT-parameter handling | OUT parameters are correctly excluded from the callable input schema |
| JSON Streamable HTTP transport | Standard MCP transport, so any MCP-compatible AI client can connect without custom glue |

## 9. Use case matrix

| Scenario | Module(s) | Notes |
|---|---|---|
| Assess Oracle → Postgres migration difficulty | Nexagres DMS | Read-only profiling, no source-side risk |
| Auto-migrate low-risk schema objects | Nexagres DMS | Writes only to the Postgres target |
| Keep a legacy Oracle-driver app running against Postgres, permanently | Polywire (orawire) | No app rewrite; TNS/TTC + TCPS supported |
| Cut over a MySQL-protocol app during a migration window | Polywire (mywire) | Temporary bridge, decommission after cutover |
| Let an AI agent call vetted stored procedures as tools | Polywire (MCP frontend) | Only `POLYWIRE_MCP_TOOLS`-registered functions are exposed, not arbitrary SQL |
| Enforce "no bulk deletes from `orders`" org-wide, DBA-editable, no redeploy | Polywire (SQL firewall) | Rule lives in Postgres, hot-reloaded |
| Multi-region app needing Okta-based access control on a DynamoDB-protocol endpoint | Polywire (dynamowire + OAuth) | SigV4 or OIDC bearer, per deployment choice |
| Horizontally shard reads across N Postgres backends | Polywire (shard group + RouterStage) | Scatter-gather via `POLYWIRE_SHARD_BACKENDS` |
| Try either module locally before committing to infrastructure | Docker Compose | See §5 |
