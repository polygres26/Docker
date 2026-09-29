# Warp — Architecture Limits, Upgrade, and Rollback

> **This is a technical/internal reference** for operators planning capacity or an upgrade. For
> deployment/HA guidance, see [`WARP_GUIDE.md`](WARP_GUIDE.md); for security posture, see
> [`SECURITY.md`](SECURITY.md); for the positioning context, see
> [`COMPETITIVE_POSITIONING_ROADMAP.md`](COMPETITIVE_POSITIONING_ROADMAP.md) (Phase H).

Every number below is read directly from the code, cited by file and line — not an estimate, and
not a marketing figure. Where a limit is configurable, the default and the env var that changes it
are both given. Where no limit exists in the code, that is stated explicitly rather than implied.

---

## 1. Real, code-derived capacity limits

### 1.1 Developer-tier license caps

[`license/License.java:47-49`](../Warp/src/main/java/com/sayonora/warp/license/License.java):

| Cap | Value | Enforced by |
|---|---|---|
| `DEVELOPER_MAX_CONNECTIONS` | 25 per instance | connection admission |
| `DEVELOPER_MAX_INSTANCES` | 3 (cluster-wide) | `NodeRegistry`'s live-instance count at startup |
| `DEVELOPER_MAX_BACKENDS` | 3 | `BackendRegistry`'s registered-backend count |

Enterprise tier (a verifying license key) removes all three (`Integer.MAX_VALUE`, line 86) — "the
only way to get Enterprise's uncapped limits is a key that verifies successfully right now" (the
class's own comment, line 35). These are licensing ceilings, not engineering ones: the code has no
separate hard architectural maximum beyond them.

### 1.2 Connection pooling

[`core/BackendConnectionPools.java:182`](../Warp/src/main/java/com/sayonora/warp/core/BackendConnectionPools.java) —
`WARP_POOL_MAX_SIZE`, default **30** connections per pool (one pool per distinct backend/user
combination). Size this per backend's own `max_connections` and the number of Warp instances
sharing it — Warp does not coordinate pool sizing across instances itself.

### 1.3 Workload capture buffer

[`server/Main.java:404`](../Warp/src/main/java/com/sayonora/warp/server/Main.java) —
`WARP_CAPTURE_BUFFER_SIZE`, default **20,000** entries, in-memory, per-instance, ring-buffered
(oldest entries silently evicted once full). Not a durable log — see
[`capture/WorkloadCaptureBuffer.java`](../Warp/src/main/java/com/sayonora/warp/capture/WorkloadCaptureBuffer.java)'s
own javadoc. Relevant to both `WorkloadReplayer` and `MigrationRehearsal` (Phase G): pull captured
entries before the buffer wraps if you need everything currently held.

### 1.4 QoS defaults

[`core/QosControlStage.java:52-54`](../Warp/src/main/java/com/sayonora/warp/core/QosControlStage.java) —
`qosRatePerSec` default **200/sec**, `qosBurst` default **rate × 2 = 400** when unset,
`qosMaxWaitMs` default **0** (reject immediately rather than queue) when unset. Per-workload-class
overrides are a `"class:rate:burst[:maxWait]"` spec on top of these defaults.

### 1.5 Cluster heartbeat and staleness windows

[`config/NodeRegistry.java`](../Warp/src/main/java/com/sayonora/warp/config/NodeRegistry.java) —
three distinct numbers, easy to conflate, kept separate here:

| Window | Value | Purpose | Line |
|---|---|---|---|
| Heartbeat period | 10s | How often each instance writes its own `warp_nodes` row | `HEARTBEAT_PERIOD_SECONDS`, line 32 |
| Liveness staleness | 30s | A node is reported "up" vs. "stale" in the admin UI/license check | line 221, 201 |
| Row deletion sweep | 24h | Stale `warp_nodes` rows are deleted, not just marked stale | `STALE_ROW_MAX_AGE_SECONDS`, line 33 |

**No documented minimum/recommended node count for HA exists in the code.** The only node-count
constant is the licensing ceiling above (§1.1), not an HA recommendation — see
[`WARP_GUIDE.md`](WARP_GUIDE.md) §4 for the actual failover mechanism (config-primary/standby),
which doesn't require a specific node count so much as a standby target being configured.

### 1.6 Cache: no Warp-level size ceiling

`WarpConfig.cacheTables`/`cacheTtlMs` control *what* is cached and *for how long*, not a maximum
cache size — no such ceiling exists at the Warp layer. The actual memory bound is the underlying
Ignite region's own configured size (Ignite's own settings, not exposed as a `WarpConfig` field
today). An operator relying on the cache for a large working set should size Ignite's data region
directly rather than looking for a Warp-side cache-size knob, because there isn't one.

### 1.7 Row/column policy authoring — no explicit ceiling

`AccessPolicy` (row filters/column grants, Phase F) has no hardcoded limit on the number of rules
— practical scaling is bounded by how many regex table-pattern checks + SQL rewrites
`AccessControlStage` performs per statement, which is linear in rule count. No measurement of
this cost exists yet; a very large rule set's per-statement overhead is unverified, not merely
undocumented.

---

## 2. Startup and shutdown behavior

[`server/Main.java`](../Warp/src/main/java/com/sayonora/warp/server/Main.java) registers exactly
**one** JVM shutdown hook — for `amqpWireServer::close` (line 987). No other wire server
(pgwire, mywire, mssqlwire, orawire, gRPC, the AWS-family HTTP frontends) registers an explicit
graceful-drain shutdown hook in `Main.java` today.

**What this means for an operator planning a restart/upgrade:** sending SIGTERM to a Warp process
does not wait for in-flight SQL-protocol statements to finish before the JVM exits, except for
AMQP. A rolling restart should assume abrupt connection drops on every other protocol unless the
orchestration layer (Kubernetes, systemd, a load balancer's connection draining) handles the drain
itself — e.g. removing the instance from a load balancer's rotation and waiting a grace period
before sending SIGTERM, rather than relying on Warp's own process to drain in-flight work. This is
a real, disclosed gap, not an assumption that graceful shutdown already happens.

---

## 3. Upgrade and rolling-restart procedure

### 3.1 Config versioning is append-only — this is what makes rollback safe

[`config/ConfigStore.java:47-56`](../Warp/src/main/java/com/sayonora/warp/config/ConfigStore.java) —
`write()` always does `INSERT INTO warp_config (payload) VALUES (?::jsonb) RETURNING version`.
**There is no `UPDATE`, `DELETE`, or dedicated rollback API anywhere in `ConfigStore.java` or
`MetricsServer.java`** — confirmed by grep, not inferred. Every config change, including a
rollback, is a new row.

**How to actually roll back a bad config change today** (there is no one-click rollback button —
this is the real, current procedure):

1. `GET /api/config` before making a change, and save the response — this is your "known good"
   snapshot. (There is no built-in "list every past version" or "read version N" endpoint today;
   `ConfigStore.readLatest()` only ever returns the single newest row. If you didn't save a
   snapshot beforehand, recovering an old version means reading `warp_config` directly via SQL:
   `SELECT payload FROM warp_config WHERE version = N`.)
2. Make your change via `PUT /api/config` as normal.
3. If the new version misbehaves, `PUT /api/config` again with the saved snapshot's fields — this
   appends a new, later version whose payload matches the old one. Every live instance picks it up
   over `LISTEN`/`NOTIFY` within milliseconds, no restart needed (see §3.2).

This is real and safe (nothing is ever destroyed — a bad version stays in `warp_config`'s history
forever, even though there's no UI to browse that history yet), but it is manual, not
one-command. Adding a dedicated `GET /api/config/versions` + `POST /api/config/rollback/{version}`
pair is a natural, small follow-up this document surfaces rather than assumes already exists.

### 3.2 Live config propagation and its real limits

The `warp_config_notify_trigger` fires `pg_notify('warp_config_changed', ...)` on every insert.
Each instance's `listenLoop()` blocks on `getNotifications(5000)` and, on any notification, simply
re-reads `readLatest()` (it does not act on the notification's payload directly) — so a node that
was disconnected during an outage window still converges to the latest config once reconnected,
by virtue of always reading "latest" rather than replaying missed notifications. This is a real
property of the design, not an explicit guarantee documented anywhere in code comments before now.

**Known limit:** if the LISTEN connection itself is down, that instance runs on stale config until
it reconnects (retried every 2 seconds) — there's no push-based fallback (e.g. a periodic poll on
a separate cadence) if `LISTEN`/`NOTIFY` delivery is silently lost at the Postgres level for longer
than the reconnect loop takes to notice.

### 3.3 Rolling upgrade — version skew

**No documented version-skew policy exists in code today** for a mixed-version fleet during a
rolling upgrade — confirmed absent, not merely unwritten, by checking the query-execution peer
gRPC protocol (`WarpPeerGrpcServer`/`WarpPeerServiceImpl`, the mechanism most sensitive to this:
an N-version coordinator dispatching a join partition to an N+1-version peer, or vice versa) for
any "N-1"/"N+1"/skew-tolerance comment or check. There is none.

**Practical guidance until a real policy is written and enforced:** avoid running a rolling
upgrade across a Warp version bump that changes the peer gRPC proto (`src/main/proto/warp.proto`)
or the `warp_config` JSON shape in an incompatible way (a new required field, a renamed field) for
longer than necessary — prefer a full-fleet upgrade in one maintenance window when either changes,
until protobuf's own forward/backward-compatible field-addition rules are relied upon deliberately
rather than accidentally. A purely additive `WarpConfig` field (the pattern every phase of this
project's own work has followed — see `AccessPolicy`'s `accessPolicy` field, Phase F) is safe
across a rolling upgrade: an older node simply never reads the new field.

### 3.4 Recommended upgrade sequence (today's real tooling, not aspirational)

1. Snapshot current config (`GET /api/config`) and record the running version's admin-reported
   version/build info.
2. Upgrade one instance at a time (not a full-fleet simultaneous restart), so the fleet is never
   fully down and a bad build is caught on the first instance before spreading.
3. After each instance restarts, confirm it re-joins `warp_nodes` (`GET` the admin topology view or
   query `warp_nodes` directly) and that its reported config version matches the fleet's latest.
4. Watch `GET /api/cache/stats`, `GET /api/metrics/history`, and the audit stream for anomalies
   before moving to the next instance — given §2's shutdown-hook gap, expect a brief connection
   blip on the instance being restarted, not a graceful drain.
5. If a schema/proto change is involved, do a full-fleet upgrade in one window per §3.3 rather
   than a slow rolling upgrade.

---

## Critical files (for whoever extends this document)

- [`Warp/src/main/java/com/sayonora/warp/license/License.java`](../Warp/src/main/java/com/sayonora/warp/license/License.java) — capacity ceilings
- [`Warp/src/main/java/com/sayonora/warp/core/BackendConnectionPools.java`](../Warp/src/main/java/com/sayonora/warp/core/BackendConnectionPools.java) / [`core/QosControlStage.java`](../Warp/src/main/java/com/sayonora/warp/core/QosControlStage.java) — pooling/QoS defaults
- [`Warp/src/main/java/com/sayonora/warp/config/NodeRegistry.java`](../Warp/src/main/java/com/sayonora/warp/config/NodeRegistry.java) — heartbeat/staleness windows
- [`Warp/src/main/java/com/sayonora/warp/config/ConfigStore.java`](../Warp/src/main/java/com/sayonora/warp/config/ConfigStore.java) — append-only config versioning, the basis of §3.1's rollback procedure
- [`Warp/src/main/java/com/sayonora/warp/server/Main.java`](../Warp/src/main/java/com/sayonora/warp/server/Main.java) — shutdown-hook gap (§2), pipeline/stage assembly
- [`Warp/src/main/java/com/sayonora/warp/grpc/WarpPeerGrpcServer.java`](../Warp/src/main/java/com/sayonora/warp/grpc/WarpPeerGrpcServer.java) / [`src/main/proto/warp.proto`](../Warp/src/main/proto/warp.proto) — the peer protocol §3.3's version-skew guidance is about
