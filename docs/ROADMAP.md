# Warp — Roadmap

This page is for anyone evaluating Warp: what it does today, what's actively being hardened, and
what's next. Everything under "Available today" is real, shipped, and independently checkable —
each item links to the actual evidence (a scorecard, a benchmark, a document) rather than asking
you to take our word for it. Where something isn't built yet, this page says so plainly.

> Looking for how to use Warp rather than what's on the roadmap? Start with
> [`USER_GUIDE.md`](USER_GUIDE.md). For deployment/operations detail, see
> [`WARP_GUIDE.md`](WARP_GUIDE.md), [`SECURITY.md`](SECURITY.md),
> [`ARCHITECTURE_LIMITS.md`](ARCHITECTURE_LIMITS.md), and
> [`SUPPORT_AND_DEPLOYMENT.md`](SUPPORT_AND_DEPLOYMENT.md).

**Where Warp fits**: a compatibility gateway that lets an application written for Oracle, MySQL,
SQL Server, MongoDB, DynamoDB, SQS, and a growing list of other client protocols keep using its
existing driver and connection code while the data moves to Postgres — with a native-relay mode
for teams who want to keep their current database engine, and Postgres-backed service emulation
for local development and testing. See [`USER_GUIDE.md`](USER_GUIDE.md) for the full picture.

**Honest starting point**: Warp is pre-1.0 (`0.1.0-SNAPSHOT`, no tagged releases yet — see
[`SUPPORT_AND_DEPLOYMENT.md`](SUPPORT_AND_DEPLOYMENT.md) §1.3). This roadmap describes real,
working capability, not a promised release schedule tied to version numbers that don't exist yet.

---

## Available today

### Protocol compatibility, honestly graded

24 wire protocols, each carrying a published maturity level (Verified / Production / Preview /
Experimental) based on real verification evidence, not marketing — see
[`SUPPORT_AND_DEPLOYMENT.md`](SUPPORT_AND_DEPLOYMENT.md) §1.1 for the full table and what each
level means. Seven protocols (Postgres, MongoDB, DynamoDB, Bolt/Neo4j, Cassandra CQL, Kafka, AMQP)
are at the top "Verified" tier, backed by large real-client conformance runs — e.g. mongowire
against 2,683 recorded steps of real `mongod` behavior, boltwire against the full openCypher TCK
suite run against real Neo4j.

For the AWS-family protocols specifically, a live **compatibility scorecard** runs the real,
third-party Floci SDK conformance suites against Warp's emulators and publishes current vs.
baseline pass rates in the admin UI — not a one-time claim, a running, re-checkable number.

### Real, reproducible performance numbers

Every latency/caching/RTT claim in [`WARP_GUIDE.md`](WARP_GUIDE.md) is backed by a live
before/after benchmark against a real client library — documented, dated, and indexed to the exact
commit that produced it in [`PERFORMANCE.md`](PERFORMANCE.md) and
[`RTT_BASELINE_2026.md`](RTT_BASELINE_2026.md). Seven real latency bugs found and fixed across five
protocols during this work are documented in full, including what was tried and reverted.

### SQL federation across backends

Cross-backend `JOIN`s (including cross-engine: Postgres + Oracle + MySQL + SQL Server in one
statement), cost-based partition sizing using real cached row-count statistics, a Bloom-filter
semi-join optimization for large-build-side joins, and — for genuinely large federated queries — a
parallel execution engine that scales from local threads to remote Warp nodes, with real work
stealing, retry/failover, and cost-based local-vs-remote placement.

### A governed MCP (Model Context Protocol) surface

Every MCP tool call's input is validated against its own JSON Schema before it runs — closing a
real gap where schemas existed but were never checked. Per-backend/per-tool scoping, real audit
logging, and policy-decision visibility are all live today.

### Row-level filtering and column masking

A real, tested SQL-rewrite engine enforces row filters (injects a `WHERE` clause based on the
caller's identity/attributes) and column-level masking or denial — configurable through a real
admin UI, not just available to integration tests. Fail-closed by design: a caller missing a
required attribute for a restricted table is rejected, not silently shown unfiltered data. See
[`SECURITY.md`](SECURITY.md) §1.6.

### Cache safety and transparency

Every cache hit, miss, invalidation, and — as of this pass — every bypass (a query that reached the
cache stage but matched none of its tiers) is counted and attributed to a table, closing what was
previously a silent "why wasn't this cached?" gap.

### Migration rehearsal — prove compatibility, don't just claim it

Replay a real, captured production workload against **both** your original database and Warp,
and get back a real diff: which statements matched exactly, which failed only on one side, which
returned different data, and the latency difference for each. This is the concrete, runnable proof
behind Warp's "migration compatibility gateway" positioning — not a claim you have to trust.

### A published, honest trust posture

*(Backend credentials in `warp_config` are encrypted with a versioned key, which can be rotated with one admin call; see [`SECURITY.md`](SECURITY.md) §1.8.)*

A real security threat model covering every protocol's actual authentication mechanism and its
fail-open/fail-closed behavior, a vulnerability disclosure and CVE response policy, documented
architecture limits (connection pools, QoS defaults, cluster timing windows) derived from the
code, and a real upgrade/rollback procedure — see [`SECURITY.md`](SECURITY.md) and
[`ARCHITECTURE_LIMITS.md`](ARCHITECTURE_LIMITS.md). Gaps are stated, not hidden: for example,
there's currently no encryption-key rotation mechanism for backend credentials, and the SQL
firewall is fail-open by default unless you author rules.

### High availability and horizontal scale, in production-shaped form

Config-primary/standby failover with automatic failback, horizontal sharding with scatter-gather
query fan-out, and optional multi-AZ cache clustering with AZ-aware backup placement (live-proven
against 3 real nodes). See [`SUPPORT_AND_DEPLOYMENT.md`](SUPPORT_AND_DEPLOYMENT.md) §2.

On top of that, for the SQL backends: **read replicas** with lag-aware routing and optional read-your-writes by log position;
**failover** (Warp follows a promotion made elsewhere, or promotes a replica itself behind a lease, a majority and an optional
external fence), **planned switchover**, **rejoin** of a returned old primary, **split-brain guards** (a replica that still hears the
primary vetoes a promotion, stale writers are frozen, instances that missed a config change catch up, an optional write fence) and a
**reader port** for relay mode. Postgres, MySQL and SQL Server were run live; Oracle's own promotion, switchover and rejoin have only run
against a scripted fake. [`REPLICAS_AND_FAILOVER.md`](REPLICAS_AND_FAILOVER.md) opens with a per-engine table of what was verified.

**Sharded writes and online rebalancing**: INSERT/UPDATE/DELETE routing by shard key, multi-shard writes that are atomic by two-phase
commit where the shards support it, and the `slots` strategy with online slot rebalancing (writes held only for the moving slots,
verified copy, crash recovery, cluster-wide hold, an optional automatic balancer, in-place conversion of a hash table), run live on
Postgres, MySQL, SQL Server and Oracle. See [`WARP_GUIDE.md`](WARP_GUIDE.md) §8.

Which protocol or backend does what, emulated or real, and what each one cannot do is in
[`PROTOCOLS_AND_BACKENDS.md`](PROTOCOLS_AND_BACKENDS.md).

---

## Actively being hardened

These are real, working capabilities with disclosed, scoped gaps — not aspirational features.

- **SQL federation depth**: the parallel join engine handles the common cross-backend join shapes
  today; skew-aware dynamic repartitioning and spill-to-disk for a partition too large even for
  bounded buffering remain deliberately out of scope until real usage data justifies the
  investment (this is the hardest problem in every distributed engine that has it).
- **MCP/enterprise governance**: input-schema validation is live; broader identity integrations
  (SSO/SCIM) and a policy simulation/approval-workflow surface are real, scoped future work, not
  yet started.
- **Cloud-native cluster discovery**: `WARP_CLUSTER_DISCOVERY=s3/gcs/azure` finders are verified
  against real Ignite discovery classes but not yet exercised against real cloud storage in this
  project's own test environment — the static-discovery path is the one exercised end-to-end today.
- **Native-backend-mode PL/SQL reach**: real, live-verified coverage for IN-only and single-OUT
  parameter calls; REF CURSOR, multi-OUT, and package-qualified calls are refused cleanly rather
  than silently mishandled, with a scoped plan to close specific gaps rather than build a general
  transpiler.
- **Oracle failover and switchover**: promotion, switchover and rejoin are written to Oracle's documentation and tested against a scripted
  fake only; no Data Guard was available. Only the stale-primary fence was run against a real Oracle (Free 23ai).
- **HA beyond Postgres and MySQL in the harder scenarios**: a shard failing over in the middle of a rebalance, two-phase commit on SQL Server
  (a stock Linux container has no XA procedures, so multi-shard writes fall back to commit-last there), and the relay reader port on Oracle and SQL
  Server were not run live. The orawire emulation path has no replica routing.
- **Rebalancing scope**: online rebalancing covers SQL tables declared with the `slots` strategy; consistent-hash, list, range and date tables and
  the non-SQL stores (DynamoDB, MongoDB, SQS, OpenSearch, ...) still do not move data when their shard set changes. It balances row counts, not
  query load, and is not self-managing the way a distributed database is.
- **Config writes**: failover and rebalancing use a locked read-modify-write of `warp_config`; several admin endpoints still do a plain
  read-then-write that can drop a concurrent change.
- **Dependency vulnerability scanning**: not yet wired into the build — see
  [`SECURITY.md`](SECURITY.md) §3 for this and the rest of the prioritized security backlog.

---

## What's next

- **A tagged release train.** Warp doesn't have versioned releases yet; establishing one is a
  prerequisite for a real version-support-window policy (see
  [`SUPPORT_AND_DEPLOYMENT.md`](SUPPORT_AND_DEPLOYMENT.md) §1.3).
- **Kubernetes/Helm deployment manifests.** Today's only deployment manifest is a single-node
  Docker Compose stack; a real HA topology (config-primary/standby + multi-AZ cache clustering) is
  fully working in code but has no packaged manifest yet.
- **CVE-numbering-authority status.** The vulnerability disclosure process in
  [`SECURITY.md`](SECURITY.md) is real and actionable today; formal CNA status (so a real CVE can
  be requested through a project-run process rather than GitHub's general flow) is a near-term,
  not-yet-started follow-up.
- **Broader emulator-workflow productization**: disposable-environment support and migration
  rehearsal are real today; fault injection/latency simulation and a CI-native CLI wrapping the
  whole workflow remain scoped, not-yet-built extensions of the same real foundation.

---

## How to verify any claim on this page

Every item above links to a real document or a live admin-UI surface, not a press release. If a
claim here doesn't hold up against the linked evidence, that's a bug in this page, not in the
underlying feature — open an issue.
