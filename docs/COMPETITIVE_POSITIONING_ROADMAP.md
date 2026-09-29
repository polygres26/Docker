# Competitive positioning & trust roadmap (long-horizon design, no code)

## Context

A competitive assessment scored Warp 7.2/10 overall, with a pointed conclusion: the primary risk
is no longer visual design or even protocol breadth — it's **proving compatibility, scalability,
and operational reliability across an exceptionally broad surface**. Warp sits at an unusual
intersection (native-protocol gateway + SQL federation + cloud-service emulation + MCP/API gateway
+ Postgres-backed migration path) that no single competitor combines, but that breadth reads as
five different, competing product identities rather than one differentiated wedge:

| Competitor | Their strength | Warp's edge | Warp's gap |
|---|---|---|---|
| Trino / Starburst | Distributed SQL federation, cost-based optimization, enterprise RBAC/ABAC | Native client protocols, operational traffic, emulation, MCP, migration story | Optimizer/connector/execution maturity; governance depth |
| Dremio / Denodo | Semantic layer, lakehouse acceleration, data virtualization | Transactional protocols, wire-level compatibility, native relay | Semantic modelling, catalog, lineage |
| Kong / Gravitee | REST/MCP/LLM/A2A traffic governance | SQL/database protocols, stateful service emulation | Plugin ecosystem, API lifecycle, identity integrations |
| LocalStack | Broad AWS emulation for local dev/CI | Multi-cloud + database-protocol emulation backed by persistent Postgres | AWS fidelity, developer-ecosystem maturity |
| Babelfish / EDB PG Advanced Server | Narrow, deep single-vendor compatibility (SQL Server/Oracle on Postgres) | Many protocols rather than one | Depth per protocol vs. a single, deeply-proven one |

Sub-scores make the shape of the gap explicit: product differentiation (9.0), protocol breadth
(9.4), migration story (8.1), and emulator/testing potential (8.5) are all strong. Enterprise
readiness (5.9), ecosystem/credibility (4.8), SQL federation (6.3), and security/governance (6.6)
are the real drag on the composite score — **the technology is unusually differentiated; the gap is
trust, narrowness of promise, and depth in the layers competitors have spent years hardening.**

**This document is a design/positioning plan only — no code is written from it in this pass.** It
exists to turn a broad, multi-dimensional critique into a sequenced set of independently-scoped
workstreams, several of which build directly on work already shipped this session (observability,
maturity metadata) or already exist in the codebase in more mature form than the review assumed
(the parallel-join execution engine, the floci compatibility harness).

## Guiding principle: narrow the promise, then expand from a trusted wedge

Warp should not attack Trino, Kong, and LocalStack head-on simultaneously. The recommended entry
position, adopted as this roadmap's organizing thesis:

> **A migration compatibility gateway that preserves existing drivers and APIs while workloads move
> to Postgres, with native relay for low-risk adoption and emulation for testing.**

Federation, MCP governance, and cache acceleration become **expansion layers** once trust is
established in the data path — not five equally-prominent, simultaneously-marketed identities. This
reframing is free (a docs/messaging change), should ship first, and should discipline the sequencing
of every workstream below: each phase either **deepens the wedge** (compatibility proof, native
relay hardening, emulator rigor) or is explicitly labeled an **expansion layer** (federation, MCP
governance, enterprise identity) that follows only once the wedge is credible.

## Non-goals

- Not a rewrite or rebrand of any shipped feature — this sequences new work and reframes
  documentation/UI language, it doesn't touch working code paths without a scoped reason.
- Not an attempt to match Trino's optimizer or Starburst's governance depth in one pass — several
  phases below explicitly bound scope to "operational migration validation and selective
  cross-backend joins," per the review's own recommendation, not a general OLAP engine.
- Does not commit to building every enterprise-identity feature (SSO, SCIM, ABAC, column masking,
  policy simulation) in one phase — Phase 4 below sequences these by what already has a real seam
  to extend versus what needs new infrastructure from zero.

## Terminology: three execution modes, named consistently

The review's proposed vocabulary — **Native Relay**, **Translation Mode**, **Emulation Mode** — is
clearer than Warp's current internal naming and maps cleanly onto real, already-load-bearing
distinctions in the codebase, with one honest wrinkle to resolve:

- **Native Relay** = Relay/NATIVE: a 1:1 raw-byte proxy, outside the shared execution pipeline
  entirely, executing directly against the native backend with no translation. The lowest-risk
  adoption path (existing driver, existing backend, Warp adds only routing/policy/observability).
- **Translation Mode** = Adapt/JDBC (+ Shim): translates the workload to run against Postgres,
  with the Shim covering dictionary/PL-SQL-shaped compatibility gaps (`DBMS_*` functions via
  Postgres's own `DO $$...$$`/plain calls). This is the actual migration surface — where "run on
  Postgres without rewriting clients" is delivered.
- **Emulation Mode** = the Postgres-backed service emulators (S3/DynamoDB/SQS/etc. wire protocols
  storing state in Postgres) — reproducing an external service's contract, not relaying to a real
  backend at all.
- **Open wrinkle, needs an explicit decision before the terminology ships everywhere**: Bridge mode
  (pooled, many-to-few connections, live PL/SQL translation, but still executing against the
  *native* Oracle backend, not Postgres) doesn't cleanly fit any of the three buckets — it shares
  Adapt/JDBC's execution pipeline and pooling model but Native Relay's "still hits the real backend"
  property. Recommendation: introduce a fourth, explicitly-named bucket (**Pooled Native**, or
  similar) rather than force Bridge into "Native Relay" (which would incorrectly imply 1:1
  connection semantics — package-variable state and pooling-related correctness risks are real and
  already tracked) or "Translation Mode" (which would incorrectly imply Postgres is the backend).
  This is a naming/documentation decision, not new code, but it should be resolved before the
  three-mode vocabulary is published anywhere customer-facing.
- For every feature going forward (caching, federation, transaction semantics), the docs/UI should
  state explicitly which mode(s) it applies to — caching and transaction semantics genuinely differ
  between a 1:1 relay, a pooled native backend, and a Postgres translation, and conflating them is
  exactly the kind of overclaim the review is warning against.

## Phased workstreams

Each phase is independently scoped and sized; sequencing within "Now" vs. "Next" vs. "Later" below
reflects how directly each builds on work already shipped or already exists in more mature form
than the review assumed — not raw importance.

### Now — lowest net-new risk, builds directly on this session's shipped work

**Phase A — Publish measurable compatibility levels (compatibility scorecards).** Sizing: small
(the infrastructure already exists; this phase surfaces it). The review's central credibility fix —
"supports Oracle" or "supports Cassandra" reading as near-complete compatibility — has a real,
underused answer already in the repo: `Warp/tests/python/floci_compat/` runs the actual
[floci](https://github.com/floci-io/floci) third-party SDK compatibility suites against Warp's AWS
frontends (dynamowire/sqswire/s3wire, plus SNS/Kinesis/SecretsManager/SSM/KMS/STS), producing
per-service `results/warp-<service>-<suite>.{json,md}` baselines with a heuristic failure
classification (a/b/c/d) already built into `run_floci_compat.py`. This is exactly the
"conformance-suite pass percentage" artifact the review asks for — it exists, runs against a real
upstream test suite, and is currently a developer-only script output, not a product surface.
  - Extend the externalized maturity-metadata pattern already shipped this session
    (`Warp/web/src/api/maturity-data.json`, `maturity.ts`, rank-based `MaturityTag`) with new,
    per-protocol fields sourced from these real conformance runs: tested driver/version, pass
    percentage, known unsupported constructs, and status (production/preview/experimental) — not
    invented values, read from the actual `results/*.json` the harness already writes.
  - Extend this to Oracle PL/SQL using the already-tracked support matrix (IN-only/single-OUT
    verified under Bridge mode; REF CURSOR, multi-OUT, package-qualified calls, and function calls
    refused cleanly; package-variable state flagged as a real pooling risk) — this is real, already-
    known information that has never been surfaced in the admin console.
  - Promote the resulting view from "secondary administration screen" (the review's own framing) to
    a first-class page — the natural home is a new tab alongside the existing maturity-tagged pages
    (Workloads, Overview, Security, Infrastructure, Certificates, MCP servers).
  - New CI-integration follow-up (separately sized, not blocking this phase): run `floci_compat`
    on a schedule and fail a build on regression, rather than the current manual/developer-invoked
    baseline capture — turns "here's a baseline from Sept 25" into "here's the current, continuously
    verified number."

**Phase B — Complete observability (the concrete gaps, not the whole list).** Sizing: medium. This
session already shipped real export-health tracking (`WarpTelemetry.ExportHealthTrackingExporter`,
time-bounded `exportVerified`), admin-togglable destinations (`ObservabilityToggles`,
`ObservabilityApi`), and a live-generated signal catalog (`MetricsCatalogGenerator`, parsing
`MetricsRenderer`'s real Prometheus output instead of a hand-maintained list). The review's
observability gaps that remain genuinely open, in priority order:
  1. **Resource attributes** — `service.name`, node/cluster identity, environment — on every OTLP
     export. `WarpTelemetry`'s `SdkMeterProvider` currently registers no `Resource` at all; this is
     a small, contained addition (`Resource.builder()...` at construction) with real payoff (every
     multi-node deployment currently reports metrics with no way to tell which node emitted them).
  2. **OTel Collector recipes + vendor presets** (Datadog, New Relic, Grafana Cloud, cloud-native
     monitoring) — the trimmed mockup's Export tab (`docs/mockups/warp-observability.html`) already
     sketches the UI shape (maturity-badged destination tiles: Available now / Requires Collector /
     Planned); this phase is building the real backing config-generation logic the mockup's form
     only illustrates today, scoped per the mockup's own real/planned split — start with the
     "Available now" tiles (generic OTLP + optional headers) since zero new Warp capability is
     needed, defer vendor-specific exporter recipes.
  3. **End-to-end tracing across protocol → policy → cache → translation → backend** — genuinely
     unbuilt (Warp has no trace exporter anywhere in the codebase, confirmed by prior investigation
     in this session; `metricsOnly: true` is a real, disclosed fact in `ObservabilitySummary`
     today). This is the largest, most valuable, and most expensive item in this phase — likely its
     own follow-up design pass rather than a checklist item here.
  4. Collector connectivity testing, export-queue/dropped-point reporting — smaller, incremental
     additions to the same `ExportHealthTrackingExporter` seam once resource attributes land.

### Next — real seams exist to extend, but each is a genuine multi-sprint investment

**Phase C — Strengthen SQL federation (mostly already in flight; close the stated gaps).**
Sizing: the review understates how much of this already exists. `ParallelJoinExecutor`/
`ParallelJoinPlanner` (see the standalone parallel-execution design, already Phase 0–2 shipped per
its own status notes: local thread-parallel hash joins, Bloom-filter semi-join pushdown, remote
peer dispatch with mTLS, work-stealing, cost-based remote-vs-local placement, bounded spill for
skewed partitions) already covers predicate/projection pushdown (via Calcite's existing
`JdbcRules`), join reordering is Calcite's own optimizer's job already exercised in
`SchemaFederationStage`, and explain-plan-shaped visibility exists implicitly in the eligibility
checks `FragmentPlanner` performs. What's genuinely still open, matching the review's own list:
  - **Cost-based optimization informed by real table/column statistics** — `StatisticsAwareTable`/
    `StatisticsScheduler`/`LeafScanProfiler` already exist as plumbing; using them to drive
    partition-count and local-vs-remote sizing (rather than today's fixed defaults/thresholds) is
    explicitly called out as future work in the parallel-execution design's own Phase 2+ section.
  - **Full skew-aware dynamic repartitioning and spill-to-disk for partitions too large even for
    today's bounded-buffer mitigation** — explicitly, deliberately out of scope in the existing
    design ("the hardest problem in every distributed engine that has one, not worth building until
    real usage data shows it's needed"). This roadmap doesn't change that call; it's flagged here so
    a future audit doesn't rediscover the same conclusion from zero.
  - **Failure recovery for long-running distributed queries** beyond the existing retry/failover
    (a failed remote dispatch already falls back to local computation) — a coordinator-side failure
    mid-query is not yet handled.
  - Positioning discipline, not code: continue explicitly NOT marketing this as a Trino replacement.
    Frame it, per the review's own recommendation, around "operational migration validation and
    selective cross-backend joins" — which is what it's actually built for and tested against today.

**Phase D — Make MCP a first-class, governed surface.** Sizing: large (multi-quarter). Real seams
already exist to build from: `McpScope` (already has enforcement, per
`McpScopeEnforcementIntegrationTest`), `McpMetricsCollector` (per-tool call/error/latency counters,
already in the live signal catalog as `warp_mcp_tool_*`), and the broader "governance/McpScope/
firewall stack" this codebase already applies uniformly (a real, stated architectural advantage over
adopting a second system like Trino with its own auth model). Gaps to close, sequenced by how
directly they extend `McpScope`:
  1. Tool-level ACLs and quotas, input/output schema validation, read-only enforcement — natural
     extensions of `McpScope`'s existing enforcement point.
  2. Human approval for dangerous tools, MCP session visibility, tool-call replay/debugging — new
     surface, but consumes the same `McpMetricsCollector`/session data already collected.
  3. Prompt-injection/data-exfiltration controls, upstream-server health/version compatibility,
     and tracing one agent request through MCP → SQL → backend execution — the last of these is the
     same end-to-end tracing gap as Phase B.3; sequencing them together once tracing exists is more
     efficient than building two separate ad hoc correlation mechanisms.
  4. Generating MCP tools from SQL, stored procedures, and REST specs — the most product-differentiating
     item in this list (directly answers Kong/Gravitee's API-to-MCP conversion story with governed
     *data* access instead of governed *API* access) but also the most net-new engineering.

**Phase E — Cache safety and transparency.** Sizing: medium. `WarpConfig.cacheTables`/`cacheTtlMs`
already exist as the config surface; there is currently no visibility into WHY a query was or
wasn't cached, no per-tenant/identity isolation guarantee surfaced to the operator, and no hit/miss/
bypass/error counters distinct from the general statement counters. Concretely:
  - Cache eligibility reason + a "why wasn't this cached?" inspector — the single highest-leverage
    item per the review, and the one most directly comparable to Starburst's explicit claim that
    existing access policies apply to accelerated/cached data; Warp needs an equally explicit,
    equally verifiable guarantee, not an implicit one.
  - Hit/miss/bypass/error counters per protocol and workload class — a natural new metric family
    for the same `WarpTelemetry`/`MetricsRenderer` pipeline Phase B is already extending.
  - Invalidation source/timestamp, staleness/TTL, memory/eviction pressure — needs whatever the
    current cache implementation's internals actually expose; scoping this precisely requires
    reading the cache stage's real code before committing to specifics (deliberately not assumed
    here).

### Later — large, real, multi-quarter investments; sequence after the wedge is trusted

**Phase F — Enterprise identity and governance.** Sizing: large (multi-quarter, possibly the
largest single item on this roadmap). Real seams exist (`AccessContextResolver` already resolves
OAuth issuer/audience/claims; `ClientAcl`/`ConnectionGate` already enforce IP-based rules), but SSO/
SCIM, RBAC/ABAC down to governed data entities, column masking/row policies, approval workflows,
immutable audit export, and "can this identity perform this request?" policy simulation are all
genuinely unbuilt. This is explicitly sequenced last: it's the review's own "biggest competitive
gap" for enterprise credibility, but it's also the workstream least connected to anything already
shipped, meaning it competes hardest for engineering time against the Now/Next phases that compound
on existing work.

**Phase G — Productize emulator workflows.** Sizing: medium-large. The `floci_compat` harness
(Phase A) is the direct foundation: it already runs real workloads against Warp's emulators and
classifies failures. Extending it toward the review's ask — disposable environments, seed/snapshot
export-import, record-production-traffic-and-replay, fault injection/latency simulation, a CI-native
CLI — is a natural generalization of infrastructure that already exists, not new-from-zero. The
single most differentiating idea here, worth calling out on its own: **migration rehearsal** — run
the same captured workload against both the original service and Warp, then diff results, errors,
and latency. This directly operationalizes Warp's own "migration compatibility gateway" positioning
from a claim into a customer-runnable proof.

**Phase H — Establish trust (process and documentation, not code).** Sizing: small per item, but
numerous: versioned compatibility matrices (Phase A's natural published form), reproducible
benchmarks (`docs/PERFORMANCE.md`, `docs/RTT_BASELINE_2026.md` already exist as a starting point —
this phase is about publishing and dating them consistently, not creating benchmarking from zero),
published architecture limits, upgrade/rollback procedure, HA/DR documentation, a security threat
model, a CVE response policy, long-duration soak tests, reference deployments, a support lifecycle,
and a customer-visible roadmap. None of this is glamorous; all of it is what the review identifies
as the actual gap versus Trino/Starburst/EDB's commercial credibility. Sequenced last only because
it has no code dependency on anything else here — it could reasonably be pulled forward in parallel
with any other phase if resourcing allows, since it's mostly writing down what's already true rather
than building anything new.

## Risks

The central risk this roadmap manages is the review's own diagnosis: spreading engineering effort
across five simultaneously-marketed product identities dilutes all of them. The Now/Next/Later
sequencing is the mitigation — Phase A and Phase B compound directly on this session's already-
shipped observability and maturity-metadata work, and Phase A additionally repurposes an existing,
under-used conformance harness rather than building new infrastructure. Phases C and D also extend
real, already-substantial seams. Phases F, G, and H are real, valuable, but should not start before
the Now/Next phases have shipped and been used to sharpen the "narrow the promise" positioning with
actual published compatibility/observability evidence — building enterprise governance for a product
whose own compatibility claims aren't yet independently verifiable would be sequencing backwards.

A second, narrower risk: the Bridge-mode terminology wrinkle (see "Terminology" above) should be
resolved with an explicit decision before "Native Relay / Translation Mode / Emulation Mode"
language ships anywhere customer-facing — shipping the three-mode vocabulary without resolving where
Bridge fits would reintroduce exactly the kind of overclaim (implying 1:1 semantics or Postgres
backing where neither is true) this whole roadmap is meant to eliminate.

## Critical files (for whoever picks up any phase)

- [Warp/web/src/api/maturity-data.json](../Warp/web/src/api/maturity-data.json) / [maturity.ts](../Warp/web/src/api/maturity.ts) — the externalized maturity-metadata pattern Phase A extends
- [Warp/tests/python/floci_compat/run_floci_compat.py](../Warp/tests/python/floci_compat/run_floci_compat.py) / [README.md](../Warp/tests/python/floci_compat/README.md) — the real conformance harness Phase A surfaces and Phase G generalizes
- [Warp/src/main/java/com/sayonora/warp/telemetry/WarpTelemetry.java](../Warp/src/main/java/com/sayonora/warp/telemetry/WarpTelemetry.java) / [ObservabilityToggles.java](../Warp/src/main/java/com/sayonora/warp/telemetry/ObservabilityToggles.java) — the export-health/admin-toggle foundation Phase B extends
- [Warp/src/main/java/com/sayonora/warp/http/admin/MetricsCatalogGenerator.java](../Warp/src/main/java/com/sayonora/warp/http/admin/MetricsCatalogGenerator.java) — the live signal-catalog generator Phase B/E's new metric families flow through
- [docs/mockups/warp-observability.html](mockups/warp-observability.html) — the Export tab's maturity-badged destination UI Phase B.2 builds real config-generation behind
- [Warp/src/main/java/com/sayonora/warp/core/ParallelJoinExecutor.java](../Warp/src/main/java/com/sayonora/warp/core/ParallelJoinExecutor.java) / [ParallelJoinPlanner.java](../Warp/src/main/java/com/sayonora/warp/core/ParallelJoinPlanner.java) — the already-substantial SQL federation engine Phase C closes remaining gaps in
- [Warp/src/main/java/com/sayonora/warp/mcp/McpScope.java](../Warp/src/main/java/com/sayonora/warp/mcp/McpScope.java) / [McpMetricsCollector.java](../Warp/src/main/java/com/sayonora/warp/mcp/McpMetricsCollector.java) — the MCP governance seam Phase D extends
- [Warp/src/main/java/com/sayonora/warp/acl/ClientAcl.java](../Warp/src/main/java/com/sayonora/warp/acl/ClientAcl.java) / [ConnectionGate.java](../Warp/src/main/java/com/sayonora/warp/acl/ConnectionGate.java) / [http/auth/AccessContextResolver.java](../Warp/src/main/java/com/sayonora/warp/http/auth/AccessContextResolver.java) — the identity/ACL seam Phase F extends
- [docs/PERFORMANCE.md](PERFORMANCE.md) / [docs/RTT_BASELINE_2026.md](RTT_BASELINE_2026.md) — existing benchmark artifacts Phase H publishes/dates consistently

## Verification

This is a design/positioning document, not code — verification here is stakeholder review of: the
"narrow the promise" thesis and Now/Next/Later sequencing, the Bridge-mode terminology decision
(explicit sign-off needed before any customer-facing three-mode language ships), and confirmation
that Phase A (compatibility scorecards from the existing floci_compat harness) and Phase B
(observability completion) are the right two workstreams to start with given how directly they
compound on this session's already-shipped work.
