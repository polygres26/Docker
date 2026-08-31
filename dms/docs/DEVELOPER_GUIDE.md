# Nexagres DMS — Developer Guide

> **This is a technical/internal reference** for contributors working on Nexagres DMS's codebase
> (formerly Polyadvisor) — package layout, class responsibilities, internal API routes. If you're
> looking to actually run an assessment, start with [`../README.md`](../README.md) instead.

## Layout

```
src/main/java/com/nexagres/dms/
  core/      Connectivity layer -- adapted from ~/Projects/Omnigate's com.omnigate.core
             (BackendTarget/BackendConnectionPools), copied in and trimmed to the dialects
             Advisor deals with (Oracle, MySQL, MariaDB, SQL Server, Postgres). No dependency
             on the Omnigate repo -- copied per project decision, not linked.
             DialectSupport.java: the single dispatch point from SourceDialect to the right
             CatalogProfiler/ObjectExplorer/ParameterReader/WorkloadCapture implementation --
             every route goes through this rather than hardcoding a vendor.
  catalog/   Deterministic catalog + source-text profilers, one per dialect:
               - OracleCatalogProfiler / OracleObjectExplorer / OracleParameterReader
                 (USER_* views, V$SQL-adjacent, V$PARAMETER; scoped to Oracle 19c)
               - MySqlCatalogProfiler / MySqlObjectExplorer / MySqlParameterReader
                 (information_schema-based; covers both MySQL and MariaDB, branching only
                 where they diverge -- e.g. MariaDB's native SEQUENCE objects)
               - SqlServerCatalogProfiler / SqlServerObjectExplorer / SqlServerParameterReader
                 (sys.* catalog views; SQL Server Agent jobs read defensively since they live
                 in msdb, a separate database the connecting login may not have access to)
             ObjectExplorer/ParameterReader are shared interfaces (ColumnDetail/ParameterInfo
             are shared record types) so the UI never needs dialect-specific rendering logic.
  score/     MigrationScorer -- rules-based, reproducible difficulty scoring over a
             CatalogSnapshot. Deliberately NOT LLM-driven; see class javadoc. Dispatches on
             CatalogSnapshot.dialect to a per-dialect weight table (Oracle's PL/SQL-shaped
             rubric, MySQL's storage-engine/syntax-shaped one, SQL Server's T-SQL-shaped one --
             different enough that sharing one table would leave meaningless entries).
  workload/  Deterministic workload capture, one per dialect, all normalized into
             CapturedStatement's Oracle-native vocabulary (elapsed time, CPU time, buffer
             gets, disk reads) with each mapping documented in its class javadoc:
               - OracleWorkloadCapture: V$SQL (needs SELECT_CATALOG_ROLE-level access)
               - MySqlWorkloadCapture: performance_schema.events_statements_summary_by_digest
               - SqlServerWorkloadCapture: sys.dm_exec_query_stats + sys.dm_exec_sql_text
             WorkloadSummary.java: roll-up over a capture (distinct statements, total
             executions/elapsed/CPU time/buffer gets/disk reads, top modules, top-by-elapsed)
             -- what the UI's Workload tab summary leads with.
  llm/       LLM/SLM layer -- explicitly downstream of catalog + workload capture, never
             feeds back into MigrationScorer's deterministic score. Configurable per the app's
             LLM configuration page (own rail item, app-wide, not scoped to a connection):
               - LlmRole (PRIMARY / JUDGE) + LlmProviderType (LOCAL / BUILTIN / EXTERNAL):
                 PRIMARY does the actual work; JUDGE is an optional, independently-configured
                 second model that reviews PRIMARY's PL/SQL summaries -- see LlmJudge's javadoc
                 for why it's scoped to summarization only, not every LLM call. LOCAL is the
                 default for a fresh install -- no API key, nothing leaves the machine.
               - LlmSettingsStore: persists both roles' config (provider type, API key, base
                 URL, local model path, model, enabled) in the same embedded HSQLDB store as
                 connections.
               - LocalLlamaProcess/LocalLlamaManager (LOCAL): manages llama-server sidecar
                 processes -- CPU-only, bound to 127.0.0.1 -- ported from Omnigate's
                 com.omnigate.assistant.LocalLlamaProcess. LocalLlamaManager runs one process per
                 distinct model path concurrently (Map<modelPath, LocalLlamaProcess>, each on its
                 own auto-assigned port starting at NEXAGRES_LLM_LOCAL_PORT) rather than a single
                 shared process -- so PRIMARY and JUDGE can run genuinely different local models
                 (e.g. Qwen + Gemma) at the same time without thrashing restarts on every call.
                 The server binary is found via
                 NEXAGRES_LLM_LOCAL_SERVER_PATH or PATH; the model file (a .gguf) is set on the
                 LLM configuration page, same "operator-provided, nothing bundled or
                 auto-downloaded" posture Omnigate takes with its own local Qwen/Gemma sidecars.
               - ClaudeLlmProvider (BUILTIN -- server's own ANTHROPIC_API_KEY) and
                 OpenAiCompatibleLlmProvider (EXTERNAL, and also what LOCAL talks to once its
                 sidecar is up, since llama-server exposes the same OpenAI-compatible
                 /v1/chat/completions shape) both implement LlmProvider; LlmProviderFactory
                 resolves an LlmSettings row into a concrete provider + model.
               - PlsqlSummarizer: deep-reasoning PL/SQL intent + portability-risk summary, one
                 object at a time (Oracle-only today), using PRIMARY (and JUDGE, if enabled).
               - SqlWorkloadClassifier: high-volume, cheap classification of captured SQL into
                 migration-relevant categories, any dialect, using PRIMARY.
             No model ids are hardcoded as defaults anywhere -- set them explicitly on the LLM
             configuration page; check current Claude model ids via the claude-api skill/reference.
  http/      Embedded Jetty server (DmsHttpServer): admin/connections/llm-settings API +
             ad-hoc scan/workload/summarize routes. Same raw-Handler route-table pattern
             Omnigate's OmniGateHttpServer uses. ConnectionsRoute/ScanRoute/WorkloadRoute all
             dispatch through DialectSupport rather than hardcoding a vendor.
  report/    FindingsReportGenerator -- the Findings tab's "Download report" PDF, built with
             PDFBox directly (manual content-stream drawing/wrapping/pagination). Scoped to
             Findings only (score, tier, feature inventory, scored findings), not a full data
             dump of Workload/Objects/Parameters -- a migration-assessment summary a
             stakeholder would read. Generated live on every request (GET
             /api/connections/{id}/report re-scans right then) -- never a cached copy from an
             earlier visit.
  http/auth/ Single-admin-account session auth (AdminAuth/AuthGuard) -- NEXAGRES_ADMIN_USER /
             NEXAGRES_ADMIN_PASSWORD, cookie session. Minimal on purpose (no OIDC/multi-user);
             see AdminAuth javadoc for the tradeoff and what Omnigate's fuller admin auth looks
             like if this needs to grow into that later.
  sizing/    SizingCalculator -- a rules-of-thumb Postgres instance-shape recommender (vCPUs,
             memory, storage, storage IOPS, max_connections), explicitly NOT a substitute for
             real load testing. Same "auditable, not a black box" philosophy as MigrationScorer:
             every number comes with a plain-English rationale string, and every missing/defaulted
             input comes with an explicit caveat string. SizingInput carries whatever signal is
             available -- CatalogSnapshot.schemaSizeBytes + WorkloadSummary totals from a live
             connection, or cpuCoresHint/memoryGBHint/dataSizeGBHint pulled by ReportAnalyzer from
             an uploaded report's text -- and degrades gracefully (documented floors/caveats) when
             a given signal isn't available rather than guessing. Storage sizing uses a 20GB floor
             and 1.6x headroom multiplier (index rebuilds/WAL/bloat/growth); vCPU/memory use
             statement-volume tiers (SMALL/MEDIUM/LARGE/XLARGE) raised to any stated hint and
             bumped further for CPU-bound or cache-friendly workload signals; storage IOPS is
             explicitly caveated as a magnitude signal, not a measured rate, since a V$SQL/DMV
             snapshot has no fixed observation window. SizingRecommendation is the shared output
             shape for both entry points below.

Admin + saved connections (com.nexagres.dms.core.ConnectionStore, ConnectionRecord):
  - CRUD over a set of named source-database connections, persisted in an embedded HSQLDB file
    at ~/.nexagres/nexagres-store (NEXAGRES_DATA_DIR to relocate). Credentials are stored
    plaintext -- a known MVP gap, called out in ConnectionRecord's javadoc.
  - Per connection: browse catalog objects, view database parameters, and run a migration
    assessment or workload capture, all using the connection's stored credentials server-side
    (the browser never sees them again after creation) and all dialect-agnostic on the UI side.

Uploaded reports (com.nexagres.dms.uploads.ReportStore, com.nexagres.dms.llm.ReportAnalyzer):
  - The no-live-connection on-ramp: upload a performance/workload report (Oracle AWR, a MySQL
    performance report, a SQL Server DMV/Query Store export) and get an LLM-assisted read on it,
    via the PRIMARY (+ optional JUDGE) model configured on the LLM configuration page.
  - HtmlTextExtractor strips markup from HTML uploads (AWR reports ship as HTML) with a simple
    regex pass, not a full HTML parser -- just enough to make the text readable to the model.
  - Deliberately NOT wired into MigrationScorer -- there's no live catalog to run deterministic
    queries against a static report, so this produces its own tier/findings/caveats shape
    (ReportAnalyzer.Analysis) and the UI labels it explicitly as heuristic, not equivalent to the
    Connections flow's deterministic scoring.
  - Report text/metadata persisted the same way connections are (embedded HSQLDB for metadata +
    cached analysis JSON; raw extracted text on disk under ~/.nexagres/reports/<id>.txt, kept out
    of the database row since a real AWR export can run to several MB).
  - Multiple reports, one combined analysis: upload several files in one action (e.g. one per
    system, or several snapshots of the same system over time), select any subset on the list
    page, and ReportAnalyzer#analyzeMultiple synthesizes one assessment across all of them --
    each report gets its own labeled section and its own share of the prompt budget (not just
    the first report eating the whole budget). Combined analyses aren't cached against any
    single report row, since they span several; re-running is cheap enough to just regenerate.

Sizing entry points (both return the same SizingRecommendation shape):
  - POST /api/connections/{id}/sizing (ConnectionsRoute#runSizing): re-profiles the connection's
    schema size + captures a fresh workload sample (best-effort -- degrades to storage-only sizing
    if capture fails, e.g. insufficient privileges) and feeds real numbers into SizingCalculator.
    No LLM involved; fully deterministic given the live source.
  - POST /api/reports/sizing (ReportsRoute#sizing), body {"ids": [...]}: runs
    ReportAnalyzer#analyzeMultiple across the selected report(s) and feeds its sizingSignals
    (cpuCores/memoryGB/dataSizeGB -- only filled in when the report text states them directly, per
    the analyzer's system prompt) into SizingCalculator. LLM-extraction-dependent: verified against
    a real local Qwen 2.5 1.5B model that it correctly applies stated hints (e.g. "32 CPU cores" /
    "256 GB RAM" in a report raised vCpus/memoryGB/storageGB accordingly) when the model extracts
    them, but extraction is inconsistent run-to-run with a model this small -- a repeat call on
    identical input can come back with sizingSignals all null and the calculator's documented
    floors/caveats used instead. Both outcomes are the calculator behaving correctly; the gap is
    small-model extraction reliability, not a code path bug. Swapping PRIMARY to Gemma 2 2B
    (Omnigate's other locally-tested model) didn't improve this on the same report.

web/         React + TS + Vite SPA: Login -> Connections (list/create/delete) ->
             ConnectionDetail (Findings / Objects / Workload / Parameters tabs), Reports
             (multi-file upload, per-report or multi-select combined analysis via the shared
             ReportAnalysisView component), and Sizing (pages/Sizing.tsx -- pick a connection or
             one-or-more uploaded reports, same stat-tile/rationale/caveats rendering for either
             source) as parallel top-level flows off the four-item nav rail.
             The original ad-hoc Connect -> Report flow still exists at /quick-scan for a
             one-off scan without saving a connection.
```

## What's next (see project plan)

- Feed real workload call-frequency back into `MigrationScorer`'s weighting (a package/routine
  that's never actually called shouldn't score as high-risk as one hit constantly)
- AWR report ingestion (license-gated -- see plan doc for the licensing note)
- Migration orchestration for the "easy" tier (wrap ora2pg/pgloader-derived DDL emission;
  MySQL/SQL Server equivalents TBD)
- Workload capture + replay/diff engine for post-migration correctness/perf validation
- Extend `PlsqlSummarizer` (or a dialect-neutral sibling) to MySQL/SQL Server routine source,
  not just Oracle PL/SQL
- Wire `/api/workload`'s LLM classification and `/api/summarize` into the saved-connection UI
  (currently only the ad-hoc routes and `/api/connections/{id}/scan`/`workload`/`findings`
  are used by the UI)
- Encrypt stored credentials at rest (see `ConnectionRecord` javadoc)
- Real multi-admin/SSO auth if this grows beyond a single shared admin account
