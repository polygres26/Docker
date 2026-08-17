# Polygres Advisor

Migration assessment and migration tool for Montdb.com's Polygres project. Connects to a source
database, profiles its schema/feature usage, scores Postgres-migration difficulty, and (for the
easy tier) will drive the actual migration plus post-migration workload replay.

**Sequencing:** Oracle first (19c baseline), then MariaDB/MySQL, then SQL Server. All three are
wired up now for catalog profiling, workload capture, object exploration, parameter viewing, and
scoring.

## Layout

```
src/main/java/com/polygres/advisor/
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
               - LocalLlamaProcess/LocalLlamaManager (LOCAL): manages a llama-server sidecar
                 process -- CPU-only, bound to 127.0.0.1 -- ported from Omnigate's
                 com.omnigate.assistant.LocalLlamaProcess. At most one local model running at a
                 time; a changed model path restarts it. The server binary is found via
                 POLYGRES_LLM_LOCAL_SERVER_PATH or PATH; the model file (a .gguf) is set on the
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
  http/      Embedded Jetty server (AdvisorHttpServer): admin/connections/llm-settings API +
             ad-hoc scan/workload/summarize routes. Same raw-Handler route-table pattern
             Omnigate's OmniGateHttpServer uses. ConnectionsRoute/ScanRoute/WorkloadRoute all
             dispatch through DialectSupport rather than hardcoding a vendor.
  http/auth/ Single-admin-account session auth (AdminAuth/AuthGuard) -- POLYGRES_ADMIN_USER /
             POLYGRES_ADMIN_PASSWORD, cookie session. Minimal on purpose (no OIDC/multi-user);
             see AdminAuth javadoc for the tradeoff and what Omnigate's fuller admin auth looks
             like if this needs to grow into that later.

Admin + saved connections (com.polygres.advisor.core.ConnectionStore, ConnectionRecord):
  - CRUD over a set of named source-database connections, persisted in an embedded HSQLDB file
    at ~/.polygres/polygres-store (POLYGRES_DATA_DIR to relocate). Credentials are stored
    plaintext -- a known MVP gap, called out in ConnectionRecord's javadoc.
  - Per connection: browse catalog objects, view database parameters, and run a migration
    assessment or workload capture, all using the connection's stored credentials server-side
    (the browser never sees them again after creation) and all dialect-agnostic on the UI side.

web/         React + TS + Vite SPA: Login -> Connections (list/create/delete) ->
             ConnectionDetail (Findings / Objects / Workload / Parameters tabs). The original
             ad-hoc Connect -> Report flow still exists at /quick-scan for a one-off scan
             without saving a connection.
```

## Running locally

Backend:
```bash
mvn package -DskipTests
POLYGRES_ADVISOR_PORT=8090 java -jar target/polygres-advisor.jar
```

Frontend (dev, proxies /api to the backend):
```bash
cd web && npm install && npm run dev
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
