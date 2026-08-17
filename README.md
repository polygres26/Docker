# Polygres Advisor

Migration assessment and migration tool for Montdb.com's Polygres project. Connects to a source
database, profiles its schema/feature usage, scores Postgres-migration difficulty, and (for the
easy tier) will drive the actual migration plus post-migration workload replay.

**Sequencing:** Oracle first (19c baseline), then MariaDB/MySQL. Only Oracle is wired up today.

## Layout

```
src/main/java/com/polygres/advisor/
  core/      Connectivity layer -- adapted from ~/Projects/Omnigate's com.omnigate.core
             (BackendTarget/BackendConnectionPools), copied in and trimmed to the four
             dialects Advisor deals with (Oracle, MySQL, MariaDB, Postgres). No dependency
             on the Omnigate repo -- copied per project decision, not linked.
  catalog/   Deterministic catalog + source-text profilers. OracleCatalogProfiler is the
             only implementation so far, scoped to Oracle 19c (see CatalogSnapshot javadoc).
  score/     MigrationScorer -- rules-based, reproducible difficulty scoring over a
             CatalogSnapshot. Deliberately NOT LLM-driven; see class javadoc.
  workload/  Deterministic workload capture. OracleWorkloadCapture snapshots V$SQL (needs
             SELECT_CATALOG_ROLE-level access) -- "what's actually running", separate signal
             from the catalog's "what objects exist".
  llm/       LLM/SLM layer -- explicitly downstream of catalog + workload capture, never
             feeds back into MigrationScorer's deterministic score:
               - PlsqlSummarizer: deep-reasoning PL/SQL intent + portability-risk summary,
                 one object at a time. Model id from POLYGRES_LLM_SUMMARY_MODEL.
               - SqlWorkloadClassifier: high-volume, cheap classification of captured SQL
                 into migration-relevant categories. Model id from POLYGRES_LLM_CLASSIFY_MODEL
                 (point this at a small/fast model -- the SLM side of the split).
             Both go through ClaudeLlmProvider (plain HttpClient call to the Anthropic Messages
             API; requires ANTHROPIC_API_KEY). No model ids are hardcoded as defaults -- set the
             env vars explicitly; check current ids via the claude-api skill/reference.
  http/      Embedded Jetty server (AdvisorHttpServer): admin/connections API + ad-hoc scan/
             workload/summarize routes. Same raw-Handler route-table pattern Omnigate's
             OmniGateHttpServer uses.
  http/auth/ Single-admin-account session auth (AdminAuth/AuthGuard) -- POLYGRES_ADMIN_USER /
             POLYGRES_ADMIN_PASSWORD, cookie session. Minimal on purpose (no OIDC/multi-user);
             see AdminAuth javadoc for the tradeoff and what Omnigate's fuller admin auth looks
             like if this needs to grow into that later.

Admin + saved connections (com.polygres.advisor.core.ConnectionStore, ConnectionRecord):
  - CRUD over a set of named source-database connections, persisted in an embedded HSQLDB file
    at ~/.polygres/polygres-store (POLYGRES_DATA_DIR to relocate). Credentials are stored
    plaintext -- a known MVP gap, called out in ConnectionRecord's javadoc.
  - Per connection: browse catalog objects (OracleObjectExplorer -- table columns, PL/SQL
    source), view database parameters (OracleParameterReader -- V$PARAMETER), and run a
    migration assessment (MigrationScorer) or workload capture, all using the connection's
    stored credentials server-side (the browser never sees them again after creation).

web/         React + TS + Vite SPA: Login -> Connections (list/create/delete) ->
             ConnectionDetail (Objects / Parameters / Assessment tabs). The original ad-hoc
             Connect -> Report flow still exists at /quick-scan for a one-off scan without
             saving a connection.
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

- MariaDB/MySQL `CatalogProfiler` + `WorkloadCapture` + object explorer/parameter reader (phase 2)
- Feed real workload call-frequency back into `MigrationScorer`'s weighting (a package that's
  never actually called shouldn't score as high-risk as one hit constantly)
- AWR report ingestion (license-gated -- see plan doc for the licensing note)
- Migration orchestration for the "easy" tier (wrap ora2pg/pgloader-derived DDL emission)
- Workload capture + replay/diff engine for post-migration correctness/perf validation
- Wire the web UI up to `/api/workload` and `/api/summarize` for saved connections (currently
  only the ad-hoc `/api/scan`/`/api/workload`/`/api/summarize` routes and the connection-scoped
  `/api/connections/{id}/scan`/`workload` are used by the UI)
- Encrypt stored credentials at rest (see `ConnectionRecord` javadoc)
- Real multi-admin/SSO auth if this grows beyond a single shared admin account
