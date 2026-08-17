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
  http/      Embedded Jetty server (AdvisorHttpServer): POST /api/scan, POST /api/workload,
             POST /api/summarize. Same raw-Handler route-table pattern Omnigate's
             OmniGateHttpServer uses.

web/         React + TS + Vite SPA: Connect page -> Report page.
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

- MariaDB/MySQL `CatalogProfiler` + `WorkloadCapture` (phase 2)
- Feed real workload call-frequency back into `MigrationScorer`'s weighting (a package that's
  never actually called shouldn't score as high-risk as one hit constantly)
- AWR report ingestion (license-gated -- see plan doc for the licensing note)
- Migration orchestration for the "easy" tier (wrap ora2pg/pgloader-derived DDL emission)
- Workload capture + replay/diff engine for post-migration correctness/perf validation
- Wire the web UI up to `/api/workload` and `/api/summarize` (currently backend-only, curl/
  Postman-testable)
