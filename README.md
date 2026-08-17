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
  http/      Embedded Jetty server (AdvisorHttpServer) + POST /api/scan, same raw-Handler
             route-table pattern Omnigate's OmniGateHttpServer uses.

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

- MariaDB/MySQL `CatalogProfiler` (phase 2)
- Real workload capture (V$SQL / performance_schema) to replace the source-text-scan signal
  with actual runtime call frequency
- AWR report ingestion (license-gated -- see plan doc for the licensing note)
- LLM layer: PL/SQL summarization + assisted PL/pgSQL rewrite proposals (explicitly downstream
  of scoring, not part of it)
- Migration orchestration for the "easy" tier (wrap ora2pg/pgloader-derived DDL emission)
- Workload capture + replay/diff engine for post-migration correctness/perf validation
