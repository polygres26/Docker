# PolyAdvisor

PolyAdvisor answers one question: **how hard is it to migrate this database to Postgres, and how
much of it can you automate?** Point it at an Oracle, MySQL, MariaDB, or SQL Server database (or
upload a performance report if you can't share a live connection), and it gives you a concrete
difficulty score, a breakdown of what's actually driving that score, and a Postgres sizing
recommendation — no guesswork, no black box.

## Two ways to get an assessment

**Connect a database** (the deterministic path). Give PolyAdvisor a read-only connection string
and it profiles the real schema, feature usage, and query workload directly — no data ever leaves
your database, only metadata and query statistics. This is the accurate, reproducible path: run it
twice and you get the same score.

**Upload a performance report** (for when you can't share a live connection). Drop in an Oracle
AWR report, a MySQL performance report, or a SQL Server DMV/Query Store export, and an LLM reads it
for you. This is a genuinely different kind of signal — a model's read of a report, not a live
query against your schema — and PolyAdvisor labels it as such everywhere it shows up, rather than
presenting it as equivalent to a live connection's score. You can upload several reports (e.g. one
per system, or several snapshots over time) and get one combined assessment across all of them.

## What you get

- **A migration difficulty score and tier** — reproducible and rules-based, not LLM-generated, so
  running it twice against the same database gives you the same answer.
- **A feature inventory** — what's actually in the schema that complicates a Postgres move (PL/SQL
  packages, triggers, stored procedures, partitioning, database links, scheduled jobs, and more),
  each weighted by how much it actually costs to migrate.
- **A downloadable findings report (PDF)** — a migration-assessment summary built for sharing with
  a stakeholder, not a raw data dump.
- **A Postgres sizing recommendation** — vCPUs, memory, storage, and IOPS, with a plain-English
  reason behind every number and an explicit caveat wherever a real measurement wasn't available.

## Supported databases

Oracle (19c baseline), MySQL, MariaDB, and SQL Server. All four support live-connection profiling,
workload capture, object browsing, and parameter inspection today.

## Getting started

Backend:
```bash
mvn package -DskipTests
POLYGRES_ADVISOR_PORT=8090 java -jar target/polygres-advisor.jar
```

Frontend (dev, proxies `/api` to the backend):
```bash
cd web && npm install && npm run dev
```

Then open the app, add a connection (or upload a report), and run your first assessment.

## A note on credentials

Connection credentials are stored server-side so the browser never sees them again after you
enter them — but they're currently stored **unencrypted** in PolyAdvisor's local data directory.
Encryption at rest is planned but not yet built; keep that in mind before pointing this at a
production credential you wouldn't want sitting in plaintext on disk.

## Where to go next

- Companion tool: [PolyWire](../wire/README.md) — once you know what you're migrating, PolyWire
  lets your existing application keep talking its native protocol (Oracle, MySQL, SQL Server,
  MongoDB, DynamoDB, Amazon SQS) while the data actually lives in Postgres, either as a permanent
  compatibility layer or a temporary bridge during the migration itself.
- Technical/internal reference for contributors (package layout, class responsibilities, internal
  routes): [`docs/DEVELOPER_GUIDE.md`](docs/DEVELOPER_GUIDE.md).
