# NexaGres

Montdb.com's NexaGres project: three independent, sibling Java/Maven modules in this repo, each
with its own `pom.xml`, build, and deploy lifecycle -- not a Maven multi-module reactor, just
projects that happen to live together.

- **[dms/](dms/README.md)** -- NexaGres DMS (Database Migration Service), formerly "NexaGres
  Advisor"/PolyAdvisor. Two halves in one module: **Migration Advisor** connects to a source
  database (Oracle/MySQL/MariaDB/SQL Server), profiles schema/feature/workload usage, and scores
  Postgres-migration difficulty; **Migration Service** launches and monitors real
  `nexagres-migration` runs (the Data Sync section). React/TS/Vite frontend in `dms/web/`.
- **[wire/](wire/)** -- Warp: a mid-tier, Postgres-only database gateway. Speaks Oracle TNS/
  TTC, Postgres wire protocol v3, MySQL client/server protocol, and gRPC to clients, translating
  and routing to real Postgres backend(s) -- wire-protocol compatibility for a pre- or
  post-migration cutover, not a schema/data migration tool itself (that's DMS's job). Ported
  from Omnigate (`~/Projects/Omnigate`, package `com.omnigate.*` -> `com.nexagres.wire.*`).
- **[migration/](migration/)** -- `nexagres-migration`: massively-parallel, low-downtime migration
  connectors (MongoDB, MySQL, SQL Server, Oracle, DynamoDB, SQS, Neo4j, InfluxDB) writing into a
  running Warp instance over its own native gRPC driver. Used both standalone (`Migrate*Cli`)
  and from DMS's Migration Service (`dms`'s `MigrationJobRunner`).

Each module builds independently:

```bash
cd dms && mvn package -DskipTests
cd wire && mvn package -DskipTests
cd migration && mvn package -DskipTests
```

See each module's own README/javadoc for details.
