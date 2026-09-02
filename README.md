# Sayonora

Montdb.com's Sayonora project: three independent, sibling Java/Maven modules in this repo, each
with its own `pom.xml`, build, and deploy lifecycle -- not a Maven multi-module reactor, just
projects that happen to live together.

- **[dms/](dms/README.md)** -- Sayonora DMS (Database Migration Service), formerly "Sayonora
  Advisor"/PolyAdvisor. Two halves in one module: **Migration Advisor** connects to a source
  database (Oracle/MySQL/MariaDB/SQL Server), profiles schema/feature/workload usage, and scores
  Postgres-migration difficulty; **Migration Service** launches and monitors real
  `sayonora-migration` runs (the Data Sync section). React/TS/Vite frontend in `dms/web/`.
- **[wire/](wire/)** -- Warp: a mid-tier database gateway. Speaks Oracle TNS/TTC, Postgres wire
  protocol v3, MySQL client/server protocol, SQL Server TDS, MongoDB wire protocol, DynamoDB/SQS
  HTTP/JSON, gRPC, and MCP to clients -- by default translating and routing every one to real
  Postgres backend(s) (wire-protocol compatibility for a pre- or post-migration cutover, not a
  schema/data migration tool itself -- that's DMS's job); orawire/mywire/mssqlwire/MCP can each
  also run in native-backend mode instead, proxying straight through to a real Oracle/MySQL/SQL
  Server database of your own with no translation, for keeping the engine you already run. See
  `docs/WARP_GUIDE.md` §8.1.1. Ported from Omnigate (`~/Projects/Omnigate`, package
  `com.omnigate.*` -> `com.sayonora.wire.*`).
- **[migration/](migration/)** -- `sayonora-migration`: massively-parallel, low-downtime migration
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
