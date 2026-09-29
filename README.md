# Sayonora

Montdb.com's Sayonora project: independent, sibling modules in this repo, each with its own build
and deploy lifecycle -- not a single reactor build, just projects that happen to live together.
Ferry/Warp/migration are each their own Java/Maven project with its own `pom.xml`; Shim's
extensions are native Postgres extensions (C, built with `make`).

- **[Ferry/](Ferry/README.md)** -- Sayonora Ferry (Database Migration Service), formerly "Sayonora
  Advisor"/PolyAdvisor. Two halves in one module: **Migration Advisor** connects to a source
  database (Oracle/MySQL/MariaDB/SQL Server), profiles schema/feature/workload usage, and scores
  Postgres-migration difficulty; **Migration Service** launches and monitors real
  `sayonora-migration` runs (the Data Sync section). React/TS/Vite frontend in `Ferry/web/`.
- **[Warp/](Warp/)** -- Warp: a mid-tier database gateway. Speaks Oracle TNS/TTC, Postgres wire
  protocol v3, MySQL client/server protocol, SQL Server TDS, MongoDB wire protocol, DynamoDB/SQS
  HTTP/JSON, gRPC, and MCP to clients -- by default translating and routing every one to real
  Postgres backend(s) (wire-protocol compatibility for a pre- or post-migration cutover, not a
  schema/data migration tool itself -- that's Ferry's job); orawire/mywire/mssqlwire/MCP can each
  also run in Relay mode instead, proxying straight through to a real Oracle/MySQL/SQL Server
  database of your own with no translation, for keeping the engine you already run, or in Bridge
  mode (orawire/mywire/mssqlwire), which parses the real client protocol and runs the shared
  pipeline like Emulate mode but executes the verbatim SQL against a real, pooled backend of that
  same engine. See `docs/WARP_GUIDE.md` §8.1.1–§8.1.5. Ported from Omnigate (`~/Projects/Omnigate`, package
  `com.omnigate.*` -> `com.sayonora.warp.*`).
- **[Shim/](Shim/)** -- Postgres extensions (`pg_oracle`, `pg_mysql`, `pg_sqlserver`) that teach a
  real Postgres backend enough of another engine's dictionary views, functions, and dialect quirks
  to make Warp's own translated-mode (Emulate) traffic land closer to native -- detected and used
  automatically when installed (see Warp's own `PgOracleSupport`/`DialectTranslations`), with a
  graceful, fully-functional fallback when it isn't.
- **[migration/](migration/)** -- `sayonora-migration`: massively-parallel, low-downtime migration
  connectors (MongoDB, MySQL, SQL Server, Oracle, DynamoDB, SQS, Neo4j, InfluxDB) writing into a
  running Warp instance over its own native gRPC driver. Used both standalone (`Migrate*Cli`)
  and from Ferry's Migration Service (`Ferry`'s `MigrationJobRunner`).

Each module builds independently:

```bash
cd Ferry && mvn package -DskipTests
cd Warp && mvn package -DskipTests
cd Shim/pg_oracle && make    # each Shim extension builds independently the same way
cd migration && mvn package -DskipTests
```

See each module's own README/javadoc for details.
