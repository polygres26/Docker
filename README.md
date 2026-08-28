# Nexagres

Montdb.com's Nexagres project: two independent, sibling Java/Maven modules in this repo, each
with its own `pom.xml`, build, and deploy lifecycle -- not a Maven multi-module reactor, just two
projects that happen to live together.

- **[advisor/](advisor/README.md)** -- Nexagres Advisor: migration assessment and migration tool.
  Connects to a source database (Oracle/MySQL/MariaDB/SQL Server), profiles schema/feature/
  workload usage, scores Postgres-migration difficulty, and drives the easy-tier migrations
  themselves. React/TS/Vite frontend in `advisor/web/`.
- **[wire/](wire/)** -- Polywire: a mid-tier, Postgres-only database gateway. Speaks Oracle TNS/
  TTC, Postgres wire protocol v3, MySQL client/server protocol, and gRPC to clients, translating
  and routing to real Postgres backend(s) -- wire-protocol compatibility for a pre- or
  post-migration cutover, not a schema/data migration tool itself (that's Advisor's job). Ported
  from Omnigate (`~/Projects/Omnigate`, package `com.omnigate.*` -> `com.nexagres.wire.*`).

Each module builds independently:

```bash
cd advisor && mvn package -DskipTests
cd wire && mvn package -DskipTests
```

See each module's own README/javadoc for details.
