# Warp — User Guide

Warp lets an application that was written for Oracle, MySQL, SQL Server, MongoDB,
DynamoDB, or Amazon SQS keep using its existing driver and connection code, while the data
actually lives in Postgres. Point your app at Warp instead of your original database —
nothing else in your application changes.

That's the default. If you'd rather keep the database engine you already run instead of moving
to Postgres, orawire/mywire/mssqlwire (and the MCP frontend) can each run in **native-backend
mode** instead — Warp proxies straight through to a real Oracle/MySQL/SQL Server database of
your own, nothing about your SQL rewritten in transit, while you still get connection pooling,
connection ACL, and observability in front of it. See
[`WARP_GUIDE.md` §8.1.1](WARP_GUIDE.md#811-native-backend-mode-proxy-straight-to-oracle-mysql-or-sql-server-instead-of-translating)
for exactly what that mode does and doesn't carry over (notably: the SQL Firewall, QoS admission
control, and caching are Postgres-pipeline features that don't apply in native mode).

## Why you'd use it

- **Moving to Postgres, but not ready to rewrite every client yet.** Run Warp as a bridge
  during the transition — old code keeps working while you migrate at your own pace.
- **Keeping legacy client code around indefinitely.** Some drivers or frameworks are old and
  not worth touching. Run Warp permanently as a compatibility layer.
- **Standardizing on one database.** Different teams' apps speak different protocols; Warp
  lets them all land on the same Postgres.
- **Keeping the database engine you already run.** Native-backend mode adds connection pooling,
  connection ACL, and observability in front of Oracle/MySQL/SQL Server without moving the data
  or translating a single statement.

## What it supports

| If your app talks to... | Point it at Warp's... | And it behaves like... |
|---|---|---|
| Oracle | orawire | Oracle, same TNS connection your app already uses |
| MySQL / MariaDB | mywire | MySQL, standard client/server protocol |
| SQL Server | mssqlwire | SQL Server, standard TDS protocol |
| Postgres | pgwire | Postgres, direct passthrough |
| MongoDB | mongowire | MongoDB, same driver and queries |
| DynamoDB | dynamowire | DynamoDB's HTTP API |
| Amazon SQS | sqswire | Amazon SQS's HTTP API — queues, messages, DLQs, FIFO |

Whichever protocol your app speaks, the request ends up as real data in Postgres. You don't
need to touch your application's code, only its connection string.

## Getting connected

Point your existing connection string, driver, or SDK at the Warp host and the port for
your protocol, using the same credentials you'd normally use. No code changes, no new
libraries.

If you're using a cloud SDK (DynamoDB, SQS), just override the endpoint URL — the SDK itself
doesn't need to know it's talking to Warp.

## What you get beyond a pass-through

- **Speed.** Frequently-read data is kept in a fast in-memory cache, so repeat lookups don't
  have to go all the way to Postgres every time. This is automatic — nothing to configure to
  benefit from it.
- **One place to see what's happening.** An admin dashboard shows live query activity,
  response times, and queue/table status across every protocol, in one view — instead of
  hunting through five different databases' tools.
- **Guardrails.** An administrator can block specific tables, statement types, or IP ranges
  without touching application code or redeploying anything.
- **No single point of failure.** Warp can run multiple instances behind a load balancer,
  and can fail over automatically if its configuration database becomes unavailable.
- **Correct `JOIN`s across sharded or split-out data.** If your data is spread across multiple
  Postgres backends (horizontally sharded, or split by table for scale), a query that joins
  across them is planned and executed for real — not silently wrong the moment a matching pair of
  rows happens to live on two different backends.

## What it's not

Warp does not move your schema or existing data for you — it's a live compatibility layer,
not a migration tool. If you need to migrate schema and data from Oracle/MySQL/SQL Server to
Postgres, see Sayonora DMS (the companion tool) or [`docs/USE_CASE_GUIDE.md`](USE_CASE_GUIDE.md).

## Where to go next

- Setting up and running Warp: [`../wire/README.md`](../wire/README.md)
- Deployment options and Docker packaging: [`USE_CASE_GUIDE.md`](USE_CASE_GUIDE.md)
- Technical/internal architecture reference (for operators and contributors, not required
  reading for application teams): [`WARP_GUIDE.md`](WARP_GUIDE.md),
  [`PERFORMANCE.md`](PERFORMANCE.md)
