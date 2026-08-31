# Polywire — User Guide

Polywire lets an application that was written for Oracle, MySQL, SQL Server, MongoDB,
DynamoDB, or Amazon SQS keep using its existing driver and connection code, while the data
actually lives in Postgres. Point your app at Polywire instead of your original database —
nothing else in your application changes.

## Why you'd use it

- **Moving to Postgres, but not ready to rewrite every client yet.** Run Polywire as a bridge
  during the transition — old code keeps working while you migrate at your own pace.
- **Keeping legacy client code around indefinitely.** Some drivers or frameworks are old and
  not worth touching. Run Polywire permanently as a compatibility layer.
- **Standardizing on one database.** Different teams' apps speak different protocols; Polywire
  lets them all land on the same Postgres.

## What it supports

| If your app talks to... | Point it at Polywire's... | And it behaves like... |
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

Point your existing connection string, driver, or SDK at the Polywire host and the port for
your protocol, using the same credentials you'd normally use. No code changes, no new
libraries.

If you're using a cloud SDK (DynamoDB, SQS), just override the endpoint URL — the SDK itself
doesn't need to know it's talking to Polywire.

## What you get beyond a pass-through

- **Speed.** Frequently-read data is kept in a fast in-memory cache, so repeat lookups don't
  have to go all the way to Postgres every time. This is automatic — nothing to configure to
  benefit from it.
- **One place to see what's happening.** An admin dashboard shows live query activity,
  response times, and queue/table status across every protocol, in one view — instead of
  hunting through five different databases' tools.
- **Guardrails.** An administrator can block specific tables, statement types, or IP ranges
  without touching application code or redeploying anything.
- **No single point of failure.** Polywire can run multiple instances behind a load balancer,
  and can fail over automatically if its configuration database becomes unavailable.
- **Correct `JOIN`s across sharded or split-out data.** If your data is spread across multiple
  Postgres backends (horizontally sharded, or split by table for scale), a query that joins
  across them is planned and executed for real — not silently wrong the moment a matching pair of
  rows happens to live on two different backends.

## What it's not

Polywire does not move your schema or existing data for you — it's a live compatibility layer,
not a migration tool. If you need to migrate schema and data from Oracle/MySQL/SQL Server to
Postgres, see Nexagres DMS (the companion tool) or [`docs/USE_CASE_GUIDE.md`](USE_CASE_GUIDE.md).

## Where to go next

- Setting up and running Polywire: [`../wire/README.md`](../wire/README.md)
- Deployment options and Docker packaging: [`USE_CASE_GUIDE.md`](USE_CASE_GUIDE.md)
- Technical/internal architecture reference (for operators and contributors, not required
  reading for application teams): [`POLYWIRE_GUIDE.md`](POLYWIRE_GUIDE.md),
  [`PERFORMANCE.md`](PERFORMANCE.md)
