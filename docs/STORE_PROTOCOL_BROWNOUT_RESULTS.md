# Postgres-backed protocols under switchover and failover (phase 1: DynamoDB, SQS, MongoDB)

Measured on 2026-10-05 with `StoreFailoverBrownoutLiveTest`. Warp serves the protocol over its Postgres backend (the data
lives in Postgres tables, enabled per backend with `WARP_BACKEND_STORES`), the backend has one streaming replica and
`failoverMode=promote`. 3 writer and 2 reader threads (about 250 operations a second per protocol), real client libraries
(AWS SDK v2, MongoDB Java driver) with **driver retries turned off and short timeouts so a failed call is visible**. One
run per cell on a laptop. Failover tuning: 1 s probes, 3 to confirm (the defaults are 5 s probes, so the default stall would
be longer, not measured). After the event every acknowledged write is read back through the protocol itself.

| protocol | scenario | writes ok | writes failed | steady max gap ms | max gap around event ms | failed window | reads ok | reads failed | acked | lost | extra |
|---|---|---|---|---|---|---|---|---|---|---|---|
| dynamowire | switchover | 4513 | 3 | 28 | 170 | 1 ms (recovered +27) | 3013 | 2 | 4513 | 0 | 0 |
| dynamowire | failover | 6337 | 6 | 27 | 5288 | 5144 ms (recovered +5165) | 4240 | 4 | 6337 | 0 | 0 |
| sqswire | switchover | 4520 | 3 | 29 | 144 | 20 ms (recovered +30) | 2979 | 2 | 4520 | 0 | 0 |
| sqswire | failover | 6341 | 6 | 27 | 5304 | 5143 ms (recovered +5182) | 4150 | 4 | 6341 | 0 | 0 |
| mongowire | switchover | 4515 | 4 | 27 | 151 | 4 ms (recovered +28) | 3010 | 2 | 4515 | 0 | 0 |
| mongowire | failover | 6354 | 7 | 31 | 5296 | 5144 ms (recovered +5173) | 4235 | 4 | 6354 | 0 | 0 |

## What it shows

- **The store-backed protocols follow Warp's failover.** They resolve their hosts from the backend registry, so after a
  switchover or a promotion they write to the new primary with no restart: writes recover on their own in about 150 ms
  (switchover) or about 5.2 s (crash). That is the same as the four SQL protocols measured earlier.
- **No acknowledged write was lost** and no unacknowledged row appeared, for any of the three, in either scenario.
- **What a client sees:** a handful of failed calls, with each protocol's own error: DynamoDB `InternalServerError` (HTTP 500),
  SQS `SqsException` 500, MongoDB error 91 (`ShutdownInProgress` / `NodeIsRecovering`). A client that retries on those (the
  AWS SDKs and the Mongo driver do by default) would mostly ride it out; these runs deliberately disabled that.
- As with the SQL protocols, the old primary is left read-only and not replicating after a switchover.

## Problem found, not fixed: mongowire's concurrent first insert

One write per run failed with `Postgres error: ERROR: type "c" already exists` (switchover) and `type "__warp_uk" already
exists` (failover), error code 8 (UnknownError). The collection and its unique-index helper are created on first use; three
writers inserting into a collection that does not exist yet race on `CREATE TABLE IF NOT EXISTS`, which Postgres does not
make safe (it can raise a duplicate-type error instead of doing nothing). It happens once, at the start, before any event;
a client sees a failed first insert for a brand-new collection. DynamoDB and SQS did not show it in these runs.

## Not covered yet

The remaining Postgres-backed protocols (S3, Kafka, CQL, Bolt, InfluxDB, OpenSearch, Redis, AMQP, Pub/Sub, GCS, Azure, Cosmos,
Bigtable, Datastore, Firestore, the AWS JSON services, Gremlin, gRPC, MCP, A2A). Many of them have only unit tests, no
end-to-end test with a real client at all, so their workloads are being added in groups. Same caveats as the SQL results:
single runs, localhost, one replica, one Warp instance, no partitions.

Re-run: `WARP_TEST_BROWNOUT_PG_BIN=<postgres bin dir> mvn test -Dtest=StoreFailoverBrownoutLiveTest`
(`WARP_TEST_STORE_WORKLOADS=sqswire` and `WARP_TEST_BROWNOUT_SCENARIOS=switchover` narrow it).
