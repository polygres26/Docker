# Postgres-backed protocols under switchover and failover (phases 1-2: DynamoDB, SQS, MongoDB, S3, Kafka, CQL, Bolt, InfluxDB, OpenSearch)

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
| mongowire | switchover | 4570 | 3 | 26 | 139 | 18 ms (recovered +26) | 3040 | 2 | 4570 | 0 | 0 |
| mongowire | failover | 6410 | 6 | 26 | 5308 | 5144 ms (recovered +5183) | 4272 | 4 | 6410 | 0 | 0 |

## What it shows

- **The store-backed protocols follow Warp's failover.** They resolve their hosts from the backend registry, so after a
  switchover or a promotion they write to the new primary with no restart: writes recover on their own in about 150 ms
  (switchover) or about 5.2 s (crash). That is the same as the four SQL protocols measured earlier.
- **No acknowledged write was lost** and no unacknowledged row appeared, for any of the three, in either scenario.
- **What a client sees:** a handful of failed calls, with each protocol's own error: DynamoDB `InternalServerError` (HTTP 500),
  SQS `SqsException` 500, MongoDB error 91 (`ShutdownInProgress` / `NodeIsRecovering`). A client that retries on those (the
  AWS SDKs and the Mongo driver do by default) would mostly ride it out; these runs deliberately disabled that.
- As with the SQL protocols, the old primary is left read-only and not replicating after a switchover.

## Found and fixed: mongowire's concurrent first insert

One write per run failed with `Postgres error: ERROR: type "c" already exists` (switchover) and `type "__warp_uk" already
exists` (failover), error code 8 (UnknownError). The collection and its helper tables are created on first use; several
writers inserting into a collection that does not exist yet raced on `CREATE TABLE IF NOT EXISTS`, which Postgres does not make
safe (it can fail with "relation/type already exists" or a unique violation on `pg_class`/`pg_type` instead of doing nothing).
In isolation, with 8 clients each inserting into 60 brand-new databases at the same moment, **203 of 480 first inserts failed**
(five different Postgres duplicate errors). The existing three immediate retries did not help because the contenders collide
again in lock-step. Collection creation now takes a Postgres advisory lock per database (also safe across several Warp
instances), so concurrent first writes queue up and the later ones find the table already there: **0 of 480 fail**.
`MongowireConcurrentCreateLiveTest` fails without the fix and passes with it. After the fix the brownout rows for mongowire
no longer contain the error (switchover: 3 failed writes, 139 ms stall; failover: 6 failed writes, 5.3 s stall; 0 lost).

## Phase 2: S3, Kafka, Cassandra (CQL), Neo4j (Bolt), InfluxDB, OpenSearch

Same setup, same caveats. Clients: AWS SDK v2 (S3, path-style), kafka-clients (producer with `acks=all`, no retries; verification
by reading the topic from the beginning), the DataStax Java driver, the Neo4j Java driver (auto-commit, no managed-transaction
retries), influxdb-java and the OpenSearch Java client.

| protocol | scenario | writes ok | writes failed | steady max gap ms | max gap around event ms | failed window | reads ok | reads failed | acked | lost | extra |
|---|---|---|---|---|---|---|---|---|---|---|---|
| s3wire | switchover | 4436 | 3 | 52 | 167 | 2 ms (recovered +24) | 3018 | 2 | 4436 | 0 | 0 |
| s3wire | failover | 6239 | 5 | 29 | 5174 | 5137 ms (recovered +5147) | 4249 | 4 | 6239 | 0 | 0 |
| kafkawire | switchover | 3691 | 3 | 32 | 159 | 16 ms (recovered +38) | 3085 | 0 | 3691 | 0 | 0 |
| kafkawire | failover | 5183 | 6 | 33 | 5326 | 5151 ms (recovered +5191) | 4344 | 0 | 5183 | 0 | 0 |
| cqlwire | switchover | 4284 | 3 | 27 | 2267 | 0 ms (recovered +66) | 2865 | 2 | 4284 | 0 | 0 |
| cqlwire | failover | 5644 | 6 | 29 | 9459 | 7236 ms (recovered +7266) | 3854 | 3 | 5644 | 0 | 0 |
| boltwire | switchover | 4056 | 3 | 30 | 401 | 8 ms (recovered +60) | 2471 | 1 | 4056 | 0 | 0 |
| boltwire | failover | 5704 | 6 | 29 | 5759 | 5370 ms (recovered +5411) | 3364 | 3 | 5704 | 0 | 0 |
| influxwire | switchover | 4066 | 3 | 30 | 327 | 16 ms (recovered +214) | 2465 | 2 | 4066 | 0 | 0 |
| influxwire | failover | 5731 | 6 | 31 | 5528 | 5144 ms (recovered +5397) | 3355 | 5 | 5731 | 0 | 0 |
| oswire | switchover | 4106 | 3 | 32 | 134 | 23 ms (recovered +24) | 2753 | 4 | 4106 | 0 | 0 |
| oswire | failover | 6395 | 6 | 29 | 5289 | 5154 ms (recovered +5176) | 4274 | 6 | 6395 | 0 | 0 |

- **All six follow the failover on their own and lost no acknowledged write.** Switchover stalls are 130-400 ms for S3, Kafka,
  Bolt, InfluxDB and OpenSearch; crash stalls are about 5.2-5.8 s (the same 1 s-probe tuning as before).
- **CQL is the slow one: 2.3 s for a switchover and 9.5 s for a crash** (writes failed for 7.2 s) against about 0.15 s and 5.2 s
  for the rest. Part of that may be the DataStax driver: when a call fails the harness drops the session and opens a new one, and
  the driver reports `AllNodesFailedException: Could not reach any contact point` while Warp's CQL listener answers its first
  system queries. Not separated into client cost vs cqlwire cost, so treat it as an open question, not a verdict.
- **Kafka's reads never failed** because the admin client retries internally; its writes failed 3-6 times with the store's own
  error (`UnknownServerException: The store is currently unavailable`) or, once the topic metadata had to be refetched,
  `Topic ... not present in metadata after 5000 ms`.
- **Error mapping quirk, Bolt:** the Neo4j driver surfaces the failure as `DatabaseException: java.lang.reflect.UndeclaredThrowableException`,
  which hides the cause (the connection was terminated); a real Neo4j would send a `Neo.TransientError`/`ServiceUnavailable` code that
  drivers know how to retry. Not looked into further.
- **Test artifacts, not Warp bugs:** the OpenSearch reads of `no such index [bo]` happen before the first write creates the index
  (the workload does not create it up front).
- **Configuration note:** `s3wire` refuses to start without `WARP_S3WIRE_CREDENTIALS` (deliberate: no unauthenticated S3). The
  first S3 run failed for exactly that reason. The test harness now also fails fast when a listener it asked for logs
  "failed to start" instead of waiting out the two-minute startup window.

## Not covered yet

The remaining Postgres-backed protocols (Redis, AMQP, Pub/Sub, GCS, Azure Blob/Queue/Table, Cosmos, Bigtable, Datastore, Firestore,
the AWS JSON services (SNS, Kinesis, KMS, SSM, STS, secrets), Gremlin, gRPC, MCP, A2A). Many of them have only unit tests, no
end-to-end test with a real client at all, so their workloads are being added in groups. Same caveats as the SQL results:
single runs, localhost, one replica, one Warp instance, no partitions.

Re-run: `WARP_TEST_BROWNOUT_PG_BIN=<postgres bin dir> mvn test -Dtest=StoreFailoverBrownoutLiveTest`
(`WARP_TEST_STORE_WORKLOADS=sqswire` and `WARP_TEST_BROWNOUT_SCENARIOS=switchover` narrow it).
