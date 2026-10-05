# Postgres-backed protocols under switchover and failover (phases 1-4: 25 protocols, listed per phase below)

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

## Phase 3: Redis, Google Cloud Storage, Azure Blob, Pub/Sub, Firestore, Datastore, Cosmos DB

Same setup and caveats, but **driven with my own small clients, not the vendors' SDKs** (their SDKs are not on the test classpath): a
RESP socket client for Redis, and plain `java.net.http` REST/JSON calls following each service's public API for the others (GCS JSON
API, Azure Blob REST with a static bearer token, Pub/Sub REST, Firestore and Datastore REST, Cosmos REST with auth disabled). Real SDK
traffic can differ in headers, retries and request shapes, so treat these as protocol-level results.

| protocol | scenario | writes ok | writes failed | steady max gap ms | max gap around event ms | failed window | reads ok | reads failed | acked | lost | extra |
|---|---|---|---|---|---|---|---|---|---|---|---|
| rediswire | switchover | 4597 | 3 | 26 | 150 | 3 ms (recovered +26) | 3096 | 1 | 4597 | 0 | 0 |
| rediswire | failover | 6450 | 6 | 27 | 5278 | 5146 ms (recovered +5170) | 4333 | 4 | 6450 | 0 | 0 |
| gcswire | switchover | 4427 | 3 | 32 | 154 | 3 ms (recovered +30) | 3053 | 2 | 4427 | 0 | 0 |
| gcswire | failover | 6144 | 6 | 32 | 5309 | 5139 ms (recovered +5182) | 4264 | 4 | 6144 | 0 | 0 |
| azblobwire | switchover | 4382 | 3 | 33 | 138 | 24 ms (recovered +30) | 3045 | 2 | 4382 | 0 | 0 |
| azblobwire | failover | 6140 | 2 | 41 | 5031 | 1 ms (recovered +4919) | 4269 | 1 | 6140 | 0 | 0 |
| pubsubwire | switchover | 4493 | 3 | 28 | 145 | 8 ms (recovered +25) | 3052 | 2 | 4493 | 0 | 0 |
| pubsubwire | failover | 6310 | 5 | 30 | 5168 | 5137 ms (recovered +5150) | 4283 | 4 | 6310 | 0 | 0 |
| firestorewire | switchover | 4428 | 3 | 27 | 155 | 5 ms (recovered +29) | 3035 | 2 | 4428 | 0 | 0 |
| firestorewire | failover | 5547 | 5 | 43 | 5187 | 5156 ms (recovered +5171) | 3815 | 3 | 5547 | 0 | 0 |
| datastorewire | switchover | 4033 | 3 | 32 | 147 | 0 ms (recovered +24) | 2740 | 0 | 4033 | 0 | 0 |
| datastorewire | failover | 5619 | 5 | 32 | 5181 | 5157 ms (recovered +5177) | 3814 | 3 | 5619 | 0 | 0 |
| cosmoswire | switchover | 4043 | 3 | 32 | 162 | 2 ms (recovered +28) | 2695 | 2 | 4043 | 0 | 0 |
| cosmoswire | failover | 5627 | 6 | 73 | 5297 | 5153 ms (recovered +5173) | 3673 | 4 | 5627 | 0 | 0 |

- **All seven follow the failover on their own and lost no acknowledged write**, with the same shape as everything else: about
  140-160 ms stall on a switchover, about 5.0-5.3 s on a crash.
- **Azure Blob behaves differently on a crash:** only 2 writes failed (against 5-6 for the others) yet writes still stalled for 5 s,
  so most requests during the outage were held until the new primary was ready instead of being answered with an error.
- **Redis:** the RESP clients saw errors as `-ERR` replies from the store; none of the 14 cells needed a restart or reconnect to
  Warp's listener beyond the harness's normal drop-and-reopen after a failed call.
- Mistakes in my first versions of these workloads (not Warp): Azure container names under 3 characters, Pub/Sub topic ids under 3
  characters, Cosmos results read only up to the first page (which looked like 991 lost writes until the page size was raised),
  and Azure needing `WARP_AZURE_DEV_ACCOUNT=true` (Warp refuses to serve unauthenticated).

## Phase 4: Azure Queue and Table, Kinesis, SSM, Secrets Manager, SNS

Driven like phase 3 with my own HTTP clients following each public API (Azure Queue/Table with the static bearer token, AWS JSON 1.1
for Kinesis, SSM and Secrets Manager, the Query protocol for SNS; no SigV4 signing since Warp does not require it unless
`WARP_AWS_IAM_CREDENTIALS` is set). Secrets Manager needs `WARP_KMS_INSECURE_DEV_KEY=true` to seal its values.

| protocol | scenario | writes ok | writes failed | steady max gap ms | max gap around event ms | failed window | reads ok | reads failed | acked | lost | extra |
|---|---|---|---|---|---|---|---|---|---|---|---|
| azqueuewire | switchover | 4509 | 3 | 39 | 142 | 15 ms (recovered +23) | 2981 | 1 | 4509 | 0 | 0 |
| azqueuewire | failover | 6370 | 2 | 27 | 5033 | 5009 ms (recovered +5016) | 4134 | 1 | 6370 | 0 | 0 |
| aztablewire | switchover | 4530 | 3 | 33 | 144 | 11 ms (recovered +28) | 3055 | 0 | 4530 | 0 | 0 |
| aztablewire | failover | 6391 | 3 | 27 | 5176 | 5 ms (recovered +41) | 4302 | 2 | 6391 | 0 | 0 |
| kinesiswire | switchover | 4542 | 3 | 26 | 152 | 4 ms (recovered +27) | 3060 | 2 | 4542 | 0 | 0 |
| kinesiswire | failover | 6358 | 6 | 26 | 5286 | 5148 ms (recovered +5174) | 4286 | 4 | 6358 | 0 | 0 |
| ssmwire | switchover | 4519 | 3 | 32 | 135 | 14 ms (recovered +26) | 3072 | 2 | 4519 | 0 | 0 |
| ssmwire | failover | 6344 | 6 | 27 | 5282 | 5139 ms (recovered +5164) | 4310 | 3 | 6344 | 0 | 0 |
| secretswire | switchover | 4514 | 3 | 27 | 154 | 1 ms (recovered +28) | 3076 | 1 | 4514 | 0 | 0 |
| secretswire | failover | 6317 | 6 | 53 | 5302 | 5143 ms (recovered +5178) | 4308 | 4 | 6317 | 0 | 0 |
| snswire | switchover | 4586 | 3 | 26 | 150 | 1 ms (recovered +22) | 3079 | 1 | 4586 | 0 | 0 |
| snswire | failover | 6420 | 5 | 26 | 5171 | 5138 ms (recovered +5158) | 4299 | 4 | 6420 | 0 | 0 |

- **All six follow the failover on their own and lost nothing**, same shape as before: ~135-155 ms on a switchover, ~5.0-5.3 s on a crash.
- **The three Azure frontends (Blob, Queue, Table) share the "hold, don't fail" behaviour on a crash**: they fail only 2-3 writes
  (against 5-6 for the rest) yet stall for the full 5 s, i.e. requests wait for the new primary. Azure Table's failed window was
  5 ms with a 5.2 s gap, the clearest case.
- Mistake in my first Azure Table version (not Warp): it read back only the first page of 1,000 entities and reported 107 writes as
  lost; it now follows the `x-ms-continuation-*` headers.
- Not benchmarked, deliberately: KMS and STS (no data worth losing: KMS is exercised through Secrets Manager's sealing, STS is
  stateless).

## Not covered yet

The remaining Postgres-backed protocols: AMQP, Gremlin, Bigtable, and Warp's own gRPC, MCP and A2A APIs (all need a custom or
generated client rather than plain HTTP). Many of them have only unit tests, no
end-to-end test with a real client at all, so their workloads are being added in groups. Same caveats as the SQL results:
single runs, localhost, one replica, one Warp instance, no partitions.

Re-run: `WARP_TEST_BROWNOUT_PG_BIN=<postgres bin dir> mvn test -Dtest=StoreFailoverBrownoutLiveTest`
(`WARP_TEST_STORE_WORKLOADS=sqswire` and `WARP_TEST_BROWNOUT_SCENARIOS=switchover` narrow it).
