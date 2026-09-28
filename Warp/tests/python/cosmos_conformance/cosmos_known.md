# cosmoswire: what is verified, what is not, known divergences

## How this was verified (and what was NOT)

**Behaviour was NOT compared with a real Azure Cosmos DB service or the official emulator.** The Linux emulator image crashes on this arm64
host under x86 emulation (verified earlier) and was not retried. Everything below rests on three things:

1. **The official `azure-cosmos` Python SDK 4.17.1 against Warp** (`test_cosmos_conformance.py`): account discovery, database/container
   lifecycle, item CRUD, ETag/If-Match, patch, transactional batch, change feed, TTL, unique keys, hierarchical keys (full keys), read feed,
   queries (single and cross-partition, continuation paging), throughput offers, stored procedure listing, and the query-plan / pkranges
   handshake (with `WARP_COSMOSWIRE_QUERY_PLAN_HANDSHAKE=true`). The .NET, Java, Node and Go SDKs were **not** run.
2. **Expected values written by hand from the published documentation as I recalled it** (the pages were not re-fetched while writing):
   REST API reference (create/replace/delete/get document, batch, change feed, query, pkranges, access control) and the NoSQL query language
   reference (SELECT, FROM/JOIN, WHERE, ORDER BY, GROUP BY, OFFSET LIMIT, subqueries, operators, system functions with their example values).
   Tests cite the section in their docstrings. A wrong recollection would be a wrong test.
3. **Randomized differential testing** (`cosmos_diff_engine.py`, 3 seeds x 60 queries over 90 random documents with mixed types): an
   independent Python implementation of the semantics. It was written by the same author from the same understanding, so it catches
   coding errors, not misreadings of the documentation.

## Behaviours that rest on documentation / recollection only (could be wrong)

* Cross-type comparison: `=` between defined values of different types is `false`, `!=` is `true`; `<`, `<=`, `>`, `>=` across types (and on
  arrays/objects) are `undefined`. Any comparison with an undefined operand is undefined; WHERE keeps only `true`.
* `undefined` sorts first in ORDER BY (`undefined < null < boolean < number < string < array < object`), as requested. **The real service
  is believed to exclude documents where the ORDER BY expression is undefined** (the sort is index based); set
  `WARP_COSMOSWIRE_ORDERBY_EXCLUDE_UNDEFINED=true` to drop them. Arrays and objects are ordered by canonical JSON text (the service does not
  order them).
* Aggregates: `SUM`/`AVG`/`MIN`/`MAX` over zero defined values are `undefined` (no row for `SELECT VALUE`); `SUM`/`AVG` are undefined when any
  defined value is not a number; `COUNT(expr)` counts defined values (null counts). `COUNT(*)` is a syntax error.
* Projection names: a property path uses its last segment, an identifier its own name, anything else `$1`, `$2`, ...
* Division or modulo by zero is `undefined`; bitwise operators work on 32-bit integers; `ROUND` rounds half away from zero.
* `ST_DISTANCE` uses a sphere of radius 6,378,137 m; `ST_WITHIN`/`ST_INTERSECTS` only handle Point-in-Polygon (planar ring test) and Point/Point.
* The synthetic `x-ms-request-charge` (payload-size derived; `WARP_COSMOSWIRE_RU` fixes it), session tokens (`0:1#lsn#1=lsn`) and error
  message texts imitate the service but are not its values.
* `x-ms-max-item-count` default 100 and 1000 for `-1`; a page is also cut at about 3.5 MB.
* The account document, offers (`content.offerThroughput`, default 400) and pkranges shapes follow the SDKs' expectations, not captured traffic.

## Known divergences (by design or unfinished)

* **JavaScript is never executed.** Stored procedures, triggers and UDFs can be created, listed, read, replaced and deleted; executing a stored
  procedure answers 501, using a pre/post trigger header answers 501, `udf.f()` in a query answers 400.
* **Query plan**: there is one partition key range (`0`, `["", "FF")`) and the plan reports an *empty* `queryInfo` (no orderBy, aggregates,
  distinct, top/offset/limit, no rewritten query) because the server already applies every clause to the whole result; only `hasSelectValue`
  is set. This is a valid plan for a single range and lets the SDKs pass results through untouched. Real plans describe the clauses so that
  the SDK can merge many ranges; that is not implemented. By default cross-partition queries are answered directly (SDKs that ask for a plan
  first still get it); the "first chance" 400/1004 handshake of the real gateway is opt-in (`WARP_COSMOSWIRE_QUERY_PLAN_HANDSHAKE=true`). Verified
  with the Python SDK only; the .NET and Java SDKs also read `queryEngineConfiguration` from the account document and may expect more.
* **One partition key range**: `pkranges` never splits; `x-ms-start-epk`/`x-ms-end-epk` requests (feed ranges, and the SDKs' hierarchical
  partition key *prefix* queries) are refused with 501. Prefix queries work when the prefix is sent in `x-ms-documentdb-partitionkey`
  (raw REST). Change feed by feed range is likewise unsupported; per partition key and whole-container feeds work.
* **Change feed** is the incremental ("latest version") feed: deletes and TTL expiries do not appear; there is no full-fidelity mode. The
  continuation (`etag`) is an opaque token of per-backend `_lsn`; `_lsn` is per container and per backend, so order is guaranteed within a
  partition key (one backend) and is `_lsn`-then-backend across backends.
* **Indexing policy** is stored and returned but not used: every query is a scan (filtered by partition key / id equality). ORDER BY on
  several properties does not need a composite index; ORDER BY / queries never fail for lack of an index.
* **Cross-partition ORDER BY / GROUP BY / aggregate / DISTINCT** are computed in Warp over the documents gathered from all backends (correct,
  but the working set is held in memory: capped by `WARP_COSMOSWIRE_MAX_MATERIALIZE_DOCS`, default 1,000,000). Continuation of such queries
  re-uses a cached result (60 s) and re-executes on a cache miss. Simple queries stream with a keyset continuation.
* `x-ms-documentdb-query-enablecrosspartition: false` is not enforced (a cross-partition query is served anyway); no RU throttling (429 is never
  returned); no partition split/merge; consistency headers are accepted and ignored (there is one copy of the data).
* Unique keys are enforced per logical partition on writes made through this frontend; changing a container's `uniqueKeyPolicy` or partition
  key after creation is refused (the service also refuses it).
* Documents are addressed by name; `_rid`-addressed databases and containers work, `_rid`-addressed documents do not. Users, permissions,
  client encryption keys, attachments, conflicts (always empty), materialized views, vector/full-text/hybrid search and computed properties
  (stored, not evaluated) are not implemented. Auth: master key (the emulator's well-known key by default, `WARP_COSMOSWIRE_KEYS`); resource
  tokens and AAD bearer tokens are matched against allow-lists only (`WARP_COSMOSWIRE_RESOURCE_TOKENS`, `WARP_COSMOSWIRE_AAD_TOKENS`), never validated.
* HTTP only (no TLS; the SDKs accept an `http://` endpoint with a key, the emulator's HTTPS certificate handling is not reproduced).
* SQL not implemented: `HAVING`-less by design (like the service); `ORDER BY RANK`, `VectorDistance`, `FullTextScore`, `LIKE ... ESCAPE` only basic,
  `DateTimeBin`, `ObjectToArray`, `StringJoin`, `CountIf`, `MakeList/MakeSet`, `Choose`, `IS_*` beyond the common set, `SUBSTRING` with a
  negative length, `TOSTRING` of large numbers, ISO 8601 durations, `x IN (SELECT ...)`.
* Numbers are IEEE-754 doubles for comparison (like the service) but stored text keeps their original form.
* Placement: the database and container catalog live on the first backend; a route by database name (connect-time routing) is not applied.
