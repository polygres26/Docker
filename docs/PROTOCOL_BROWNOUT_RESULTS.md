# Brownout across the wire protocols (Postgres backend, Adapt mode, one streaming replica)

Measured with `ProtocolFailoverBrownoutLiveTest` on 2026-10-05, one run per scenario, on a laptop (macOS, Postgres 17,
everything on localhost). It is a measurement, not a benchmark suite: one run, no confidence intervals.

## Setup

- One Warp process serving **pgwire, mywire, mssqlwire and orawire at the same time** (all Adapt mode, Postgres
  backend `pg` with one streaming replica, `failoverMode=promote`). Clients select the backend by database/service
  name `pg`. A separate Postgres holds `warp_config` and the failover lease.
- Per protocol: 3 writer threads (`insert`) and 2 reader threads (`select count(*) ... where proto = ?`), one
  connection each, ~50 ops/s per thread, so about 1,000 ops/s in total. Real drivers: pgjdbc, MySQL Connector/J,
  mssql-jdbc, ojdbc11. A failed operation is **not retried**; the thread drops the connection, waits 100 ms and
  reconnects, then continues (what a pooled app does). Client timeouts: 5 s connect, 10 s socket.
- Failover tuning: probe every 1 s, 3 probes to confirm, cooldown 5 s. **The defaults are 5 s probes, so with defaults
  expect a longer stall (confirmation alone would be ~15 s); that was not measured.**
- 12 s of steady state, then the event, then 25 s (switchover) or 45 s (failover) more.
- "Lost" = an acknowledged write that is missing from whichever node is the primary afterwards.

## Planned switchover (`POST /api/failover/pg/switchover`, 141 ms for the API call)

| protocol | writes ok | writes failed | steady max gap (ms) | max gap around the event (ms) | reads failed | acked writes lost |
|---|---|---|---|---|---|---|
| pgwire | 4615 | 3 | 25 | 141 | 0 | 0 |
| mywire | 4600 | 3 | 27 | 148 | 0 | 0 |
| mssqlwire | 4620 | 3 | 27 | 158 | 0 | 0 |
| orawire | 4562 | 6 | 26 | 177 | 2 | 0 |

The write stall is about 140-180 ms (the longest pause between two successful writes), against a normal ~25 ms.
Each protocol lost 1-3 in-flight writes: one "cannot execute INSERT in a read-only transaction" while the old primary
is frozen, and the client connections Warp terminates (`terminating connection due to administrator command`, or the
protocol's equivalent: MySQL "Lost connection", SQL Server "Communication link failure", Oracle ORA-03113).

## Unplanned failover (primary crashed with `pg_ctl -m immediate`)

| protocol | writes ok | writes failed | steady max gap (ms) | max gap around the event (ms) | reads failed | acked writes lost |
|---|---|---|---|---|---|---|
| pgwire | 6464 | 4 | 27 | 5161 | 0 | 0 |
| mywire | 6457 | 6 | 39 | 5158 | 0 | 0 |
| mssqlwire | 6466 | 6 | 46 | 5293 | 0 | 0 |
| orawire | 6420 | 11 | 39 | 5173 | 4 | 0 |

Writes stall for **about 5.2 s** on every protocol (3 probes x 1 s to confirm, then lease, WAL ranking and promotion,
then the first write after it). Reads did not stall (max gap 30-130 ms) because they are served from the replica
until it is promoted.

## What this shows

- **No acknowledged write was lost** in either scenario, on any protocol (also no unacknowledged rows).
- **Switchover is nearly invisible** (sub-200 ms stall); failover costs the detection window plus about 2 s.
- **All four protocols behave alike** at the stall level. The error text differs per protocol, as expected.
- **Read scaling is gone after either event.** The old primary cannot be turned into a replica by Warp on Postgres,
  so afterwards every read goes to the new primary: `no_eligible_replica` decisions rose to 8,125 (switchover) and
  13,556 (failover) by the end of the run, while `routed_to_replica` stayed flat. The old primary has to be rebuilt as a
  standby before replica reads return.

## Problems found (not fixed here)

1. **orawire throws `ArrayIndexOutOfBoundsException` after the event.** `TtcReader.readUint8` overruns while parsing a
   fetch request (`FetchRequest.read` <- `RequestLoop.handleData`), surfacing to the client as ORA-00600. It hit 2-3
   reads and 3-6 writes in both scenarios (the other protocols had no equivalent), and one orawire failure came about
   19 s (switchover) and 43 s (failover) after the event: a session desynchronizes and fails later. Not diagnosed;
   likely a client request arriving on a session whose previous statement was interrupted by the backend connection
   being killed.
2. **mywire rejected 2 reconnects with "Access denied for user 'warp'"** in the failover run (5-44 per run in an earlier,
   rate-limited run). The credentials were right; the check in `MySqlWireSessionHandler` is deterministic, so this looks
   like a race or stale state during reconnect. Not diagnosed.
3. An earlier run at the harness's default admission limit (1,000 requests per second) was discarded: it showed
   hundreds of "rate limit exceeded" / cancelled statements per protocol that had nothing to do with failover. These
   results use a raised limit and a paced load.

## Not covered

Other Warp protocols (Mongo, DynamoDB, Cassandra, Kafka, ...), multiple Warp instances (lease contention), network
partitions or fencing, more than one replica, MySQL/SQL Server/Oracle backends, prepared statements with binds, and
default failover timings. Re-run with `WARP_TEST_BROWNOUT_PG_BIN=<postgres bin dir> mvn test
-Dtest=ProtocolFailoverBrownoutLiveTest`; the full report is written to `Warp/target/protocol-brownout-report.md`.
