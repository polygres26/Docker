# Read replicas and failover-follow

> Technical reference. Status: **Postgres and MySQL**. Oracle and SQL Server replicas are accepted in
> the config but never used for reads, and are not monitored for failover, until their lag/role probes
> exist. Failover has two modes: `follow` (a promotion made by your database's own HA
> tooling is followed) and `promote` (Warp itself promotes a replica, behind the safeguards below).

## Configuring replicas

Replicas are the optional 5th and 6th `|`-fields of a `WARP_BACKENDS` entry (also settable through
`POST/PATCH /api/backend-sets/{set}/backends/{name}` with `replicas` and `failoverMode`):

```
name=url|user|password|fallback|replicaUrl~maxLagSeconds^replicaUrl2~maxLagSeconds|failoverMode
pg=jdbc:postgresql://p/db|app|secret||jdbc:postgresql://r1/db~5^jdbc:postgresql://r2/db~10|follow
```

- Replicas reuse the primary's user and password, are **not** registered backends, and do not count
  against the Developer-tier backend cap. `maxLagSeconds` defaults to 5.
- `failoverMode`: `follow` (default whenever replicas exist), `promote`, or `off`.
- `~` and `^` separate lag and replicas; `;` inside a URL is written `%3B` as for the primary.

## Read routing

A read goes to a replica only when **all** hold: it is a plain read (`SELECT` / `WITH … SELECT`; no
`FOR UPDATE/SHARE`, `SELECT INTO`, writable CTE, multi-statement string, or side-effect/session-local
function such as `nextval`, `set_config`, advisory locks, `last_insert_id`, `GET_LOCK`); it is not
inside a transaction; the session holds no connection-bound state (SET values, pins, open cursors);
the session has not written in the last `WARP_READ_AFTER_WRITE_WINDOW_MS` (default 2000); and a replica
is eligible. Otherwise it runs on the primary. If a replica errors, the read is retried on the primary.

A replica is eligible when its lag was sampled within ~3 sampling intervals, the probe confirmed it
is a replica, lag ≤ its `maxLagSeconds`, and it is not quarantined.

| Variable | Default | Meaning |
|---|---|---|
| `WARP_REPLICA_READ_ROUTING` | on | `false` keeps every read on its primary |
| `WARP_REPLICA_LAG_CHECK_SECONDS` | 5 | lag sampling period; `0` disables replica reads |
| `WARP_REPLICA_QUARANTINE_SECONDS` | 30 | pause after a connect/read-only failure |
| `WARP_READ_AFTER_WRITE_WINDOW_MS` | 2000 | read-your-writes window per session |

Postgres lag is `0` when the WAL receiver is `streaming` and everything received has been replayed;
otherwise it is `now() - pg_last_xact_replay_timestamp()`, which can only over-report (the safe side).

`GET /api/replicas` shows each replica's lag sample, eligibility, reads routed, and why reads stayed on
the primary.

## Failover-follow

For each backend with replicas and mode `follow`, every instance probes `pg_is_in_recovery()` on the
primary and each replica every `WARP_FAILOVER_PROBE_SECONDS` (default 5; `0` disables). Warp repoints
the backend at the replica that is now writable when **all** hold:

1. the configured primary has been non-writable (unreachable, or reachable but in recovery) for
   `WARP_FAILOVER_CONFIRM_PROBES` consecutive probes (default 3);
2. exactly one replica is writable, and has been for as many probes;
3. no switch for this backend happened in the last `WARP_FAILOVER_COOLDOWN_SECONDS` (default 60).

A writable replica while the primary is also writable, or two writable replicas while it is down, is
reported as a suspected split brain and **nothing changes**. The old primary becomes a replica entry
(taking the promoted node's slot and lag allowance); if it comes back in recovery it is used for reads
once its lag probe passes.

The change is one new `warp_config` version, written compare-and-set on the old primary URL, so
instances that reach the same decision collapse into one write, every other instance reloads it over
LISTEN/NOTIFY, and a restart starts from the right primary. If `warp_config` cannot be written (for
example the config database is the node that died) the switch is applied in memory on that instance
and re-applied across reloads until the config names the new primary.

`GET /api/failover` shows nodes, observed roles, streaks, last switch and recent events;
`POST /api/failover/{backend}/evaluate` forces an immediate evaluation (one observation instead of the
confirmation window; every other rule still applies). Switches and suspected split brains are written
to the audit log (`BACKEND_FAILOVER`, `BACKEND_SPLIT_BRAIN_SUSPECTED`).

## MySQL

Supported: asynchronous and semi-synchronous source/replica replication, with GTIDs or binary-log
coordinates, MySQL 5.7 through 9.x (`SHOW REPLICA STATUS`, falling back to `SHOW SLAVE STATUS` and the
legacy column names). The Warp user needs `REPLICATION CLIENT` for the probes and, for promote mode,
`REPLICATION_SLAVE_ADMIN` + `SYSTEM_VARIABLES_ADMIN` (`SUPER` before 8.0). Replica URLs follow the same
grammar; add `allowPublicKeyRetrieval`/SSL options to the URL as your authentication requires.

- **Lag:** `Seconds_Behind_Source`, and only while **both** replication threads are running; a replica
  with a stopped or connecting thread, or a NULL lag, is unmeasurable and never eligible. MySQL's
  number only covers events the replica has already *retrieved*: transactions committed on the primary
  but not yet fetched (a slow link) are invisible to it. Keep `maxLagSeconds` conservative.
- **Role:** writable only if `read_only` is off **and** the server is not configured as a replica of
  anything. A server still replicating is never called writable, even with `read_only` left off.
- **Promotion** (promote mode): stop the I/O thread, wait for the SQL thread to apply **everything
  already received** (`WARP_FAILOVER_MYSQL_APPLY_WAIT_SECONDS`, 60; if it cannot, Warp does not promote
  rather than drop received transactions), `STOP/RESET REPLICA ALL`, `super_read_only` and `read_only`
  off. Candidate ranking is by transactions received (executed + retrieved-but-unapplied GTIDs; binlog
  file/position without GTIDs).
- Warp does not repoint the remaining replicas (`CHANGE REPLICATION SOURCE`); they leave the read pool
  until their lag probe passes again.

## Promote mode

`failoverMode=promote` does everything `follow` does, and additionally promotes a replica when the
primary is confirmed down **and nothing is writable**. If some node is already writable (someone else
promoted) Warp follows it instead of promoting a second one. Warp promotes with `pg_promote()` (PG 12+;
the Warp user needs superuser or `EXECUTE` on it) and **refuses** unless all of these hold:

1. the full confirmation window has elapsed (a manual evaluate cannot shorten it) and the backend is
   outside its cooldown;
2. **majority:** more than half of the live Warp instances (those heartbeating in `warp_nodes`) report
   the same primary down within `WARP_FAILOVER_VOTE_FRESH_SECONDS` (30);
3. **lease:** this instance holds the per-backend lease in the config database
   (`warp_failover_lease`, atomic, expiry on the database clock, `WARP_FAILOVER_LEASE_SECONDS` 120,
   released afterwards) -- so only one instance promotes at a time;
4. **fence:** if `WARP_FAILOVER_FENCE_COMMAND` is set it is run with `FAILED_PRIMARY_URL` in its
   environment and must exit 0 (use it to power off / firewall the old primary); a failure aborts;
5. a fresh re-probe, after taking the lease, still shows no writable node;
6. the candidate is the reachable replica that **received the most WAL** (ties: the earlier one in the
   list) and it was within `WARP_FAILOVER_MAX_PROMOTE_LAG_SECONDS` (default 30; negative disables the
   gate) the last time its lag was measured **while the primary was still up** -- never measured means
   refused, because lag read after the outage includes the outage itself.

After promotion Warp requires the replica to report itself writable, then switches exactly like follow
mode (one `warp_config` version, old primary takes the promoted node's replica slot) and releases the
lease. If the config database is unreachable the majority and the lease cannot be established, so
**Warp does not promote**; there is deliberately no fallback decider.

Warp does not repoint the *other* replicas at the new primary (that needs replication-source changes
on those servers, i.e. your HA tooling); until that is done their lag probe fails and they are not used
for reads. When the old primary comes back writable while the new one is writable, Warp reports a
suspected split brain and changes nothing.

Without external fencing the majority rule is the only protection against promoting while the old
primary is merely partitioned away from Warp but still serving others. Configure
`WARP_FAILOVER_FENCE_COMMAND` if that can happen in your network.

## Known limits

- In-flight transactions and statements on the dead primary fail; clients retry. New statements go to
  the new primary.
- In `follow` mode every instance decides independently (redundant, idempotent). Promotion is the only
  action gated by the lease and majority.
- The config database is usually the default backend; its own failover is the existing
  `WARP_STANDBY_HOST` mechanism, not this one.
- `BackendHealthChecker` still marks a backend DOWN after one failed connect; that is independent of
  failover-follow, which needs a confirmed window.
- Relay mode and the orawire emulation path have no replica routing (no session-state predicate there).
