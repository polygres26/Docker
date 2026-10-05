# Read replicas and failover-follow

> Technical reference. Status: **Postgres only** so far. MySQL, Oracle and SQL Server replicas are
> accepted in the config but never used for reads, and are not monitored for failover, until their
> lag/role probes exist. Warp does **not** promote replicas; it follows a promotion made by your
> database's own HA tooling.

## Configuring replicas

Replicas are the optional 5th and 6th `|`-fields of a `WARP_BACKENDS` entry (also settable through
`POST/PATCH /api/backend-sets/{set}/backends/{name}` with `replicas` and `failoverMode`):

```
name=url|user|password|fallback|replicaUrl~maxLagSeconds^replicaUrl2~maxLagSeconds|failoverMode
pg=jdbc:postgresql://p/db|app|secret||jdbc:postgresql://r1/db~5^jdbc:postgresql://r2/db~10|follow
```

- Replicas reuse the primary's user and password, are **not** registered backends, and do not count
  against the Developer-tier backend cap. `maxLagSeconds` defaults to 5.
- `failoverMode`: `follow` (default whenever replicas exist) or `off`. `promote` is rejected for now.
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

## Known limits

- In-flight transactions and statements on the dead primary fail; clients retry. New statements go to
  the new primary.
- Every instance decides independently; there is no cluster-wide decider yet (it arrives with promote
  mode, where disagreement would be unsafe rather than redundant).
- The config database is usually the default backend; its own failover is the existing
  `WARP_STANDBY_HOST` mechanism, not this one.
- `BackendHealthChecker` still marks a backend DOWN after one failed connect; that is independent of
  failover-follow, which needs a confirmed window.
- Relay mode and the orawire emulation path have no replica routing (no session-state predicate there).
