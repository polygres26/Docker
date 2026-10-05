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
  against the Developer-tier backend cap. The lag allowance is in **seconds** (decimals allowed, e.g.
  `0.5`), defaults to 5, and must be between 0 and **3600**; the server rejects anything else. `0` means
  "only when fully caught up". (Seconds, not milliseconds: MySQL, Oracle and SQL Server only report whole
  seconds, so a sub-second limit would be false precision there.)
- `failoverMode`: `follow` (default whenever replicas exist), `promote`, or `off`.
- `~` and `^` separate lag and replicas; `;` inside a URL is written `%3B` as for the primary.

## Admin UI

Infrastructure -> **Replicas and failover** shows, per backend with replicas: each node's observed role,
its lag against its allowance, why it is or is not eligible, reads served, the reasons reads stayed on
the primary, failover mode/confirmation/cooldown/last switch, and recent failover events; it refreshes
every 5 s. **Evaluate now** triggers a manual failover evaluation. **Edit replicas** (and **Add replicas**
for a backend without any) edits replica URLs, each replica's max lag in seconds (0-3600, validated in the
form and again by the server), and the failover mode; `promote` is greyed out for engines Warp does not
promote. Saves go through the backend-sets API and apply live. Replica URLs are shown with any embedded
credentials masked; saving a masked URL unchanged keeps the real one.

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
- Remaining replicas are repointed after a promotion: see "Repointing the other replicas" below. Only
  replicas using GTID auto-positioning are repointed; a replica on binlog file/offset coordinates is
  refused (Warp cannot know the matching position on the new primary) and left as it was.

## Oracle Data Guard (follow only, unverified)

**Verification status:** written to Oracle's documentation; Data Guard is not available in any edition
this project can run in tests, so only the decision logic (role, lag, interval parsing, the
"never promote" rule) is unit-tested. Treat it as untested against real Data Guard until you have
tried it in your environment.

- **Topology:** a physical standby opened **read-only with redo apply (Active Data Guard)**. The
  Warp user needs `SELECT` on `V_$DATABASE` and `V_$DATAGUARD_STATS`.
- **Lag:** the larger of the standby's *apply lag* and *transport lag* from `V$DATAGUARD_STATS`.
  Trusted only if the standby is open read-only, an apply-lag value exists, and its statistics were
  computed within the last 120 s (database clock); otherwise the replica is unmeasurable and never
  eligible. A standby that is only `MOUNTED` cannot serve reads and cannot be probed by an ordinary
  login.
- **Role:** writable only for a `PRIMARY` that is open `READ WRITE`. If the Warp user cannot read
  `V$DATABASE` the node is reported unreachable (and a warning is logged), never writable.
- **Failover:** **follow only.** Warp does *not* promote Oracle standbys -- a Data Guard failover loses
  data if mishandled and cannot be undone. Run it with the Data Guard Broker (Fast-Start Failover or
  `dgmgrl FAILOVER TO ...`); once the new primary is open `READ WRITE`, `follow` mode repoints the
  backend at it (this also works for a mounted standby, which becomes visible the moment it opens).
  `failoverMode=promote` is rejected by the API for Oracle, and a `promote` entry in `WARP_BACKENDS`
  is monitored but recorded as blocked.

## SQL Server Availability Groups (follow only)

**Verification status:** exercised live (`SqlServerAgLiveTest`) against SQL Server 2022 Developer in a
two-node **read-scale** AG (`CLUSTER_TYPE = NONE`, asynchronous commit, manual failover, readable
secondary): role and lag probes on the real DMVs, plain reads served by the secondary while a locking
read stays on the primary, a suspended secondary skipped (never reported as 0 lag) and restored after
resume, and the primary killed + `FORCE_FAILOVER_ALLOW_DATA_LOSS` run on the secondary, which Warp
followed after the confirmation window. **Not exercised:** Windows Server Failover Cluster or Pacemaker
clustered AGs, synchronous commit, AG listeners, more than one secondary, Azure SQL. Treat those as
untested.

- **Topology:** the backend URL names the availability-group database (`databaseName=...`); replica
  URLs point at **readable secondaries** and should carry `applicationIntent=ReadOnly`. Write `;` as
  `%3B` inside the `WARP_BACKENDS` spec. The Warp user needs `VIEW SERVER STATE` and connect rights on
  the database.
- **Role:** writable iff the database is `READ_WRITE` and, if it is in an AG, this replica is its
  primary (`sys.fn_hadr_is_primary_replica`). A non-readable secondary rejects ordinary logins, so it
  is invisible to the probes until it becomes the primary and starts accepting connections.
- **Lag** (from the secondary's local row of `sys.dm_hadr_database_replica_states`): `SYNCHRONIZED` is
  0; else the server's `secondary_lag_seconds` when present; else redo backlog ÷ redo rate (an empty
  backlog is 0). A suspended, unhealthy or otherwise unmeasurable database is never eligible, and is
  never reported as 0. A secondary cannot see log the primary has not yet shipped to it, so this can
  under-report on a slow link: keep `maxLagSeconds` conservative.
- **Failover:** follow only, exactly as for Oracle -- Warp does not perform AG failovers
  (`failoverMode=promote` is rejected). Use the cluster's automatic failover or
  `ALTER AVAILABILITY GROUP ... FAILOVER`; Warp repoints the backend once the new primary accepts
  writes. Azure Synapse is not supported.

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

When the old primary comes back writable while the new one is writable, Warp reports a
suspected split brain and changes nothing.

### Repointing the other replicas

After Warp promotes a replica, the others still follow the dead primary, so their lag probe fails and they
drop out of the read pool. Warp therefore repoints each remaining reachable read-only replica at the new
primary, one at a time; a failure on one never affects the others or undoes the promotion. Each outcome is
an event (`repointed`, `repoint-failed`, `repoint-skipped`) and `repointed`/`repoint-failed` are audited.
`WARP_FAILOVER_REPOINT=false` turns it off (replicas then wait for your HA tooling). It applies only to
promotions Warp performs; in `follow` mode whoever promoted is responsible for the other replicas.

- **Postgres:** reads the replica's `primary_conninfo`, replaces only `host` and `port` (replication user,
  password, sslmode... are kept), `ALTER SYSTEM`s it, reloads, and waits (`WARP_FAILOVER_REPOINT_WAIT_SECONDS`,
  30) until `pg_stat_wal_receiver` shows `streaming` from the new host. If it does not, the old setting is
  restored. If the replica has a `primary_slot_name`, that slot is created on the new primary when missing.
  Needs a superuser (or equivalent `ALTER SYSTEM` / `pg_read_all_settings` rights) on each replica.
- **MySQL:** `STOP REPLICA; CHANGE REPLICATION SOURCE TO SOURCE_HOST, SOURCE_PORT, SOURCE_AUTO_POSITION=1;
  START REPLICA` (stored replication user, password and TLS settings are kept), then waits for both threads
  running against the new host; otherwise restores the old source. Needs `REPLICATION_SLAVE_ADMIN`.
- **Limits:** the new host and port are taken from the new primary's JDBC URL, so replicas must be able to
  reach that same address (a JDBC address that differs from the replication-network address is not
  translated). A Postgres replica that is *ahead* of the promoted node (it received WAL the promoted one
  did not) cannot follow it; it is reported `repoint-failed` and needs a rebuild (`pg_rewind`/re-basebackup).
  The old primary is not repointed; it must be rebuilt as a replica before it can rejoin. Oracle and SQL
  Server are follow-only and never repointed by Warp.

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

## Running the live tests

All live tests are opt-in and skipped unless their environment is set; they run real servers, not mocks.

- **Postgres / MySQL:** `ReplicaReadRoutingLiveTest`, `FailoverFollowLiveTest`, `FailoverPromoteLiveTest`,
  `MySqlReplicaFailoverLiveTest` (`WARP_TEST_*` variables are documented in each class's javadoc; they need
  local Postgres 17 and MySQL 9 binaries and a throwaway Postgres for `warp_config`).
- **SQL Server Availability Group:** `Warp/tests/sqlserver-ag/ag.sh up` starts two SQL Server 2022 Developer
  containers (`sql1` primary on 14331, `sql2` readable secondary on 14332) in a read-scale AG with no cluster
  manager, then run `WARP_TEST_MSSQL_AG=1 mvn test -Dtest=SqlServerAgLiveTest`; `ag.sh down` removes them. The
  test performs the failover itself the way a DBA would (kills `sql1`, runs `FORCE_FAILOVER_ALLOW_DATA_LOSS` on
  `sql2`) because Warp only follows. (It has been run: it passes on SQL Server 2022 Developer.) **SQL Server
  cannot start under QEMU emulation** (it aborts with an
  address-space error), which is what a default Colima/Docker on Apple silicon uses; use Docker Desktop with
  Rosetta, or a Colima profile started with `--vm-type vz --vz-rosetta`. Needs about 4 GB of Docker memory.
- **Oracle Data Guard:** `OracleDataGuardLiveTest` needs a real physical standby open read-only with apply
  (Active Data Guard, Enterprise Edition) and has **never been run** by this project's authors. Set
  `WARP_TEST_ORACLE_DG_PRIMARY_URL`, `_STANDBY_URL`, `_USER`, `_PASSWORD` (the user needs `SELECT` on
  `V_$DATABASE` and `V_$DATAGUARD_STATS`), and optionally `WARP_TEST_ORACLE_DG_FAILOVER_CMD`, a shell command that
  fails the pair over (for example a `dgmgrl` script); without it the failover-follow part is skipped. Expect to
  adjust the test the first time it meets a real configuration.

