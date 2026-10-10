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

**Which sessions get replica reads:** a client that selects a backend by database name (connect to database
`pg` to use backend `pg`) or is routed to it by a router rule, and, when no `WARP_BACKENDS` is configured,
a session on the implicit `WARP_*` backend if `WARP_REPLICAS` is set (same grammar as the 5th
`WARP_BACKENDS` field: `url[~maxLagSeconds][^url...]`). Replicas of the implicit backend are env-only (it is
not a backend set member, so the UI shows them read-only in a "Default backend" card and cannot edit them) and have **failover off**: there is no config entry to
switch. On that default connection a replica read additionally requires that the session holds no
`SET`/pin/open-transaction state and is anonymous: a session with a real per-user identity always reads the
primary, because its connection carries that user's role and RLS settings. A session on the implicit
default connection when `WARP_BACKENDS` *is* configured has no replicas and reads the primary. Relay mode
never routes (it is a raw-byte proxy); Oracle Bridge's server is not a registered backend.
Both forms were verified end to end with a pgwire client, and the named-backend form with a MySQL client through
mywire against a MySQL primary and replica, and with a SQL Server client through mssqlwire against a read-scale
availability group (`WARP_TEST_WIRE_MSSQL_AG=1` after `ag.sh up`) (`ReplicaWireRoutingLiveTest`; the mywire test needs
`WARP_TEST_WIRE_MY_PORTS` too). Note that mywire answers `select @@port`-style system-variable reads itself, so
they say nothing about which backend served a read.

A replica is eligible when its lag was sampled within ~3 sampling intervals, the probe confirmed it
is a replica, lag ≤ its `maxLagSeconds`, and it is not quarantined.

| Variable | Default | Meaning |
|---|---|---|
| `WARP_REPLICA_READ_ROUTING` | on | `false` keeps every read on its primary |
| `WARP_REPLICA_LAG_CHECK_SECONDS` | 5 | lag sampling period; `0` disables replica reads |
| `WARP_REPLICA_QUARANTINE_SECONDS` | 30 | pause after a connect/read-only failure |
| `WARP_READ_AFTER_WRITE_WINDOW_MS` | 2000 | read-your-writes window per session |
| `WARP_READ_YOUR_WRITES` | off | `true` replaces the fixed window with a per-session log-position check (below) |

**Read-your-writes by log position (`WARP_READ_YOUR_WRITES=true`).** The fixed window is wrong both ways: a replica lagging by more than the
window serves a read that cannot see the session's own write (up to the replica's `maxLag`, 10 s in the examples), and a replica that caught up long
ago is still avoided. With the flag on, right after a session's write Warp records the primary's log position (after commit for a transaction), and a
read goes to a replica only once that replica has **applied** the log up to it; a replica that has done so is remembered until the session's next
write, so the check costs one small query per replica per write, not per read. Positions: Postgres `pg_current_wal_lsn()` against
`pg_last_wal_replay_lsn()`; MySQL binary log file and position against `Relay_Source_Log_File`/`Exec_Source_Log_Pos` (not GTIDs, so it needs the
default file-based coordinates, which every source has); SQL Server `last_commit_lsn` on both nodes (`last_redone_lsn` does not work: it trails the
primary's hardened LSN even when fully caught up); Oracle the database's `current_scn` on both. When an engine returns no position or the capture
fails, that write falls back to the time window, so routing is never less safe than without the flag. A token is dropped when the backend's primary
changes. It costs one extra query on the primary after each write of a session that uses a replicated backend. Live: Postgres end to end (replica
replay paused for longer than the window: the control run read its stale replica and missed its own row, the flagged run read its own write, and
reads returned to the replica after it caught up); MySQL and SQL Server position probes against a stopped/suspended replica and after catch-up.
**Not run:** Oracle positions (no standby available), MySQL and SQL Server end to end through Warp, and chained or multi-source MySQL replication.

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

## Oracle Data Guard (follow only by default, unverified)

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

### Optional: driving Data Guard from Warp (`WARP_ORACLE_DATAGUARD_CONTROL=true`) -- **never run against a real Data Guard**

Off by default. Switched on, Warp can perform a **planned switchover** and an automatic **failover** of a physical standby with SQL, so Oracle gets
`promote` mode and the switchover API like the other engines. **This has been written to Oracle's documentation and tested only against a scripted fake
database** (it enforces the documented preconditions of each statement, so it checks Warp's sequence, preflight and error handling; it says nothing
about whether the statements behave so on a real configuration). No Oracle edition with Data Guard was available. Rehearse it in staging before relying on it.

Requirements: a SYSDBA account (the backend user, e.g. `sys as sysdba`; the administrative connections set `internal_logon=sysdba`); **no Data Guard
Broker** on either database (`DG_BROKER_START=FALSE`; behind the broker's back it loses track, so Warp refuses and says to use `dgmgrl SWITCHOVER`); redo
transport already configured in both directions; the standby open read only with redo apply for Warp to see it as a replica.

- **Switchover** (`POST /api/failover/{backend}/switchover`): preflight (primary is a PRIMARY with `SWITCHOVER_STATUS` `TO STANDBY` or `SESSIONS ACTIVE`, the
  target is a physical standby, no broker; nothing is changed if any of that is wrong), then `ALTER DATABASE COMMIT TO SWITCHOVER TO PHYSICAL STANDBY WITH
  SESSION SHUTDOWN` on the primary (it ends every session, flushes and sends the end-of-redo marker, so nothing can write to it), wait until the standby's
  `SWITCHOVER_STATUS` is `TO PRIMARY` (starting redo apply if it reports `RECOVERY NEEDED`), `ALTER DATABASE COMMIT TO SWITCHOVER TO PRIMARY WITH SESSION
  SHUTDOWN` on the standby and `ALTER DATABASE OPEN` if it is only mounted, and finally the old primary is opened and redo apply started on it. An abort before
  the standby took over switches the old primary back (starting redo apply and waiting for it to report it can become the primary first); if that does not
  work in time Warp **says so** (the old primary is a standby and there is no primary) rather than reporting writes re-enabled.
- **Failover** (`promote` mode, primary down): the usual safeguards (confirmation window, lease, majority, lag gate, optional fence), the standby with the highest
  received change number (`V$ARCHIVED_LOG` `NEXT_CHANGE#`) is chosen, then `ALTER DATABASE FAILOVER TO <db_unique_name>` (12.1 and later) and
  `ALTER DATABASE OPEN`. A failover discards the redo the standby did not receive and **cannot be undone**: the old primary has to be reinstated
  (Flashback Database) or rebuilt, which Warp does not do, and Warp has no signal that a standby still hears from the primary, so the replica veto of
  the split-brain guards does not apply to Oracle (use the fence command if the primary may still be reachable by others).
- Not done: other standbys are not repointed (a standby follows its primary through the Data Guard configuration), a returned old primary is not
  rejoined, and a stale second primary cannot be stopped with SQL.

### Oracle: fencing a stale primary and rejoining it

`fenceStaleWriter` (used when a failover left an old primary writable) runs `ALTER SYSTEM ENABLE RESTRICTED SESSION` in the container the URL names and then ends every
other user session, so applications cannot write; logins are refused with ORA-01035, which `writesFrozen` recognises, so the node is treated as read-only and not as a
second writer. It needs a SYSDBA account (`WARP_ORACLE_FENCE_USER` / `WARP_ORACLE_FENCE_PASSWORD`, else the backend user) and, for Warp's own traffic to be stopped too, a
backend user **without** the RESTRICTED SESSION privilege (a DBA has it): after restricting, Warp tries an ordinary login and, if it still works, lifts the restriction
and reports why. It restarts nothing and is lifted with `ALTER SYSTEM DISABLE RESTRICTED SESSION`. `rejoin` finds the SCN the new primary became primary at, passes it to
`WARP_FAILOVER_REJOIN_COMMAND` (`WARP_REJOIN_STANDBY_BECAME_PRIMARY_SCN`, and `WARP_REJOIN_DIVERGED=true` when the old primary committed past it, which the flashback
discards), fences the node while the command runs, and waits for the node to be a physical standby with redo apply running; with no command it answers `NEEDS_REBUILD` with
the `REINSTATE DATABASE` / flashback-and-convert steps. Tested against a scripted fake (statement order, that only `sid,serial#` text reaches a `KILL SESSION`, the separate
fence account, the rejoin decisions). The fence against a real Oracle Free 23ai is covered by `OracleFenceLiveTest`; **the rejoin has never run against a real Data Guard**.

## SQL Server Availability Groups (follow, promote and planned switchover)

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
- **Failover, follow mode:** use the cluster's automatic failover or `ALTER AVAILABILITY GROUP ... FAILOVER`;
  Warp repoints the backend once the new primary accepts writes.
- **Failover, promote mode:** Warp runs `ALTER AVAILABILITY GROUP ... FORCE_FAILOVER_ALLOW_DATA_LOSS` on the
  best secondary (ranked by `last_hardened_lsn`) after the usual safeguards (confirmation window, lease,
  majority, lag gate, optional fencing). **Only for `CLUSTER_TYPE = NONE`** (read-scale AGs with no cluster
  manager); an AG owned by WSFC or Pacemaker is refused, because forcing it behind the cluster's back risks two
  primaries. A forced failover can lose transactions the secondary had not hardened; the lag gate limits that.
  **Survivors:** after the forced failover the other secondaries stay suspended, so Warp runs
  `SET (ROLE = SECONDARY)` then `SET HADR RESUME` on each (repeated until it works, because a resume issued right after
  the failover can be accepted and do nothing) and waits for it to synchronize from the new primary. Verified live on
  a three-node AG.

- **Planned switchover** (read-scale AG, `CLUSTER_TYPE = NONE`): `POST /api/failover/{backend}/switchover`. Warp puts the primary's and the target's
  replicas in synchronous commit, requires one synchronized secondary to commit (`REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT = 1`) and waits for the target
  to be `SYNCHRONIZED` (`WARP_SWITCHOVER_SYNC_SECONDS`, 30), so a commit is acknowledged only after the target has hardened it. It then runs
  `FORCE_FAILOVER_ALLOW_DATA_LOSS` on the target (which loses nothing, being synchronized), puts the required-secondaries setting back at once (the new primary
  has no synchronized secondary yet and would refuse access with error 988), resumes the old primary, which the group has made a secondary, and restores the
  original availability modes. There is no separate "stop writes" step on SQL Server: `SET (ROLE = SECONDARY)` on a live primary fails with error 41104 and
  changes nothing (a finding that overturns the earlier note here that demotion "happens" despite the error). A transaction that was still waiting for its
  acknowledgement when the target took over is rolled back, which its client sees as an error. If the target never synchronizes, nothing changes and the
  modes are restored. Verified live on SQL Server 2022, there and back under write load (every acknowledged write present, about 2-3 s per switchover) and with
  an aborted attempt.
- **Old primary after a crash failover.** When its instance starts, a replica of a group with no cluster manager brings itself online as **primary** (the
  error log says it is "preparing to transition to the primary role"), so it is always a *second primary* that takes writes, and **no T-SQL demotes it**:
  `SET (ROLE = SECONDARY)` fails with 41104 and changes nothing, even after `OFFLINE`. What works (verified live): take its group offline
  (`ALTER AVAILABILITY GROUP ... OFFLINE`, which stops its writes and leaves it RESOLVING), restart the instance (it then joins as a secondary), and run
  `ALTER DATABASE ... SET HADR RESUME`, which **discards whatever it committed that the new primary lacks**. Warp cannot restart an instance, so it does this
  only when `WARP_FAILOVER_REJOIN_COMMAND` is set to a command that restarts it (the same variable the Postgres rebuild uses; the command gets
  `WARP_REJOIN_NODE_URL`, `WARP_REJOIN_PRIMARY_URL`, `_HOST`, `_PORT` and `_USER` in its environment): Warp takes the group offline, runs it, waits for the node to
  return as a secondary and resumes it. Verified live (container killed, survivor promoted by Warp, old primary restarted, offline, restarted by the
  command, resumed, holding the new primary's rows). Without the command Warp reports `rejoin-needed` with these steps, and the stale-writer guard takes the node
  offline. Not exercised: clustered AGs (refused by design), synchronous commit as a standing configuration, Azure Synapse (not supported).

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
4. **fence:** every configured fencer must succeed, in this order, or the promotion is aborted: `WARP_FAILOVER_FENCE_COMMAND` (shell, with
   `FAILED_PRIMARY_URL` in its environment, exit 0), `WARP_FAILOVER_FENCE_WEBHOOK` (POSTs `{"event":"failover-fence","failedPrimaryUrl":<password-masked>,
   "host":..,"port":..}`, any 2xx; optional `WARP_FAILOVER_FENCE_WEBHOOK_TOKEN` as a bearer token), `WARP_FAILOVER_FENCE_SSH_TARGET` (e.g. `root@{host}`) with
   `WARP_FAILOVER_FENCE_SSH_COMMAND` (BatchMode SSH; extra options in `WARP_FAILOVER_FENCE_SSH_OPTS`) and `WARP_FAILOVER_FENCE_EXEC` (an argument vector run
   without a shell, with `{host}`, `{port}`, `{url}` placeholders, e.g. `docker -H ssh://{host} stop pg1`, `kubectl -n db delete pod pg-0 --wait=true`,
   `aws ec2 stop-instances --instance-ids i-0abc`). `WARP_FAILOVER_REQUIRE_FENCE=true` refuses to promote at all when none is configured
   (`promote-blocked` event), which is the setting to use if the old primary could be alive but unreachable from Warp;
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
  The old primary is not repointed; it must be rebuilt as a replica before it can rejoin (see
  `WARP_FAILOVER_REJOIN_COMMAND` under rejoin below). Oracle is follow-only and is
  never repointed by Warp.

### Planned switchover

The Replicas page has a "Make primary" button on each replica (with a confirmation step) that calls this API.
`POST /api/failover/{backend}/switchover` with `{"target": "<replica url>"}` (admin role) swaps the primary
on purpose, without losing a committed transaction. Postgres, MySQL and SQL Server (read-scale availability groups); Oracle only with
`WARP_ORACLE_DATAGUARD_CONTROL=true` and **unverified** (see its section). Allowed in
`follow` or `promote` mode (not `off`). SQL Server does not follow the freeze, catch-up, promote steps below: it prepares synchronous commit, promotes the
synchronized target and resumes the old primary instead (see its section).

1. Preconditions: the primary is writable, the target is a configured, reachable, read-only replica.
   If the config database is reachable Warp takes the failover lease for the duration, so no other
   instance's automatic promotion can start a second writer.
2. **Freeze** the primary. Postgres: `default_transaction_read_only=on` plus terminating other client
   sessions (superuser needed). MySQL: `super_read_only=ON` (needs `gtid_mode=ON`).
3. **Catch up**: wait (`WARP_SWITCHOVER_CATCHUP_SECONDS`, 30) until the target replayed the primary's final
   WAL position (Postgres) or executed its final GTID set (MySQL).
4. **Promote** the target, switch the backend to it (one `warp_config` version), repoint the other
   replicas, and turn the old primary into a replica where possible.

Any failure before the promotion re-enables writes on the old primary and changes nothing. If the target
is promoted but does not report itself writable, the old primary is deliberately left frozen (never two
writers) and the result says so.

Limits: writes fail for the seconds the swap takes (clients retry; Warp does not hold and replay them).
Postgres' freeze can be undone by a client that reconnects and explicitly sets `transaction_read_only=off`,
though the catch-up check still guards the target. **Postgres: the old primary is left read-only, not
replicating**: making a running primary a standby needs a restart with `standby.signal` (and usually
`pg_rewind`) on the host, which Warp cannot do over SQL. **MySQL: the old primary becomes a GTID replica
using the backend's own user/password as the replication account** (so that user needs REPLICATION SLAVE;
`GET_SOURCE_PUBLIC_KEY=1` is used so caching_sha2 works without TLS). **SQL Server: the old primary becomes a synchronizing secondary of the new primary.**
**Oracle (opt-in, unverified): the old primary is opened read only and redo apply started.**

### Rejoining a returned old primary

While the primary is writable, Warp watches for a configured replica that is writable or reachable but not
replicating (typically the old primary coming back after a failover) and tries to rejoin it, at most once per
`WARP_FAILOVER_COOLDOWN_SECONDS` per node. `WARP_FAILOVER_AUTO_REJOIN=false` turns it off.

- **MySQL:** rejoined as a GTID replica of the current primary only if it holds **no transaction the
  current primary lacks** (`GTID_SUBTRACT`), checked before and again after making it read-only. A node
  with errant transactions, or without `gtid_mode=ON`, is left exactly as found and reported
  `rejoin-needed`; it must be rebuilt. The replication account is the backend's own user and password
  (as for a planned switchover).
- **SQL Server:** a returned old primary is always a second primary and no T-SQL demotes it; Warp takes its group offline, runs
  `WARP_FAILOVER_REJOIN_COMMAND` (a command that restarts the instance), waits for it to return as a secondary and resumes it, discarding what it committed
  that the new primary lacks (see the SQL Server section). Without the command it is only reported.
- **Postgres:** Warp cannot demote a running primary over SQL and has no host access, so by default it only reports
  `rejoin-needed` (stop the node and rebuild it with `pg_rewind` or `pg_basebackup`). Set
  `WARP_FAILOVER_REJOIN_COMMAND` to have Warp run that rebuild for you: the command is run through `sh -c` with
  `WARP_REJOIN_NODE_URL`, `WARP_REJOIN_PRIMARY_URL`, `WARP_REJOIN_PRIMARY_HOST`, `WARP_REJOIN_PRIMARY_PORT`,
  `WARP_REJOIN_PRIMARY_USER` and `PGPASSWORD` (the current primary's) in its environment, for at most
  `WARP_FAILOVER_REJOIN_TIMEOUT_SECONDS` (default 120), and the node counts as rejoined only if it is then in recovery.
  `Warp/scripts/pg-rebuild-standby.sh <bin-dir> <old-primary-pgdata> [--basebackup | --basebackup-fallback]` is a
  ready-made command: it stops the node, runs `pg_rewind` against the current primary (keeping the node's own
  `postgresql.conf`, `pg_hba.conf` and `pg_ident.conf`) or takes a fresh `pg_basebackup`, and starts it as a standby.
  Live-verified against real Postgres 17, through the script alone and through Warp after both a planned switchover
  and a crash failover (the old primary came back streaming and eligible for read routing). Caveats: anything the old
  primary wrote that never reached the new primary is **discarded**; `pg_rewind` needs `wal_log_hints=on` or data
  checksums on the old primary and the WAL back to the divergence point (`wal_keep_size` or an archive; otherwise use
  `--basebackup-fallback`); the command runs on its own thread (a rejoin never blocks probing or failover, and at most one runs per node), but
  Warp's view of that node stays unchanged until it finishes; the
  command must be able to stop and start the old primary, so it has to run where that Postgres runs; one command
  serves every node, so it must pick the data directory from `WARP_REJOIN_NODE_URL` if there is more than one.
- A node that needs a rebuild but is still writable keeps raising the split-brain alarm, deliberately.
- Events: `rejoined` (audited), `rejoin-needed`, `rejoin-failed`.

## Split-brain guards

A split brain is two writable nodes for one backend. Besides the promotion rules above (confirmation window, majority of Warp
instances, lease, optional fence command, fresh re-probe, most-WAL candidate), two guards work over SQL and need no host access.
Both are on by default and are engine-neutral: each engine supplies its own signal.

**1. A replica that still hears from the "down" primary vetoes the promotion** (`WARP_FAILOVER_STANDBY_CONFIRM`, default `true`;
`WARP_FAILOVER_STANDBY_HEARD_SECONDS`, default 40). When Warp cannot reach the primary but a replica is still receiving from it over its
own replication link, the primary is alive and Warp is the one cut off, so promoting would create a second writer. The check runs after the
fresh re-probe and **before** the fence command (which may power the primary off); a replica that cannot be queried, or cannot tell, is "no
evidence", never a veto. A real crash leaves no such evidence, so genuine failovers are not delayed.

**2. A stale writable node is frozen** (`WARP_FAILOVER_FREEZE_STALE_WRITER`, default `true`). A node Warp lists as a replica that has been
writable for the full confirmation window while the configured primary is also writable is a second writer, typically the old primary coming
back after a failover. Warp treats its configured primary as authoritative, stops the other node taking writes, announces it once
(`stale-writer-frozen`, audited) and repeats it each cooldown in case a session set it writable again. It never runs during a planned
switchover. The node is not demoted: it still has to be rebuilt (see rejoin above), but it stops diverging in seconds instead of
accepting writes until someone notices.

| engine | replica veto: signal | freeze a stale writer | live-verified |
|---|---|---|---|
| Postgres | `pg_stat_wal_receiver`: streaming from the primary's host:port, age of `last_msg_receipt_time` (a crash removes the row at once; an idle healthy primary sends about every 30 s, hence the 40 s threshold) | `default_transaction_read_only = on` and client sessions terminated (a session that runs `SET transaction_read_only = off` can still write; superusers included) | yes, through a running Warp, with controls that show both hazards |
| MySQL | `performance_schema.replication_connection_status`: IO thread `ON`, source host:port matches, age of the later of the last heartbeat and the last queued transaction | `super_read_only = ON` and every other connection killed (no GTIDs needed) | yes, against real mysqld 9.7 (engine level) |
| SQL Server | the secondary's own row in `dm_hadr_availability_replica_states` reads `CONNECTED` (an idle primary does not advance `last_received_time`, so there is no age: connected counts as 0). Stays `CONNECTED` while an idle primary is alive, flips to `DISCONNECTED` about 13 s after it is killed | `ALTER AVAILABILITY GROUP ... OFFLINE` on a read-scale (`CLUSTER_TYPE = NONE`) AG: role `RESOLVING`, every access fails with error 983 until the instance restarts (it then rejoins as a secondary). `ALTER DATABASE ... SET READ_ONLY / SINGLE_USER` is refused on an availability database (error 1468). A clustered AG is refused: the cluster manager owns it | yes, against a real two-node AG (engine level) |
| Oracle | none: Warp never promotes Oracle, so there is no promotion to veto | none: unsupported with a clear event (`stale-writer-unfenced`); a Data Guard primary cannot be demoted over SQL and there was no Oracle to verify a restricted-session approach on | n/a |

**Frozen nodes are not second writers.** A node listed as a replica that is not in recovery but refuses writes (the primary a planned
switchover left read-only, or a stale writer the guard above froze) is treated as read-only, not as a writable second node
(`WARP_FAILOVER_FROZEN_AWARE`, default `true`; event `frozen-node`). No split-brain alarm is raised for it and nothing re-freezes it every
cooldown, but it is still reported as `rejoin-needed` (or rebuilt by `WARP_FAILOVER_REJOIN_COMMAND`) because it has to be rebuilt as a replica.
Postgres checks `transaction_read_only` on a fresh session (set cluster-wide by `default_transaction_read_only`); MySQL already reports a
`read_only` server as read-only; SQL Server's `OFFLINE` node is unreachable. The check is never applied to the configured primary: a primary
that is read-only for another reason must not look like a failure and start a promotion.

**An instance cannot stay on an old config because it missed a notification.** Instances learn a new primary from the shared config over
Postgres LISTEN/NOTIFY, and Postgres never redelivers a notification sent while the listening connection was down. Before this fix such an
instance kept the old primary until the next config change, which is exactly the situation the guards above are most exposed to (live repro:
cut both LISTEN connections, switch over through one instance, the other stayed on the old primary for the full 40 s of the test and
indefinitely by the code). The config listener now compares the stored version with the last one it applied whenever it (re)connects and
every `WARP_CONFIG_POLL_SECONDS` (default 30, `0` turns the poll off), and applies a newer one (log line `found version N newer than the
applied M ...`). Both paths are covered by live tests, which suppress the notification entirely for the poll case. The window in which a
stale instance can still write to the old primary is therefore bounded by that interval, and the old primary is frozen by the instances that
did learn of the change. For a tighter bound turn on the write fence below.

**Write fence (`WARP_WRITE_FENCE=true`, off by default).** Every config change, a switchover included, is a new increasing `warp_config`
version; that version is the term. With the fence on, an instance reads the newest stored version every `WARP_WRITE_FENCE_REFRESH_MILLIS`
(default 500) and applies it at once when it is behind, and each write checks two things before it runs: the newest version seen must be the
one this instance has applied (one inline catch-up is tried first, otherwise the write is refused with `ERR_WRITE_FENCE_BEHIND`), and the last
successful look at the config database must be younger than `WARP_WRITE_FENCE_MAX_STALENESS_MILLIS` (default 5000), otherwise the write is
refused with `ERR_WRITE_FENCE_UNCONFIRMED`. The window for a stale write is therefore about the refresh interval while the config database is
reachable and at most the staleness limit when it is not (the partitioned-instance case), instead of the config poll interval. Live
(Postgres, two instances, notification suppressed and poll off): the fenced instance followed the switchover within the test's 5 s bound; the
same instance without the fence was still on the old primary after 8 s. The unreachable-config-database refusal is covered by unit tests only.
Trade-offs: it is fail-closed, so writes depend on the config database being reachable (a longer outage than the staleness limit stops writes
on every fenced instance, on every engine); it costs one in-memory check per write plus one small read per refresh; it applies to any write
once any backend in the process has replicas, not only to writes for the replicated backend; reads are not fenced. The term is held in the
instance, not on the database node, so a client that bypasses Warp is not fenced, and the check is a bound on the window, not a guarantee of
zero stale writes (a write can pass the check just before a switchover lands). It is engine independent: the same live case (notification suppressed, poll off, switchover through one of two instances) also passed against
a MySQL primary with a replica and a SQL Server read-scale availability group, each with a control run showing the unfenced instance staying behind.
Oracle has not been run (Warp does not switch Oracle over unless Data Guard control is enabled).

What the guards do **not** cover: a partition that also cuts the replica off from the primary while clients can still reach the primary (the
replica then hears nothing, so only the fence command helps, or the majority of Warp instances); two Warp instances with stale
configuration writing to different nodes inside the fence's window (the lease controls who decides, not who writes); and the config database being a single point (Warp
fails safe and does not promote without it). Without external fencing the majority rule remains the only protection against promoting while the
old primary is partitioned away from Warp but serving others. Configure a fencer (step 4 above) and set `WARP_FAILOVER_REQUIRE_FENCE=true` if that can happen in your network: Warp cannot stop a primary it cannot
reach, so the fence has to act on the machine, the hypervisor or the network, and only a fence that really stops writes closes this case. Live
(Postgres, one instance, primary crashed): with the webhook fencer Warp asked exactly once and then promoted; a refusing webhook and a missing fencer
under `REQUIRE_FENCE` both left the replica unpromoted with a `promote-blocked` event. The SSH fencer was tested against a stand-in `ssh`, the others
against local processes and an HTTP server; none was run against real infrastructure.

## Reader port for Relay mode

A `RELAY` frontend is a raw-byte pipe to one database, so reads cannot be told from writes. Instead the choice is made per connection by the
port the client dials. Set, per protocol, `WARP_ORACLE_RELAY_READ_PORT` / `WARP_MYWIRE_RELAY_READ_PORT` / `WARP_MSSQLWIRE_RELAY_READ_PORT` and
the matching `..._RELAY_BACKEND` (the `WARP_BACKENDS` entry whose replicas serve it). Each connection to the reader port is relayed to the
next lag-eligible replica of that backend, with the same eligibility, round-robin and quarantine as statement-level routing: a replica that
cannot be connected to is quarantined and the next one tried. With no replica available the connection goes to the primary
(`WARP_RELAY_READER_FALLBACK=primary`, the default) or is closed (`refuse`). The replica list is read from the registry at every connection, so
it follows failovers. Limits: a session on the reader port can read behind its own writes by up to the replica's lag allowance (no
read-your-writes); a write sent to it reaches a replica and fails with the database's own read-only error (or succeeds if the fallback landed on
the primary); the main relay port goes to the primary of the same `..._RELAY_BACKEND` (see below), or to the single `WARP_ORACLE_HOST`/`WARP_MYSQL_HOST`/`WARP_MSSQL_HOST`
when that is not set. Live-verified for MySQL (main port reaches the primary, reader port a replica, a write on the reader port is refused, a write on the
main port is read on the reader port, and the reader port falls back to the primary when the replica is stopped). Oracle and SQL Server use the
same protocol-blind code and were only unit-tested (fake servers, URL parsing); a SQL Server secondary may also need `ApplicationIntent=ReadOnly`
or `ALLOW_CONNECTIONS = ALL`, which the relay cannot add because it does not see the login.

### The main Relay port follows failovers

With `..._RELAY_BACKEND` set (it does not need a reader port), the main relay port connects each new session to that backend's **current primary**
from the registry instead of the fixed `WARP_*_HOST`/`PORT`, so a failover or switchover redirects new connections without a restart. Sessions already
open are not moved: they end when the old primary goes away or turns read-only, and the client reconnects to the new one. Live (MySQL relay, planned
switchover there and back through the admin API): new connections on the main port reached the new primary each time, and the reader port moved to the
former primary. Not run for Oracle or SQL Server (same code, unit-tested for target resolution only). Without the variable nothing changes.

## Metrics and alerts

`GET /metrics` now includes (labels `backend` = the primary's name, `replica` = replica URL with credentials masked):
`warp_replica_lag_seconds`, `warp_replica_sample_age_seconds`, `warp_replica_eligible`, `warp_replica_quarantined`,
`warp_replica_reads_routed_total`, `warp_replica_read_decisions_total{reason}`, `warp_failover_primary_writable`,
`warp_failover_last_switch_timestamp_seconds` and `warp_failover_events_total{kind}`. Counters are per Warp
process and reset on restart; in a multi-instance deployment each instance reports its own view (and its own
event counts), so alert on `sum`/`max` across instances where that matters. A replica whose lag cannot be
measured has no `warp_replica_lag_seconds` series (never a fake 0). The series are listed in the Signal
Catalog on `/api/observability`; as for every catalog entry, a metric's label set only appears once a data
series for it exists (for example `warp_failover_events_total` has no labels until the first event).

Ready-made alert rules are in `docs/alerts/warp-replicas.rules.yml` (lag, no eligible replica, primary not
writable, failover happened, failover blocked, split brain, node needs rebuild). They have been syntax-checked
as YAML but not run through `promtool` or a live Prometheus.

## Known limits

- In-flight transactions and statements on the dead primary fail; clients retry. New statements go to
  the new primary.
- In `follow` mode every instance decides independently (redundant, idempotent). Promotion is the only
  action gated by the lease and majority.
- The config database is usually the default backend; its own failover is the existing
  `WARP_STANDBY_HOST` mechanism, not this one.
- `BackendHealthChecker` still marks a backend DOWN after one failed connect; that is independent of
  failover-follow, which needs a confirmed window.
- Relay mode (Oracle, MySQL, SQL Server) cannot route per statement, because a raw-byte relay does not parse the protocol; use a reader port
  instead (below). The orawire emulation path has no replica routing (no session-state predicate there).

## Running the live tests

All live tests are opt-in and skipped unless their environment is set; they run real servers, not mocks.

- **Postgres / MySQL:** `ReplicaWireRoutingLiveTest` (`WARP_TEST_WIRE_PG_PORTS=primary,replica`; a real pgwire client against a real Warp process), `MySqlRejoinLiveTest` (`WARP_TEST_REJOIN_MY_PORTS`), `SwitchoverLiveTest` (`WARP_TEST_SWITCH_PG_PORTS`, `WARP_TEST_SWITCH_MY_PORTS`), `ReplicaReadRoutingLiveTest`, `FailoverFollowLiveTest`, `FailoverPromoteLiveTest`,
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
- **SQL Server promote:** `SqlServerPromoteLiveTest` on a *fresh* AG (`ag.sh up`; it kills `sql1`); `SqlServerRejoinLiveTest`
  (`WARP_TEST_MSSQL_REJOIN=1`) on a fresh three-node AG (`AG_NODES=3 ag.sh up`), with
  `WARP_TEST_MSSQL_PROMOTE=1`, `DOCKER_CONTEXT` set if needed, and a Postgres config database via
  `WARP_HOST`/`WARP_PORT`/`WARP_USER`/`WARP_PASSWORD`.
- **Oracle Data Guard:** `OracleDataGuardLiveTest` needs a real physical standby open read-only with apply
  (Active Data Guard, Enterprise Edition) and has **never been run** by this project's authors. Set
  `WARP_TEST_ORACLE_DG_PRIMARY_URL`, `_STANDBY_URL`, `_USER`, `_PASSWORD` (the user needs `SELECT` on
  `V_$DATABASE` and `V_$DATAGUARD_STATS`), and optionally `WARP_TEST_ORACLE_DG_FAILOVER_CMD`, a shell command that
  fails the pair over (for example a `dgmgrl` script); without it the failover-follow part is skipped. Expect to
  adjust the test the first time it meets a real configuration.

