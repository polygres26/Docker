# Connecting PolyWire to your Postgres

PolyWire needs exactly one thing to run: a Postgres it can reach. Everything below points the
same image at a different kind of Postgres — an existing on-prem instance, Supabase, Amazon RDS,
Cloud SQL, Azure Database for PostgreSQL, or Oracle Cloud Infrastructure's Database with
PostgreSQL — using the same five env vars every time:

```
POLYWIRE_HOST=<host>
POLYWIRE_PORT=<port>            # default 5432
POLYWIRE_DATABASE=<database>    # default postgres
POLYWIRE_USER=<user>
POLYWIRE_PASSWORD=<password>
```

Two more control TLS:

```
POLYWIRE_PG_SSLMODE=<disable|allow|prefer|require|verify-ca|verify-full>
POLYWIRE_PG_SSLROOTCERT=<path to a PEM CA bundle, inside the container>   # optional
```

`POLYWIRE_PG_SSLMODE` is unset by default (pgjdbc's own default, `prefer`) — **Supabase and Azure
both require it set to `require` or stronger**, since they reject a plaintext connection outright.
`POLYWIRE_PG_SSLROOTCERT` is optional even with `verify-full` — pgjdbc falls back to the JVM's own
trust store, which already trusts every provider below's default certificate.

Once it's up, point any client at whichever wire protocol you want — `psql -h localhost -p 15432`
for plain Postgres wire, or MySQL/Oracle/SQL Server/MongoDB/DynamoDB/SQS/OpenSearch clients against
their own ports (see the [main README](polywire/README.md) for the full port list). Every guide
below verifies with plain `psql` since it's the most universal check, but the backend Postgres
you've pointed PolyWire at is available through all of them identically.

**A note on verification depth**: the on-prem example below is directly, repeatably tested against
a real Postgres container as part of this repo's own test suite. The five cloud examples are
written and checked against each provider's own current documented connection format, but — unlike
everything else in this repo — haven't been run against a live account of each service. If you hit
something that doesn't match what's below, it's more likely a provider detail that's shifted than a
PolyWire bug; please open an issue either way so the doc can be corrected.

---

## An existing on-prem (or otherwise directly reachable) Postgres

The simple case — nothing above the five basic env vars is usually needed.

```bash
docker run \
  -p 19090:19090 -p 15432:15432 \
  -e POLYWIRE_HOST=your-postgres-host \
  -e POLYWIRE_PORT=5432 \
  -e POLYWIRE_DATABASE=postgres \
  -e POLYWIRE_USER=postgres \
  -e POLYWIRE_PASSWORD=your-password \
  ghcr.io/polygres26/polywire:latest
```

```bash
psql -h localhost -p 15432 -U postgres -d postgres
```

**Gotchas:**
- If PolyWire is running in Docker and your Postgres is on the Docker host itself (not another
  container), `POLYWIRE_HOST=host.docker.internal` reaches it on Mac/Windows; on Linux, either add
  `--add-host=host.docker.internal:host-gateway` to the `docker run` command, or use the host's
  real LAN/bridge IP directly.
- If your Postgres already requires TLS (a common hardening step even on-prem), add
  `POLYWIRE_PG_SSLMODE=require` — see the top of this doc.
- `pg_hba.conf` on your Postgres needs a rule that actually allows the connection from wherever
  PolyWire's container lands — the most common first failure isn't PolyWire at all, it's
  `FATAL: no pg_hba.conf entry for host ...`.

---

## Supabase

Supabase is the one with real, previously-hit friction worth reading before you start:

- **The direct connection (`db.<project-ref>.supabase.co:5432`) is IPv6-only in most regions.**
  A Docker host without IPv6 egress (the common case) gets `Network is unreachable`, not a
  helpful auth error. Use the **pooler** instead — reachable over IPv4, and what you want for a
  gateway like PolyWire anyway (it already pools connections on its own side; stacking PolyWire's
  pool on top of Supabase's own pooler, rather than Supabase's single-connection-per-client direct
  path, is the standard shape).
- **The pooler needs a different username shape**: not `postgres`, but `postgres.<project-ref>`.
  Using the direct connection's plain `postgres` username against the pooler fails auth outright.
- The pooler has two modes on two different ports — **6543 (transaction mode)** is what most
  gateways/poolers in front of it should use; 5432 (session mode) exists for clients that need
  session-level features (advisory locks, `LISTEN`/`NOTIFY`, prepared statements outside a single
  transaction) the transaction pooler can't support. PolyWire's own control-plane tables use
  `LISTEN`/`NOTIFY` (`polywire_config`, `polywire_firewall_rules`) — **use session mode (5432) if
  you plan to rely on PolyWire's live config reload**; transaction mode (6543) is fine if you'll
  restart PolyWire to pick up config changes instead.

Find the exact pooler host, project ref, and both ports on your project's **Database → Connection
pooling** settings page in the Supabase dashboard — they're project-specific, not something to
guess at.

```bash
docker run \
  -p 19090:19090 -p 15432:15432 \
  -e POLYWIRE_HOST=aws-0-<region>.pooler.supabase.com \
  -e POLYWIRE_PORT=5432 \
  -e POLYWIRE_DATABASE=postgres \
  -e POLYWIRE_USER=postgres.<project-ref> \
  -e POLYWIRE_PASSWORD=your-database-password \
  -e POLYWIRE_PG_SSLMODE=require \
  ghcr.io/polygres26/polywire:latest
```

```bash
psql -h localhost -p 15432 -U postgres -d postgres
```

---

## Amazon RDS for PostgreSQL

```bash
docker run \
  -p 19090:19090 -p 15432:15432 \
  -e POLYWIRE_HOST=your-instance.xxxxxxxxxxxx.us-east-1.rds.amazonaws.com \
  -e POLYWIRE_PORT=5432 \
  -e POLYWIRE_DATABASE=postgres \
  -e POLYWIRE_USER=your_master_user \
  -e POLYWIRE_PASSWORD=your-password \
  -e POLYWIRE_PG_SSLMODE=require \
  ghcr.io/polygres26/polywire:latest
```

```bash
psql -h localhost -p 15432 -U your_master_user -d postgres
```

**Gotchas:**
- RDS's **security group** has to allow inbound traffic on 5432 from wherever PolyWire actually
  runs — its own IP if it's outside AWS, or the right security group/CIDR if it's inside the same
  VPC. This is the single most common first failure — a connection that just hangs until timeout,
  not a clean rejection.
- `POLYWIRE_PG_SSLMODE=require` is enough for encryption in transit; go to `verify-full` plus
  `POLYWIRE_PG_SSLROOTCERT` pointed at
  [RDS's own downloadable CA bundle](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html)
  (mounted into the container) if you also need to verify RDS's certificate identity, not just
  encrypt the wire.
- IAM database authentication (short-lived tokens instead of a static password) isn't something
  PolyWire's simple env-var path supports today — it needs a static, standing credential.

---

## Google Cloud SQL for PostgreSQL

Two real ways to reach Cloud SQL; pick based on what you're already running:

**Public IP + authorized networks** (simplest — matches every other guide's shape):

```bash
docker run \
  -p 19090:19090 -p 15432:15432 \
  -e POLYWIRE_HOST=<the instance's public IP> \
  -e POLYWIRE_PORT=5432 \
  -e POLYWIRE_DATABASE=postgres \
  -e POLYWIRE_USER=postgres \
  -e POLYWIRE_PASSWORD=your-password \
  -e POLYWIRE_PG_SSLMODE=require \
  ghcr.io/polygres26/polywire:latest
```

Add the machine PolyWire runs on to the instance's **Authorized networks** (Cloud SQL console →
your instance → Connections → Networking) first, or every connection attempt is refused before it
ever reaches Postgres.

**Cloud SQL Auth Proxy** (recommended for anything beyond a quick test — no public IP, no
authorized-networks list to maintain, IAM-based access instead of a network allowlist):

```yaml
services:
  cloud-sql-proxy:
    image: gcr.io/cloud-sql-connectors/cloud-sql-proxy:latest
    command: ["--address=0.0.0.0", "--port=5432", "<project>:<region>:<instance>"]
    volumes:
      - ./service-account-key.json:/config/key.json:ro
    environment:
      GOOGLE_APPLICATION_CREDENTIALS: /config/key.json

  polywire:
    image: ghcr.io/polygres26/polywire:latest
    depends_on:
      - cloud-sql-proxy
    environment:
      POLYWIRE_HOST: cloud-sql-proxy
      POLYWIRE_PORT: "5432"
      POLYWIRE_DATABASE: postgres
      POLYWIRE_USER: postgres
      POLYWIRE_PASSWORD: your-password
      # No POLYWIRE_PG_SSLMODE needed here -- the proxy itself terminates a real, separately
      # authenticated TLS tunnel to Cloud SQL; the hop from PolyWire to the proxy is local/trusted.
    ports:
      - "19090:19090"
      - "15432:15432"
```

```bash
psql -h localhost -p 15432 -U postgres -d postgres
```

---

## Azure Database for PostgreSQL (Flexible Server)

```bash
docker run \
  -p 19090:19090 -p 15432:15432 \
  -e POLYWIRE_HOST=your-server.postgres.database.azure.com \
  -e POLYWIRE_PORT=5432 \
  -e POLYWIRE_DATABASE=postgres \
  -e POLYWIRE_USER=your_admin_user \
  -e POLYWIRE_PASSWORD=your-password \
  -e POLYWIRE_PG_SSLMODE=require \
  ghcr.io/polygres26/polywire:latest
```

```bash
psql -h localhost -p 15432 -U your_admin_user -d postgres
```

**Gotchas:**
- `POLYWIRE_PG_SSLMODE=require` (or stronger) is **not optional** — Flexible Server rejects a
  plaintext connection unconditionally by default.
- **Plain username, not `user@servername`.** That `@servername` suffix was required on the older,
  now-deprecated Single Server tier; Flexible Server dropped it. Using the old shape against a
  Flexible Server instance fails auth with a confusing error, not a clear "wrong format" message.
- Add the machine PolyWire runs on under **Networking → Firewall rules** on the server (or enable
  "Allow public access from any Azure service" if PolyWire itself runs inside Azure) — same
  allowlist-before-anything-else requirement as RDS's security group and Cloud SQL's authorized
  networks.

---

## Oracle Cloud Infrastructure — Database with PostgreSQL

The one target here with no public-endpoint option at all: OCI's managed Postgres DB systems are
deployed into a **private VCN subnet only** — there's no equivalent of RDS's "publicly accessible"
toggle or Cloud SQL's authorized-networks list. Reaching it needs one of:

- PolyWire already running on a compute instance inside the same VCN (simplest if that's where
  it's going to live in production anyway), or
- an **OCI Bastion port-forwarding session** — no jump-box compute instance needed, just a
  short-lived tunnel from the OCI console/CLI mapping a local port to the DB system's 5432 inside
  the VCN, or
- a site-to-site VPN (or FastConnect) between wherever PolyWire runs and the VCN.

The examples below assume a bastion session already forwards `localhost:5432` on the machine
running `docker run` to the DB system — adjust `POLYWIRE_HOST`/`POLYWIRE_PORT` to wherever your
own tunnel actually lands.

```bash
docker run \
  -p 19090:19090 -p 15432:15432 \
  -e POLYWIRE_HOST=host.docker.internal \
  -e POLYWIRE_PORT=5432 \
  -e POLYWIRE_DATABASE=postgres \
  -e POLYWIRE_USER=your_admin_user \
  -e POLYWIRE_PASSWORD=your-password \
  -e POLYWIRE_PG_SSLMODE=verify-full \
  -e POLYWIRE_PG_SSLROOTCERT=/certs/dbsystem.pub \
  -v /local/path/to/dbsystem.pub:/certs/dbsystem.pub:ro \
  ghcr.io/polygres26/polywire:latest
```

```bash
psql -h localhost -p 15432 -U your_admin_user -d postgres
```

**Gotchas:**
- **TLS is mandatory, not optional** — OCI's Postgres service refuses a plaintext connection
  unconditionally, same as Azure Flexible Server. Unlike the other providers here, going all the
  way to `verify-full` is realistic to set up from the start: download the CA bundle from the DB
  system's **Connection details** panel in the console (referred to there as `dbsystem.pub`) and
  mount it in, as above.
- **The admin username is chosen when you create the DB system, not fixed to `postgres`** — closer
  to RDS's master-username model than Supabase/Azure's fixed convention — and can't be changed
  afterward (the password can be rotated; the username can't).
- **There's no single documented endpoint format to predict** — unlike the other providers, OCI
  doesn't publish a template hostname pattern. Get the exact FQDN from the DB system's own
  **Connection details** page in the console, not by guessing at a shape.
- A DB system exposes both a **primary (floating) endpoint** — always the current read-write
  node, safe default for `POLYWIRE_HOST` — and separate per-node endpoints for directing read
  traffic at specific replicas. Use the primary endpoint here unless you specifically want to
  point PolyWire's standby-aware read routing (`POLYWIRE_STANDBY_HOST`, see the main README) at a
  particular replica instead.

## Optional: Oracle built-in function compatibility (DECODE, XS_SYS_CONTEXT, ...)

SQL*Plus's own connection banner sends `DECODE(USER, 'XS$NULL', XS_SYS_CONTEXT('XS$SESSION',
'USERNAME'), USER) FROM SYS.DUAL` right after login — PolyWire already recognizes and rewrites
that *exact* query internally, so a stock SQL*Plus session works with zero setup on your Postgres.

If your own application code also calls Oracle built-in functions like `DECODE` or
`XS_SYS_CONTEXT` directly — not just the SQL*Plus startup probe — install
[`oracle_compat_functions.sql`](oracle_compat_functions.sql) on your backend Postgres once:

```bash
psql -h <host> -p <port> -U <user> -d <database> -f oracle_compat_functions.sql
```

Nothing in it is created automatically — PolyWire never modifies your backend's schema on its
own. This is entirely optional: skip it unless your own queries call these functions by name.
That file is also the running reference for every Oracle built-in this project has needed to
shim so far; it grows the same way each time (DECODE and XS_SYS_CONTEXT today).
