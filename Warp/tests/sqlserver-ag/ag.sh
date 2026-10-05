#!/bin/bash
# Two SQL Server 2022 (Developer) containers in a read-scale Always On Availability Group
# (CLUSTER_TYPE = NONE, no cluster manager): sql1 (primary) -> sql2 (readable secondary), database w.
# Test credentials only. Used by the opt-in SqlServerAgLiveTest; see docs/REPLICAS_AND_FAILOVER.md.
#
#   ag.sh up      start both, build the AG, wait until the secondary is synchronizing
#   ag.sh down    remove the containers and the network
#
# Ports on the host: 14331 (sql1), 14332 (sql2). Needs Docker (on Apple silicon the amd64 image runs
# under emulation and needs ~4 GB of Docker memory).
set -euo pipefail
PW='Warp_Test_1234!'
IMG=mcr.microsoft.com/mssql/server:2022-latest
NET=wpag

sqlc() { docker exec "$1" /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$PW" -b -Q "$2"; }

wait_ready() {
  for _ in $(seq 1 120); do sqlc "$1" "select 1" >/dev/null 2>&1 && return 0; sleep 2; done
  echo "$1 did not become ready" >&2; return 1
}

up() {
  docker network create "$NET" >/dev/null 2>&1 || true
  for spec in "sql1 14331" "sql2 14332"; do
    set -- $spec
    docker rm -f "$1" >/dev/null 2>&1 || true
    docker run -d --platform linux/amd64 --name "$1" --hostname "$1" --network "$NET" -p "$2:1433" \
      -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD="$PW" -e MSSQL_PID=Developer -e MSSQL_ENABLE_HADR=1 "$IMG" >/dev/null
  done
  wait_ready sql1; wait_ready sql2

  DBM="CREATE LOGIN dbm_login WITH PASSWORD = 'Dbm_Pass_1234!'; CREATE USER dbm_user FOR LOGIN dbm_login; CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Master_Pass_1234!';"
  ENDPOINT="CREATE ENDPOINT [Hadr_endpoint] AS TCP (LISTENER_PORT = 5022) FOR DATABASE_MIRRORING (ROLE = ALL, AUTHENTICATION = CERTIFICATE dbm_certificate, ENCRYPTION = REQUIRED ALGORITHM AES); ALTER ENDPOINT [Hadr_endpoint] STATE = STARTED; GRANT CONNECT ON ENDPOINT::[Hadr_endpoint] TO [dbm_login];"

  sqlc sql1 "$DBM CREATE CERTIFICATE dbm_certificate WITH SUBJECT = 'dbm'; BACKUP CERTIFICATE dbm_certificate TO FILE = '/var/opt/mssql/data/dbm_certificate.cer' WITH PRIVATE KEY (FILE = '/var/opt/mssql/data/dbm_certificate.pvk', ENCRYPTION BY PASSWORD = 'Pvk_Pass_1234!'); $ENDPOINT"
  TMP=$(mktemp -d)
  docker cp sql1:/var/opt/mssql/data/dbm_certificate.cer "$TMP/"
  docker cp sql1:/var/opt/mssql/data/dbm_certificate.pvk "$TMP/"
  docker cp "$TMP/dbm_certificate.cer" sql2:/var/opt/mssql/data/
  docker cp "$TMP/dbm_certificate.pvk" sql2:/var/opt/mssql/data/
  docker exec -u 0 sql2 chown mssql:root /var/opt/mssql/data/dbm_certificate.cer /var/opt/mssql/data/dbm_certificate.pvk
  rm -rf "$TMP"
  sqlc sql2 "$DBM CREATE CERTIFICATE dbm_certificate AUTHORIZATION dbm_user FROM FILE = '/var/opt/mssql/data/dbm_certificate.cer' WITH PRIVATE KEY (FILE = '/var/opt/mssql/data/dbm_certificate.pvk', DECRYPTION BY PASSWORD = 'Pvk_Pass_1234!'); $ENDPOINT"

  sqlc sql1 "CREATE DATABASE [w]; ALTER DATABASE [w] SET RECOVERY FULL; BACKUP DATABASE [w] TO DISK = '/var/opt/mssql/data/w.bak'; "
  sqlc sql1 "CREATE TABLE w.dbo.t (id int PRIMARY KEY, v varchar(50)); INSERT INTO w.dbo.t VALUES (1, 'a');"
  sqlc sql1 "BACKUP DATABASE [w] TO DISK = '/var/opt/mssql/data/w2.bak' WITH INIT;"
  sqlc sql1 "CREATE AVAILABILITY GROUP [ag1] WITH (CLUSTER_TYPE = NONE) FOR DATABASE [w] REPLICA ON
    N'sql1' WITH (ENDPOINT_URL = N'tcp://sql1:5022', AVAILABILITY_MODE = ASYNCHRONOUS_COMMIT, FAILOVER_MODE = MANUAL, SEEDING_MODE = AUTOMATIC, SECONDARY_ROLE (ALLOW_CONNECTIONS = ALL)),
    N'sql2' WITH (ENDPOINT_URL = N'tcp://sql2:5022', AVAILABILITY_MODE = ASYNCHRONOUS_COMMIT, FAILOVER_MODE = MANUAL, SEEDING_MODE = AUTOMATIC, SECONDARY_ROLE (ALLOW_CONNECTIONS = ALL));"
  sqlc sql2 "ALTER AVAILABILITY GROUP [ag1] JOIN WITH (CLUSTER_TYPE = NONE); ALTER AVAILABILITY GROUP [ag1] GRANT CREATE ANY DATABASE;"

  for _ in $(seq 1 90); do
    n=$(docker exec sql2 /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$PW" -h -1 -W -Q "SET NOCOUNT ON; SELECT count(*) FROM sys.dm_hadr_database_replica_states WHERE is_local = 1 AND synchronization_state_desc IN ('SYNCHRONIZING','SYNCHRONIZED')" 2>/dev/null | tr -d '[:space:]' || true)
    [ "${n:-0}" = "1" ] && { echo "availability group ready"; return 0; }
    sleep 2
  done
  echo "secondary never started synchronizing" >&2; return 1
}

down() {
  docker rm -f sql1 sql2 >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}

case "${1:-}" in up) up ;; down) down ;; *) echo "usage: $0 up|down" >&2; exit 2 ;; esac
