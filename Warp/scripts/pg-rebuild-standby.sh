#!/bin/sh
# Turns a Postgres node that used to be the primary into a streaming standby of the current primary.
# Meant to be the body of WARP_FAILOVER_REJOIN_COMMAND, which Warp runs with WARP_REJOIN_PRIMARY_HOST /
# WARP_REJOIN_PRIMARY_PORT / WARP_REJOIN_PRIMARY_USER (and PGPASSWORD) in the environment.
#
#   pg-rebuild-standby.sh <postgres-bin-dir> <old-primary-pgdata> [--basebackup | --basebackup-fallback]
#
# 1. stops the old primary, 2. pg_rewind against the current primary (needs wal_log_hints=on or data checksums
# on the old primary), writing standby.signal and primary_conninfo; with --basebackup it instead replaces the data
# directory with a fresh pg_basebackup (the old one is kept as <pgdata>.old.<pid>), and with --basebackup-fallback it
# does that only if pg_rewind fails. 3. starts it again.
# Anything the old primary wrote that never reached the new primary is DISCARDED by either method.
set -eu
BIN=${1:?postgres bin dir}
PGDATA=${2:?old primary data dir}
MODE=${3:-}
HOST=${WARP_REJOIN_PRIMARY_HOST:?}
PORT=${WARP_REJOIN_PRIMARY_PORT:?}
USER=${WARP_REJOIN_PRIMARY_USER:?}

"$BIN/pg_ctl" -D "$PGDATA" -m fast -w stop 2>/dev/null || true

basebackup() {
  mv "$PGDATA" "$PGDATA.old.$$"
  # keep the node's own port/socket settings: copy its config over the fresh backup
  "$BIN/pg_basebackup" -h "$HOST" -p "$PORT" -U "$USER" -D "$PGDATA" -R -X stream
  grep -E '^(port|unix_socket_directories|listen_addresses)\b' "$PGDATA.old.$$/postgresql.conf" >> "$PGDATA/postgresql.conf" || true
}

if [ "$MODE" = "--basebackup" ]; then
  basebackup
else
  # pg_rewind copies the source's whole data directory, including its postgresql.conf, pg_hba.conf and pg_ident.conf
  # (its port, paths and access rules). Keep this node's own and put them back afterwards.
  KEEP=$(mktemp -d)
  for f in postgresql.conf pg_hba.conf pg_ident.conf; do [ -f "$PGDATA/$f" ] && cp "$PGDATA/$f" "$KEEP/$f"; done
  if "$BIN/pg_rewind" --target-pgdata="$PGDATA" --source-server="host=$HOST port=$PORT user=$USER dbname=postgres" \
        --write-recovery-conf; then
    for f in postgresql.conf pg_hba.conf pg_ident.conf; do [ -f "$KEEP/$f" ] && cp "$KEEP/$f" "$PGDATA/$f"; done
  else
    [ "$MODE" = "--basebackup-fallback" ] || exit 1
    basebackup
  fi
  rm -rf "$KEEP"
fi
"$BIN/pg_ctl" -D "$PGDATA" -l "$PGDATA.log" -w start
