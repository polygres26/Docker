#!/bin/sh
# Private build copy so a dev Warp running target/sayonora-wire.jar is never disturbed.
# usage: build.sh <scratch-build-dir> [mvn goal, default package]
set -e
SRC="$(cd "$(dirname "$0")/../../.." && pwd)"
DST="$1"; GOAL="${2:-package}"
rsync -a --delete --exclude target --exclude node_modules --exclude .git "$SRC/" "$DST/"
cd "$DST" && mvn -q -o -Dmaven.test.skip=true "$GOAL"
